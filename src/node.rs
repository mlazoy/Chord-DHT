#![allow(dead_code, non_snake_case, unused_imports)]

use tokio::net::{TcpListener, TcpStream};
use std::net::{Ipv4Addr,SocketAddrV4};
use std::collections::{HashMap,BTreeMap};
use tokio::sync::{Notify,RwLock};
use std::sync::Arc;
use num_traits::Bounded;
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicBool, Ordering};
// use std::io::{Read,Write, BufReader};
use serde_json::Value;
use std::{thread, vec};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt,BufReader,AsyncWriteExt};
use std::fmt;

use crate::messages::{Message, MsgType, MsgData};
use crate::utils::{Consistency, DebugMsg, HashFunc, HashIP, HashType, Item, Range, UnionRange};
use crate::network::{ConnectionHandler, Server};
use crate::NUM_THREADS; 
use crate::utils;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodeInfo {
    ip_addr: Ipv4Addr,
    port: u16,
    id : HashType
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    replication_factor: u8,
    replication_mode: Consistency,
    replica_ranges: UnionRange<HashType>,
}


#[derive(Debug, Clone)]
pub struct Node {
    info: NodeInfo,                                         /* wraps ip, port, id
                                                            no lock needed - is immutable */                              
    previous : Arc<RwLock<Option<NodeInfo>>>,                  
    successor : Arc<RwLock<Option<NodeInfo>>>, 
    bootstrap : Option<NodeInfo>,                               // no lock because it is read only
    replication: Arc<RwLock<ReplicationConfig>>,                // wraps k, m, ids             
    records : Arc<RwLock<BTreeMap<HashType, Item>>>,            // list of hashed records per node
    pendings : Arc<RwLock<HashMap<HashType, Arc<Notify>>>>,    // keeps track of blocked queries at head
    status: Arc<AtomicBool>                                     // denotes if server is alive
}

impl NodeInfo {
    pub fn new(ip_addr:Ipv4Addr, port:u16) -> Self {
        NodeInfo {
            ip_addr,
            port,
            id: HashIP(ip_addr, port)
        }
    }

    pub fn get_id(&self) -> HashType {
        self.id
    }

    pub fn get_ip(&self) -> Ipv4Addr {
        self.ip_addr
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }

    async fn send_msg(&self, msg: &Message) -> Option<TcpStream> { 
        let sock_addr = std::net::SocketAddrV4::new(self.ip_addr, self.port);
        let jsonify = serde_json::json!(msg).to_string();
        let msg_bytes = jsonify.as_bytes();
        
        match TcpStream::connect(sock_addr).await {
            Ok(mut stream) => {
                if let Err(e) = stream.write_all(msg_bytes).await {
                    eprintln!(
                        "❌ Message {:?} failed to deliver to {}:{} - {}",
                        msg,
                        self.ip_addr,
                        self.port,
                        e
                    );
                    return None;
                }

                self.print_debug_msg(&format!(
                    "✅ Message {} sent to {}:{} successfully",
                    msg,
                    self.ip_addr,
                    self.port
                ));
                return Some(stream);
            }
            Err(e) => {
                eprintln!(
                    "❌ Connection failed to node {}:{} - {}",
                    self.ip_addr,
                    self.port,
                    e
                );
                None
            }
        }
    }

}

impl Node  {

    // fileds startin with _ can be initilaised to None
    pub fn new( ip:&Ipv4Addr, _port: Option<u16>, 
                _k_repl: Option<u8>, _m_repl: Option<Consistency>, 
                _boot_ref: Option<NodeInfo>) -> Self {

        let init_info = NodeInfo {
            ip_addr: *ip,
            port: _port.unwrap_or(0),  
            id : HashIP(*ip, _port.unwrap_or(0)),                                     
        };

        let init_replication = ReplicationConfig {
            replication_factor: _k_repl.unwrap_or(0),
            replica_ranges: UnionRange::new(),           
            replication_mode: _m_repl.unwrap_or(Consistency::Eventual),
        };
        

        Node {
            info: init_info,                
            successor: Arc::new(RwLock::new(None)),
            previous: Arc::new(RwLock::new(None)),
            bootstrap: _boot_ref,
            replication: Arc::new(RwLock::new(init_replication)),
            records: Arc::new(RwLock::new(BTreeMap::new())),
            pendings: Arc::new(RwLock::new(HashMap::new())),
            status: Arc::new(AtomicBool::new(false)) 
        }
    }

    pub fn clone (&self) -> Self {
        Node {
            info: self.info,
            previous: Arc::clone(&self.previous),
            successor: Arc::clone(&self.successor),
            bootstrap: self.bootstrap,
            replication: self.replication.clone(),
            records: Arc::clone(&self.records),
            pendings : Arc::clone(&self.pendings),
            status: Arc::clone(&self.status)
        }
    }

    fn get_id(&self) -> HashType {
        self.info.id
    }

    fn get_ip(&self) -> Ipv4Addr {
        self.info.ip_addr
    }

    fn get_port(&self) -> u16 {
        self.info.port
    }

    fn get_status(&self) -> bool {
        self.status.load(Ordering::SeqCst)
    }

    fn set_status(&self, new_status:bool) {
        self.status.store(new_status, Ordering::Relaxed);
    }

    async fn get_prev(&self) -> Option<NodeInfo> {
        *self.previous.read().await
    }

    async fn get_succ(&self) -> Option<NodeInfo> {
        *self.successor.read().await
    }

    async fn set_prev(&self, new_node:Option<NodeInfo>) {
        *self.previous.write().await = new_node;
    }

    async fn set_succ(&self, new_node:Option<NodeInfo>) {
        *self.successor.write().await = new_node;
    }

    fn get_info(&self) -> NodeInfo {
        self.info
    }

    async fn get_replica_ranges(&self) -> UnionRange<HashType> {
        self.replication.read().await.replica_ranges.clone()
    }

    async fn get_consistency(&self) -> Consistency {
        self.replication.read().await.replication_mode
    }

    async fn max_replication(&self) -> u8 {
        self.replication.read().await.replication_factor
    }

    // dynamically adjusts replication factor when online nodes are less than k
    async fn get_current_k(&self) -> u8 {
        let k = self.replication.read().await.replication_factor;
        std::cmp::min(self.get_replica_ranges().await.get_size() as u8 , k) 
    }

    async fn insert_aux(&self, key: HashType, new_record: &Item) {
        let mut record_writer = self.records.write().await;
        // check if an id already exists and if so merge item data
        if let Some(exist) = record_writer.get_mut(&key) { 
            // Concatenate value 
            exist.value = format!("{}{}", exist.value, new_record.value);  
            // perform 'OR' on 'pending' 
            exist.pending = exist.pending || new_record.pending;
        } else {
            record_writer.insert(key, new_record.clone());  // Insert 
        }
    }

    async fn send_msg(&self, dest_node: Option<NodeInfo>, msg: &Message) -> Option<TcpStream> {
        if let Some(dest) = dest_node {
            dest.send_msg(&msg).await
        } else {
            eprintln!("Failed to send message: destination node not found");
            None
        }
    }

    async fn is_responsible(&self, key: &HashType) -> bool {
        // get read locks first 
        let prev_rd = self.get_prev().await;
        let succ_rd = self.get_succ().await;
        if prev_rd.is_none() || succ_rd.is_none() {
            return true;
        }
        let prev_id = prev_rd.unwrap().id;
         // Check if this node is responsible for the key
        if prev_id < self.get_id() {
            // Normal case: key falls within (prev, self]
            *key > prev_id && *key <= self.get_id()
        } else {
            // Wrapped case: previous is greater due to ring wrap-around
            *key > prev_id || *key <= self.get_id()
        }
    }


    // returns -1 if not a replica manager, otherwise the replica_idx of key in this node
    async fn is_replica_manager(&self, key:&HashType) -> i16 {
        if self.is_responsible(key).await { return 0; }
        let replica_reader = self.get_replica_ranges().await;
        return replica_reader.is_subset(*key);
    }

    /* used to check whether a key should be passed to successor or predecessor node
        taking into account wrapping around on last node 
        to avoid traversing the whole ring backwards */
    async fn maybe_next_responsible(&self, key: &HashType) -> bool {
        let succ_rd = self.get_succ();
        let succ_id = succ_rd.await.unwrap().id;
        if self.get_id() < succ_id {
            // Normal case: key falls within (self, any forward successor]
            *key > self.get_id() 
        } else {
            // Wrapped case
            *key > self.get_id() || *key <= succ_id
        }
    }

    async fn relocate_replicas(&self) {
        let k = self.get_current_k().await;
        let mut records_writer = self.records.write().await;
        let mut to_remove: Vec<HashType> = Vec::new();
        for (key, item) in records_writer.iter_mut(){
            if item.replica_idx == k {
                to_remove.push(*key);
            } else if (item.replica_idx > 0 && item.replica_idx < k) || 
                      (item.replica_idx == 0 && !self.is_responsible(key).await) {
                item.replica_idx += 1;
            } 
        }

        for key in to_remove.iter(){
            records_writer.remove(key);
        }
    }

    pub async fn init(&self) { 
        let sock_addr = SocketAddrV4::new(self.get_ip(), self.get_port());
        match TcpListener::bind(sock_addr).await {
            Ok(listener) => {
                if self.bootstrap.is_none() {
                    self.set_prev(Some(self.get_info())).await;
                    self.set_succ(Some(self.get_info())).await;
                }
                let node_server = Server::new(self.clone());
                self.set_status(true);
                match self.bootstrap {
                    Some(_) => self.print_debug_msg(&format!("Node with id: {} is listening on {}", self.get_id(), sock_addr)),
                    _ => self.print_debug_msg(&format!("Bootstrap has id:{} and is listening on {}", self.get_id(), sock_addr))
                }
                //node_server.wait_for_requests(listener, NUM_THREADS);
                node_server.wait_for_requests(listener).await; 
            }
            Err(e) => panic!("Failed to bind to {}: {}", sock_addr, e)    
        }
    }

    pub async fn join_ring(&self, client:Option<&NodeInfo>) {
        // forward the Join Request to bootsrap
        self.print_debug_msg("Preparing 'Join' Request...");
        if let Some(bootstrap_node) = self.bootstrap {
            let join_msg = Message::new(
                MsgType::FwJoin,
                client,
                &MsgData::FwJoin { new_node: self.get_info() } 
            );
            bootstrap_node.send_msg(&join_msg).await;
        } 
        else {
            // bootstrap node just changes its status
            self.set_status(true);

            let user_msg = Message::new(
                MsgType::Reply,
                None,
                &MsgData::Reply { reply: format!("Bootstrap node joined the ring successfully!") }
            );

            client.unwrap().send_msg(&user_msg).await;
        } 
    }

    async fn handle_join(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwJoin { new_node } => {
                self.print_debug_msg(&format!("Handling Join Request - {} ", new_node));
                let id = new_node.id;
                let peer_port = new_node.port;
                let peer_ip = new_node.ip_addr;
                if id == self.get_id() {
                    let user_msg = Message::new(
                        MsgType::Reply,
                        None,
                        &MsgData::Reply{ reply: format!("Node {} is already part of the network", new_node)}
                    );
                    client.unwrap().send_msg(&user_msg).await;
                    return;
                } 
                // get a read lock on neighbors and k
                let prev_rd = self.get_prev().await;
                let succ_rd = self.get_succ().await;
                let max_k = self.max_replication().await;
                // create the new node
                let new_node = Some(NodeInfo::new(peer_ip, peer_port));
                
                //self.print_debug_msg(&format!("My ranges: {:?}", self.get_replica_ranges()));

                if self.is_responsible(&id).await { 
                    self.print_debug_msg(&format!("Preparing 'AckJoin' for new node {}", new_node.unwrap()));

                    // define replica ranges for current and new node 
                    let mut transferred_ranges = self.get_replica_ranges().await;
                    let mut wrap = false;
                    let new_range = Range::new(
                        prev_rd.unwrap().id,
                        id, 
                        false, 
                        true); 
                    //Update current replica ranges 
                    {
                        let mut replication_writer = self.replication.write().await;
                        let my_replica_ranges = &mut replication_writer.replica_ranges;
                        my_replica_ranges.insert(new_range);   // add new node's key range
                        if my_replica_ranges.get_size() == (max_k + 1) as usize { 
                            my_replica_ranges.pop_head(); 
                        } else {
                            wrap = true;
                            let wrap_range = Range::new(
                                id,
                                self.get_id(),
                                false,
                                true
                            );
                            transferred_ranges.insert(wrap_range); // wrap around
                        }
                    } // release replica locks here 

                    let replica_config = ReplicationConfig {
                        replication_factor : max_k,
                        replication_mode : self.get_consistency().await,
                        replica_ranges : transferred_ranges
                    };

                    // update always locally 
                    self.print_debug_msg(&format!("Updating previous locally to {}", new_node.unwrap()));
                    self.set_prev(new_node).await;

                    // find records to share with the new node according to new managers and previous
                    let mut vec_items: Vec<Item> = Vec::new();
                    {
                        let records_read = self.records.read().await;
                        for (key, item) in records_read.iter() {
                            if item.replica_idx > 0 || (item.replica_idx == 0 && !self.is_responsible(key).await) {
                                vec_items.push(item.clone());
                                continue;
                            } 
                            // TODO! check this 
                            if wrap && item.replica_idx == 0 && self.is_responsible(key).await {
                                vec_items.push(Item {
                                    replica_idx: 1, // Increment only replica_idx
                                    ..item.clone() // Keep other fields unchanged
                                });
                            }
                        }
                    } // drop locks here


                    // send a compact message with new neighbours, all new records and replica managers
                    let ack_msg = Message::new(
                        MsgType::AckJoin,
                        client,
                        &MsgData::AckJoin {  prev_info: prev_rd, succ_info: Some(self.get_info()), 
                                                  new_items: vec_items, replica_config: replica_config}
                    );

                    self.send_msg(new_node, &ack_msg).await;

                    // inform previous about the new node join
                    if !prev_rd.is_none() && self.get_id() != prev_rd.unwrap().id {
                        self.print_debug_msg(&format!("Sending 'Update' to previous node {}", prev_rd.unwrap()));
                        let prev_msg = Message::new(
                            MsgType::Update,
                            None,
                            &MsgData::Update { prev_info: None, succ_info: new_node }
                        );
                        self.send_msg(prev_rd, &prev_msg).await;
                    

                    } else {
                        self.print_debug_msg(&format!("Updating successor locally to {}", new_node.unwrap()));
                        self.set_succ(new_node).await;
                    }


                    // update my replica indices
                    self.relocate_replicas().await;

                    // forward replica relocation to successors
                    let k = self.get_current_k().await;

                    if k > 1 && succ_rd.unwrap().id != self.get_id() {
                        let rel_msg = Message::new(
                            MsgType::Relocate,
                            None,
                            &MsgData::Relocate { k_remaining: k - 2, inc: true, new_copies: None, range: Some(new_range) }
                        );

                        self.send_msg(succ_rd, &rel_msg).await;
                    }

                }
                
                else {
                    self.print_debug_msg(&format!("Forwarding 'Join' Request to successor {}", succ_rd.unwrap()));
                    let fw_msg = Message::new(
                        MsgType::FwJoin,
                        client,
                        &MsgData::FwJoin { new_node: new_node.unwrap() }
                    );
                    self.send_msg(succ_rd, &fw_msg).await;
                } 

            }
            _ => self.print_debug_msg(&format!("Unexpected message data - {:?}", data)),
        }
    }

    async fn handle_ack_join(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::AckJoin { prev_info, succ_info, 
                               new_items, replica_config } => {
                self.set_prev(*prev_info).await;
                self.set_succ(*succ_info).await;
                // insert new_items
                for item in new_items.iter() {
                    let new_key = HashFunc(&item.title);
                    self.insert_aux(new_key, item).await;
                }
                
                {
                    let mut replication_writer = self.replication.write().await;
                    replication_writer.replication_factor = replica_config.replication_factor;
                    replication_writer.replication_mode = replica_config.replication_mode;
                    // get replica managers assert vector is empty in this point
                    let ranges_writer = &mut replication_writer.replica_ranges;
                    for range in replica_config.replica_ranges.iter() {
                        ranges_writer.insert(*range);
                    }
                } // release replica locks here

                // change status 
                self.set_status(true);
                //inform user
                let user_msg = Message::new(
                    MsgType::Reply,
                    None,
                    &MsgData::Reply { reply: format!("New node {} joined the ring sucessfully!", self.get_id()) }
                );
                client.unwrap().send_msg(&user_msg).await;
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data))
        }
    }

    async fn handle_update(&self, data:&MsgData) {
        match data {
            MsgData::Update { prev_info, succ_info} => {
                if !prev_info.is_none() {
                    self.set_prev(*prev_info).await;
                    self.print_debug_msg(&format!("Updated 'previous' to {}", prev_info.unwrap()));
                }

                if !succ_info.is_none() {
                    self.set_succ(*succ_info).await;
                    self.print_debug_msg(&format!("Updated 'successor' to {}", succ_info.unwrap()));
                }
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data)),
        }
    }

    async fn handle_relocate(&self, data:&MsgData) {
        match data {
            MsgData::Relocate { k_remaining, inc, new_copies, range} => {
                let k = self.get_current_k().await;
                let max_k = self.max_replication().await;

                if *inc { // case 'join'
                    {
                        let mut records_writer = self.records.write().await;
                        let mut to_remove: Vec<HashType> = Vec::new();
                        for (key, item) in records_writer.iter_mut(){
                            if item.replica_idx == k {
                                to_remove.push(*key);
                            } else if item.replica_idx > 0 && item.replica_idx < k {
                                item.replica_idx += 1;
                            } 
                        }
                        for key in to_remove.iter(){
                            records_writer.remove(key);
                        }
                    } // release locks

                    // update ranges 
                    if let Some(split) = range {
                        let mut replication_writer = self.replication.write().await;
                        let ranges = &mut replication_writer.replica_ranges;

                        ranges.split_range(split.get_bounds().1);

                        if ranges.get_size() > max_k as usize {
                            ranges.pop_head();
                        }
                    }

                    if *k_remaining > 0 {
                        // inform next one 
                        let rel_msg = Message::new(
                            MsgType::Relocate,
                            None,
                            &MsgData::Relocate { k_remaining: *k_remaining-1, inc: true, new_copies: None, range: *range }
                        );

                        self.send_msg(self.get_succ().await, &rel_msg).await;
                    }
                } 
                else { // case 'depart'
                let mut to_transfer: Vec<Item> = Vec::new();
                {
                    let mut records_writer = self.records.write().await;
                    for (_key, item) in records_writer.iter_mut(){
                        if item.replica_idx == k {
                            to_transfer.push(item.clone());
                        }
                        if item.replica_idx > 0 {
                            item.replica_idx -= 1;
                        } 
                    }
                } // release write locks here
                    let ranges_tmp = self.get_replica_ranges().await;
                    let mut range_to_transfer = ranges_tmp.get_head();
                    if ranges_tmp.get_size() == 1 {
                        range_to_transfer.set_upper(self.get_succ().await.unwrap().id);
                    }
                    if let Some(range) = range {
                        let mut replica_writer = self.replication.write().await;
                        let ranges = &mut replica_writer.replica_ranges;
                        ranges.merge_at(*k_remaining as usize);
                        ranges.insert_head(range.clone());
                        
                    }
                    // create one more replica manager for last copies
                    if let Some(copies) = new_copies { 
                        for copy in copies.iter(){
                            let key_copy = HashFunc(&copy.title);
                            if self.records.read().await.get(&key_copy).is_none() {
                                self.insert_aux(key_copy, &copy).await;
                            }
                        }
                    }

                    if *k_remaining > 0 {
                        // inform next one 
                        let rel_msg = Message::new(
                            MsgType::Relocate,
                            None,
                            &MsgData::Relocate { k_remaining: *k_remaining-1, inc: false, new_copies: Some(to_transfer), range: Some(range_to_transfer) }
                        );

                        self.send_msg(self.get_succ().await, &rel_msg).await;
                        return;
                    } 
                }

            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data)),
        }
    }

    async fn handle_quit(&self, client:Option<&NodeInfo>, _data:&MsgData) {
        self.print_debug_msg("Preparing to Quit...");
        // grab read locks here 
        let prev = self.get_prev().await;
        let succ = self.get_succ().await;

        if self.bootstrap.is_none() {
            let reply:&str;
            if prev.is_none() || succ.is_none() || prev.unwrap().id == self.get_id() || succ.unwrap().id == self.get_id() {
                self.print_debug_msg("Bootstrap node is alone in the network");
                self.set_status(false);
                reply = "Bootstrap node has left the network";
            } else {
                reply = "Bootstrap node cannot leave the network, depart the other nodes first";
            }
            let user_msg = Message::new(
                MsgType::Reply, 
                None,
                &MsgData::Reply { reply: reply.to_string() }
            );
            client.unwrap().send_msg(&user_msg).await;
            return;
        }
        /* construct an Update Message for previous
            only neighbours change ? */ 
        if let Some(prev_node) = prev {
            if prev_node.id != self.get_id() {
                let quit_msg_prev = Message::new(
                    MsgType::Update,
                    None,
                    &MsgData::Update { prev_info: None, succ_info: succ } 
                );
                prev_node.send_msg(&quit_msg_prev).await;
                self.print_debug_msg(&format!("Sent Quit Message to {} succesfully ", prev_node));
            }
        }

        if let Some(succ_node) = succ{
            if succ_node.id != self.get_id() {
            // construct an Update Message for successor 
                let quit_msg_succ = Message::new(
                    MsgType::Update,
                    None,
                    &MsgData::Update { prev_info: prev, succ_info: None }
                );
                succ_node.send_msg(&quit_msg_succ).await;
                self.print_debug_msg(&format!("Sent Quit Message to {} succesfully ", succ_node));
            }

            // gather last repicas
            let mut last_replicas = Vec::new();
            let k = self.get_current_k().await;
            let record_reader = self.records.read().await;
            for (_key, item) in record_reader.iter(){
                if item.replica_idx == k {
                    last_replicas.push(item.clone());
                }
            }
            
            // TODO! Test this
            let succ = self.get_succ().await;
            let ranges = self.get_replica_ranges().await;
            let mut range = ranges.get_head();
            if ranges.get_size() == 1 {
                range.set_upper(succ.unwrap().id);
            }
            let rel_msg = Message::new(
                MsgType::Relocate,
                None,
                &MsgData::Relocate { k_remaining: k-1 , inc: false, new_copies: Some(last_replicas), range: Some(range) }
            );
            
            if succ.unwrap().id != self.get_id() {
                self.send_msg(succ, &rel_msg).await;
            }
        }
        // delete all records 
        let mut map = self.records.write().await;
        map.clear();
        

        let mut replica = self.replication.write().await;
        replica.replica_ranges.clear();
        
        // change status and inform user
        self.set_status(false);
        let user_msg = Message::new(
            MsgType::Reply, 
            None,
            &MsgData::Reply { reply: format!("Node {} has left the network", self) }
        );
        client.unwrap().send_msg(&user_msg).await;

    }

    async fn handle_insert(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Insert { key, value } => {
                let key_hash = HashFunc(key);
                let prev = self.get_prev().await;
                let succ = self.get_succ().await;
                let cons = self.get_consistency().await;
                match cons {
                    Consistency::Eventual => {
                        /* every replica manager can save the new item loally 
                            and reply to client immediately. */
                        let replica= self.is_replica_manager(&key_hash).await;
                        if replica >= 0 {
                            let new_item = Item{ 
                                title:key.clone(), 
                                value:value.clone(), 
                                replica_idx:replica as u8, 
                                pending:false };
                            self.insert_aux(key_hash, &new_item).await;

                            let user_msg = Message::new(
                                MsgType::Reply,
                                None,
                                &MsgData::Reply { reply: format!("Inserted (🔑 {} : 🔒{}) successfully!", key, value) }
                            );
                            client.unwrap().send_msg(&user_msg).await;

                            // propagate insert to other replica managers
                            if replica > 0 {
                                // previous' replica_idx -= 1
                                let fw_back = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica:(replica - 1), forward_back:true }
                                );

                                self.send_msg(prev, &fw_back).await;
                            }

                            if (replica as u8) < self.get_current_k().await {
                                // successor's replica_idx += 1
                                let fw_next = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica: (replica + 1), forward_back:false }
                                );

                                self.send_msg(prev, &fw_next).await;
                            }
                        } else {
                            // forward same message to another node in the primary direction 
                            let fw_ins = Message::new(
                                MsgType::Insert,
                                client,
                                &MsgData::Insert { key: key.clone(), value: value.clone() }
                            );
                            if self.maybe_next_responsible(&key_hash).await {
                                self.send_msg(succ, &fw_ins).await;
                            } else {
                                self.send_msg(prev, &fw_ins).await;
                            }
                        }
                    }

                    Consistency::Chain => {
                        /* Only the primary node can perform the first insertion.
                           It forwards the insert request to all other replica managers without replying to client.
                           Meanwhile the 'pending' field remains true until an ack is received. */
                        if self.is_responsible(&key_hash).await {
                            let new_item = Item{
                                title: key.clone(),
                                value: value.clone(),
                                replica_idx: 0,
                                pending:true
                            };

                            self.insert_aux(key_hash, &new_item).await;

                            if self.get_current_k().await > 0 {
                                let fw_ins = Message::new(
                                    MsgType::FwInsert,
                                    client,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                                replica: 1, forward_back: false }
                                );
                                self.send_msg(succ, &fw_ins).await;
                            }
                        } else {
                            let fw_ins = Message::new(
                                MsgType::Insert,
                                client,
                                &MsgData::Insert { key: key.clone(), value: value.clone() }
                            );

                            if self.maybe_next_responsible(&key_hash).await {
                                self.send_msg(succ, &fw_ins).await;
                            } else {
                                self.send_msg(prev, &fw_ins).await;
                            }
                        }
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:?}", cons))
                }
            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data)),
        } 
    }

    async fn handle_fw_insert(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwInsert { key, value, replica, forward_back } => {
                // forward_back is used to avoid ping-pong messages
                let key_hash = HashFunc(key);
                let prev = self.get_prev().await;
                let succ = self.get_succ().await;
                let cons = self.get_consistency().await;
                match cons {
                    Consistency::Eventual => {
                        if *replica >= 0 {
                            self.insert_aux(key_hash, &Item { 
                                title: key.clone(), 
                                value: value.clone(), 
                                replica_idx: *replica as u8, 
                                pending: false }).await;

                            if *replica > 0 && *forward_back == true {
                                let fw_ins = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica: (replica - 1), forward_back: true }
                                );
                                self.send_msg(prev, &fw_ins).await;
                                return;
                            }

                            if (*replica as u8) < self.get_current_k().await && *forward_back == false {
                                let fw_ins = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica: (replica + 1), forward_back: false }
                                );
                                self.send_msg(succ, &fw_ins).await;
                                return;
                            }

                        } else {
                            self.print_debug_msg(&format!("Invalid replica_idx provided: {}", replica));
                        }
                    }

                    Consistency::Chain => {
                        let new_item = Item{
                            title: key.clone(),
                            value: value.clone(),
                            replica_idx: *replica as u8, 
                            pending: true
                        };
                        // no need to keep pending lists on intermediate nodes
                        self.insert_aux(key_hash, &new_item).await;

                        if (*replica as u8) < self.get_current_k().await {
                            let fw_msg = Message::new(
                                MsgType::FwInsert,
                                client,
                                &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                          replica: *replica + 1, forward_back: false }
                            );

                            self.send_msg(succ, &fw_msg).await;
                        } 
                        else if (*replica as u8) == self.get_current_k().await {
                            /* If reached tail reply to client and send an ack to previous node */
                            let user_msg = Message::new(
                                MsgType::Reply,
                                None,
                                &MsgData::Reply {reply: format!("Inserted (🔑 {} : 🔒{}) successfully!", new_item.title, new_item.value)}
                            );
                            
                            client.unwrap().send_msg(&user_msg).await;

                            let ack_msg = Message::new(
                                MsgType::AckInsert,
                                None,
                                &MsgData::AckInsert { key: key_hash }
                            );

                            self.send_msg(prev, &ack_msg).await;
                        }
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:?}", cons))
                }

            }
            _ => self.print_debug_msg(&format!("unexpected data - {:?}", data)),
        }
    }


    async fn handle_ack_insert(&self, data:&MsgData) {
        /* used for linearizability only
            change 'pending' to false and inform previous */
            match data {
                MsgData::AckInsert { key } => {
                    let mut record_writer = self.records.write().await;
                    if let Some(record) = record_writer.get_mut(&key) {
                        record.pending = false;

                        if record.replica_idx > 0 {
                            let fw_ack = Message::new(
                                MsgType::AckInsert,
                                None,
                                &MsgData::AckInsert { key: *key }
                            );

                            self.send_msg(self.get_prev().await, &fw_ack).await;
                        }
                        else if record.replica_idx == 0  {
                            // notify waiting readers on this key
                            let mut waiting_list = self.pendings.write().await;

                            if let Some(notify) = waiting_list.get(&key) {
                                notify.notify_waiters();  
                                // remove this from queue
                                waiting_list.remove(&key);
                            }
                        }
                    }
                }
                _ => self.print_debug_msg(&format!("unexpected data - {:?}", data)),
            }
    }

    async fn handle_query(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Query { key } => {
                let key_hash = HashFunc(key);
                let cons = self.get_consistency().await;
                let prev = self.get_prev().await;
                let succ = self.get_succ().await;
                match cons {
                    Consistency::Eventual => {
                        // whoever has a replica can reply
                        if self.is_replica_manager(&key_hash).await >= 0 {
                            let records_reader = self.records.read().await;
                            let res = records_reader.get(&key_hash);
                            let reply: &str = match res {
                                Some(found) => &format!("Found data: (🔑 {} : 🔒{})", found.title, found.value),
                                _ => &format!("Error: 🔑{} doesn't exist", key)
                            };
                            
                            let user_msg = Message::new(
                                MsgType::Reply,
                                None,
                                &MsgData::Reply { reply: reply.to_string() }
                            );
                            // send to user
                            client.unwrap().send_msg(&user_msg).await;
                            return;
                        } else {
                            // jsut forward Query to the direction of the primary node
                            let fw_query = Message::new(
                                MsgType::FwQuery,
                                client,
                                &MsgData::FwQuery { key: key_hash, forward_tail: false }
                            );
                            if self.maybe_next_responsible(&key_hash).await {
                                self.send_msg(succ, &fw_query).await;
                            } else {
                                self.send_msg(prev, &fw_query).await;
                            }
                        }
                    }
    
                    Consistency::Chain => {
                    /* An insert/delete operation on "head" or any intermediate node
                        followed by a read at the "tail" results in non-linear behaviour. 
                        To avoid this, reads are blocked until 'pending' field becomes false.
                        Use the field 'forward_tail' to denote a read can be safely propagated to successor. */

                        if self.is_responsible(&key_hash).await {
                            loop {
                                let record_reader = self.records.read().await;
                                let record = record_reader.get(&key_hash);
                                match record {
                                    Some(exist) => {
                                        if exist.pending == true {
                                            self.print_debug_msg(&format!("Item {} is being updated. Going to sleep...", key_hash));
                                            // add this item on pending list 
                                            let notify = Arc::new(Notify::new());  // Create a new notifier
                                            {
                                                let mut notifiers = self.pendings.write().await;
                                                notifiers.insert(key_hash.clone(), notify.clone());  
                                            }
                                            // go to sleep and wait to get notified ...
                                            //drop(record_reader); // release locks first
                                            self.print_debug_msg(&format!("Item {} is being updated. Going to sleep...", exist.title));
                                            notify.notified().await;
                                            self.print_debug_msg(&format!("Item {} is ready. Waking up...", exist.title));
                                            continue;
                                        } 
                                        else if exist.replica_idx < self.get_current_k().await {
                                            let fw_msg = Message::new(
                                                MsgType::FwQuery,
                                                client,
                                                &MsgData::FwQuery { key: key_hash, forward_tail: true }
                                            );
                                            self.send_msg(succ, &fw_msg).await;
                                            return;
                                        } 
                                        else {
                                            let user_msg = Message::new(
                                                MsgType::Reply,
                                                None,
                                                &MsgData::Reply { reply: format!("Found (🔑 {} : 🔒{})", exist.title, exist.value) }
                                            );

                                            client.unwrap().send_msg(&user_msg).await;
                                            return;
                                        }
                                        
                                    }
                                    
                                    _ => {
                                        let user_msg = Message::new(
                                            MsgType::Reply,
                                            None,
                                            &MsgData::Reply { reply: format!("Error: Title 🔑{} doesn't exist", key) }
                                        );

                                        client.unwrap().send_msg(&user_msg).await;
                                        return;
                                    }
                                }
                                
                            }
                        }
                        else {
                            let fw_query = Message::new(
                                MsgType::FwQuery,
                                client,
                                &MsgData::FwQuery { key:key_hash, forward_tail:false }
                            ); 

                            if self.maybe_next_responsible(&key_hash).await {
                                self.send_msg(succ, &fw_query).await;
                            } else {
                                self.send_msg(prev, &fw_query).await;
                            }
                        }
    
                    }
                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:?}", cons))
                }
            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data))
        }
    }

    async fn handle_fw_query(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwQuery { key, forward_tail } => {
                let cons = self.get_consistency().await;
                match cons {
                    Consistency::Eventual => {
                        // same as Query but hash is pre-computed
                        if self.is_replica_manager(&key).await >= 0 {
                            let records_reader = self.records.read().await;
                            let res = records_reader.get(&key);
                            let reply: &str = match res {
                                Some(found) => &format!("Found (🔑 {} : 🔒{})", found.title, found.value),
                                _ => &format!("Error: {} doesn't exist", key)
                            };
                            
                            let user_msg = Message::new(
                                MsgType::Reply,
                                None,
                                &MsgData::Reply { reply: reply.to_string() }
                            );
                            // send to user
                            client.unwrap().send_msg(&user_msg).await;
                            return;
                        } else {
                            // jsut forward Query to the direction of the primary node
                            let fw_query = Message::new(
                                MsgType::FwQuery,
                                client,
                                &MsgData::FwQuery { key: *key, forward_tail: false }
                            );
                            if self.maybe_next_responsible(key).await {
                                self.send_msg(self.get_succ().await, &fw_query).await;
                            } else {
                                self.send_msg(self.get_prev().await, &fw_query).await;
                            }
                        }
                    }

                    Consistency::Chain => {
                        if *forward_tail == true {
                            let record_reader = self.records.read().await;
                            let record = record_reader.get(key);
                            match record {
                                Some(exist) => {
                                    if exist.replica_idx < self.get_current_k().await {
                                        let fw_tail = Message::new(
                                            MsgType::FwQuery,
                                            client,
                                            &MsgData::FwQuery { key: *key, forward_tail: true }
                                        );

                                        self.send_msg(self.get_succ().await, &fw_tail).await;
                                    } 
                                    else if exist.replica_idx == self.get_current_k().await {
                                        // reached tail so can finally reply to client
                                        let user_msg = Message::new(
                                            MsgType::Reply,
                                            None,
                                            &MsgData::Reply { reply: format!("Found (🔑 {} : 🔒{})", exist.title, exist.value) }
                                        );

                                        client.unwrap().send_msg(&user_msg).await;
                                    }
                                }
                                _ => self.print_debug_msg("Error: Wrong Query tail forwarding")
                            }
                        }
                        else {
                            // continue forwarding in the primary direction
                            let fw_query = Message::new(
                                MsgType::FwQuery,
                                client,
                                &MsgData::FwQuery { key: *key, forward_tail: false }
                            );

                            if self.maybe_next_responsible(key).await {
                                self.send_msg(self.get_succ().await, &fw_query).await;
                            } else {
                                self.send_msg(self.get_prev().await, &fw_query).await;
                            }
                        }
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:?}", cons))
                }
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data)),
        }
    }

    async fn handle_query_all(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::QueryAll {  } => {
                let records_reader = self.records.read().await;
                self.print_debug_msg(&format!("All records: {:?}", records_reader));
                let mut res = Vec::new();
                // works as barrier for printing items per node
                let node_item = Item{
                    title: format!("__nodeID__"),
                    value: self.get_id().to_string(),
                    pending:false,
                    replica_idx:0
                };
                res.push(node_item);
                for (_key, item) in records_reader.iter() {
                    if item.replica_idx == 0 && item.pending == false {
                        res.push(item.clone());
                    }
                }

                let succ_node = self.get_succ().await;
                if succ_node.unwrap().id == self.get_id() {
                    // node is alone 
                    let user_msg = Message::new(
                        MsgType::Reply,
                        None,
                        &MsgData::Reply { reply: utils::format_queryall_msg(&res) }
                    );
                    client.unwrap().send_msg(&user_msg).await;
                    return;
                }

                let fw_msg = Message::new(
                    MsgType::FwQueryAll,
                    client,
                    &MsgData::FwQueryAll { record_list: res, header:self.get_id() }
                );

                self.send_msg(succ_node, &fw_msg).await; 
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data)),
        }
    }

    async fn handle_fw_query_all(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwQueryAll { record_list, header } => {
                let records_reader = self.records.read().await;
                self.print_debug_msg(&format!("All records: {:?}", self.records.read().await));
                let mut record_clone = record_list.clone();
                // works as barrier for printing items per node
                let node_item = Item{
                    title: format!("__nodeID__"),
                    value: self.get_id().to_string(),
                    pending:false,
                    replica_idx:0
                };
                record_clone.push(node_item);
                // Append current node's relevant records
                for (_, item) in records_reader.iter() {
                    if item.replica_idx == 0 && item.pending == false {
                        record_clone.push(item.clone());
                    }
                }
            
                let succ_node = self.get_succ().await;
                if !succ_node.is_none(){
                    if succ_node.unwrap().id == *header{
                        // If this is the original sender, reply with the accumulated data
                        let user_msg = Message::new(
                            MsgType::Reply,
                            None,
                            &MsgData::Reply { reply: utils::format_queryall_msg(&record_clone)}
                        );
                        client.unwrap().send_msg(&user_msg).await;
                    }
                    else {
                        // Otherwise, forward the query along the ring
                        let fw_msg = Message::new(
                            MsgType::FwQueryAll,
                            client,
                            &MsgData::FwQueryAll { record_list: record_clone, header: *header }
                        );
            
                        self.send_msg(succ_node, &fw_msg).await;
                    }
                }

            }

            _ => self.print_debug_msg(&format!("unexpected data - {:?}", data))
        }
    }
    

    async fn handle_delete(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Delete {key} => {
                let key_hash = HashFunc(key);
                let cons = self.get_consistency().await;
                match cons {
                    Consistency::Eventual => {
                        /* Any replica manager can delete and inform client immediately.
                           If delete initiated from an intermediate node, the propagation must 
                           be delivered to both directions. To avoid ping-pong messaged each 
                           forwarded message will then follow only one direction, denoted by the 
                           special field 'forward_back' */
                        if self.is_replica_manager(&key_hash).await >= 0 {
                            let res = self.records.write().await.remove(&key_hash);
                            match res {
                                Some(found) => {
                                    let user_msg = Message::new(
                                        MsgType::Reply,
                                        None,
                                        &MsgData::Reply { reply: format!("Deleted (🔑 {} : 🔒{}) sucessfully!", found.title, found.value) }
                                    );
                                    client.unwrap().send_msg(&user_msg).await;

                                    // propagate to other replica managers if needed (async)
                                    if found.replica_idx < self.get_current_k().await {
                                        let fw_next = Message::new(
                                            MsgType::FwDelete,
                                            None,
                                            &MsgData::FwDelete { key: key_hash, forward_back: false }
                                        );
                                        self.send_msg(self.get_succ().await, &fw_next).await;
                                    }
                                    if found.replica_idx > 0 {
                                        let fw_back = Message::new(
                                            MsgType::FwDelete,
                                            None,
                                            &MsgData::FwDelete { key: key_hash, forward_back: true }
                                        );
                                        self.send_msg(self.get_prev().await, &fw_back).await;
                                    }
                                }

                                _ => {
                                    let user_msg =Message::new(
                                        MsgType::Reply,
                                        None,
                                        &MsgData::Reply { reply: format!("Error: Title 🔑 {} doesn't exist!", key) }
                                    );
                                    client.unwrap().send_msg(&user_msg).await;
                                }
                            }

                        } else {
                            // just forward to the primary node direction
                            let fw_del = Message::new(
                                MsgType::Delete,
                                client,
                                &MsgData::Delete { key: key.clone() }
                            );
                            if self.maybe_next_responsible(&key_hash).await {
                                self.send_msg(self.get_succ().await, &fw_del).await;
                            } else {
                                self.send_msg(self.get_prev().await, &fw_del).await;
                            }
                        }
                    }

                    Consistency::Chain => {
                        /* Only the primary node can perform the first 'logical' delete request.
                            by setting 'pending' to true. Forwarding happens as in insert. */
                            if self.is_responsible(&key_hash).await {
                                let mut record_writer = self.records.write().await;
                                let record = record_writer.get_mut(&key_hash);
                                match record {
                                    Some(exist) => {
                                        exist.pending = true;
                                        if exist.replica_idx < self.get_current_k().await {
                                            let fw_del = Message::new(
                                                MsgType::FwDelete,
                                                client,
                                                &MsgData::FwDelete { key: key_hash, forward_back: false }
                                            );
                                            self.send_msg(self.get_succ().await, &fw_del).await;
                                        }
                                    }

                                    _ => {
                                        let user_msg = Message::new(
                                            MsgType::Reply,
                                            None,
                                            &MsgData::Reply { reply: format!("Error: 🔑 {} doesn't exist!", key) }
                                        );

                                        client.unwrap().send_msg(&user_msg).await;
                                        return;
                                    }
                                }
                            }
                            else {
                                // just forward to the primary direction
                                let fw_del = Message::new(
                                    MsgType::Delete,
                                    client,
                                    &MsgData::Delete { key: key.clone() }
                                );
                                if self.maybe_next_responsible(&key_hash).await {
                                    self.send_msg(self.get_succ().await, &fw_del).await;
                                } else {
                                    self.send_msg(self.get_prev().await, &fw_del).await;
                                }
                            }
                    }

                    _ => self.print_debug_msg(&format!("Unsupported consistency model - {:?}", cons))
                }
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data))
        }
             
    }

    async fn handle_fw_delete(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwDelete { key, forward_back } => {
                // forward back is used to avoid ping-pong messages between nodes...
                let cons = self.get_consistency().await;
                match cons {
                    Consistency::Eventual => {
                        if self.is_replica_manager(key).await >= 0 {
                            let res = self.records.write().await.remove(key);
                            match res {
                                Some(found) => {
                                    let fw_del = Message::new(
                                        MsgType::FwDelete,
                                        None,
                                        &MsgData::FwDelete { key: key.clone(), forward_back: *forward_back }
                                    );
                                    if found.replica_idx > 0 && *forward_back == true {
                                        self.send_msg(self.get_prev().await, &fw_del).await;
                                        return;
                                    } 
                                    if found.replica_idx < self.get_current_k().await && *forward_back == false {
                                        self.send_msg(self.get_succ().await, &fw_del).await;
                                        return;
                                    }
                                }
                                _ => self.print_debug_msg("Error: Wrong delete forwarding"),
                            }
                        }
                    }

                    Consistency::Chain => {
                        let mut record_writer = self.records.write().await;
                        let record = record_writer.get_mut(&key);
                        match record {
                            Some(exist) => {
                                exist.pending = true;
                                if exist.replica_idx < self.get_current_k().await {
                                    let fw_del = Message::new(
                                        MsgType::FwDelete,
                                        client,
                                        &MsgData::FwDelete { key: *key, forward_back: false }
                                    );

                                    self.send_msg(self.get_succ().await, &fw_del).await;
                                }
                                else if exist.replica_idx == self.get_current_k().await {
                                /* When reach tail: perform first 'physical' delete, reply to client
                                   and initiate acks to previous nodes */
                                   self.records.write().await.remove(key);

                                   let user_msg = Message::new(
                                    MsgType::Reply,
                                    None,
                                    &MsgData::Reply { reply: format!("Deleted (🔑 {} : 🔒{}) successfully!", exist.title, exist.value) }
                                   );

                                   client.unwrap().send_msg(&user_msg).await;

                                   if exist.replica_idx > 0 {
                                    let ack_del = Message::new(
                                        MsgType::AckDelete,
                                        None,
                                        &MsgData::AckDelete { key: *key }
                                    );
                                    self.send_msg(self.get_prev().await, &ack_del).await;
                                    }

                                }
                            }

                            _ => self.print_debug_msg("Wrong delete forwarding")
                        }
                        
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:?}", cons))
                }
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data))
        }
    }

    async fn handle_ack_delete(&self, data:&MsgData) {
        /* used for linearizability only
            implement the physical delete here */
        match data {
            MsgData::AckDelete { key } => {
                let record_reader = self.records.read().await;
                let record = record_reader.get(&key);
                match record {
                    Some(exist) => {
                        if exist.pending == true {
                            self.records.write().await.remove(&key);

                        } else {
                            self.print_debug_msg("Error: 'logical' delete must occur first");
                            return;
                        }
                        if exist.replica_idx > 0 {
                            let ack_del = Message::new(
                                MsgType::AckDelete,
                                None,
                                &MsgData::AckDelete { key: *key }
                            );
                            
                            self.send_msg(self.get_prev().await, &ack_del).await;
                        }
                        else if exist.replica_idx == 0  {
                            // notify waiting readers on this key
                            let mut waiting_list = self.pendings.write().await;

                            if let Some(notify) = waiting_list.get(&key) {
                                notify.notify_waiters();  
                                // remove this from queue
                                waiting_list.remove(&key);
                            }
                        }
                    }
                    _ => self.print_debug_msg("Wrong delete ack received"),
                }
            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data)),
        }
    }


    async fn handle_fw_overlay(&self, client:Option<&NodeInfo>, data:&MsgData) {
    /* send an Info message to successor in a circular loop 
        until it reaches myself again */
        match data {
            MsgData::FwOverlay { peers } => {
                if peers[0].id == self.get_id() {
                    // circle completed here so return peers to user
                    let user_msg = Message::new (
                        MsgType::Reply,
                        None,
                        &MsgData::Reply { reply: utils::format_overlay_msg(&peers)}
                    );
                    client.unwrap().send_msg(&user_msg).await;
                } else {
                    let mut peers_clone = peers.clone();
                    peers_clone.push(self.get_info());
                    let fw_msg = Message::new(
                        MsgType::FwOverlay,
                        client,
                        &MsgData::FwOverlay { peers: peers_clone }
                    );
            
                    self.send_msg(self.get_succ().await, &fw_msg).await; 
                }

            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data))
        }

    }

    async fn handle_overlay(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Overlay {  } => {
                let mut netvec : Vec<NodeInfo> = Vec::new();
                netvec.push(self.get_info());

                let succ_node = self.get_succ().await;
                if succ_node.unwrap().id == self.get_id() {
                    // node is alone 
                    let user_msg = Message::new(
                        MsgType::Reply,
                        None,
                        &MsgData::Reply{ reply: utils::format_overlay_msg(&netvec)}
                    );

                    client.unwrap().send_msg(&user_msg).await;
                    return;
                }
                // begin the traversal
                let fw_msg = Message::new(
                    MsgType::FwOverlay,
                    client,
                    &MsgData::FwOverlay { peers: netvec }
                );
                self.send_msg(succ_node, &fw_msg).await;  

            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:?}", data))
        }
    }

}

#[async_trait]
impl ConnectionHandler for Node {
    async fn handle_request(&self, stream: TcpStream) {
        let peer_addr = match stream.peer_addr() {
            Ok(addr) => addr,
            Err(e) => {
                eprintln!("Failed to get peer address: {}", e);
                return;
            }
        };

        self.print_debug_msg(&format!("New message from {}", peer_addr));

        let mut reader = BufReader::new(stream);
        let mut total_data = Vec::new();
        let mut buffer = [0; 1024];

        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => {
                    eprintln!("Connection closed by peer.");
                    return;
                }
                Ok(n) => {
                    total_data.extend_from_slice(&buffer[..n]);
                    
                    // Try to parse as JSON
                    match serde_json::from_slice::<Value>(&total_data) {
                        Ok(json_value) => {
                            // Ensure the "size" field exists
                            let total_size = match json_value.get("size").and_then(|v| v.as_u64()) {
                                Some(size) => size as usize,
                                None => {
                                    eprintln!("Missing 'size' field in JSON");
                                    return;
                                }
                            };

                            // Keep reading until we receive the expected number of bytes
                            while total_data.len() < total_size {
                                let mut chunk = vec![0; 1024];
                                let bytes_read = match reader.read(&mut chunk).await {
                                    Ok(0) => break, // Connection closed
                                    Ok(n) => n,
                                    Err(e) => {
                                        eprintln!("Error while reading from stream: {}", e);
                                        return;
                                    }
                                };
                                total_data.extend_from_slice(&chunk[..bytes_read]);
                            }

                            // Deserialize the complete JSON
                            let full_json: Value = match serde_json::from_slice(&total_data) {
                                Ok(value) => value,
                                Err(e) => {
                                    eprintln!("Failed to deserialize full JSON: {}", e);
                                    return;
                                }
                            };

                            // Convert JSON Value into Message struct
                            let msg: Message = match serde_json::from_value(full_json) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    eprintln!("Failed to convert JSON value to Message: {}", e);
                                    return;
                                }
                            };

                            self.print_debug_msg(&format!("Received: {}", msg));

                            let sender_info = msg.extract_client();
                            let msg_type = msg.extract_type();
                            let msg_data = msg.extract_data();

                            match msg_type {
                                MsgType::Join | MsgType::AckJoin => (),
                                _ => {
                                    if !self.get_status() {
                                        let error_msg = Message::new(
                                            MsgType::Reply,
                                            None,
                                            &MsgData::Reply {
                                                reply: format!("Node {} is offline", self.get_info()),
                                            },
                                        );
                                        if let Some(sender) = sender_info {
                                            sender.send_msg(&error_msg).await;
                                        }
                                        return;
                                    }
                                }
                            }

                            match msg_type {
                                MsgType::Join => self.join_ring(sender_info).await,
                                MsgType::FwJoin => self.handle_join(sender_info, &msg_data).await,
                                MsgType::AckJoin => self.handle_ack_join(sender_info, &msg_data).await,
                                MsgType::Update => self.handle_update(&msg_data).await,
                                MsgType::Quit => self.handle_quit(sender_info, &msg_data).await,
                                MsgType::Query => self.handle_query(sender_info, &msg_data).await,
                                MsgType::FwQuery => self.handle_fw_query(sender_info, &msg_data).await,
                                MsgType::QueryAll => self.handle_query_all(sender_info, &msg_data).await,
                                MsgType::FwQueryAll => self.handle_fw_query_all(sender_info, &msg_data).await,
                                MsgType::Insert => self.handle_insert(sender_info, &msg_data).await,
                                MsgType::FwInsert => self.handle_fw_insert(sender_info, &msg_data).await,
                                MsgType::AckInsert => self.handle_ack_insert(&msg_data).await,
                                MsgType::Delete => self.handle_delete(sender_info, &msg_data).await,
                                MsgType::FwDelete => self.handle_fw_delete(sender_info, &msg_data).await,
                                MsgType::AckDelete => self.handle_ack_delete(&msg_data).await,
                                MsgType::Overlay => self.handle_overlay(sender_info, &msg_data).await,
                                MsgType::FwOverlay => self.handle_fw_overlay(sender_info, &msg_data).await,
                                MsgType::Relocate => self.handle_relocate(&msg_data).await,
                                _ => eprintln!("Invalid message type: {:?}", msg_type),
                            }

                            return; // Successfully processed the message
                        }
                        Err(_) => {
                            // JSON is incomplete; continue reading more bytes
                            continue;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read from stream: {}", e);
                    return;
                }
            }
        }
    }
       
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NodeInfo [ ID: {}, IP: {}, Port: {}]",
            self.id, self.ip_addr, self.port
        )
    }
}

impl fmt::Display for ReplicationConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Replication [ k: {}, consistency: {:?}, replica_ranges: {:?}]",
            self.replication_factor, self.replication_mode, self.replica_ranges
        )
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let prev = &self.previous;
        let succ = &self.successor;
        let replica_config = &self.replication;
        //let records_count = &self.records; // Only show count for brevity

        write!(
            f,
            "Node [\n  {},\n  Previous: {:?},\n  Successor: {:?},\n  
            Replica Managers: {:?},\n Status: {:?}\n]",
            self.info, *prev, *succ, replica_config, self.status
        )
    }
}
