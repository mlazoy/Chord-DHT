use std::net::{TcpListener, TcpStream};
use std::net::{Ipv4Addr,SocketAddrV4};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use tokio::net;
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{Read,Write};
use serde_json::{json,Value};
use std::thread;

use crate::messages::{Message, MsgType, MsgData};
use crate::utils::{Consistency, DebugMsg, HashFunc, HashIP, HashType, Item};
use crate::network::{ConnectionHandler, Server};
use crate::NUM_THREADS; 

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodeInfo {
    ip_addr: Ipv4Addr,
    port: u16,
    id : HashType
}

#[derive(Debug, Clone)]
pub struct Node {
    info: NodeInfo,                                         /* wraps ip, port, id
                                                            no lock needed - is immutable */                              
    previous : Arc<RwLock<Option<NodeInfo>>>,                  
    successor : Arc<RwLock<Option<NodeInfo>>>, 
    bootstrap : Option<NodeInfo>,                           // no lock because it is read only
    replication_factor : u8,                                // number of replicas per Item
    replication_mode : Consistency,                         // replication mode                
    records : Arc<RwLock<BTreeMap<HashType, Item>>>,        // list of hashed records per node
    status: Arc<AtomicBool>                                 // denotes if server is alive
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

    pub fn send_msg(&self, msg: &Message) -> Option<TcpStream> { 
        let sock_addr = std::net::SocketAddrV4::new(self.ip_addr, self.port);
        let jsonify = serde_json::json!(msg).to_string();
        let msg_bytes = jsonify.as_bytes();
        
        match TcpStream::connect(sock_addr) {
            Ok(mut stream) => {
                if let Err(e) = stream.write_all(msg_bytes) {
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
                    "✅ Message {:#?} sent to {}:{} successfully",
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

        Node {
            info: init_info,                
            replication_factor: _k_repl.unwrap_or(0),
            replication_mode: _m_repl.unwrap_or(Consistency::Eventual),
            successor: Arc::new(RwLock::new(None)),
            previous: Arc::new(RwLock::new(None)),
            bootstrap: _boot_ref,
            records: Arc::new(RwLock::new(BTreeMap::new())),
            status: Arc::new(AtomicBool::new(false)) 
        }
    }

    pub fn clone (&self) -> Self {
        Node {
            info: self.info,
            previous: Arc::clone(&self.previous),
            successor: Arc::clone(&self.successor),
            bootstrap: self.bootstrap,
            replication_factor: self.replication_factor,
            replication_mode: self.replication_mode,
            records: Arc::clone(&self.records),
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

    fn get_prev(&self) -> Option<NodeInfo> {
        *self.previous.read().unwrap()
    }

    fn get_succ(&self) -> Option<NodeInfo> {
        *self.successor.read().unwrap()
    }

    fn set_prev(&self, new_node:Option<NodeInfo>) {
        *self.previous.write().unwrap() = new_node;
    }

    fn set_succ(&self, new_node:Option<NodeInfo>) {
        *self.successor.write().unwrap() = new_node;
    }

    fn get_info(&self) -> NodeInfo {
        self.info
    }

    fn insert_aux(&self, key: HashType, new_record: &Item) {
        let mut record_writer = self.records.write().unwrap();
        // check if an id already exists and if so just concatenate 'value' field
        if let Some(exist) = record_writer.get_mut(&key) {  
            exist.value = format!("{}{}", exist.value, new_record.value);  // Concatenation
        } else {
            record_writer.insert(key, new_record.clone());  // Insert 
        }
    }

    fn send_msg(&self, dest_node: Option<NodeInfo>, msg: &Message) -> Option<TcpStream> {
        if let Some(dest) = dest_node {
            dest.send_msg(&msg)
        } else {
            eprintln!("Failed to send message: destination node not found");
            None
        }
    }

    fn has_replica(&self, rec_hash:HashType) -> bool {
        if let Some(record) = self.records.read().unwrap().get(&rec_hash){
            if record.replica_idx > 0 { return true; }
            else { return false; }
        }
        false
    }

    fn is_responsible(&self, key: &HashType) -> bool {
        // get read locks first 
        let prev_rd = self.get_prev();
        let succ_rd = self.get_succ();
        if prev_rd.is_none() || succ_rd.is_none() {
            return true;
        }
        let prev_id = prev_rd.unwrap().id;
        let self_id = self.get_id();
         // Check if this node is responsible for the key
        if prev_id < self_id {
            // Normal case: key falls within (prev, self]
            *key > prev_id && *key <= self_id
        } else {
            // Wrapped case: previous is greater due to ring wrap-around
            *key >= prev_id || *key <= self_id
        }
    }

    fn is_next_responsible(&self, key: &HashType) -> bool {
        let succ_rd = self.get_succ();
        let succ_id = succ_rd.unwrap().id;
        let self_id = self.get_id();
        // Check if the successor node is responsible for the key
        if self_id < succ_id {
            // Normal case: key falls within (self, successor]
            self.print_debug_msg("Normal case");
            *key > self_id && *key <= succ_id
        } else {
            // Wrapped case: self is greater due to ring wrap-around
            *key >= self_id || *key <= succ_id
        }
    }

    fn is_replica_manager(&self, key:&HashType) -> i16 {
        /* returns the corresponding replica_idx if it is a replica manager 
            otherwise -1 */
        panic!("Implement this!");
        
    }

    pub fn init(&self) { 
        let sock_addr = SocketAddrV4::new(self.get_ip(), self.get_port());
        match TcpListener::bind(sock_addr) {
            Ok(listener) => {
                if self.bootstrap.is_none() {
                    self.set_prev(Some(self.get_info()));
                    self.set_succ(Some(self.get_info()));
                    self.set_status(true);
                }
                let node_server = Server::new(self.clone());
                self.set_status(true);
                match self.bootstrap {
                    Some(_) => self.print_debug_msg(&format!("Node with id: {} is listening on {}", self.get_id(), sock_addr)),
                    _ => self.print_debug_msg(&format!("Bootstrap has id:{} and is listening on {}", self.get_id(), sock_addr))
                }
                node_server.wait_for_requests(listener, NUM_THREADS); 
            }
            Err(e) => panic!("Failed to bind to {}: {}", sock_addr, e)    
        }
    }

    pub fn join_ring(&self) {
        // construct a "Join" Request Message
        self.print_debug_msg("Preparing 'Join' Request...");
        let join_msg = Message::new(
            MsgType::FwJoin,
            None,
            &MsgData::FwJoin { new_node: self.get_info() } // TODO! replace this with client info
        );
        if let Some(bootstrap_node) = self.bootstrap {
            bootstrap_node.send_msg(&join_msg);
            self.print_debug_msg("Sent 'Join' Request sucessfully");
        } else {
            self.print_debug_msg("Cannot locate bootstrap node");
        }
    }

    pub fn handle_quit(&self, client:Option<&NodeInfo>, _data:&MsgData) {
        self.print_debug_msg("Preparing to Quit...");
        if self.bootstrap.is_none() {
            let reply:&str;
            if self.get_prev().is_none() || self.get_succ().is_none() || self.get_prev().unwrap().id == self.get_id() || self.get_succ().unwrap().id == self.get_id() {
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
            client.unwrap().send_msg(&user_msg);
            return;
        }
        // construct a "Quit" Request Message for previous
        let prev = self.get_prev();
        if let Some(prev_node) = prev {
            if prev_node.id != self.get_id() {
                let quit_msg_prev = Message::new(
                    MsgType::Update,
                    None,
                    &MsgData::Update { prev_info: None, succ_info: self.get_succ() }
                );
                prev_node.send_msg(&quit_msg_prev);
                self.print_debug_msg(&format!("Sent Quit Message to {:?} succesfully ", prev_node));
            }
        }
        let succ = self.get_succ();
        if let Some(succ_node) = succ{
            if succ_node.id != self.get_id() {
            // construct a "Quit" Request Message for successor
                let quit_msg_succ = Message::new(
                    MsgType::Update,
                    None,
                    &MsgData::Update { prev_info: self.get_prev(), succ_info: None }
                );
                succ_node.send_msg(&quit_msg_succ);
                self.print_debug_msg(&format!("Sent Quit Message to {:?} succesfully ", succ_node));
            }
        }
        // change status and inform user
        self.set_status(false);
        let user_msg = Message::new(
            MsgType::Reply, 
            None,
            &MsgData::Reply { reply: format!("Node {} has left the network", self.get_id()) }
        );
        client.unwrap().send_msg(&user_msg);

    }

    fn handle_join(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwJoin { new_node } => {
                self.print_debug_msg(&format!("Handling Join Request - {:#?} ", new_node));
                let id = new_node.id;
                let peer_port = new_node.port;
                let peer_ip = new_node.ip_addr;
                if id == self.get_id() {
                    println!("Node is already part of the network.");
                    return;
                } 
                // get a read lock on neighbors
                let prev_rd = self.get_prev();
                let succ_rd = self.get_succ();
                // create the new node
                let new_node = Some(NodeInfo::new(peer_ip, peer_port));

                if self.is_responsible(&id) { 
                    self.print_debug_msg(&format!("Preparing 'AckJoin' for new node {:#?}", new_node));
                    // share records with the new node 
                    let mut keys_to_transfer = Vec::new();
                    {
                        let records_read = self.records.read().unwrap();
                        for (key, item) in records_read.iter() {
                            if *key <= id {
                                keys_to_transfer.push(key.clone());
                            }
                        }
                    }

                    let mut records_write = self.records.write().unwrap();
                    let mut vec_items: Vec<Item> = Vec::with_capacity(keys_to_transfer.len());
                    for key in keys_to_transfer {
                        if let Some(item) = records_write.remove(&key) {
                            vec_items.push(item);
                        }
                    }
                    // send a compact message with new neighbours and all new records
                    let ack_msg = Message::new(
                        MsgType::AckJoin,
                        client,
                        &MsgData::AckJoin { prev_info: prev_rd, succ_info: succ_rd, new_items: vec_items }
                    );

                    self.send_msg(new_node, &ack_msg);

                    // inform previous about the new node join
                    if !prev_rd.is_none() && self.get_id() != prev_rd.unwrap().id {
                        self.print_debug_msg(&format!("Sending 'Update' to previous node {:#?}", prev_rd));
                        let prev_msg = Message::new(
                            MsgType::Update,
                            None,
                            &MsgData::Update { prev_info: None, succ_info: new_node }
                        );
                        self.send_msg(prev_rd, &prev_msg);

                    } else {
                        self.print_debug_msg(&format!("Updating successor locally to {:#?}", new_node));
                        self.set_succ(new_node);
                    }
                    // update always locally 
                    self.print_debug_msg(&format!("Updating previous locally to {:#?}", new_node));
                    self.set_prev(new_node);
                }
                // TODO! Check this point -  removed is_next_responsible as unnecessary...
                else {
                    self.print_debug_msg(&format!("Forwarding 'Join' Request to successor {:#?}", succ_rd));
                    let fw_msg = Message::new(
                        MsgType::FwJoin,
                        client,
                        &MsgData::FwJoin { new_node: new_node.unwrap() }
                    );
                    self.send_msg(succ_rd, &fw_msg);
                } 

            }
            _ => self.print_debug_msg(&format!("Unexpected message data - {:?}", data)),
        }
    }

    fn handle_ack_join(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::AckJoin { prev_info, succ_info, new_items } => {
                self.set_prev(*prev_info);
                self.set_succ(*succ_info);
                // insert new_items
                for item in new_items.iter() {
                    let new_key = HashFunc(&item.title);
                    self.insert_aux(new_key, item);
                }
                //inform user
                let user_msg = Message::new(
                    MsgType::Reply,
                    None,
                    &MsgData::Reply { reply: format!("New node {} joined the ring sucessfully!", self.get_id()) }
                );
                client.unwrap().send_msg(&user_msg);
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data))
        }
    }

    fn handle_update(&self, data:&MsgData) {
        match data {
            MsgData::Update { prev_info, succ_info } => {
                if !prev_info.is_none() {
                    self.set_prev(*prev_info);
                    self.print_debug_msg(&format!("Updated 'previous' to {:#?}", prev_info));
                }

                if !succ_info.is_none() {
                    self.set_succ(*succ_info);
                    self.print_debug_msg(&format!("Updated 'successor' to {:#?}", succ_info));
                }
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data)),
        }
    }

    fn handle_insert(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Insert { key, value } => {
                let key_hash = HashFunc(key);
                match self.replication_mode {
                    Consistency::Eventual => {
                        /* every replica manager can save the new item loally 
                            and reply to client immediately. */
                        let replica= self.is_replica_manager(&key_hash);
                        if replica >= 0 {
                            let new_item = Item{ 
                                title:key.clone(), 
                                value:value.clone(), 
                                replica_idx:replica as u8, 
                                pending:false };
                            self.insert_aux(key_hash, &new_item);

                            let user_msg = Message::new(
                                MsgType::Reply,
                                None,
                                &MsgData::Reply { reply: format!("Inserted({} : {}) successfully!", key, value) }
                            );
                            client.unwrap().send_msg(&user_msg);

                            // propagate insert to other replica managers
                            if replica > 0 {
                                // previous' replica_idx -= 1
                                let fw_back = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica:(replica - 1), forward_back:true }
                                );

                                self.send_msg(self.get_prev(), &fw_back);
                            }

                            if (replica as u8) < self.replication_factor {
                                // successor's replica_idx += 1
                                let fw_next = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica: (replica + 1), forward_back:false }
                                );

                                self.send_msg(self.get_prev(), &fw_next);
                            }
                        } else {
                            // forward same message to another node in the primary direction 
                            let fw_ins = Message::new(
                                MsgType::Insert,
                                client,
                                &MsgData::Insert { key: key.clone(), value: value.clone() }
                            );
                            if key_hash > self.get_id() {
                                self.send_msg(self.get_succ(), &fw_ins);
                            } else {
                                self.send_msg(self.get_prev(), &fw_ins);
                            }
                        }
                    }

                    Consistency::Chain => {
                        // TODO!
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:#?}", self.replication_mode))
                }
            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data)),
        } 
    }

    fn handle_fw_insert(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwInsert { key, value, replica, forward_back } => {
                // forward_back is used to avoid ping-pong messages
                let key_hash = HashFunc(key);
                match self.replication_mode {
                    Consistency::Eventual => {
                        if *replica >= 0 {
                            self.insert_aux(key_hash, &Item { 
                                title: key.clone(), 
                                value: value.clone(), 
                                replica_idx: *replica as u8, 
                                pending: false });

                            if *replica > 0 && *forward_back == true {
                                let fw_ins = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica: (replica - 1), forward_back: true }
                                );
                                self.send_msg(self.get_prev(), &fw_ins);
                                return;
                            }

                            if (*replica as u8) < self.replication_factor && *forward_back == false {
                                let fw_ins = Message::new(
                                    MsgType::FwInsert,
                                    None,
                                    &MsgData::FwInsert { key: key.clone(), value: value.clone(), 
                                                               replica: (replica + 1), forward_back: false }
                                );
                                self.send_msg(self.get_succ(), &fw_ins);
                                return;
                            }

                        } else {
                            self.print_debug_msg(&format!("Invalid replica_idx provided: {}", replica));
                        }
                    }

                    Consistency::Chain => {
                        // TODO!
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:#?}", self.replication_mode))
                }

            }
            _ => self.print_debug_msg(&format!("unexpected data - {:#?}", data)),
        }
    }


    fn handle_ack_insert(&self, data:&MsgData) {
        /* used for linearizability only
            change pending to false and inform previous */
            match data {
                MsgData::AckInsert { key } => {
                    // TODO!
                }
                _ => self.print_debug_msg(&format!("unexpected data - {:#?}", data)),
            }
    }

    fn handle_query(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Query { key } => {
                let key_hash = HashFunc(key);
                match self.replication_mode {
                    Consistency::Eventual => {
                        // whoever has a replica can reply
                        if self.is_replica_manager(&key_hash) >= 0 {
                            let records_reader = self.records.read().unwrap();
                            let res = records_reader.get(&key_hash);
                            let reply: &str = match res {
                                Some(found) => &format!("Found data: ({},{})", found.title, found.value),
                                _ => &format!("Error: {} doesn't exist", key)
                            };
                            
                            let user_msg = Message::new(
                                MsgType::Reply,
                                None,
                                &MsgData::Reply { reply: reply.to_string() }
                            );
                            // send to user
                            client.unwrap().send_msg(&user_msg);
                            return;
                        } else {
                            // jsut forward Query to the direction of the primary node
                            let fw_query = Message::new(
                                MsgType::FwQuery,
                                client,
                                &MsgData::FwQuery { key: key_hash, forward_tail: false }
                            );
                            if key_hash > self.get_id() {
                                self.send_msg(self.get_succ(), &fw_query);
                            } else {
                                self.send_msg(self.get_prev(), &fw_query);
                            }
                        }
                    }
    
                    Consistency::Chain => {
                    /* An insert/delete operation on "head" (that can be initiated from intermediate nodes as well)
                        followed by a read at the "tail" results in non-linear behaviour. 
                        To avoid this, reads are blocked until 'pending' field becomes false.
                        Use the field 'forward_tail' to denote a read can be safely propagated to successor. */
                    //     let fw_tail = match q_msg.get("forward_tail").unwrap().as_bool() {
                    //         Some(fw) => fw,
                    //         _ => false
                    //     };
                    //     if self.is_responsible(key_hash) {
                    //         // check if 'pending' is false and forward to tail
                    //         let records_reader = self.records.read().unwrap();
                    //         if let Some(record) = records_reader.get(&key_hash){
                    //             if record.pending == false {
                    //                 let q_msg_fw = json!({
                    //                     "type": MsgType::Query,
                    //                     "sender": sender_info,
                    //                     "hashed": true,
                    //                     "key": key_hash,
                    //                     "forward_tail": true
                    //                 });
    
                    //                 self.send_msg(self.get_succ(), &q_msg_fw.to_string());
    
                    //             } else {
                    //                 // TODO !
                    //                 // go to sleep and wake up on pending = false ... 
                    //             }
                    //         }
                    //     } 
                    //     else if fw_tail {
                    //         // check if tail is reached 
                    //         let record_reader = self.records.read().unwrap();
                    //         if let Some(record) = record_reader.get(&key_hash){
                    //             if record.replica_idx == self.replication_factor {
                    //                 // return answer to user 
                    //                 let user_msg = json!({
                    //                     "type": MsgType::Reply,
                    //                     "data": (&record.title, &record.value)
                    //                 });
                    //                 // send to user 
                    //                 sender_info.send_msg(&user_msg.to_string());
                    //                 return;
                                    
                    //             } else {
                    //                 // continue forwarding 
                    //                 let q_msg_fw = json!({
                    //                     "type": MsgType::Query,
                    //                     "sender": sender_info,
                    //                     "hashed": true,
                    //                     "key": key_hash,
                    //                     "forward_tail": true
                    //                 });
    
                    //                 self.send_msg(self.get_succ(), &q_msg_fw.to_string());
                    //             }
                    //         }
    
                    //     }
                    //     else {
                    //         let q_msg_fw = json!({
                    //             "type": MsgType::Query,
                    //             "sender": sender_info,
                    //             "hashed": true,
                    //             "key": key_hash
                    //         });
                    //         // just forward to head direction
                    //         if self.is_next_responsible(key_hash){
                    //             self.send_msg(self.get_succ(), &q_msg_fw.to_string());
                    //         } else {
                    //             self.send_msg(self.get_prev(), &q_msg_fw.to_string());
                    //         }
                    //     } 
    
                    }
                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:#?}", self.replication_mode))
                }
            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data))
        }
    }

    fn handle_fw_query(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwQuery { key, forward_tail } => {
                match self.replication_mode {
                    Consistency::Eventual => {
                        // same as Query but hash is pre-computed
                        if self.is_replica_manager(&key) >= 0 {
                            let records_reader = self.records.read().unwrap();
                            let res = records_reader.get(&key);
                            let reply: &str = match res {
                                Some(found) => &format!("Found item ({} : {})", found.title, found.value),
                                _ => &format!("Error: {} doesn't exist", key)
                            };
                            
                            let user_msg = Message::new(
                                MsgType::Reply,
                                None,
                                &MsgData::Reply { reply: reply.to_string() }
                            );
                            // send to user
                            client.unwrap().send_msg(&user_msg);
                            return;
                        } else {
                            // jsut forward Query to the direction of the primary node
                            let fw_query = Message::new(
                                MsgType::FwQuery,
                                client,
                                &MsgData::FwQuery { key: *key, forward_tail: false }
                            );
                            if *key > self.get_id() {
                                self.send_msg(self.get_succ(), &fw_query);
                            } else {
                                self.send_msg(self.get_prev(), &fw_query);
                            }
                        }
                    }

                    Consistency::Chain => {
                        // TODO !
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:#?}", self.replication_mode))
                }
            }
            _ => self.print_debug_msg(&format!("unexpected data - {:#?}", data)),
        }
    }

    fn handle_query_all(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::QueryAll {  } => {
                let records_reader = self.records.read().unwrap();
                let mut res = Vec::new();
                for (_key, item) in records_reader.iter() {
                    if item.replica_idx == 0 && item.pending == false {
                        res.push(item.clone());
                    }
                }

                let succ_node = self.get_succ();
                if succ_node.unwrap().id == self.get_id() {
                    // node is alone 
                    let user_msg = Message::new(
                        MsgType::Reply,
                        None,
                        &MsgData::Reply { reply: format!("{:#?}", res) }
                    );
                    client.unwrap().send_msg(&user_msg);
                    return;
                }

                let fw_msg = Message::new(
                    MsgType::FwQueryAll,
                    client,
                    &MsgData::FwQueryAll { record_list: res, header:self.get_id() }
                );

                self.send_msg(succ_node, &fw_msg); 
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data)),
        }
    }

    fn handle_fw_query_all(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwQueryAll { record_list, header } => {
                let records_reader = self.records.read().unwrap();
                let mut record_clone = record_list.clone();
            
                // Append current node's relevant records
                for (_, item) in records_reader.iter() {
                    if item.replica_idx == 0 && item.pending == false {
                        record_clone.push(item.clone());
                    }
                }
            
                let succ_node = self.get_succ();
                if !succ_node.is_none(){
                    if succ_node.unwrap().id == *header{
                        // If this is the original sender, reply with the accumulated data
                        let user_msg = Message::new(
                            MsgType::Reply,
                            None,
                            &MsgData::Reply { reply: format!("{:#?}", record_clone) }
                        );
                        client.unwrap().send_msg(&user_msg);
                    }
                    else {
                        // Otherwise, forward the query along the ring
                        let fw_msg = Message::new(
                            MsgType::FwQueryAll,
                            client,
                            &MsgData::FwQueryAll { record_list: record_clone, header: *header }
                        );
            
                        self.send_msg(succ_node, &fw_msg);
                    }
                }

            }

            _ => self.print_debug_msg(&format!("unexpected data - {:#?}", data))
        }
    }
    

    fn handle_delete(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Delete {key} => {
                let key_hash = HashFunc(key);
                match self.replication_mode {
                    Consistency::Eventual => {
                        // any replica manager can delete and inform client immediately
                        if self.is_replica_manager(&key_hash) >= 0 {
                            let res = self.records.write().unwrap().remove(&key_hash);
                            match res {
                                Some(found) => {
                                    let user_msg = Message::new(
                                        MsgType::Reply,
                                        None,
                                        &MsgData::Reply { reply: format!("Deleted ({} : {}) sucessfully!", found.title, found.value) }
                                    );
                                    client.unwrap().send_msg(&user_msg);

                                    // propagate to other replica managers async
                                    if found.replica_idx < self.replication_factor {
                                        let fw_next = Message::new(
                                            MsgType::FwDelete,
                                            None,
                                            &MsgData::FwDelete { key: key_hash, forward_back: false }
                                        );
                                        self.send_msg(self.get_succ(), &fw_next);
                                    }
                                    if found.replica_idx > 0 {
                                        let fw_back = Message::new(
                                            MsgType::FwDelete,
                                            None,
                                            &MsgData::FwDelete { key: key_hash, forward_back: true }
                                        );
                                        self.send_msg(self.get_prev(), &fw_back);
                                    }
                                }

                                _ => {
                                    let user_msg =Message::new(
                                        MsgType::Reply,
                                        None,
                                        &MsgData::Reply { reply: format!("Error: Title {} doesn't exist!", key) }
                                    );
                                    client.unwrap().send_msg(&user_msg);
                                }
                            }

                        } else {
                            // just forward to the primary node direction
                            let fw_del = Message::new(
                                MsgType::Delete,
                                client,
                                &MsgData::Delete { key: key.clone() }
                            );
                            if key_hash > self.get_id() {
                                self.send_msg(self.get_succ(), &fw_del);
                            } else {
                                self.send_msg(self.get_prev(), &fw_del);
                            }
                        }
                    }

                    Consistency::Chain => {
                        // TODO!
                    }

                    _ => self.print_debug_msg(&format!("Unsupported consistency model - {:#?}", self.replication_mode))
                }
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data))
        }
        // let key_hash:HashType;
        // if let Some(key) = del_msg.get("key") {
        //     if let Some(is_hashed) = del_msg.get("hashed").unwrap().as_bool(){
        //         match is_hashed {
        //             true => key_hash = serde_json::from_value::<HashType>(key.clone()).unwrap(),
        //             false => key_hash = HashFunc(key.as_str().unwrap())
        //         }
        //     /* 1. Delete of "genuine" item
        //        This step covers all consistency models */
        //     if self.is_responsible(key_hash) {
        //         let record = self.records.write().unwrap().remove(&key_hash).unwrap();
        //         if self.replication_factor > 0 {
        //             // delete replicas by forwarding to successor
        //             let del_fw_msg = json!({
        //                 "type": MsgType::Delete,
        //                 "sender": sender_info,
        //                 "hashed": true,
        //                 "key": key_hash
        //             });

        //             self.send_msg(self.get_succ(), &del_fw_msg.to_string());
        //         } else {
        //             let user_msg = json!({
        //                 "type": MsgType::Reply,
        //                 "data": format!("{} removed successfully!", &record.title)
        //             });
        //             // send to client 
        //             sender_info.send_msg(&user_msg.to_string());
        //         }
        //         return;
        //     }

        //     if self.replication_factor > 0 && self.has_replica(key_hash) {
        //             match self.replication_mode {
        //                 Consistency::Eventual => {
        //                     /*  Need to check if Delete Request initiated from an intermediate node. 
        //                         In this case the message should be delivered to both previous and successor.
        //                         Otherwise, delete of a replica must follow only one direction.
        //                         Use an extra field forward_back to avoid ping-pong messages. */
        //                     let backwards: bool;

        //                     if !is_hashed { 
        //                         backwards = true;   // first backward sender
        //                     } else if let Some(back_fw) = del_msg.get("forward_back").unwrap().as_bool() {
        //                             backwards = back_fw;
        //                     } else {
        //                         backwards = false;
        //                     }
                            
        //                     let mut record_lock = self.records.write().unwrap();
        //                     if let Some(record) = record_lock.remove(&key_hash){
        //                         // client can be notified immediately in this relaxed model
        //                         let user_msg = json!({
        //                             "type": MsgType::Reply,
        //                             "data": format!("{} removed successfully!", &record.title)
        //                         });
        //                         // send to client 
        //                         sender_info.send_msg(&user_msg.to_string());

        //                         if !backwards && record.replica_idx < self.replication_factor {
        //                             // forward to successor
        //                             let del_msg_fw = json!({
        //                                 "type" : MsgType::Delete,
        //                                 "sender": sender_info,
        //                                 "key" : key_hash,  
        //                                 "hashed": true
        //                             });
        //                             self.send_msg(self.get_succ(), &del_msg_fw.to_string());
        //                         }
        //                         if backwards && record.replica_idx > 0 {
        //                             // forward to previous
        //                             let del_msg_fw = json!({
        //                                 "type" : MsgType::Delete,
        //                                 "sender": sender_info,
        //                                 "key" : key_hash,  
        //                                 "hashed": true, 
        //                                 "forward_back": true
        //                             });
        //                             self.send_msg(self.get_prev(), &del_msg_fw.to_string());
        //                         }
        //                     }

        //                 }
        //                 Consistency::Chain => {
        //                     /* If incoming request is from previous node, it means it has been propagated by root, 
        //                     so node can proceed to delete replica and forward to successor.
        //                     Otherwise it must just forward to previous until it reaches root. 
        //                     Use the 'forward_back' field again. */
        //                     let backwards: bool;

        //                     if !is_hashed { 
        //                         backwards = true;   // first backward sender
        //                     } else if let Some(back_fw) = del_msg.get("forward_back").unwrap().as_bool() {
        //                             backwards = back_fw;
        //                     } else {
        //                         backwards = false;
        //                     }

        //                     if backwards && self.replication_factor > 0 {
        //                         let del_fw_msg = json!({
        //                             "type": MsgType::Delete,
        //                             "sender": sender_info,
        //                             "hashed": true,
        //                             "key": key_hash,
        //                             "forward_back": true
        //                         });

        //                     self.send_msg(self.get_prev(), &del_fw_msg.to_string());
        //                     } else if !backwards {
        //                         // now replica can be deleted safely
        //                         if let Some(record) = self.records.write().unwrap().remove(&key_hash) {
        //                             // propagate to successor 
        //                             if record.replica_idx < self.replication_factor {
        //                                 let del_fw_msg = json!({
        //                                     "type": MsgType::Delete,
        //                                     "sender": sender_info,
        //                                     "hashed": true,
        //                                     "key": key_hash
        //                                 });

        //                                 self.send_msg(self.get_succ(), &del_fw_msg.to_string());
        //                             } 
        //                             else if record.replica_idx == self.replication_factor {
        //                                 // reached "tail" so can inform client here
        //                                 let user_msg = json!({
        //                                     "type": MsgType::Reply,
        //                                     "data": format!("{} was removed sucessfully!", &record.title)
        //                                 });
        //                                 // send msg to client
        //                                 sender_info.send_msg(&user_msg.to_string());
        //                                 return;
        //                             }
        //                         }
        //                     }
        //                 }

        //                 _  => ()
        //             }
        //         }

        //         else { // just forward the request to either succ or prev depending on hash
        //             let del_msg_fw = json!({
        //                 "type": MsgType::Delete,
        //                 "sender": sender_info,
        //                 "hashed": true,
        //                 "key" : key_hash,
        //             });
        //             if self.is_next_responsible(key_hash) {
        //                 self.send_msg(self.get_succ(),&del_msg_fw.to_string());
        //             } else {
        //                 self.send_msg(self.get_prev(), &del_msg_fw.to_string());
        //             }
        //         }
             
    }

    fn handle_fw_delete(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::FwDelete { key, forward_back } => {
                // forward back is used to avoid ping-pong messages between nodes...
                match self.replication_mode {
                    Consistency::Eventual => {
                        if self.is_replica_manager(key) >= 0 {
                            let res = self.records.write().unwrap().remove(key);
                            match res {
                                Some(found) => {
                                    let fw_del = Message::new(
                                        MsgType::FwDelete,
                                        None,
                                        &MsgData::FwDelete { key: key.clone(), forward_back: *forward_back }
                                    );
                                    if found.replica_idx > 0 && *forward_back == true {
                                        self.send_msg(self.get_prev(), &fw_del);
                                        return;
                                    } 
                                    if found.replica_idx < self.replication_factor && *forward_back == false {
                                        self.send_msg(self.get_succ(), &fw_del);
                                        return;
                                    }
                                }
                                _ => self.print_debug_msg("Error: Wrong delete forwarding"),
                            }
                        }
                    }

                    Consistency::Chain => {
                        // TODO!
                    }

                    _ => self.print_debug_msg(&format!("Unsupported Consistency model - {:#?}", self.replication_mode))
                }
            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data))
        }
    }

    fn handle_ack_delete(&self, data:&MsgData) {
        /* used for linearizability only
            implement the physical delete here */
        match data {
            MsgData::AckDelete { key } => {
                let record = self.records.write().unwrap().remove(&key);
                // inform previous with an ack
                if let Some(rec) = record {
                    if rec.replica_idx > 0 {
                        let fw_ack = Message::new(
                            MsgType::AckDelete,
                            None,
                            &MsgData::AckDelete { key: *key }
                        );
                        
                        self.send_msg(self.get_succ(), &fw_ack);
                    }
                }
            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data)),
        }
    }


    fn handle_fw_overlay(&self, client:Option<&NodeInfo>, data:&MsgData) {
    /* send an Info message to successor in a circular loop 
        until it reaches myself again */
        match data {
            MsgData::FwOverlay { peers } => {
                if peers[0].id == self.get_id() {
                    // circle completed here so return peers to user
                    let user_msg = Message::new (
                        MsgType::Reply,
                        None,
                        &MsgData::Reply { reply: format!("{:#?}", peers)}
                    );
                    client.unwrap().send_msg(&user_msg);
                } else {
                    let mut peers_clone = peers.clone();
                    peers_clone.push(self.get_info());
                    let fw_msg = Message::new(
                        MsgType::FwOverlay,
                        client,
                        &MsgData::FwOverlay { peers: peers_clone }
                    );
            
                    self.send_msg(self.get_succ(), &fw_msg); 
                }

            }

            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data))
        }

    }

    fn handle_overlay(&self, client:Option<&NodeInfo>, data:&MsgData) {
        match data {
            MsgData::Overlay {  } => {
                let mut netvec : Vec<NodeInfo> = Vec::new();
                netvec.push(self.get_info());

                let succ_node = self.get_succ();
                if succ_node.unwrap().id == self.get_id() {
                    // node is alone 
                    let user_msg = Message::new(
                        MsgType::Reply,
                        None,
                        &MsgData::Reply{ reply: format!("{:#?}", netvec)}
                    );

                    client.unwrap().send_msg(&user_msg);
                    return;
                }
                // begin the traversal
                let fw_msg = Message::new(
                    MsgType::FwOverlay,
                    client,
                    &MsgData::FwOverlay { peers: netvec }
                );
                self.send_msg(succ_node, &fw_msg);  

            }
            _ => self.print_debug_msg(&format!("Unexpected data - {:#?}", data))
        }
    }

}

impl ConnectionHandler for Node {
    fn handle_request(&self, mut stream: TcpStream) {
        let peer_addr = stream.peer_addr().unwrap();
        self.print_debug_msg(&format!("New message from {}", peer_addr));
        
        let peer_ip: Ipv4Addr = match peer_addr.ip() {
            std::net::IpAddr::V4(ipv4) => ipv4,  // Extract IPv4
            std::net::IpAddr::V6(_) => {
                panic!("Received IPv6 address, expected only IPv4");
            }
        };
                    
        let mut buffer = [0; 1024]; 
        let n = match stream.read(&mut buffer) {
            Ok(size) => size,
            Err(e) => {
                eprintln!("Failed to read from stream: {}", e);
                return;
            }
        };
                    
        // Convert the buffer to a string
        let received_msg = String::from_utf8_lossy(&buffer[..n]);
        self.print_debug_msg(&format!("Received message: {:#?}", received_msg));
                    
        // Deserialize the received JSON message
        let json_value: Value = match serde_json::from_str(&received_msg) {
            Ok(value) => value,
            Err(e) => {
                eprintln!("Failed to deserialize message: {}", e);
                return;
            }
        };

        // Convert Value to Message
        let msg: Message = match serde_json::from_value(json_value) {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("Failed to convert JSON value to Message: {}", e);
                return;
            }
        };
            let sender_info = msg.extract_client();        
            let msg_type = msg.extract_type();
            let msg_data = msg.extract_data();

            if !self.get_status() {
                let error_msg = Message::new(
                    MsgType::Reply,
                    None,
                    &MsgData::Reply { reply: format!("Node is offline")}
                );
                sender_info.unwrap().send_msg(&error_msg);
                return; 
            }

            match msg_type {
                MsgType::Join => self.join_ring(),
                
                MsgType::FwJoin => self.handle_join(sender_info, &msg_data),
                
                MsgType::AckJoin => self.handle_ack_join(sender_info, &msg_data),
                
                // no sender needed cause it's an internal process
                MsgType::Update => self.handle_update(&msg_data), 

                MsgType::Quit => self.handle_quit(sender_info, &msg_data),  

                MsgType::Query => self.handle_query(sender_info, &msg_data),

                MsgType::FwQuery => self.handle_fw_query(sender_info, &msg_data),

                MsgType::QueryAll => self.handle_query_all(sender_info, &msg_data),

                MsgType::FwQueryAll => self.handle_fw_query_all(sender_info, &msg_data),

                MsgType::Insert => self.handle_insert(sender_info, &msg_data),

                MsgType::FwInsert => self.handle_fw_insert(sender_info, &msg_data),

                MsgType::AckInsert => self.handle_ack_insert(&msg_data), 

                MsgType::Delete => self.handle_delete(sender_info, &msg_data),
                   
                MsgType::FwDelete => self.handle_fw_delete(sender_info, &msg_data),
                
                MsgType::AckDelete=> self.handle_ack_delete(&msg_data),

                MsgType::Overlay => self.handle_overlay(sender_info, &msg_data),
                    
                MsgType::FwOverlay => self.handle_fw_overlay(sender_info, &msg_data),

                _ => eprintln!("Invalid message type: {:#?}", msg_type) 
            }
            
        } 
       
    }
