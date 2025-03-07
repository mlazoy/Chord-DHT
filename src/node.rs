use std::net::{TcpListener, TcpStream};
use std::net::{Ipv4Addr,SocketAddrV4};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{Read,Write};
use serde_json::Value;
use std::thread;
use serde_json::json;

use crate::utils::{Consistency, DebugMsg, HashFunc, HashIP, HashType, Item, MsgType};
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
    replication_factor : usize,                             // number of replicas per node
    replication_mode : Consistency,                         // replication mode                
    records : Arc<RwLock<BTreeMap<HashType, Item>>>,       // list of hasehd records per node
    status: Arc<AtomicBool>                                // denotes if server is alive
}

impl NodeInfo {
    pub fn new(ip_addr:Ipv4Addr, port:u16) -> Self {
        NodeInfo {
            ip_addr,
            port,
            id: HashIP(ip_addr, port)
        }
    }

    pub fn send_msg(&self, msg: &str) -> Option<TcpStream> { 
        let sock_addr = std::net::SocketAddrV4::new(self.ip_addr, self.port);
        
        match TcpStream::connect(sock_addr) {
            Ok(mut stream) => {
                if let Err(e) = stream.write_all(msg.as_bytes()) {
                    eprintln!(
                        "❌ Message {} failed to deliver to {}:{} - {}",
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
                _k_repl: Option<usize>, _m_repl: Option<Consistency>, 
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

    // fn set_id(&self, id: HashType) {
    // 
    // }

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
                    Some(_) => {
                        self.print_debug_msg(&format!("Node is listening on {}", sock_addr));
                        let node_server = Server::new(self.clone());
            
                        let listener_clone = listener.try_clone().expect("Failed to clone listener");
                        let server_thread = thread::spawn(move || {
                            node_server.wait_for_requests(listener_clone, NUM_THREADS);
                        });

                        let node_clone = self.clone();
                        thread::spawn(move || {
                            std::thread::sleep(std::time::Duration::from_secs(1));  // Give time for server setup
                            node_clone.join_ring();
                        });
            
                        // Keep the main thread alive so the server keeps running
                        while self.get_status() {
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                        // Ensure the server thread does not exit early
                        server_thread.join().expect("Server thread panicked"); 
                    }
                    _ => {
                        self.print_debug_msg(&format!("Bootstrap is listening on {}", sock_addr));
                        node_server.wait_for_requests(listener, NUM_THREADS); 
                    }
                }
            }
            Err(e) => panic!("Failed to bind to {}: {}", sock_addr, e)    
        }
    }

    pub fn join_ring(&self) {
        // construct a "Join" Request Message
        self.print_debug_msg("Preparing 'Join' Request...");
        let join_data = serde_json::json!({
            "type": MsgType::Join,
            "info": self.get_info()      // serializable
        });
        if let Some(bootstrap_node) = self.bootstrap {
            bootstrap_node.send_msg(&join_data.to_string());
            self.print_debug_msg("Sent 'Join' Request sucessfully");
        } else {
            self.print_debug_msg("Cannot locate bootstrap node");
        }
    }

    pub fn handle_quit(&self, msg: &Value) {
        self.print_debug_msg("Preparing to Quit...");
        if self.bootstrap.is_none() {
            let mut reply = "";
            if self.get_prev().is_none() || self.get_succ().is_none() || self.get_prev().unwrap().id == self.get_id() || self.get_succ().unwrap().id == self.get_id() {
                self.print_debug_msg("Bootstrap node is alone in the network");
                self.set_status(false);
                reply = "Bootstrap node has left the network";
            } else {
                reply = "Bootstrap node cannot leave the network, depart the other nodes first";
            }
            if let Some(sender) = msg.get("sender") {
                let sender_info = serde_json::from_value::<NodeInfo>(sender.clone()).unwrap();
                sender_info.send_msg(&json!({
                    "type": MsgType::Reply,
                    "msg": reply
                }).to_string());
            }
            return;
        }
        // construct a "Quit" Request Message for previous
        let prev = self.get_prev();
        if let Some(prev_node) = prev {
            if prev_node.id != self.get_id() {
                let quit_data_prev = serde_json::json!({
                    "type": MsgType::Update,
                    "succ_info": self.get_succ()
                });
                prev_node.send_msg(&quit_data_prev.to_string());
                self.print_debug_msg(&format!("Sent Quit Message to {:?} succesfully ", prev_node));
            }
        }
        let succ = self.get_succ();
        if let Some(succ_node) = succ{
            if succ_node.id != self.get_id() {
            // construct a "Quit" Request Message for successor
                let quit_data_succ = serde_json::json!({
                    "type": MsgType::Update,
                    "prev_info": self.get_prev()
                });
                succ_node.send_msg(&quit_data_succ.to_string());
                self.print_debug_msg(&format!("Sent Quit Message to {:?} succesfully ", succ_node));
            }
        }
        // close the server and terminate gracefully
        self.set_status(false);
        if let Some(sender) = msg.get("sender") {
            let sender_info = serde_json::from_value::<NodeInfo>(sender.clone()).unwrap();
            sender_info.send_msg(&json!({
                "type": MsgType::Reply,
                "msg": format!("Node {} has left the network", self.get_id())
            }).to_string());
        }
    }

    fn handle_join(&self, msg:&Value) {
        
        if let Some(info) = msg.get("info") {
            if let Ok(new_node) = 
            serde_json::from_value::<NodeInfo>(info.clone()) { 
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

                if self.is_responsible(id) { 
                    self.print_debug_msg(&format!("Sending 'AckJoin' Request to new node {}", peer_ip));
                    let new_node = Some(NodeInfo::new(peer_ip, peer_port));

                    self.send_msg(new_node, &json!({
                        "type": MsgType::AckJoin,
                        "prev_info": prev_rd,
                        "succ_info": self.get_info()
                    }).to_string());

                    if !prev_rd.is_none() && self.get_id() != prev_rd.unwrap().id {
                        self.print_debug_msg(&format!("Sending 'Update' Request to previous node {:?}", prev_rd));
                        self.send_msg(prev_rd, &json!({
                            "type": MsgType::Update,
                            "succ_info": new_node
                        }).to_string());
                    } else {
                        self.print_debug_msg(&format!("Updating successor locally to {:?}", new_node));
                        self.set_succ(new_node);
                    }
                    self.set_prev(new_node);

                    let mut keys_to_transfer = Vec::new();
                    {
                        let records_read = self.records.read().unwrap();
                        for (key, item) in records_read.iter() {
                            if *key <= id {
                                keys_to_transfer.push(key.clone());
                            }
                        }
                    }

                    // Remove and send keys to the new node
                    let mut records_write = self.records.write().unwrap();
                    let mut vec_items: Vec<Item> = Vec::with_capacity(keys_to_transfer.len());
                    for key in keys_to_transfer {
                        if let Some(item) = records_write.remove(&key) {
                            vec_items.push(item);
                        }
                    }
                    // send a compact message with all records
                    self.send_msg(prev_rd, &json!({
                        "sender": self.get_info(),
                        "type": MsgType::Insert,
                        "id": id,
                        "record": vec_items        // is serializable
                    }).to_string());

                } else if self.is_next_responsible(id) {
                    self.print_debug_msg(&format!("Sending 'Join' Request to successor {}", succ_rd.unwrap().ip_addr));
                    let new_node = Some(NodeInfo::new(peer_ip, peer_port));
                    self.send_msg(succ_rd, &json!({
                        "type": MsgType::Join,
                        "info": new_node
                    }).to_string());

                    self.set_succ(new_node);

                } else {
                    self.print_debug_msg(&format!(
                        "Forwarding 'Join' Request to successor {:?}", succ_rd
                    ));
                    self.send_msg(succ_rd, &json!({
                        "type": MsgType::Join,
                        "info": new_node
                    }).to_string());
                }
            } else { 
                self.print_debug_msg(&format!(
                    "Invalid info "
                )); 
            }
        } 
    }

    fn handle_ack_join(&self, ack_msg:&Value) {
        if let (Some(prev_info), Some(succ_info)) = 
        (ack_msg.get("prev_info"), ack_msg.get("succ_info"))
        {
            if let (Ok(prev_node), Ok(succ_node)) = 
            (serde_json::from_value::<NodeInfo>(prev_info.clone()), 
             serde_json::from_value::<NodeInfo>(succ_info.clone())) { // TODO! maybe lock here ?
                self.set_prev(Some(prev_node));
                self.set_succ(Some(succ_node));
            } else { 
                self.print_debug_msg(&format!(
                    "Invalid info provided for either prev or succ node {}-{}", prev_info, succ_info
                )); 
            }
        } 
    }

    fn handle_update(&self, msg:&Value) {
        if let Some(succ_info) = msg.get("succ_info") {
            if let Ok(succ_node) = serde_json::from_value::<NodeInfo>(succ_info.clone()) {
                self.set_succ(Some(succ_node)); 
            } else {
                self.print_debug_msg(&format!(
                    "Invalid info provided for successor node {}", succ_info
                ));
            }
        } 
        if let Some(prev_info) = msg.get("prev_info") {
            if let Ok(prev_node) = serde_json::from_value::<NodeInfo>(prev_info.clone()) {
                self.set_prev(Some(prev_node)); 
            } else {
                self.print_debug_msg(&format!(
                    "Invalid info provided for previous node {}", prev_info
                ));
            }
        }
    }

    fn handle_insert(&self, msg: &Value) {
        if let Some(input) = msg.get("record") {
            if input.is_array() {
                for item in input.as_array().unwrap() {
                    self.insert_aux(item, msg.get("sender"));
                }
            } else {
                self.insert_aux(input, msg.get("sender"));
            }
        } else {
            eprintln!("Message doesn't contain record field");
        }
    }

    fn insert_aux(&self, record: &Value, sender: Option<&Value>) {
        // TODO! handle replicas 
        let key = HashFunc(record.get("key").unwrap().as_str().unwrap());
        let title = record.get("title").unwrap().as_str().unwrap().to_string();
        let item = Item {
            title,
            key,
            replica_idx: 0
        };

        if self.is_responsible(key) {
            let mut records = self.records.write().unwrap();
            records.insert(key, item);
            match serde_json::from_value::<NodeInfo>(sender.unwrap().clone()) {
                Ok(sender) => {
                    self.send_msg(
                        Some(sender)
                        , &json!({
                            "type": MsgType::Reply,
                            "msg": "Record inserted successfully"
                        }).to_string());
                }
                Err(e) => {
                    eprintln!("Failed to deserialize sender info: {}", e);
                }
            }
        } else {
            self.send_msg(*self.successor.read().unwrap(), &json!({
                "sender" : sender.unwrap(),
                "type": MsgType::Insert,
                "record": record
            }).to_string());
        }

    }

    fn handle_query(&self) {

    }

    fn has_replica(&self, rec_hash:HashType) -> bool {
        if let Some(record) = self.records.read().unwrap().get(&rec_hash){
            if record.replica_idx > 0 { return true; }
            else { return false; }
        }
        false
    }

    fn handle_delete(&self, del_msg:&Value) {
        /* To avoid computing Hash for every replica, use the extra boolean field 'hashed',
            if hashed true key == hash, else key == title */
        let key_hash:HashType;
        if let Some(key) = del_msg.get("key") {
            if let Some(is_hashed) = del_msg.get("hashed").unwrap().as_bool(){
                match is_hashed {
                    true => key_hash = serde_json::from_value::<HashType>(key.clone()).unwrap(),
                    false => key_hash = HashFunc(key.as_str().unwrap())
                }
            /* 1. Delete of "genuine" item
               This step covers all consistency models */
            if self.is_responsible(key_hash) {
                self.records.write().unwrap().remove(&key_hash);
                if self.replication_factor > 0 {
                    // delete replicas by forwarding to successor
                    let del_fw_msg = json!({
                        "type": MsgType::Delete,
                        "hashed": true,
                        "key": key_hash
                    });

                    self.send_msg(self.get_succ(), &del_fw_msg.to_string());
                }
                return;
            }

            if self.replication_factor > 0 && self.has_replica(key_hash) {
                    match self.replication_mode {
                        Consistency::Eventual => {
                            /*  Need to check if Delete Request initiated from an intermediate node. 
                                In this case the message should be delivered to both previous and successor.
                                Otherwise, delete of a replica must follow only one direction.
                                Use an extra field forward_back to avoid ping-pong messages. */
                            let backwards: bool;

                            if !is_hashed { 
                                backwards = true;   // first backward sender
                            } else if let Some(back_fw) = del_msg.get("forward_back").unwrap().as_bool() {
                                    backwards = back_fw;
                            } else {
                                backwards = false;
                            }
                            
                            let mut record_lock = self.records.write().unwrap();
                            if let Some(record) = record_lock.remove(&key_hash){
                                // removed my replica 
                                if !backwards && record.replica_idx < self.replication_factor {
                                    // forward to successor
                                    let del_msg_fw = json!({
                                        "type" : MsgType::Delete,
                                        "key" : key_hash,  
                                        "hashed": true
                                    });
                                    self.send_msg(self.get_succ(), &del_msg_fw.to_string());
                                }
                                if backwards && record.replica_idx > 0 {
                                    // forward to previous
                                    let del_msg_fw = json!({
                                        "type" : MsgType::Delete,
                                        "key" : key_hash,  
                                        "hashed": true, 
                                        "forward_back": true
                                    });
                                    self.send_msg(self.get_prev(), &del_msg_fw.to_string());
                                }
                            }

                        }
                        Consistency::Chain => {
                            /* If incoming request is from previous node, it means it has been propagated by root, 
                            so node can proceed to delete replica and forward to successor.
                            Otherwise it must just forward to previous until it reaches root. 
                            Use the 'forward_back' field again. */
                            let backwards: bool;

                            if !is_hashed { 
                                backwards = true;   // first backward sender
                            } else if let Some(back_fw) = del_msg.get("forward_back").unwrap().as_bool() {
                                    backwards = back_fw;
                            } else {
                                backwards = false;
                            }

                            if backwards && self.replication_factor > 0 {
                                let del_fw_msg = json!({
                                    "type": MsgType::Delete,
                                    "hashed": true,
                                    "key": key_hash,
                                    "forward_back": true
                                });

                            self.send_msg(self.get_prev(), &del_fw_msg.to_string());
                            } else if !backwards {
                                // now replica can be deleted safely
                                if let Some(record) = self.records.write().unwrap().remove(&key_hash) {
                                    // propagate to successor 
                                    if record.replica_idx < self.replication_factor {
                                        let del_fw_msg = json!({
                                            "type": MsgType::Delete,
                                            "hashed": true,
                                            "key": key_hash
                                        });

                                        self.send_msg(self.get_succ(), &del_fw_msg.to_string());
                                    }
                                }
                            }
                        }

                        _  => ()
                    }
                }

                else { // just forward the request to either succ or prev depending on hash
                    let del_msg_fw = json!({
                        "type": MsgType::Delete,
                        "hashed": true,
                        "key" : key_hash,
                    });
                    if self.is_next_responsible(key_hash) {
                        self.send_msg(self.get_succ(),&del_msg_fw.to_string());
                    } else {
                        self.send_msg(self.get_prev(), &del_msg_fw.to_string());
                    }
                }
            } else {
                self.print_debug_msg("'replica' field has errors");
            }
        } else {
            self.print_debug_msg("Missing key value");
        } 
    }

    pub fn send_msg(&self, dest_node: Option<NodeInfo>, msg: &str) -> Option<TcpStream> {
        if let Some(dest) = dest_node {
            dest.send_msg(msg)
        } else {
            eprintln!("Failed to send message: destination node not found");
            None
        }
    }

    fn is_responsible(&self, key: HashType) -> bool {
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
            key > prev_id && key <= self_id
        } else {
            // Wrapped case: previous is greater due to ring wrap-around
            key >= prev_id || key <= self_id
        }
    }

    fn is_next_responsible(&self, key: HashType) -> bool {
        let succ_rd = self.get_succ();
        let succ_id = succ_rd.unwrap().id;
        let self_id = self.get_id();
        // Check if the successor node is responsible for the key
        if self_id < succ_id {
            // Normal case: key falls within (self, successor]
            self.print_debug_msg("Normal case");
            key > self_id && key <= succ_id
        } else {
            // Wrapped case: self is greater due to ring wrap-around
            key >= self_id || key <= succ_id
        }
    }


    fn handle_overlay(&self, info_msg:&Value) {
    /* send an Info message to successor in a circular loop 
        until it reaches myself again */
        if let Some(topology) = info_msg.get("topology"){
            if let Ok(mut ring_list) = serde_json::from_value::<Vec<NodeInfo>>(topology.clone()) {
                // check head 
                if ring_list[0].id == self.get_id() {
                    // circle completed here so print network
                    print!("RING: ");
                    // sort just to start from smallest ID
                    ring_list.sort_by_key(|node| node.id);
                    for peer in ring_list.iter() {
                        print!("(id:{}, IP:{}:{}) --> ", peer.id, peer.ip_addr, peer.port);
                    }
                    println!("(wrap to start)");
                } else {
                    ring_list.push(self.get_info());
                    let ring_data = serde_json::json!({
                        "type": MsgType::Overlay,
                        "topology": ring_list     // serializable ?
                    }); 
            
                    self.send_msg(self.get_succ(), &ring_data.to_string()); 
                }
            }
        }

    }

    fn get_overlay(&self) {
        let succ_node = self.get_succ();
        if succ_node.unwrap().id == self.get_id() {
            // node is alone 
            println!("RING: (id:{}, IP:{}:{})", self.get_id(), self.get_ip(), self.get_port());
            return;
        }
        // simply begins the traversal by adding self
        let mut netvec : Vec<NodeInfo> = Vec::new();
        netvec.push(self.get_info());
        let ring_data = serde_json::json!({
            "type": MsgType::Overlay,
            "topology": netvec     // serializable ?
        }); 

        self.send_msg(succ_node, &ring_data.to_string());  
    }

}

    // fn get_records(&self, ring_size: usize) {
    //     // TODO! netsize
    //     //let network_size = self.bootstrap.get_netsize();

    //     // Acquire a read lock on `records`
    //     let records_rlock = self.records.read().unwrap();
    //     if ring_size <= self.replication_factor {
    //         for (key, value) in records_rlock.iter() {
    //             let record = serde_json::json!({
    //                 "key": key,
    //                 "title": value.title
    //             });
    //             let record_msg = serde_json::json!({
    //                 "type": format!("{:?}", MsgType::Insert),
    //                 "record": record
    //             });
    //             self.send_msg(self.previous, &record_msg.to_string());
    //         }
    //     } else {
    //         for (key, value) in records_rlock.iter() {
    //             let key_hash = HashFunc(&value.title);
    //             if !self.is_responsible(key_hash) {
    //                 // a lot of stuff TODO here
    //             }
    //         }

    //     }
    // }

impl ConnectionHandler for Node {
    fn handle_request(&self, mut stream: TcpStream) {
        let peer_addr = stream.peer_addr().unwrap();
        self.print_debug_msg(&format!("New message from {}", peer_addr));
        
        let peer_ip: Ipv4Addr = match peer_addr.ip() {
            std::net::IpAddr::V4(ipv4) => ipv4,  // Extract IPv4
            std::net::IpAddr::V6(_) => {
                eprintln!("Received IPv6 address, expected only IPv4");
                return;
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
        self.print_debug_msg(&format!("Received message: {}", received_msg));
                    
        // Deserialize the received JSON message
        let msg_value: Value = match serde_json::from_str(&received_msg) {
            Ok(value) => value,
            Err(e) => {
                eprintln!("Failed to deserialize message: {}", e);
                return;
            }
        };
                    
        if let Some(msg_type) = msg_value.get("type").and_then(Value::as_str) {
            self.print_debug_msg(&format!("Message type: {}", msg_type));
            match msg_type {
                "JoinRing" => {
                    self.join_ring();
                    if let Some(sender) = msg_value.get("sender") {
                        let sender_info = serde_json::from_value::<NodeInfo>(sender.clone()).unwrap();
                        sender_info.send_msg(&json!({
                            "type": MsgType::Reply,
                            "msg": "Node joined the ring"
                        }).to_string());
                    }
                }
                "Join" => {
                    self.handle_join(&msg_value);
                }
                "AckJoin" => {
                    self.handle_ack_join(&msg_value);
                }
                "Update" => {
                    self.handle_update(&msg_value);
                }
                "Quit" => {
                    self.handle_quit(&msg_value);
                }
                "Query" => {
                    self.handle_query();
                }
                "Insert" => { 
                    if self.get_status() {
                        self.handle_insert(&msg_value);
                    } else {
                        if let Some(sender) = msg_value.get("sender") {
                            let sender_info = serde_json::from_value::<NodeInfo>(sender.clone()).unwrap();
                            sender_info.send_msg(&json!({
                                "type": MsgType::Reply,
                                "msg": "Node is not active"
                            }).to_string());
                        }
                    }
                }
                "Delete" => {
                    if self.get_status() {
                        self.handle_delete(&msg_value);
                    } else {
                        if let Some(sender) = msg_value.get("sender") {
                            let sender_info = serde_json::from_value::<NodeInfo>(sender.clone()).unwrap();
                            sender_info.send_msg(&json!({
                                "type": MsgType::Reply,
                                "msg": "Node is not active"
                            }).to_string());
                        }
                    }
                }
                "GetOverlay" => {
                    if self.get_status() {
                        self.get_overlay();
                    } else {
                        if let Some(sender) = msg_value.get("sender") {
                            let sender_info = serde_json::from_value::<NodeInfo>(sender.clone()).unwrap();
                            sender_info.send_msg(&json!({
                                "type": MsgType::Reply,
                                "msg": "Node is not active"
                            }).to_string());
                        }
                    }
                }
                "Overlay" => {
                    self.handle_overlay(&msg_value);
                }
                _ => {
                    eprintln!("Invalid message type: {}", msg_type);
                }
            }
            
        } else {
            eprintln!("Received message does not contain a valid 'type' field.");
        }
       
    }

}
