use std::net::{TcpListener, TcpStream, Shutdown};
use std::net::{Ipv4Addr,SocketAddrV4};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use std::io::Write;
use std::io::Read;
use std::io;
use serde_json::Value;
use std::thread;
use serde_json::json;

use crate::{BOOT_ADDR, BOOT_PORT};
use crate::utils::{Consistency, DebugMsg, HashFunc, HashType, MsgType, PeerTrait, Item};
use crate::network::{ConnectionHandler, Server};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodeInfo {
    ip_addr: Ipv4Addr,
    port: u16,
    status: bool                            // denotes if is already in ring
}

pub const BOOTSTRAP_INFO: NodeInfo = NodeInfo {
    ip_addr: BOOT_ADDR,
    port: BOOT_PORT,
    status: true
};

#[derive(Debug, Clone)]
pub struct Node {
    info: NodeInfo,                                         // wraps ip, port, status
    id : Option<HashType>,                                  // generated from hash function
    previous : Option<NodeInfo>,                  
    successor : Option<NodeInfo>, 
    replication_factor : usize,                             // number of replicas per node
    replication_mode : Consistency,                         // replication mode                
    records : Arc<RwLock<BTreeMap<HashType, Item>>>,       // list of hasehd records per node
    // TODO! make this thread safe
}

impl NodeInfo {
    pub fn new(ip_addr:Ipv4Addr, port:u16, status:bool) -> Self {
        NodeInfo {
            ip_addr,
            port,
            status,
        }
    }

    pub fn get_peer_ip(&self) -> Ipv4Addr {
        self.ip_addr
    }

    pub fn get_peer_port(&self) -> u16 {
        self.port
    }

    pub fn get_peer_status(&self) -> bool {
        self.status
    }

    pub fn set_peer_status(&mut self, new_status:bool) {
        self.status = new_status
    }

    // wrap ip and port in a single string and call global hashing function
    pub fn hash_id(&self) -> HashType { 
        let input = format!("{}:{}", self.ip_addr, self.port);
        HashFunc(&input)
    }

    pub fn send_msg(&self, msg: &str) -> Option<TcpStream> { 
        let sock_addr = std::net::SocketAddrV4::new(self.ip_addr, self.port);
        
        match TcpStream::connect(sock_addr) {
            Ok(mut stream) => {
                if let Err(e) = stream.write_all(msg.as_bytes()) {
                    eprintln!(
                        "❌ Message {} failed to deliver to {}:{} - {}",
                        msg,
                        self.get_ip(),
                        self.get_port(),
                        e
                    );
                    return None;
                }

                self.print_debug_msg(&format!(
                    "✅ Message {} sent to {}:{} successfully",
                    msg,
                    self.get_ip(),
                    self.get_port()
                ));
                return Some(stream);
            }
            Err(e) => {
                eprintln!(
                    "❌ Connection failed to node {}:{} - {}",
                    self.get_ip(),
                    self.get_port(),
                    e
                );
                None
            }
        }
    }

}

impl Node {

    pub fn new(ip_addr:Ipv4Addr, _port: Option<u16>, _k: Option<usize>, _m: Option<Consistency>) -> Self {
        Node {
            info: NodeInfo {
                ip_addr,
                port: _port.unwrap_or(0),  
                status: false
            },
            id: None,                      // default 
            replication_factor: _k.unwrap_or(0),
            replication_mode: _m.unwrap_or(Consistency::Eventual),
            successor: None,
            previous: None,
            records: Arc::new(RwLock::new(BTreeMap::new()))
        }
    }


    pub fn join_ring(&mut self) {
        // send join message to bootstrap
        let start_id = HashFunc(&format!("{}:{}", self.info.ip_addr, self.info.port));
        let join_msg = serde_json::json!({
            "type": format!("{:?}", MsgType::Join),
            "id" : start_id,
        });
        match self.send_msg(Some(BOOTSTRAP_INFO), &join_msg.to_string()) {
            Some(mut stream) => self.wait_for_setup(&mut stream),
            None => eprintln!("Failed to send join message to bootstrap")
        }
        
    }

    pub fn depart_ring(&mut self) {
        let record_msg = serde_json::json!({
            "type" : format!("{:?}", MsgType::Quit),
            "id" : self.get_id()
        });
        self.send_msg(Some(BOOTSTRAP_INFO), &record_msg.to_string());
    }

    fn handle_update(&mut self, msg:&Value) {
        // 1st case : "Previous Update"
        if let Some(prev_info) = msg.get("prev_info") {
            if let Ok(prev_node) = serde_json::from_value::<NodeInfo>(prev_info.clone()) {
                self.set_prev(Some(prev_node));
            } else { 
                self.print_debug_msg(&format!("Invalid prev info provided {}", prev_info)); 
            }
            return;
        } 
        // 2nd case : "SuccessorUpdate"
        if let Some(succ_info) = msg.get("succ_info") {
            if let Ok(succ_node) = serde_json::from_value::<NodeInfo>(succ_info.clone()) {
                self.set_prev(Some(succ_node));
            } else { 
                self.print_debug_msg(&format!("Invalid prev info provided {}", succ_info));
            }
            return;
        }
        // 3rd case : "Records Update"
        if let Some(records) = msg.get("record") {

            return;
        }
        
    }


    fn wait_for_setup(&mut self, stream: &mut TcpStream) {
        let mut buffer = [0; 1024]; 
        match stream.read(&mut buffer) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    println!("Connection lost.");
                    return;
                }
                // Convert buffer to string safely
                let request_str = match std::str::from_utf8(&buffer[..bytes_read]) {
                    Ok(s) => s,
                    Err(_) => {
                        println!("Invalid UTF-8 received.");
                        return;
                    }
                };
                // Parse JSON
                let response_msg: Result<Value, _> = serde_json::from_str(request_str);
                match response_msg {
                    Ok(msg) => {
                        if let Some(msg_type) = msg.get("type").and_then(Value::as_str) {
                            if msg_type == "Setup" {
                                // setup new node
                                let new_port = msg.get("port").and_then(Value::as_u64).unwrap();
                                self.info.port = new_port as u16;
                                if let Some(id_str) = msg.get("id").and_then(Value::as_str) {
                                    match HashType::from_hex(id_str) {
                                        Ok(id) => self.id = Some(id),
                                        Err(e) => panic!("Failed to setup id - {},", e)
                                    } 
                                } else {
                                    panic!("Id is missing or invalid");
                                }
                                if let Some(prev_value) = msg.get("prev") {
                                    if let Ok(prev_node) = serde_json::from_value::<NodeInfo>(prev_value.clone()) {
                                        self.set_prev(Some(prev_node));
                                    }
                                } else { // exit on error
                                    panic!("Failed to retrieve previous node info");
                                }
                                if let Some(succ_value) = msg.get("succ") {
                                    if let Ok(succ_node) = serde_json::from_value::<NodeInfo>(succ_value.clone()) {
                                        self.set_succ(Some(succ_node));
                                    }
                                } else { // exit on error
                                    panic!("Failed to retrieve successor node info");
                                }
                                // update status after successfull setup & binding
                                let sock_addr = SocketAddrV4::new(self.info.ip_addr, self.info.port);
                                match TcpListener::bind(sock_addr) {
                                    Ok(listener) => {
                                        self.set_status(true);
                                        let join_ack_msg = serde_json::json!({
                                            "type": format!("{:?}", MsgType::AckSetup),
                                            "id": self.get_id() // Automatically serialized as a hex string
                                        });
                                        self.send_msg(Some(BOOTSTRAP_INFO), &join_ack_msg.to_string());
                                        // create a multi-threaded server and start listening to it 
                                        let node_server = Server::new(self.clone());
                                        self.print_debug_msg(&format!("Node started listening on: {}", sock_addr));
                                        node_server.wait_for_requests(listener,4);
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to bind to received port, {} ", e);
                                    }
                                }
                                // close this stream and accept messages from setup port 
                                stream.shutdown(Shutdown::Both);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Couldn't parse json message: {}", e);
                        return;
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to receive setup message from bootstrap: {}", e);
                return;
            }
        }
    }
  

    fn is_responsible(&self, key:HashType) -> bool {
        if let Some(prev) = self.previous {
            if let Some(succ) = self.successor {
                let prev_input = format!("{}:{}", prev.ip_addr, prev.port);
                let prev_key = HashFunc(&prev_input);
                let succ_input = format!("{}:{}", succ.ip_addr, succ.port);
                let prev_key = HashFunc(&succ_input);
                if prev_key < self.id.unwrap() {
                    if key > prev_key && key <= self.id.unwrap() {
                        return true;
                    }
                } else {
                    if key > prev_key || key <= self.id.unwrap() {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn send_msg(&self, destNode: Option<NodeInfo>, msg: &str) -> Option<TcpStream> {
        if let Some(destNode) = destNode {
            destNode.send_msg(msg)
        } else {
            eprintln!("Failed to send message: destination node not found");
            None
        }
    }

    fn get_records(&self, ring_size: usize) {
        // TODO! netsize
        //let network_size = self.bootstrap.get_netsize();

        // Acquire a read lock on `records`
        let records_rlock = self.records.read().unwrap();
        if ring_size <= self.replication_factor {
            for (key, value) in records_rlock.iter() {
                let record = serde_json::json!({
                    "key": key,
                    "title": value.title
                });
                let record_msg = serde_json::json!({
                    "type": format!("{:?}", MsgType::Insert),
                    "record": record
                });
                self.send_msg(self.previous, &record_msg.to_string());
            }
        } else {
            for (key, value) in records_rlock.iter() {
                let key_hash = HashFunc(&value.title);
                if !self.is_responsible(key_hash) {
                    // a lot of stuff TODO here
                }
            }

        }
    }

}

impl ConnectionHandler for Node {
    fn handle_request(&mut self, mut stream: TcpStream) {
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
        self.print_debug_msg(&format!("Message value: {}", msg_value));
                    
        if let Some(msg_type) = msg_value.get("type").and_then(Value::as_str) {
            self.print_debug_msg(&format!("Message type: {}", msg_type));
            match msg_type {
                "AckJoin" => {
                    /* request for records from neighbors here 
                    after successfully inserting the ring */
                    let ring_size = msg_value.get("ring_size").unwrap().as_u64().unwrap() as usize;
                    let get_records_msg = serde_json::json!({
                        "type": format!("{:?}", MsgType::RequestRecords),
                        "ring_size": ring_size,
                        "id": self.id.unwrap()
                    });
                    self.send_msg(self.successor, &get_records_msg.to_string());

                }
                "Update" => {
                    self.handle_update(&msg_value);
                }
                "RequestRecords" => {
                    // TODO!
                }
                "Query" => {
                    // TODO!
                }
                "Insert" => { 
                    // TODO!
                }
                "Delete" => {
                    // TODO!
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


impl PeerTrait for Node {

    fn get_ip(&self) -> Ipv4Addr {
        self.info.ip_addr
    }

    fn get_id(&self) -> Option<HashType> {
        self.id
    }

    fn set_id(&mut self, id:HashType) {
        self.id = Some(id);
    }

    fn get_port(&self) -> u16 {
        self.info.port
    }

    fn set_status(&mut self, status:bool) {
        self.info.status= status
    }

    fn get_status(&self) -> bool {
        self.info.status
    }

    fn set_prev(&mut self, prev:Option<NodeInfo>) {
        self.previous = prev;
    }

    fn set_succ(&mut self, succ:Option<NodeInfo>) {
        self.successor = succ;
    }

    fn get_prev(&self) -> Option<NodeInfo> {
        self.previous
    }

    fn get_succ(&self) -> Option<NodeInfo> {
        self.successor
    }

    fn get_repl_factor(&self) -> usize {
        self.replication_factor
    }

    fn set_repl_factor(&mut self, k:usize){
        self.replication_factor = k;
    }

    fn get_repl_mode(&self) -> Consistency {
        self.replication_mode
    }

    fn set_repl_mode(&mut self, mode:Consistency) {
        self.replication_mode = mode;
    }

    fn insert(&mut self, key:HashType, value:Item) {
        //self.records.insert(key,value);
    }

    fn delete(&mut self, key:HashType) {
        //self.records.remove(&key);
    }

    fn query(&self, key:HashType) -> Option<Vec<Item>>  // returns list of values for special case '*'
    {
        return None
    }

}