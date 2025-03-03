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
use crate::utils::{Consistency, DebugMsg, HashFunc, HashIP, HashType, Item, MsgType};
use crate::network::{ConnectionHandler, Server};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodeInfo {
    ip_addr: Ipv4Addr,
    port: u16,
    id : HashType,
    status: bool                            // denotes if is already in ring
}

pub const BOOTSTRAP_INFO: NodeInfo = NodeInfo {
    ip_addr: BOOT_ADDR,
    port: BOOT_PORT,
    id : HashIP(BOOT_ADDR, BOOT_PORT),
    status: true
};

#[derive(Debug, Clone)]
pub struct Node {
    info: NodeInfo,                                         // wraps ip, port, status                                  // generated from hash function
    previous : Option<NodeInfo>,                  
    successor : Option<NodeInfo>, 
    replication_factor : usize,                             // number of replicas per node
    replication_mode : Consistency,                         // replication mode                
    records : Arc<RwLock<BTreeMap<HashType, Item>>>,       // list of hasehd records per node
}

impl NodeInfo {
    pub fn new(ip_addr:Ipv4Addr, port:u16, status:bool) -> Self {
        NodeInfo {
            ip_addr,
            port,
            id: HashIP(ip_addr, port),
            status,
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

impl Node {

    pub fn new(ip_addr:Ipv4Addr, _port: Option<u16>, _k: Option<usize>, _m: Option<Consistency>) -> Self {
        Node {
            info: NodeInfo {
                ip_addr,
                port: _port.unwrap_or(0),  
                id : HashIP(ip_addr, _port.unwrap_or(0)),
                status: false
            },                     // default 
            replication_factor: _k.unwrap_or(0),
            replication_mode: _m.unwrap_or(Consistency::Eventual),
            successor: None,
            previous: None,
            records: Arc::new(RwLock::new(BTreeMap::new()))
        }
    }

    pub fn get_id(&self) -> HashType {
        self.info.id
    }

    pub fn set_id(&mut self, id: HashType) {
        self.info.id = id;
    }

    pub fn get_ip(&self) -> Ipv4Addr {
        self.info.ip_addr
    }

    pub fn get_port(&self) -> u16 {
        self.info.port
    }

    pub fn get_status(&self) -> bool {
        self.info.status
    }

    pub fn set_status(&mut self, new_status:bool) {
        self.info.status = new_status
    }

    pub fn join_ring(&self) {

    }

    fn handle_join(&self) {

    }

    fn handle_ack_join(&self) {

    }

    fn handle_update(&self) {

    }

    fn handle_insert(&self) {

    }

    fn handle_query(&self) {

    }

    fn handle_delete(&self) {

    }

    fn is_responsible(&self, key:HashType) -> bool {
        if let Some(prev) = self.previous {
            if let Some(succ) = self.successor {
                let prev_input = format!("{}:{}", prev.ip_addr, prev.port);
                let prev_key = HashFunc(&prev_input);
                let succ_input = format!("{}:{}", succ.ip_addr, succ.port);
                let prev_key = HashFunc(&succ_input);
                if prev_key < self.get_id() {
                    if key > prev_key && key <= self.get_id() {
                        return true;
                    }
                } else {
                    if key > prev_key || key <= self.get_id() {
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
                "Join" => {
                    self.handle_join();
                }
                "AckJoin" => {
                    self.handle_ack_join();
                }
                "Update" => {
                    self.handle_update();
                }
                "Query" => {
                    self.handle_query();
                }
                "Insert" => { 
                    self.handle_insert();
                }
                "Delete" => {
                    self.handle_delete();
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
