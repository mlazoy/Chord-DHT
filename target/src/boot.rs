use std::net::{TcpListener, TcpStream, Shutdown};
use std::net::{Ipv4Addr,SocketAddrV4} ;
use std::collections::BTreeMap;
use std::io;
use std::io::{Read,Write};
use serde_json;
use serde_json::Value;

use crate::node::{Node, NodeInfo, BOOTSTRAP_INFO};
use crate::utils::{DebugMsg, HashType, PeerTrait, Consistency, MsgType, Item};
use crate::network::ConnectionHandler;
use crate::{BOOT_ADDR, BOOT_PORT};

/* Handling concurrent join / departs is out of scope of the project
    TODO! add locking mechanisms in bootstrap fields */

#[derive(Debug)]
pub struct Bootstrap {
    node: Node,
    port_offs: u16,                            //offset from boot port to assign to incoming nodes                                           
    peers: BTreeMap<HashType, NodeInfo>,       // Maps ID -> Node instance  ( sorted ) 
    ring_size : usize,                          // total nodes in the network
}

impl Bootstrap {

    pub fn new(k_repl: Option<usize>, m_repl: Option<Consistency>) -> Self {
        Bootstrap {
            node : Node::new(BOOT_ADDR, Some(BOOT_PORT), k_repl, m_repl),
            port_offs: 1,                    
            peers:BTreeMap::new(),
            ring_size: 1
        }
    }
    
    pub fn init(&mut self) -> io::Result<TcpListener> { 
        self.print_debug_msg("Setting up Bootstrap node...");
        // add itself to the ring 
        let my_hash = BOOTSTRAP_INFO.hash_id();
        self.peers.insert(my_hash,BOOTSTRAP_INFO); 
        self.set_id(my_hash);
        let sock_addr = SocketAddrV4::new(self.get_ip(), self.get_port());

        match TcpListener::bind(sock_addr) {
            Ok(listener) => {
                self.print_debug_msg(&format!("Bootstrap is listening on {}", sock_addr));
                self.node.set_status(true);
                Ok(listener)
                // maybe call wait for requests immediately here ?
            }
            Err(e) => {
                eprintln!("Bootstrap Failed to bind to {}: {}", sock_addr, e);
                Err(e)
            }
        }
    }

    fn find_succ(&self, node_hash: HashType) -> Option<&NodeInfo> {
        if self.peers.is_empty() {
            return None; // No peers exist in the ring
        }
    
        // Find the first node strictly greater than `node_hash`
        self.peers.range((node_hash..)).skip_while(|(key, _)| *key == &node_hash).next()
            // Wrap around: find the smallest node, but exclude `node_hash`
            .or_else(|| self.peers.iter().filter(|(key, _)| *key != &node_hash).min_by_key(|(key, _)| *key))
            .map(|(_, succ)| succ)
    }
    
    fn find_prev(&self, node_hash: HashType) -> Option<&NodeInfo> {
        if self.peers.is_empty() {
            return None; // No peers exist in the ring
        }
    
        // Find the last node strictly smaller than `node_hash`
        self.peers.range(..node_hash).next_back()
            // Wrap around: find the largest node, but exclude `node_hash`
            .or_else(|| self.peers.iter().filter(|(key, _)| *key != &node_hash).max_by_key(|(key, _)| *key))
            .map(|(_, prev)| prev)
    }    

    // used to update the "prev pointer" of the neighbour to the "curr"
    fn update_prev(&mut self, neighbor:&NodeInfo, curr:&NodeInfo){
        self.print_debug_msg(&format!(
            "Updating prev of node {}:{}", neighbor.get_ip(), neighbor.get_port()
           ));
        // check if botstrap needs to update its neighbor locally
        let neighbor_hash = neighbor.hash_id();
        if neighbor_hash == self.get_id().unwrap() { 
            self.set_prev(Some(*curr));
            return;
        } else {
            let data = serde_json::json!({
                "type": format!("{:?}", MsgType::Update), // Convert MsgType to string
                "prev_info": curr, 
            });
            neighbor.send_msg(&data.to_string());  
        }
    }

    // used to update the "succ pointer" of the neighbour to the "curr"
    fn update_succ(&mut self, neighbor:&NodeInfo, curr:&NodeInfo) {
        self.print_debug_msg(&format!(
            "Updating succ of node {}:{}", neighbor.get_ip(), neighbor.get_port()
           ));
        // check if botstrap needs to update its neighbor locally
        let neighbor_hash = neighbor.hash_id();
        if neighbor_hash == self.get_id().unwrap() { 
            self.set_succ(Some(*curr));
            return;
        } else {
            let data = serde_json::json!({
                "type": format!("{:?}", MsgType::Update), // Convert MsgType to string
                "succ_info": curr, 
            });
            neighbor.send_msg(&data.to_string());
        }
    }

    /* stab method because immediately updating curr immediately 
        is not always allowed because of borrowing rules */
    fn update_status(&mut self, node_hash:HashType) {
        let curr_node = self.peers.get_mut(&node_hash);
        match curr_node {
            Some(curr) => curr.set_peer_status(true),
            None => self.print_debug_msg(&format!(
                "Node {} is not in the ring", node_hash
            )),
        }
    }

    fn ack_node(&self, curr:&NodeInfo) {
        self.print_debug_msg("Sending an AckJoin");
        let data = serde_json::json!({
            "type": format!("{:?}", MsgType::AckJoin), // Convert MsgType to string
            "ring_size": self.ring_size,
        });
        curr.send_msg(&data.to_string());
    }

    fn broadcast(&self, msg:&str) {
        for peer in self.peers.iter() {
            // exclude myself & inactive peers from communication
            if *peer.0 != self.get_id().unwrap() && peer.1.get_peer_status() == true {
                peer.1.send_msg(msg);
            }
        }
    }

    /* sends a setup message to the node asking to join and waits for an ack 
        to actually add it on the record and update prev & succ pointers of neighbours */
    fn handle_join_request(&mut self, peer_ip : Ipv4Addr, node_hash : HashType, curr_stream: &mut TcpStream) {
        /* check the hash according to the current port to see 
            if node is already part of the network */
        self.print_debug_msg(&format!(
            "Handling join request of node {}", node_hash
        ));
        if let Some(peer_node) = self.peers.get(&node_hash) {
            if peer_node.get_peer_status() == true {
                self.print_debug_msg(&format!(
                    "Node with id :{} has already joined the ring", node_hash
                ));
                return;
            } else {
                self.print_debug_msg(&format!(
                    "Node with id :{} is waiting for acknowledgement", node_hash
                ));
                return;
            }
        } else {
            // assign a new port num
            let new_port = self.get_port() + self.port_offs;
            /* create a new node with status false in the beginning &&
                 wait for an Ack to turn status into true */
            let new_node = NodeInfo::new(peer_ip, new_port, false);
            let new_hash = new_node.hash_id();
            // find prev and succ according to hash value on the ring
            let prev_node = self.find_prev(new_hash);
            let succ_node = self.find_succ(new_hash);

            self.print_debug_msg(&format!(
                "prev:{:?}, succ:{:?} ", prev_node, succ_node
            ));
            // Send a setup message to the new node
            if let (Some(prev), Some(succ)) = (prev_node, succ_node) {
                let setup_info = serde_json::json!({
                    "type": format!("{:?}", MsgType::Setup), // Convert MsgType to string
                    "id": new_hash,
                    "port": new_port,
                    "prev": prev, 
                    "succ": succ,   // this must be serialized
                    "repl_factor": self.get_repl_factor(),
                    "repl_mode": self.get_repl_mode(),
                    //"ring_size": self.ring_size, // send this during "AckJoin"
                });
                // use the same stream for replying as node hasn't reveived setup values yet
                if let Err(e) = curr_stream.write_all(setup_info.to_string().as_bytes()) {
                        eprintln!("Failed to send setup info to node {}:{} - {}", self.get_ip(), new_port, e);
                        return;
                    }
                    self.print_debug_msg(&format!(
                        "✅ Sent setup message: {} to node {}", setup_info.to_string(), node_hash
                    ));

                    // finally add the new node on the ring temporarily 
                        self.peers.insert(
                            new_hash, 
                            new_node
                        );

                        self.print_debug_msg(&format!(
                            "Added node {} in the ring", new_hash
                        ));
            }
        }
    }

    fn handle_ack_setup(&mut self, node_hash: HashType) {
        
        if let Some(curr_node) = self.peers.get(&node_hash).cloned() {
            if curr_node.get_peer_status() == true {
                self.print_debug_msg(&format!(
                    "Node with id:{} has already been acknowledged", node_hash
                ));
                return;
            }
        
        // find prev and succ once more 
        let prev_node= self.find_prev(node_hash).cloned();
        let succ_node= self.find_succ(node_hash).cloned();

        if let (Some(prev), Some(succ)) = (prev_node, succ_node) {
            // succ node updates its prev pointer
            self.update_prev(&succ, &curr_node);
            // prev node updates its succ pointer
            self.update_succ(&prev, &curr_node);
            }
            
            // update new nodes's status on the BTree
            self.update_status(node_hash);

            // broadcast the new ring size to everyone with WITH AN UPDATE message
            self.ring_size +=1;

            // mark this port as used by incrementing port offset
            self.port_offs += 1;

            // send an ack join back to the new node
            self.ack_node(&curr_node);

            self.print_debug_msg(&format!(
                "Node with id: {} acknowledged successfully ", node_hash
            ));

        } else {
            self.print_debug_msg(&format!(
                "Node with id: {} not found on the ring hashmap", node_hash
            ));
        }
    }

    fn handle_quit(&mut self, node_hash :HashType) {
        // find curr->succ && curr->prev
        let prev_node= self.find_prev(node_hash).cloned();
        let succ_node= self.find_succ(node_hash).cloned();
        // prev.succ := curr.succ && succ.prev := curr.prev                                     
        if let (Some(prev), Some(succ)) = (prev_node, succ_node) {
            self.update_succ(&prev, &succ);
            self.update_prev(&succ,&prev);
        }
        // remove node from hashmap
        self.peers.remove(&node_hash);
        // decrement size 
        self.ring_size -= 1;
    }

}

impl ConnectionHandler for Bootstrap {
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
                    
        if let Some(msg_type) = msg_value.get("type").and_then(Value::as_str) {
            self.print_debug_msg(&format!("Message type: {}", msg_type));
            self.print_debug_msg(&format!("Message value: {}", msg_value));
            if let Some(id_str) = msg_value.get("id").and_then(Value::as_str) {
                match HashType::from_hex(id_str) {
                    Ok(id) => {
                        match msg_type {
                            "Join" => {
                                self.handle_join_request(peer_ip, id, &mut stream);
                            }
                            "AckSetup" => { 
                                self.handle_ack_setup(id);
                            }
                            "Quit" => {
                                self.handle_quit(id);
                            }
                            "RequestRecords" => {
                                //
                            }
                            _ => {
                                eprintln!("Invalid message type: {}", msg_type);
                            }
                        }
                    }
                    Err(e) => {
                        self.print_debug_msg(&format!("Failed to parse id: {}", e));
                    }
                }
            } else {
                self.print_debug_msg("Received invalid or missing id field");
            } 
            
        } else {
            eprintln!("Received message does not contain a valid 'type' field.");
        }
       
    }
}

impl PeerTrait for Bootstrap {

    fn get_ip(&self) -> Ipv4Addr {
        self.node.get_ip()
    }

    fn get_id(&self) -> Option<HashType> {
        self.node.get_id()
    }

    fn get_status(&self) -> bool {
        self.node.get_status()
    }

    fn set_status(&mut self, status:bool) {
        self.node.set_status(status);
    }

    fn get_port(&self) -> u16 {
        self.node.get_port()
    }

    fn set_id(&mut self, id:HashType) {
        self.node.set_id(id);
    }

    fn get_prev(&self) -> Option<NodeInfo> {
        self.node.get_prev()
    }

    fn get_succ(&self) -> Option<NodeInfo> {
        self.node.get_succ()
    }

    fn set_prev(&mut self, prev:Option<NodeInfo>) {
        self.node.set_prev(prev);
    }

    fn set_succ(&mut self, succ:Option<NodeInfo>) {
        self.node.set_succ(succ);
    }

    fn get_repl_factor(&self) -> usize {
        self.node.get_repl_factor()
    }

    fn set_repl_factor(&mut self, k:usize){
        self.node.set_repl_factor(k);
    }

    fn get_repl_mode(&self) -> Consistency {
        self.node.get_repl_mode()
    }

    fn set_repl_mode(&mut self, mode:Consistency) {
        self.node.set_repl_mode(mode);
    }

    fn insert(&mut self, key:HashType, value:Item) {
        self.node.insert(key, value); 
    }

    fn delete(&mut self, key:HashType) {
        self.node.delete(key);
    }

    fn query(&self, key:HashType) -> Option<Vec<Item>> {
        self.node.query(key)
    }

}