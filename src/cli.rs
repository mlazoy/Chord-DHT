#![allow(dead_code, non_snake_case, unused_imports)]

use std::env;
use std::io::{self, Write, Read};
use std::net::{TcpStream, Ipv4Addr, TcpListener};
use std::process;
use serde_json::Value;

use crate::messages::{MsgType,MsgData,Message}; 
use crate::node::NodeInfo;  
use crate::utils::get_local_ip;


/// Sends a request to the node and reads a response.
fn send_request(ip: Ipv4Addr, port: u16, request_msg: &Message) -> Result<String, String> {
    let request = serde_json::json!(request_msg).to_string();
    let address = format!("{}:{}", ip, port);
    let response_ip = get_local_ip();
    let response_port = port + (process::id() % 1000) as u16;
    eprintln!("Sending request to {}: {}", address, request);
    let response_address = format!("{}:{}", response_ip, response_port);

    // 🚀 Step 1: Start a listening socket on response_port
    let listener = TcpListener::bind(&response_address).map_err(|e| format!("Failed to bind response port: {}", e))?;
    eprintln!("Listening for response on {}", response_address);

    // 🚀 Step 2: Send request to the node, including the response port
    let full_request = format!("{}", request);
    match TcpStream::connect(&address) {
        Ok(mut stream) => {
            writeln!(stream, "{}", full_request).map_err(|e| format!("Failed to send request: {}", e))?;
            stream.flush().map_err(|e| format!("Failed to flush request: {}", e))?;
        }
        Err(e) => return Err(format!("Could not connect to node at {}: {}", address, e)),
    }

    // 🚀 Step 3: Accept response connection and read response
    match listener.accept() {
        Ok((mut response_stream, _)) => {
            let mut buffer = [0; 1024];
            let mut response = Vec::new();

            loop {
                match response_stream.read(&mut buffer) {
                    Ok(0) => break, // Connection closed
                    Ok(n) => response.extend_from_slice(&buffer[..n]),
                    Err(e) => return Err(format!("Failed to read response: {}", e)),
                }
            }

            let response_str = String::from_utf8_lossy(&response).to_string();

            // 🚀 Step 4: Deserialize and extract only Reply messages

            let json_value: Value = match serde_json::from_str(&response_str) {
                Ok(value) => value,
                Err(e) => return Err(format!("Failed to deserialize message: {}", e))
            };
    
            // Convert Value to Message
            let msg: Message = match serde_json::from_value(json_value) {
                Ok(msg) => msg,
                Err(e) => return Err(format!("Failed to convert JSON value to Message: {}", e))
            };
            // extract only the data part
            let msg_data = msg.extract_data();

            match msg_data {
                MsgData::Reply { reply } => {
                    Ok(reply)
                }
                _ => Err(format!("Unexpected message data"))
            }
        }
        Err(e) => Err(format!("Failed to accept response connection: {}", e)),
    }
}


/// CLI routine to send requests to the chord network.
pub fn run_cli() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: cargo run cli <ip> <port> <command> [args]");
        process::exit(1);
    }

    let node_ip = args[2].parse().expect("Invalid IP address");
    let node_port = args[3].parse().expect("Invalid port number");
    let command = args[4].as_str();
    match command {
        "insert" => {
            if args.len() < 6 {
                println!("Usage:");
                println!("cargo run cli <ip> <port> insert [<key> <value> | -f <file>]");
                process::exit(1);
            }

            if args[5] == "-f" {
                let filename = args[6].as_str();
                let file_content = std::fs::read_to_string(filename).expect("Failed to read file");
                let lines: Vec<&str> = file_content.lines().collect();
                for line in lines {
                    let request = Message::new(
                        MsgType::Insert,
                        Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                        &MsgData::Insert { key: line.trim().to_string(), value: line.trim().to_string() }
                    );
                    match send_request(node_ip, node_port, &request) {
                        Ok(response) => println!("{}", response),
                        Err(e) => eprintln!("Error: {}", e),
                    }
                }
                return;
            }

            let request = Message::new(
                MsgType::Insert,
                Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                &MsgData::Insert { key: args[5].to_string(), value: args[6].to_string() }
            );
        
            match send_request(node_ip, node_port, &request) {
                Ok(response) => println!("{}", response),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        "delete" => {
            if args.len() < 5 {
                println!("Usage:");
                println!("cargo run cli <ip> <port> delete <key>");
                process::exit(1);
            }
            let request = Message::new(
                MsgType::Delete,
                Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                &MsgData::Delete { key: args[5].to_string() }
            );
            match send_request(node_ip, node_port, &request) {
                Ok(response) => println!("{}", response),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        "query" => {
            if args.len() < 5 {
                println!("Usage:");
                println!("cargo run cli <ip> <port> query [<key> | *] ");
                process::exit(1);
            } 
            let request:Message;
            if args[5].as_str() == "*" {
                request = Message::new(
                    MsgType::QueryAll,
                    Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                    &MsgData::QueryAll {  }
                );
            } else if args[5].as_str() == "-f" { 
                let filename = args[6].as_str();
                let file_content = std::fs::read_to_string(filename).expect("Failed to read file");
                let lines: Vec<&str> = file_content.lines().collect();
                for line in lines {
                    let request = Message::new(
                        MsgType::Query,
                        Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                        &MsgData::Query { key: line.trim().to_string() }
                    );
                    match send_request(node_ip, node_port, &request) {
                        Ok(response) => println!("{}", response),
                        Err(e) => eprintln!("Error: {}", e),
                    }
                }
                return;
            } 
            else {
                request = Message::new(
                    MsgType::Query,
                    Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                    &MsgData::Query{key: args[5].to_string() }
                );
            }
            match send_request(node_ip, node_port, &request) {
                Ok(response) => println!("{}", response),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        "overlay" => {
            let request = Message::new(
                MsgType::Overlay,
                Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                &MsgData::Overlay {  }
            );
            
            match send_request(node_ip, node_port, &request) {
                Ok(response) => {
                    println!("{}", response);
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        "depart" => {
            let request = Message::new(
                MsgType::Quit,
                Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                &MsgData::Quit { id: format!("") } // TODO! 
            );
            
            match send_request(node_ip, node_port, &request) {
                Ok(response) => {
                    println!("{}", response);
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        "join" => {
            let request = Message::new(
                MsgType::Join,
                Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                &MsgData::Join { id: format!("") }   // TODO!
            );
            
            match send_request(node_ip, node_port, &request) {
                Ok(response) => println!("{}", response),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        "requests" => {
            if args.len() < 5 {
                println!("Usage:");
                println!("cargo run cli <ip> <port> requests <file>");
                process::exit(1);
            }

            let filename = args[5].as_str();
            let file_content = std::fs::read_to_string(filename).expect("Failed to read file");
            let response_filename = format!("{}_response.txt", filename);
            let mut response_file = std::fs::File::create(response_filename).expect("Failed to create response file");
            let lines: Vec<&str> = file_content.lines().collect();
            for line in lines {
                let request: Vec<&str> = line.split(", ").collect();
                match request[0] {
                    "insert" => {
                        let request = Message::new(
                            MsgType::Insert,
                            Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                            &MsgData::Insert { key: request[1].to_string(), value: request[2].to_string() }
                        );
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => println!("{}", response),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    "query" => {
                        let request = Message::new(
                            MsgType::Query,
                            Some(&NodeInfo::new(get_local_ip(), node_port + (process::id() % 1000) as u16)),
                            &MsgData::Query { key: request[1].to_string() }
                        );
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => { 
                                println!("{}", response);
                                writeln!(response_file, "Request: {} | Response: {}", line, response)
                                .expect("Failed to write to response file");
                            }
                            ,
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    _ => {
                        eprintln!("Invalid request type: {}", request[0]);
                    }
                }
            }
        }
        "help" => {
            println!("Options:");
            println!("  <ip>                  => IP address of the node to connect to");
            println!("  <port>                => Port of the node to connect to");
            println!("Available commands:");
            println!("  insert <key> <value>  => Insert a (key,value) in the DHT");
            println!("  delete <key>          => Delete the given key from the DHT");
            println!("  query <key>           => Query the DHT for a specific key or '*' for all");
            println!("  overlay               => Print the chord ring topology");
            println!("  join                  => Join the ring");
            println!("  depart                => Gracefully remove this node from the ring");
            println!("  help                  => Show this help message");
        }
        _ => {
            println!("Unknown command. Type 'help' to see available commands.")
        }
    }
}
