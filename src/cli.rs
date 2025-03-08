use std::env;
use std::io::{self, Write, Read};
use std::net::{TcpStream, Ipv4Addr, TcpListener};
use std::process;
use serde_json::json;
use std::str::FromStr;

use crate::messages::{MsgType,MsgData,Message};
use crate::utils::*;  
use crate::node::{Node, NodeInfo};  


/// Sends a request to the node and reads a response.
fn send_request(ip: Ipv4Addr, port: u16, request_msg: &Message) -> Result<String, String> {
    let request = serde_json::json!(request_msg).to_string();
    let address = format!("{}:{}", ip, port);
    let response_port = port + 42;
    println!("Sending request to {}: {}", address, request);
    let response_address = format!("{}:{}", ip, response_port);

    // 🚀 Step 1: Start a listening socket on response_port
    let listener = TcpListener::bind(&response_address).map_err(|e| format!("Failed to bind response port: {}", e))?;
    println!("Listening for response on {}", response_address);

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
            Ok(response_str.trim().to_string()) // Trim to remove newlines
        }
        Err(e) => Err(format!("Failed to accept response connection: {}", e)),
    }
}


/// CLI loop that takes user input, sends requests to the node, and prints responses.
pub fn run_cli(node_ip: Ipv4Addr, node_port: u16) {
    loop {
        print!("chordify> ");
        if let Err(_) = io::stdout().flush() {
            eprintln!("Error flushing stdout.");
        }

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let line = input.trim();
                if line.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = line.split_whitespace().collect();
                let command = parts[0].to_lowercase();
                match command.as_str() {
                    "insert" => {
                        if parts.len() < 2 {
                            println!("Usage:");
                            println!("  insert <key> <value>");
                            continue;
                        }
                        let request = Message::new(
                            MsgType::Insert,
                            Some(&NodeInfo::new(node_ip, node_port + 42)),
                            &MsgData::Insert { key: parts[1].to_string(), value: parts[2].to_string() }
                        );
                    
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => println!("{}", response),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }

                    "delete" => {
                        if parts.len() < 2 {
                            println!("Usage:");
                            println!("  delete <key>");
                            continue;
                        }
                        let request = Message::new(
                            MsgType::Delete,
                            Some(&NodeInfo::new(node_ip, node_port + 42)),
                            &MsgData::Delete { key: parts[1].to_string() }
                        );
    
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => println!("{}", response),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }

                    "query" => {
                        if parts.len() < 2 {
                            println!("Usage:");
                            println!("  query [<key> | *]");
                            continue;
                        }

                        let request:Message;

                        if parts[1] == "*" {
                            request = Message::new(
                                MsgType::QueryAll,
                                Some(&NodeInfo::new(node_ip, node_port + 42)),
                                &MsgData::QueryAll {  }
                            );
                        } else {
                            request = Message::new(
                                MsgType::Query,
                                Some(&NodeInfo::new(node_ip, node_port + 42)),
                                &MsgData::Query{key: parts[1].to_string() }
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
                            Some(&NodeInfo::new(node_ip, node_port + 42)),
                            &MsgData::Overlay {  }
                        );
                        
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => println!("{}", response),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }

                    "depart" => {
                        let request = Message::new(
                            MsgType::Quit,
                            Some(&NodeInfo::new(node_ip, node_port + 42)),
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
                            Some(&NodeInfo::new(node_ip, node_port + 42)),
                            &MsgData::Join { id: format!("") }   // TODO!
                        );
                        
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => println!("{}", response),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }

                    "help" => {
                        println!("Available commands:");
                        println!("  insert <key> <value>  => Insert a (key,value) in the DHT");
                        println!("  delete <key>          => Delete the given key from the DHT");
                        println!("  query <key>           => Query the DHT for a specific key or '*' for all");
                        println!("  overlay               => Print the chord ring topology");
                        println!("  depart                => Gracefully remove this node from the ring");
                        println!("  help                  => Show this help message");
                        println!("  exit                  => Quit the CLI");
                    }

                    "exit" | "quit" => {
                        println!("Exiting CLI...");
                        break;
                    }

                    _ => {
                        println!("Unknown command. Type 'help' to see available commands.")
                    }
                }
            }
            Err(e) => {
                println!("Error reading from stdin: {}", e);
            }
        }
    }

    println!("CLI terminated.");
}
