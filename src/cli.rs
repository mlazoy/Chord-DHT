use std::env;
use std::io::{self, Write, Read};
use std::net::{TcpStream, Ipv4Addr, TcpListener};
use std::process;
use serde_json::json;
use std::str::FromStr;

use crate::utils::*;  
use crate::node::NodeInfo;  


/// Sends a request to the node and reads a response.
fn send_request(ip: &str, port: u16, request: &str) -> Result<String, String> {
    let address = format!("{}:{}", ip, port);
    let response_port = port + 42;
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
pub fn run_cli(node_ip: &str, node_port: u16) {
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
                let node_ip_addr = Ipv4Addr::from_str(node_ip).unwrap();
                match command.as_str() {
                    "insert" | "delete" | "query" => {
                        if parts.len() < 2 {
                            println!("Usage:");
                            println!("  insert <key> <value>");
                            continue;
                        }
                        let request = json!({
                                        "sender": NodeInfo::new(node_ip_addr, node_port + 42),
                                        "type": MsgType::Insert,
                                        "record": {
                                            "title": parts[1],
                                            "key": HashFunc(parts[1]),
                                            "replica_idx": 0
                                        }

                                      }).to_string();
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
                        
                        let request = json!({
                                        "sender": NodeInfo::new(node_ip_addr, node_port + 42),
                                        "type": MsgType::Delete,
                                        "key": HashFunc(parts[1])
                                      }).to_string();
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

                        let request = json!({
                                        "sender": NodeInfo::new(node_ip_addr, node_port + 42),
                                        "type": MsgType::Query,
                                        "key": parts[1]
                                      }).to_string();
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => println!("{}", response),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }

                    "overlay" => {
                        let request = json!({
                                        "sender": NodeInfo::new(node_ip_addr, node_port + 42),
                                        "type": MsgType::Overlay
                                      }).to_string();
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => println!("{}", response),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }

                    "depart" => {
                        let request = json!({
                                        "sender": NodeInfo::new(node_ip_addr, node_port + 42),
                                        "type": MsgType::Quit
                                      }).to_string();
                        match send_request(node_ip, node_port, &request) {
                            Ok(response) => {
                                println!("{}", response);
                                break;
                            }
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
