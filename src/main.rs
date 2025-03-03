use std::net::Ipv4Addr;
use std::env;

use utils::HashType;


mod utils;
mod node;
mod network;

const BOOT_ADDR: Ipv4Addr = Ipv4Addr::new(0,0,0,0);  //localhost 
const BOOT_PORT: u16 = 8000; 

// for testing locally only
const NODE_ADDR:Ipv4Addr = Ipv4Addr::new(0,0,0,0);  //localhost 

fn main() {
    println!("Starting CHORD DHT...");

    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} [bootstrap|node]", args[0]);
        return;
    }

    match args[1].as_str() {
        "bootstrap" => {  
            //
        }

        "node" => {
            let mut node_instance = node::Node::new(NODE_ADDR, None, None, None);
            node_instance.join_ring();
        }
        _ => {
            eprintln!("Invalid argument: {}", args[1]);
        }
    }

}

