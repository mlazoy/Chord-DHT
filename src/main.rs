use std::net::Ipv4Addr;
use std::env;

use utils::Consistency;
use utils::get_local_ip;


mod utils;
mod node;
mod network;

// Bootsrap node info are globally known 
const BOOT_ADDR: Ipv4Addr = Ipv4Addr::new(0,0,0,0);  //localhost 
const BOOT_PORT: u16 = 8000; 
const NUM_THREADS: usize = 4;

// for testing locally only

fn main() {
    println!("Entering Chord-DHT Network...");

    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} [bootstrap <k> <m> |node]", args[0]);
        return;
    }

    // create a reference for each app starting 
    let bootstrap_info= node::NodeInfo::new(
        BOOT_ADDR, 
        BOOT_PORT, 
        true);

    match args[1].as_str() {
        "bootstrap" => {
            if args.len() < 4 {
                panic!("Usage: {} bootstrap <k> <m>", args[0]);
            } else {
                let k: usize = match args[2].parse(){
                    Ok(val) => val,
                    Err(_) => panic!("Invalid parameter for replication factor: k\n")
                };
                let m_code: usize = match args[3].parse() {
                    Ok(val) => val,
                    Err(_) => panic!("Invalid parameter for replication mode: m\n 
                                        <m> = \t\t [0 -> Eventual | 1 -> Chain | 2 -> Quorum]")
                };
                let m = match m_code {
                    0 => Consistency::Eventual,
                    1 => Consistency::Chain,
                    2 => Consistency::Quorum,
                    _ => panic!("Invalid parameter for replication mode: m\n 
                                <m> = \t\t [0 -> Eventual | 1 -> Chain | 2 -> Quorum]")
                };
                let mut boot_node = node::Node::new(
                    &BOOT_ADDR,
                    Some(BOOT_PORT),
                    Some(k),
                    Some(m),
                    None            // denotes ptr to itself
                );
                boot_node.init();
            }

        }
        "node" => {
            let mut node_instance = node::Node::new(
                &get_local_ip(), 
                Some(8001), 
                None, 
                None,
                Some(bootstrap_info));
                node_instance.init();
        }
        _ => {
            eprintln!("Usage: {} [bootstrap <k> <m> |node]", args[0]);
        }
    }

}


