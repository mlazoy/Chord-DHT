use std::net::Ipv4Addr;
use std::env;

use utils::Consistency;
use utils::get_local_ip;


mod utils;
mod node;
mod network;
mod cli;

// Bootsrap node info are globally known 
const BOOT_ADDR: Ipv4Addr = Ipv4Addr::new(0,0,0,0);  //localhost 
const API_PORT: u16 = 8000; 
const NUM_THREADS: usize = 4;

// for testing locally only

fn main() {
    println!("Entering Chord-DHT Network...");

    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} [bootstrap <k> <m> |node| cli <ip_address> <port>]", args[0]);
        return;
    }

    // create a reference for each app starting 
    let bootstrap_info= node::NodeInfo::new(
        BOOT_ADDR, 
        API_PORT); 

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
                let boot_node = node::Node::new(
                    &BOOT_ADDR,
                    Some(API_PORT),
                    Some(k),
                    Some(m),
                    None            // denotes ptr to itself
                );
                boot_node.init();
            }

        }
        "node" => {
            let node_instance = node::Node::new(
                &get_local_ip(), 
                Some(API_PORT+1),     // test this
                None, 
                None,
                Some(bootstrap_info));
            
            // let node_clone = node_instance.clone();
            // // test departure with a timer 
            // let _quit_thread = std::thread::spawn(move || {
            //     std::thread::sleep(std::time::Duration::from_secs(20)); 
            //     node_clone.quit_ring();  
            // });

            node_instance.init();
        }

        "cli" => {
            if args.len() < 4 {
                panic!("Usage: cargo run cli <node_ip> <node_port>");
            }
            let node_ip = &args[2];
            let node_port: u16 = args[3].parse().expect("Invalid port");

            println!("Connecting to node at {}:{}...", node_ip, node_port);
            cli::run_cli(node_ip, node_port);
        }
        _ => {
            eprintln!("Usage: {} [bootstrap <k> <m> |node| cli <ip_address> <port>]", args[0]);
        }
    }

}


