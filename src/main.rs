#![allow(dead_code, non_snake_case, unused_imports)]

use std::net::Ipv4Addr;
use std::env;

use utils::Consistency;
use utils::get_local_ip;


mod utils;
mod node;
mod network;
mod cli;
mod messages;

// Bootsrap node info are globally known 
//const BOOT_ADDR: Ipv4Addr = Ipv4Addr::new(0,0,0,0);  //localhost 
const BOOT_ADDR: Ipv4Addr = Ipv4Addr::new(10,0,24,44);  
const API_PORT: u16 = 8000; 
const NUM_THREADS: usize = 8;

// for testing locally only

#[tokio::main]
async fn main() {
    println!("Entering Chord-DHT Network...");

    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} [bootstrap <k> <m> |node <n> | cli <command> [args]]", args[0]);
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
                let k: u8 = match args[2].parse(){
                    Ok(val) => val,
                    Err(_) => panic!("Invalid parameter for replication factor: k\n")
                };
                if k < 1 { panic!("Invalid k. Must be > 0.\n"); }
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
                    Some(k-1),
                    Some(m),
                    None            // denotes ptr to itself
                );
                boot_node.init().await;
            }

        }
        "node" => {
            if args.len() < 3 {
                panic!("Usage: {} node <n>", args[0]);
            } else {
                let n: u16 = match args[2].parse(){
                    Ok(val) => val,
                    Err(_) => panic!("Invalid parameter for n.\n")
                };
                
                let node_instance = node::Node::new(
                    &get_local_ip(), 
                    Some(API_PORT+n),     // offset 
                    None, 
                    None,
                    Some(bootstrap_info));
            

                node_instance.init().await;
            }
        }

        "cli" => {
            cli::run_cli();
        }
        _ => {
            eprintln!("Usage: {} [bootstrap <k> <m> |node| cli <command> [args]]", args[0]);
        }
    }

}


