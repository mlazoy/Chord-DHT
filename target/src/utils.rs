use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha1::{Digest,Sha1};
use std::fmt;
use hex::{FromHex, ToHex};
use std::cmp::Ord;
use std::net::Ipv4Addr;

use crate::node::NodeInfo;

/* Simple function to print either success or failure messages on the console
    when running in debug mode */
pub trait DebugMsg {
    #[cfg(debug_assertions)]
    fn print_debug_msg(&self, msg: &str) {
        println!("{:?}", msg);
    }

    #[cfg(not(debug_assertions))]
    fn print_debug_msg(&self, _msg: &str) {}
}

// Blanket implementation: every type implements DebugMsg.
impl<T> DebugMsg for T {}

// type synonym for actual hash returned from SHA-1
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HashType(pub [u8; 20]); 


// just for Debugging 
impl fmt::Display for HashType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?; // Format as hexadecimal
        }
        Ok(())
    }
}

// Implement custom serialization (store as hex string)
impl Serialize for HashType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

// Implement custom deserialization (convert hex string back to bytes)
impl<'de> Deserialize<'de> for HashType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_str = String::deserialize(deserializer)?;
        let bytes = <[u8; 20]>::from_hex(hex_str).map_err(serde::de::Error::custom)?;
        Ok(HashType(bytes))
    }
}

impl HashType {
    /// Convert `HashType` to a hex string
    pub fn to_hex(&self) -> String {
        self.0.encode_hex::<String>()
    }

    /// Convert a hex string to `HashType`
    pub fn from_hex(hex_str: &str) -> Result<Self, hex::FromHexError> {
        <[u8; 20]>::from_hex(hex_str).map(HashType)
    }
}

/*  Hash function used to hash records and ip-port combos
    Both peer nodes and bootstrap use this method */
pub fn HashFunc(input: &str) -> HashType {
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    HashType(result.into()) 
}

#[derive(Debug, Clone)]
pub struct Item {
    pub title : String, 
    pub key : HashType
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Consistency  {
    Quorum,
    Chain,  
    Eventual,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MsgType {
    Join,
    Setup,
    Update,         /* next field is one of the following:
                    1. prev_info
                    2. succ_info
                    3. records */

    AckJoin,
    AckSetup,
    Quit,
    Insert,
    Delete,
    Query,
    RequestRecords,
}

/* Create a wrapper for Node and Bootstrap so we can call
    common (node) methods from both objects */
pub struct Peer<'a> {
    node: &'a dyn PeerTrait, 
}

pub trait PeerTrait {
    fn get_ip(&self) -> Ipv4Addr;
    fn get_port(&self) -> u16;
    fn get_status(&self) -> bool;
    fn set_status(&mut self, status:bool);
    fn get_id(&self) -> Option<HashType>;
    fn set_id(&mut self, id:HashType);
    fn set_prev(&mut self, prev:Option<NodeInfo>);
    fn set_succ(&mut self, succ:Option<NodeInfo>);
    fn get_prev(&self) -> Option<NodeInfo>;
    fn get_succ(&self) -> Option<NodeInfo>;
    fn get_repl_factor(&self) -> usize;
    fn set_repl_factor(&mut self, k:usize);
    fn get_repl_mode(&self) -> Consistency;
    fn set_repl_mode(&mut self, mode:Consistency);
    fn insert(&mut self, key:HashType, value:Item);
    fn delete(&mut self, key:HashType);
    fn query(&self, key:HashType) -> Option<Vec<Item>> ; // returns list of values for special case '*'

    // TODO! add all common methods here!
}

