use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha1::{Digest,Sha1};
use std::fmt;
use std::net::{Ipv4Addr, UdpSocket};
use hex::{FromHex, ToHex};
use std::cmp::Ord;
use num_traits::Bounded;

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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

impl fmt::Debug for HashType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // Reuse Display formatting
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

impl Bounded for HashType {
    fn min_value() -> Self {
        HashType([0u8; 20])
    }

    fn max_value() -> Self {
        HashType([0xFF; 20])
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

// wrap ip and port in a single string and call global hashing function
pub fn HashIP(ip_addr: Ipv4Addr, port: u16) -> HashType { 
    // extract only numbers from ip
    let ip_numeric = ip_addr.octets().iter().map(|n| n.to_string()).collect::<String>(); 
    // concatenate result with port
    let input = ip_numeric + &port.to_string();
    HashFunc(&input)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub title : String, 
    pub value : String,
    pub replica_idx : u8,
    // used for Chain replication to block dirty tail reads
    pub pending: bool,      
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Consistency  {
    Eventual,
    Chain,
    Quorum
}

pub fn get_local_ip() -> Ipv4Addr {
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind UDP socket");
    socket.connect("8.8.8.8:80").expect("Failed to connect to external server");
    if let Ok(local_addr) = socket.local_addr() {
        if let std::net::IpAddr::V4(ipv4) = local_addr.ip() {
            return ipv4;
        }
    }
    Ipv4Addr::new(127, 0, 0, 1) // Fallback to loopback if something fails
}

pub fn format_overlay_msg(ring_list: &Vec<NodeInfo>) -> String {
    let mut result = String::from("***************\nRING OVERLAY🔗\n***************\n"); 
    // sort just to start from smallest ID 
    // -- TODO! do we need this ?
    //ring_list.sort_by_key(|node| node.get_id());

    for peer in ring_list.iter() {
        result.push_str(&format!(
            "(nodeID:{}, IP:{}:{}) ↔️ \n ", peer.get_id(), peer.get_ip(), peer.get_port()));
    }
    result.push_str("🔄");
    result
}

pub fn format_queryall_msg(items: &Vec<Item>) -> String {
    let mut result = String::from("****************\nALL RECORDS⭐\n****************\n"); 
    for item in items.iter() {
        if item.title == "__nodeID__" {
            result.push_str(&format!("\n📋Node: {} \n", item.value));
        } else {
        result.push_str(&format!("(🔑{} : 🔒{})\n", item.title, item.value));
        }
    }
    result
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Range<T> {
    lower: T,
    upper: T,
    lc: bool, // lower is included
    uc: bool, // upper is included
}

impl<T> Range<T>
where
    T: PartialOrd + PartialEq + Copy,
{
    pub fn new(lower: T, upper: T, lc: bool, uc: bool) -> Self {
        Range { lower, upper, lc, uc }
    }

    pub fn in_range(&self, number: T) -> bool {
        // Check if number equals lower and lower is inclusive,
        // or equals upper and upper is inclusive,
        // or lies strictly between lower and upper.
        (self.lc && number == self.lower)
            || (self.uc && number == self.upper)
            || (self.lower < number && number < self.upper)
    }
}

#[derive(Debug, Clone , Serialize, Deserialize)]
pub struct UnionRange<T> {
    replication_vector: Vec<Range<T>>,
}

impl<T> UnionRange<T>
where
    T: PartialOrd + PartialEq + Copy 
{

    pub fn new() -> Self {
        UnionRange {
            replication_vector: Vec::new()
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Range<T>> {
        self.replication_vector.iter()
    }

    pub fn insert(&mut self, range: Range<T>) {
        self.replication_vector.push(range);
    }

    pub fn pop_head(&mut self) {
        self.replication_vector.remove(0);
    }

    pub fn pop_tail(&mut self) {
        self.replication_vector.pop();
    }

    pub fn get_size(&self) -> usize {
        self.replication_vector.len()
    }

    pub fn get_my_range(&self) -> Option<&Range<T>> {
        self.replication_vector.last()
    }

    pub fn is_subset(&self, element: T ) -> i16 {
        for (i, set) in self.replication_vector.iter().enumerate(){
            if set.lower < set.upper { // normal case
                if set.in_range(element) {
                    return i as i16;
                }
            } else { // wrap-around set
                if element > set.lower || element <= set.upper { // wrap-around case
                    return i as i16;
                }
            }
        }
        -1
    }
}
   


