use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha1::{Digest,Sha1};
use std::fmt;
use std::net::{Ipv4Addr, UdpSocket};
use hex::{FromHex, ToHex};
use std::cmp::Ord;

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
    pub key : HashType,
    pub replica_idx : usize 
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Consistency  {
    Eventual,
    Chain,
    Quorum
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MsgType {
    Join,
    Update,         /* next field is one of the following:
                    1. prev_info
                    2. succ_info
                    3. records */

    AckJoin,
    Quit,
    Insert,
    Delete,
    Query,
    Overlay,        // retrieve topology or size
    Success
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


