use std::fmt;
use crate::{node::{NodeInfo,ReplicationConfig}, utils::HashType, utils::Item, utils::Range};

use serde::{Deserialize,Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MsgType {
    Join,
    FwJoin,
    AckJoin,
    Update,
    Quit,
    Insert,
    FwInsert,
    AckInsert,
    Delete,
    FwDelete,
    AckDelete,
    Query,
    FwQuery,
    QueryAll,
    FwQueryAll,
    Overlay,
    FwOverlay,
    Reply,
    Relocate
} 

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    size: usize,                                // used to create stroing buffer of appropriate size
    r#type:MsgType,
    client: Option<NodeInfo>,
    data: MsgData
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]  // Enables JSON with type-discriminated serialization
pub enum MsgData {
    Join { id: String },
    FwJoin { new_node: NodeInfo },
    AckJoin { prev_info: Option<NodeInfo>, succ_info : Option<NodeInfo>, new_items:Vec<Item>, replica_config: ReplicationConfig  },
    Quit { id: String },
    Update { prev_info: Option<NodeInfo>, succ_info: Option<NodeInfo> },
    Insert { key: String, value: String },
    FwInsert { key: String, value: String, replica:i16, forward_back:bool },
    AckInsert {key : HashType },
    Delete {key : String },
    FwDelete { key: HashType, forward_back:bool },
    AckDelete { key: HashType },
    Query { key: String },
    FwQuery {key : HashType },
    QueryAll { },
    FwQueryAll { record_list: Vec<Item>, header: HashType },
    Overlay { },
    FwOverlay { peers: Vec<NodeInfo> },
    Reply { reply: String },
    Relocate { k_remaining:u8, inc: bool, new_copies: Option<Vec<Item>>, range: Option<Range<HashType>> } 
}

impl Message {
    pub fn new(r#type:MsgType, client:Option<&NodeInfo>, data:&MsgData) -> Self {
        let msg = Message {
                            size: 0,                // stub fix later
                            r#type,
                            client: client.cloned(),
                            data: data.clone()
                        };
        let actual_size = serde_json::to_string(&msg)
        .map(|s| s.len())
        .unwrap_or(0);

        // Return a new instance with the correct size
        Message { size:actual_size, ..msg }
    }

    pub fn extract_client(&self) -> Option<&NodeInfo> {
        self.client.as_ref()
    }

    pub fn extract_type(&self) -> MsgType {
        self.r#type
    }

    pub fn extract_size(&self) -> usize {
        self.size
    }

    pub fn extract_data(&self) -> MsgData {
        self.data.clone()
    }

}

impl fmt::Display for MsgType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self) // Print the enum variant name
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Message [ Type: {}", self.r#type)?;

        if let Some(client) = &self.client {
            write!(f, ", Client: {}", client)?;
        }

        write!(f, ", Data: {:?} ]", self.data)
    }
}


