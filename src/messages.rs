use std::fmt;
use crate::{node::{NodeInfo,ReplicationConfig}, utils::HashType, utils::Item};

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
    Reply
} 

impl fmt::Display for MsgType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self) // Print the enum variant name
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    r#type:MsgType,
    client: Option<NodeInfo>,
    data: MsgData
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Message [ Type: {}", self.r#type)?;

        if let Some(client) = &self.client {
            write!(f, ", Client: {}", client)?;
        }

        write!(f, ", Data: {} ]", self.data)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]  // Enables JSON with type-discriminated serialization
pub enum MsgData {
    Join { id: String },
    FwJoin { new_node: NodeInfo },
    AckJoin { prev_info: Option<NodeInfo>, succ_info : Option<NodeInfo>, new_items:Vec<Item>, replica_config: ReplicationConfig  },
    Quit { id: String },
    Update { prev_info: Option<NodeInfo>, succ_info: Option<NodeInfo>, new_items: Option<Vec<Item>> },
    Insert { key: String, value: String },
    FwInsert { key: String, value: String, replica:i16, forward_back:bool },
    AckInsert {key : HashType },
    Delete {key : String },
    FwDelete { key: HashType, forward_back:bool },
    AckDelete { key: HashType },
    Query { key: String },
    FwQuery {key : HashType, forward_tail: bool },
    QueryAll { },
    FwQueryAll { record_list: Vec<Item>, header: HashType },
    Overlay { },
    FwOverlay { peers: Vec<NodeInfo> },
    Reply { reply: String }
}

impl fmt::Display for MsgData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MsgData::Join { id } => write!(f, "{}", id),
            MsgData::FwJoin { new_node } => write!(f, "{}", new_node),
            MsgData::AckJoin { prev_info, succ_info, new_items, replica_config } => 
                write!(f, "{:?}, {:?}, {:?}, {:?}", prev_info, succ_info, new_items, replica_config),
            MsgData::Quit { id } => write!(f, "{}", id),
            MsgData::Update { prev_info, succ_info, new_items } => 
                write!(f, "{:?}, {:?}, {:?}", prev_info, succ_info, new_items),
            MsgData::Insert { key, value } => write!(f, "{}, {}", key, value),
            MsgData::FwInsert { key, value, replica, forward_back } => 
                write!(f, "{}, {}, {}, {}", key, value, replica, forward_back),
            MsgData::AckInsert { key } => write!(f, "{}", key),
            MsgData::Delete { key } => write!(f, "{}", key),
            MsgData::FwDelete { key, forward_back } => write!(f, "{}, {}", key, forward_back),
            MsgData::AckDelete { key } => write!(f, "{}", key),
            MsgData::Query { key } => write!(f, "{}", key),
            MsgData::FwQuery { key, forward_tail } => write!(f, "{}, {}", key, forward_tail),
            MsgData::QueryAll {} => write!(f, ""),
            MsgData::FwQueryAll { record_list, header } => write!(f, "{:?}, {}", record_list, header),
            MsgData::Overlay {} => write!(f, ""),
            MsgData::FwOverlay { peers } => write!(f, "{:?}", peers),
            MsgData::Reply { reply } => write!(f, "{}", reply),
        }
    }
}

impl Message {
    pub fn new(r#type:MsgType, client:Option<&NodeInfo>, data:&MsgData) -> Self {
        Message {
            r#type,
            client: client.cloned(),
            data: data.clone()
        }
    }

    pub fn extract_client(&self) -> Option<&NodeInfo> {
        self.client.as_ref()
    }

    pub fn extract_type(&self) -> MsgType {
        self.r#type
    }

    pub fn extract_data(&self) -> MsgData {
        self.data.clone()
    }

}


