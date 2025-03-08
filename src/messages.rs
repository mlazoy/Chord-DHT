use crate::{node::NodeInfo, utils::HashType, utils::Item};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    r#type:MsgType,
    client: Option<NodeInfo>,
    data: MsgData
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]  // Enables JSON with type-discriminated serialization
pub enum MsgData {
    Join { id: String },
    FwJoin { new_node: NodeInfo },
    AckJoin { prev_info: Option<NodeInfo>, succ_info : Option<NodeInfo>, new_items:Vec<Item>  },
    Quit { id: String },
    Update { prev_info: Option<NodeInfo>, succ_info: Option<NodeInfo> },
    Insert { key: String, value: String },
    FwInsert { key: HashType, value: String },
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

impl Message {
    pub fn new(r#type:MsgType, client:Option<&NodeInfo>, data:&MsgData) -> Self {
        Message {
            r#type,
            client: client.cloned(),
            data: data.clone()
        }
    }

    pub fn extract_client(&self) -> NodeInfo {
        if self.client.is_none() { panic!("No client provided"); }
        else { self.client.unwrap() }
    }

    pub fn extract_type(&self) -> MsgType {
        self.r#type
    }

    pub fn extract_data(&self) -> MsgData {
        self.data.clone()
    }

}


