use const_fnv1a_hash::fnv1a_hash_str_32;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

pub trait Message: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    const NAME: &str;
    const HASH: i32 = fnv1a_hash_str_32(Self::NAME) as i32;
}

#[derive(Debug, Clone)]
pub struct RawMessage {
    /// Unique identifier
    pub id: Uuid,
    /// Event type name
    pub name: String,
    /// Hash of name for lookup performance
    pub hash: i32,
    /// Serialized payload
    pub payload: serde_json::Value,
    /// The number of times processing this message have been attempted
    pub attempted: i32,
}
