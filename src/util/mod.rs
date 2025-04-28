// Utility modules will go here
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct EventHeader {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event<T> {
    pub event_id: String,
    pub event_type: String,
    pub source: String,
    pub payload: T,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

impl<T> Event<T> {
    // pub fn new(event_type: &str, source: &str, payload: T) -> Self {
    //     Event {
    //         event_id: Uuid::new_v4().to_string(),
    //         event_type: event_type.to_string(),
    //         timestamp: None,
    //         source: source.to_string(),
    //         payload,
    //     }
    // }

    // pub fn with_timestamp(mut self, timestamp: &str) -> Self {
    //     self.timestamp = Some(timestamp.to_string());
    //     self
    // }
    //
    // pub fn with_source(mut self, source: &str) -> Self {
    //     self.source = source.to_string();
    //     self
    // }
}

