use crate::util::Event;
use event_router::event_handler;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ExampleEvent2Payload {
    foo: String,
    lina_is_cool: String,
}

#[event_handler(event_type = "example_event_2", source = "example_source")]
pub fn handle_example_event(event: &Event<ExampleEvent2Payload>) {
    println!("Handling example event 2: {:?}", event);
    // You would put your event handling logic here
    // Since this is an async function, you can await other async functions
    // like database calls, HTTP requests, etc.
}
