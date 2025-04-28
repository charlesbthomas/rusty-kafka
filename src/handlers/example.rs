use event_router::event_handler;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ExampleEvent {
    foo: String,
}

#[event_handler("example_event")]
pub async fn handle_example_event(event: &ExampleEvent) {
    println!("Handling example event: {:?}", event);
    // You would put your event handling logic here
    // Since this is an async function, you can await other async functions
    // like database calls, HTTP requests, etc.
}
