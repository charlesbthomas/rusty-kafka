use event_router::event_handler;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct ExampleEvent {
    foo: String,
}

#[event_handler("example_event")]
fn handle_example_event(event: &ExampleEvent) {
    println!("Handling example event: {:?}", event);
}
