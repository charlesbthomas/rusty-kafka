use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

pub async fn route_kafka_message<'a>(msg: &'a BorrowedMessage<'a>) {
    if let Some(payload) = msg.payload() {
        match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(json) => {
                let event_type = json
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();

                let mut handler_found = false;

                for registration in inventory::iter::<crate::EventHandlerRegistration> {
                    if registration.event_type == event_type {
                        (registration.handler)(&json).await;
                        handler_found = true;
                        break; // Exit the loop once a handler is found
                    }
                }

                if !handler_found {
                    eprintln!("No handler found for event type: {}", event_type);
                }
            }
            Err(err) => {
                eprintln!("Failed to parse Kafka message as JSON: {}", err);
            }
        }
    } else {
        eprintln!("Received Kafka message with no payload");
    }
}
