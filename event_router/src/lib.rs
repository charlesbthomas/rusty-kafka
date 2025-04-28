use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitStr};

// The main macro that generates the router function
#[proc_macro]
pub fn generate_router(_input: TokenStream) -> TokenStream {
    let expanded = quote! {
        pub mod router {
            use rdkafka::message::BorrowedMessage;
            use rdkafka::Message;
            use std::collections::HashMap;
            use std::future::Future;
            use std::pin::Pin;
            use std::sync::Mutex;
            use std::sync::OnceLock;

            type EventHandler = fn(&serde_json::Value) -> Pin<Box<dyn Future<Output = ()> + Send>>;

            // Initialize the handler registry
            fn get_handlers() -> &'static Mutex<HashMap<&'static str, EventHandler>> {
                static EVENT_HANDLERS: OnceLock<Mutex<HashMap<&'static str, EventHandler>>> = OnceLock::new();
                EVENT_HANDLERS.get_or_init(|| {
                    Mutex::new(HashMap::new())
                })
            }

            // Register a handler at initialization time
            pub fn register_handler(event_type: &'static str, handler: EventHandler) {
                let mut handlers = get_handlers().lock().unwrap();
                if handlers.contains_key(event_type) {
                    // Handler already registered, ignore
                    return;
                }
                handlers.insert(event_type, handler);
            }

            // List all registered event handlers
            pub fn list_registered_handlers() {
                let handlers = get_handlers().lock().unwrap();
                println!("Registered event handlers:");
                if handlers.is_empty() {
                    println!("  No handlers registered");
                } else {
                    for event_type in handlers.keys() {
                        println!("  - {}", event_type);
                    }
                }
            }

            pub async fn route_kafka_message(msg: &BorrowedMessage<'_>) {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<serde_json::Value>(payload) {
                        Ok(json) => {
                            let event_type = json
                                .get("event_type")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default();

                            let handler = {
                                let handlers = get_handlers().lock().unwrap();
                                handlers.get(event_type).cloned()
                            };

                            if let Some(handler) = handler {
                                handler(&json).await;
                            } else {
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
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn event_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attribute as a literal string (e.g., "user_created")
    let event_type_literal = parse_macro_input!(attr as LitStr);
    let event_type_str = event_type_literal.value();

    // Parse the function the attribute is attached to
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_args = &input_fn.sig.inputs;
    let is_async = input_fn.sig.asyncness.is_some();

    // Extract the event struct type from the function signature
    let event_struct_type = match fn_args.first() {
        Some(syn::FnArg::Typed(pat)) => match &*pat.ty {
            syn::Type::Reference(r) => &r.elem,
            _other => panic!("expected &EventStruct type"),
        },
        _ => panic!("expected exactly one argument"),
    };

    // Create the handler function based on whether the original function is async
    let handler_impl = if is_async {
        quote! {
            let handler = |json: &serde_json::Value| -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                let evt: #event_struct_type = match serde_json::from_value(json.clone()) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Failed to deserialize event {}: {}", #event_type_str, e);
                        return Box::pin(async {});
                    }
                };
                Box::pin(async move {
                    #fn_name(&evt).await;
                })
            };
        }
    } else {
        quote! {
            let handler = |json: &serde_json::Value| -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                let evt: #event_struct_type = match serde_json::from_value(json.clone()) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Failed to deserialize event {}: {}", #event_type_str, e);
                        return Box::pin(async {});
                    }
                };
                Box::pin(async move {
                    #fn_name(&evt);
                })
            };
        }
    };

    // Generate the final code
    let expanded = quote! {
        // The original function
        #input_fn

        // Register this handler at module initialization
        #[doc(hidden)]
        #[automatically_derived]
        #[allow(non_snake_case)]
        #[::ctor::ctor]
        fn __event_handler_init() {
            #handler_impl
            crate::router::register_handler(#event_type_str, handler);
        }
    };

    TokenStream::from(expanded)
}
