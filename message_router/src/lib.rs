use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

mod args;
use args::MacroArgs;

#[proc_macro]
pub fn generate_event_router(_input: TokenStream) -> TokenStream {
    let expanded = quote! {
        pub mod event_router {
            use rdkafka::message::BorrowedMessage;
            use rdkafka::Message;
            use std::collections::HashMap;
            use std::future::Future;
            use std::pin::Pin;
            use std::sync::Mutex;
            use std::sync::OnceLock;

            type EventHandler = fn(&serde_json::Value) -> Pin<Box<dyn Future<Output = ()> + Send>>;

            // Composite key for event handlers (event_type, source)
            #[derive(PartialEq, Eq, Hash, Clone, Debug)]
            struct HandlerKey {
                event_type: String,
                source: Option<String>,
            }

            impl HandlerKey {
                fn new(event_type: &str, source: Option<&str>) -> Self {
                    HandlerKey {
                        event_type: event_type.to_string(),
                        source: source.map(|s| s.to_string()),
                    }
                }
            }

            // Initialize the handler registry
            fn get_handlers() -> &'static Mutex<HashMap<HandlerKey, EventHandler>> {
                static EVENT_HANDLERS: OnceLock<Mutex<HashMap<HandlerKey, EventHandler>>> = OnceLock::new();
                EVENT_HANDLERS.get_or_init(|| {
                    Mutex::new(HashMap::new())
                })
            }

            // Register a handler at initialization time
            pub fn register_handler(event_type: &str, source: Option<&str>, handler: EventHandler) {
                let key = HandlerKey::new(event_type, source);
                let mut handlers = get_handlers().lock().unwrap();

                if handlers.contains_key(&key) {
                    // Handler already registered, ignore
                    return;
                }
                handlers.insert(key, handler);
            }

            // List all registered event handlers
            pub fn list_registered_handlers() {
                let handlers = get_handlers().lock().unwrap();
                println!("Registered event handlers:");
                if handlers.is_empty() {
                    println!("  No handlers registered");
                } else {
                    for key in handlers.keys() {
                        match &key.source {
                            Some(source) => println!("  - {} (source: {})", key.event_type, source),
                            None => println!("  - {}", key.event_type),
                        }
                    }
                }
            }

            // Extract event type and source from a JSON event
            fn extract_event_metadata(json: &serde_json::Value) -> (String, Option<String>) {
                let event_type = json
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();

                let source = json
                    .get("source")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                (event_type, source)
            }

            // Find the most appropriate handler for an event
            fn find_handler(event_type: &str, source: Option<&str>) -> Option<EventHandler> {
                let handlers = get_handlers().lock().unwrap();

                // Try source-specific handler first, then fall back to generic handler
                match source {
                    Some(source_str) => {
                        // Try a source-specific handler first
                        let source_key = HandlerKey::new(event_type, Some(source_str));

                        handlers.get(&source_key)
                            .cloned()
                            .or_else(|| {
                                // Fall back to a source-agnostic handler
                                let generic_key = HandlerKey::new(event_type, None);
                                handlers.get(&generic_key).cloned()
                            })
                    },
                    None => {
                        // No source in the event, use generic handler
                        let generic_key = HandlerKey::new(event_type, None);
                        handlers.get(&generic_key).cloned()
                    }
                }
            }

            // Log a message when no handler is found
            fn log_missing_handler(event_type: &str, source: Option<&str>) {
                match source {
                    Some(source_str) => {
                        eprintln!("No handler found for event type: {} (source: {})", event_type, source_str);
                    },
                    None => {
                        eprintln!("No handler found for event type: {}", event_type);
                    }
                }
            }

            // Parse a Kafka message payload as JSON
            fn parse_message(payload: &[u8]) -> Result<serde_json::Value, serde_json::Error> {
                serde_json::from_slice(payload)
            }

            // Main routing function
            pub async fn route_kafka_message(msg: &BorrowedMessage<'_>) {
                // Check if the message has a payload
                let payload = match msg.payload() {
                    Some(payload) => payload,
                    None => {
                        eprintln!("Received Kafka message with no payload");
                        return;
                    }
                };

                // Parse the payload as JSON
                let json = match parse_message(payload) {
                    Ok(json) => json,
                    Err(err) => {
                        eprintln!("Failed to parse Kafka message as JSON: {}", err);
                        return;
                    }
                };

                // Extract event metadata
                let (event_type, source) = extract_event_metadata(&json);
                let source_ref = source.as_deref();

                // Find an appropriate handler
                let handler = find_handler(&event_type, source_ref);

                // Execute or log missing handler
                match handler {
                    Some(handler) => {
                        handler(&json).await;
                    },
                    None => {
                        log_missing_handler(&event_type, source_ref);
                    }
                }
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn event_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attribute to get event_type and optional source
    let args = parse_macro_input!(attr as MacroArgs);
    let event_type_str = args.event_type.value();
    let source_option = args.source.map(|s| s.value());

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

    // Generate the source literal for registration
    let source_literal = match source_option {
        Some(source) => quote! { Some(#source) },
        None => quote! { None },
    };

    // Generate a unique name for the initialization function using the function name
    let init_fn_name = format!("__event_handler_init_{}", fn_name);
    let init_fn_ident = syn::Ident::new(&init_fn_name, proc_macro2::Span::call_site());

    // Generate the final code
    let expanded = quote! {
        // The original function
        #input_fn

        // Register this handler at module initialization
        #[doc(hidden)]
        #[automatically_derived]
        #[allow(non_snake_case)]
        #[::ctor::ctor]
        fn #init_fn_ident() {
            #handler_impl
            crate::event_router::register_handler(#event_type_str, #source_literal, handler);
        }
    };

    TokenStream::from(expanded)
}
