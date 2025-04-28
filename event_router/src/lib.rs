use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitStr};

#[proc_macro_attribute]
pub fn event_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attribute as a literal string (e.g., "user_created")
    let event_type_literal = parse_macro_input!(attr as LitStr);

    // Parse the function the attribute is attached to
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_args = &input_fn.sig.inputs;
    let is_async = input_fn.sig.asyncness.is_some();

    let event_struct_type = match fn_args.first() {
        Some(syn::FnArg::Typed(pat)) => match &*pat.ty {
            syn::Type::Reference(r) => &r.elem,
            _other => panic!("expected &EventStruct type"),
        },
        _ => panic!("expected exactly one argument"),
    };

    let expanded = if is_async {
        quote! {
            #input_fn

            inventory::submit! {
                crate::EventHandlerRegistration {
                    event_type: #event_type_literal,
                    handler: |json: &serde_json::Value| -> std::pin::Pin<Box<dyn std::future::Future<Output=()> + Send>> {
                        let evt: #event_struct_type = match serde_json::from_value(json.clone()) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Failed to deserialize event {}: {}", #event_type_literal, e);
                                return Box::pin(async {});
                            }
                        };
                        Box::pin(async move {
                            #fn_name(&evt).await;
                        })
                    }
                }
            }
        }
    } else {
        // If the function is NOT async, we can call it directly inside an async block
        quote! {
            #input_fn
            inventory::submit! {
                crate::EventHandlerRegistration {
                    event_type: #event_type_literal,
                    handler: |json: &serde_json::Value| -> std::pin::Pin<Box<dyn std::future::Future<Output=()> + Send>> {
                        let evt: #event_struct_type = match serde_json::from_value(json.clone()) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Failed to deserialize event {}: {}", #event_type_literal, e);
                                return Box::pin(async {});
                            }
                        };
                        Box::pin(async move {
                            #fn_name(&evt);
                        })
                    }
                }
            }
        }
    };

    TokenStream::from(expanded)
}
