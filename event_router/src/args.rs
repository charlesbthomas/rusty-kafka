use syn::{
    parse::Parse, parse::ParseStream, Ident, LitStr, Result, Token,
};

pub struct MacroArgs {
    pub event_type: LitStr,
    pub source: Option<LitStr>,
}

impl Parse for MacroArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut event_type = None;
        let mut source = None;

        // Parse the initial argument which could be either a positional string literal
        // or a named argument with "event_type = "
        if input.peek(LitStr) {
            event_type = Some(input.parse::<LitStr>()?);
        } else if input.peek(Ident) {
            let ident = input.parse::<Ident>()?;
            if ident == "event_type" {
                input.parse::<Token![=]>()?;
                event_type = Some(input.parse::<LitStr>()?);
            } else if ident == "source" {
                input.parse::<Token![=]>()?;
                source = Some(input.parse::<LitStr>()?);
            } else {
                return Err(syn::Error::new_spanned(
                    ident,
                    "Expected `event_type` or `source` parameter",
                ));
            }
        }

        // Parse additional named arguments if any
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;

            if input.peek(Ident) {
                let ident = input.parse::<Ident>()?;

                if ident == "event_type" && event_type.is_none() {
                    input.parse::<Token![=]>()?;
                    event_type = Some(input.parse::<LitStr>()?);
                } else if ident == "source" && source.is_none() {
                    input.parse::<Token![=]>()?;
                    source = Some(input.parse::<LitStr>()?);
                } else if ident == "event_type" {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "Duplicate `event_type` parameter",
                    ));
                } else if ident == "source" {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "Duplicate `source` parameter",
                    ));
                } else {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "Expected `event_type` or `source` parameter",
                    ));
                }
            }
        }

        // Ensure event_type is provided
        let event_type = event_type.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "Missing required parameter `event_type`",
            )
        })?;

        Ok(MacroArgs { event_type, source })
    }
}