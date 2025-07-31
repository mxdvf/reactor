use std::collections::HashMap;

use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{DeriveInput, parse_macro_input, parse_str};
use virtue::{generate::Parent, prelude::*};

#[proc_macro_derive(DefaultPrio)]
pub fn auto_default_priority(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    TokenStream::from(quote! {
        impl reactor_actor::HasPriority for #name {}
    })
}

#[proc_macro_derive(Msg)]
pub fn auto_msg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    TokenStream::from(quote! {
        impl reactor_actor::Msg for #name {}
    })
}

#[proc_macro_derive(SubDecoder)]
pub fn sub_decoders(input: TokenStream) -> TokenStream {
    let generated: TokenStream =
        sub_decoders_inner(input).unwrap_or_else(|e| e.into_token_stream());
    generated
}

fn sub_decoders_inner(input: proc_macro::TokenStream) -> Result<TokenStream> {
    let parse: Parse = virtue::parse::Parse::new(input.clone())?;
    let (mut generator, _attributes, body) = parse.into_generator();
    let mut name_to_type: HashMap<String, String> = HashMap::new();
    let enum_name = generator.name().clone().to_string();

    match body {
        Body::Enum(body) => {
            for variant in body.variants {
                let field_type = if let Some(variant_fields) = variant.fields {
                    if let Fields::Tuple(fields) = &variant_fields {
                        let tokens: TokenStream = fields[0].r#type.iter().cloned().collect();
                        Ok(tokens.to_string())
                    } else {
                        Err(virtue::Error::Custom {
                            error: "This macro is valid only for Enums".to_string(),
                            span: None,
                        })
                    }?
                } else {
                    variant.name.to_string()
                };
                generator
                    .impl_for("From")
                    .with_trait_generics([field_type.clone()])
                    .generate_fn("from")
                    .with_arg("v", field_type.clone())
                    .with_return_type("Self")
                    .body(|b| {
                        b.push_parsed(format!("Self::{}(v)", variant.name))?;
                        Ok(())
                    })?;

                name_to_type.insert(variant.name.to_string(), field_type);
            }
        }
        _ => Err(virtue::Error::Custom {
            error: "This macro is valid only for Enums".to_string(),
            span: None,
        })?,
    }

    let impls: TokenStream = generator.finish()?;
    let map: TokenStream = generate_decoder_provider(enum_name.clone(), name_to_type);
    let sum = impls.into_iter().chain(map).collect();
    export_to_file("actor", &enum_name, &sum);
    Ok(sum)
}

fn generate_decoder_provider(
    enum_name: String,
    name_to_type: std::collections::HashMap<String, String>,
) -> TokenStream {
    let enum_ident: syn::Type = parse_str(&enum_name).unwrap();

    let inserts = name_to_type.into_iter().map(|(name, ty)| {
        let ty: syn::Type = parse_str(&ty).unwrap();
        quote! {
            if name == #name{
                fn decoder_cons() -> Box<dyn tokio_util::codec::Decoder<Item = #enum_ident, Error = std::io::Error> + Sync + Send> {
                    Box::new(BincodeSubdecoder::<#ty, #enum_ident>::default())
                }
                fn any_to_m(msg: Box<dyn std::any::Any>) -> #enum_ident {
                    let msg = msg.downcast::<#ty>().unwrap();
                    (*msg).into()
                }
                return Some(reactor_actor::DecoderProvider{
                    decoder_cons,
                    any_to_m
                })
            }
        }
    });

    let function_ident: syn::Type = parse_str(&format!("{enum_name}_DECODER_MAP")).unwrap();
    let expanded = quote! {
        fn #function_ident(name: &str) -> Option<reactor_actor::DecoderProvider<#enum_ident>>{
            #(#inserts)*
            None
        }
    };

    TokenStream::from(expanded)
}

fn export_to_file(crate_name: &str, file_name: &str, item: &TokenStream) -> bool {
    use std::io::Write;

    if let Ok(var) = std::env::var("CARGO_MANIFEST_DIR") {
        let mut path = std::path::PathBuf::from(var);
        loop {
            {
                let mut path = path.clone();
                path.push("target");
                if path.exists() {
                    path.push("generated");
                    path.push(crate_name);
                    if std::fs::create_dir_all(&path).is_err() {
                        return false;
                    }
                    path.push(format!("{file_name}.rs"));
                    if let Ok(mut file) = std::fs::File::create(path) {
                        let _ = file.write_all(item.to_string().as_bytes());
                        return true;
                    }
                }
            }
            if let Some(parent) = path.parent() {
                path = parent.into();
            } else {
                break;
            }
        }
    }
    false
}
