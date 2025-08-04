use std::collections::HashMap;

use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{
    DeriveInput, Ident, Result, Token,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_str,
    punctuated::Punctuated,
    token,
};
// use virtue::{generate::Parent, prelude::*};

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

// use syn::parse::{Parse, ParseStream};
// use syn::{Ident, Result, parenthesized};

// A single item: either just an Ident, or Ident(source) if wrapped in ()
// #[derive(Debug)]
// enum Variants {
//     Direct(Ident),
//     Routed { name: Ident, source: Ident },
// }

// #[derive(Debug)]
// struct UnionInput {
//     enum_name: Ident,
//     variants: Vec<Variants>,
// }

// impl Parse for UnionInput {
//     fn parse(input: ParseStream) -> Result<Self> {
//         // First item: main identifier
//         let main: Ident = input.parse()?;
//         input.parse::<Token![,]>()?;

//         let mut rest = Vec::new();

//         while !input.is_empty() {
//             let name: Ident = input.parse()?;

//             let item = if input.peek(syn::token::Paren) {
//                 let content;
//                 parenthesized!(content in input);
//                 let source: Ident = content.parse()?;
//                 Variants::Routed { name, source }
//             } else {
//                 Variants::Direct(name)
//             };

//             rest.push(item);

//             if input.peek(Token![,]) {
//                 input.parse::<Token![,]>()?;
//             } else {
//                 break;
//             }
//         }

//         Ok(UnionInput {
//             enum_name: main,
//             variants: rest,
//         })
//     }
// }

// #[proc_macro]
// pub fn union3(item: TokenStream) -> TokenStream {
//     let input = parse_macro_input!(item as UnionInput);

//     export_to_file2("actor", "blah", &format!("{:?}", input));
//     let enum_name = input.enum_name;
//     let variants = input.variants.into_iter().map(|v| match v {
//         Variants::Direct(name) => {
//             quote! {
//                 #name(#name)
//             }
//         }
//         Variants::Routed { name, source } => {
//             quote! {
//                 #name(#name)
//             }
//         }
//     });

//     let expanded = quote! {
//         enum #enum_name {
//             #(#variants,)*
//         }
//     };

//     expanded.into()
// }

// fn export_to_file(crate_name: &str, file_name: &str, item: &TokenStream) -> bool {
//     use std::io::Write;

//     if let Ok(var) = std::env::var("CARGO_MANIFEST_DIR") {
//         let mut path = std::path::PathBuf::from(var);
//         loop {
//             {
//                 let mut path = path.clone();
//                 path.push("target");
//                 if path.exists() {
//                     path.push("generated");
//                     path.push(crate_name);
//                     if std::fs::create_dir_all(&path).is_err() {
//                         return false;
//                     }
//                     path.push(format!("{file_name}.rs"));
//                     if let Ok(mut file) = std::fs::File::create(path) {
//                         let _ = file.write_all(item.to_string().as_bytes());
//                         return true;
//                     }
//                 }
//             }
//             if let Some(parent) = path.parent() {
//                 path = parent.into();
//             } else {
//                 break;
//             }
//         }
//     }
//     false
// }

// fn export_to_file2(crate_name: &str, file_name: &str, item: &str) -> bool {
//     use std::io::Write;

//     if let Ok(var) = std::env::var("CARGO_MANIFEST_DIR") {
//         let mut path = std::path::PathBuf::from(var);
//         loop {
//             {
//                 let mut path = path.clone();
//                 path.push("target");
//                 if path.exists() {
//                     path.push("generated");
//                     path.push(crate_name);
//                     if std::fs::create_dir_all(&path).is_err() {
//                         return false;
//                     }
//                     path.push(format!("{file_name}.rs"));
//                     if let Ok(mut file) = std::fs::File::create(path) {
//                         let _ = file.write_all(item.to_string().as_bytes());
//                         return true;
//                     }
//                 }
//             }
//             if let Some(parent) = path.parent() {
//                 path = parent.into();
//             } else {
//                 break;
//             }
//         }
//     }
//     false
// }

#[proc_macro]
pub fn union(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as UnionInput);
    let enum_name = input.enum_name;
    let variants = input.variants;

    let variant_defs = variants.iter().map(|v| {
        quote! { #v(#v), }
    });

    let from_impls = variants.iter().map(|v| {
        quote! {
            impl From<#v> for #enum_name {
                fn from(value: #v) -> Self {
                    #enum_name::#v(value)
                }
            }

            impl From<#enum_name> for #v {
                fn from(value: #enum_name) -> Self {
                    match value {
                        #enum_name::#v(inner) => inner,
                        _ => panic!(concat!("Not a ", stringify!(#v))),
                    }
                }
            }
        }
    });

    let expanded = quote! {
        #[derive(bincode::Encode, bincode::Decode)]
        pub enum #enum_name {
            #(#variant_defs)*
        }

        #(#from_impls)*
    };

    TokenStream::from(expanded)
}

struct UnionInput {
    enum_name: Ident,
    variants: Punctuated<Ident, Token![,]>,
}

impl Parse for UnionInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let enum_name: Ident = input.parse()?;
        let _comma: Option<Token![,]> = input.parse().ok(); // allow optional comma
        let variants = Punctuated::parse_separated_nonempty(input)?;
        Ok(UnionInput {
            enum_name,
            variants,
        })
    }
}
