use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(DefaultPrio)]
pub fn auto_default_priority(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    TokenStream::from(quote! {
        impl HasPriority for #name {}
    })
}

#[proc_macro_derive(Msg)]
pub fn auto_msg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    TokenStream::from(quote! {
        impl Msg for #name {}
    })
}

#[proc_macro_attribute]
pub fn sub_decoders(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::ItemEnum);
    let enum_name = &input.ident;

    let mut struct_defs = vec![];
    let mut decode_impls = vec![];

    for variant in &input.variants {
        let variant_name = &variant.ident;
        let struct_name = syn::Ident::new(
            &format!("{}BincodeCodec", variant_name),
            variant_name.span(),
        );

        struct_defs.push(quote! {
            pub struct #struct_name{
                config: bincode::config::Configuration,
                length_codec: tokio_util::codec::LengthDelimitedCodec,
            }
        });
        decode_impls.push(quote! {
            impl tokio_util::codec::Decoder for #struct_name {
                type Item = #enum_name;
                type Error = std::io::Error;
                fn decode(&mut self, src: &mut tokio_util::bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
                    let frame = match self.length_codec.decode(src).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "failed to decode length-delimited data",
                        )
                    })? {
                        Some(frame) => frame,
                        None => return Ok(None),
                    };
                    let (message, _) = bincode::decode_from_slice(&frame, self.config).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "failed to decode length-delimited data",
                        )
                    })?;
                    Ok(Some(message.into()))
                }
            }
      });
    }

    // $(
    //     // #[derive(Clone)]
    //     $vis struct $variant {
    //         // config: bincode::config::Configuration,
    //         // length_codec: tokio_util::codec::LengthDelimitedCodec,
    //         // phantom_data: PhantomData<T>,
    //     }
    // )*

    // // $(
    // //     impl crate::tokio_util::codec::Decoder for $name {
    // //         fn from(v: $ty) -> Self {
    // //             $name::$variant(v)
    // //         }
    // //     }
    // // )*

    // ::lazy_static::lazy_static! {
    //     static ref DECODER_MAP: std::collections::HashMap<&'static str, $name> = {
    //         let mut m = std::collections::HashMap::new();
    //         // $(
    //             // m.insert(stringify!($variant), $crate::codec::BincodeCodec::<$crate::$name::$variant>::default());
    //             // m.insert(stringify!($variant), "blah");
    //         // )*
    //         m
    //     };
    // }
    let expanded = quote! {
        #input

        #(#struct_defs)*
        #(#decode_impls)*
    };

    TokenStream::from(expanded)
}

use virtue::{generate::Parent, prelude::*};

#[proc_macro_attribute]
pub fn sub_decoders2(_attr: TokenStream, item: TokenStream) -> TokenStream {
    sub_decoders_inner(item).unwrap_or_else(|e| e.into_token_stream())
}

fn sub_decoders_inner(input: proc_macro::TokenStream) -> Result<TokenStream> {
    let parse: Parse = virtue::parse::Parse::new(input)?;
    let (mut generator, _attributes, body) = parse.into_generator();
    // let crate_name = "::reactor_actor".to_string();

    match body {
        Body::Enum(body) => {
            let enum_name = generator.name().to_string();
            for variant in body.variants {
                let field_type = if let Fields::Tuple(fields) = &variant.fields.unwrap() {
                    let tokens: TokenStream = fields[0].r#type.iter().cloned().collect();
                    Ok(tokens.to_string())
                } else {
                    Err(virtue::Error::Custom {
                        error: "This macro is valid only for Enums".to_string(),
                        span: None,
                    })
                }?;
                generator
                    .impl_for("From")
                    .with_trait_generics([field_type.clone()])
                    .generate_fn("from")
                    .with_arg("v", field_type)
                    .with_return_type("Self")
                    .body(|b| {
                        b.push_parsed(format!("Self::{}(v)", variant.name))?;
                        Ok(())
                    })?;
            }
        }
        _ => Err(virtue::Error::Custom {
            error: "This macro is valid only for Enums".to_string(),
            span: None,
        })?,
    }

    generator.append(*(StreamBuilder::new().push(parse).unwrap()));
    generator.export_to_file("actor", "SubDecoders");
    generator.finish()
}
