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
