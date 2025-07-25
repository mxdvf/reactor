use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(MsgWithDefaultPriority)]
pub fn auto_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl HasPriority for #name {}
        impl Msg for #name {}
    };

    TokenStream::from(expanded)
}
