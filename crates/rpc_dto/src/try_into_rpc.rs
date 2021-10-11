use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_error::{abort, OptionExt};
use quote::quote;
use syn::{parse_quote, spanned::Spanned};

//TODO: unify structure & analysis with try_from_rpc
pub struct TryIntoRpc {
    ident: syn::Ident,
    rpc: syn::Path,
    details: Details,
}

pub enum Details {
    Enum {
        tag: syn::Path,
        tag_rpc: syn::Path,
        variants: Vec<Variant>,
    },
    Transparent {
        field: Box<syn::Field>,
    },
    Struct(Struct),
}

pub struct Struct {
    fields: syn::Fields,
}

pub struct Variant {
    ident: syn::Ident,
    rpc_name: syn::Ident,
    fields: syn::Fields,
    into_boxed: bool,
}

pub fn analysis(input: syn::DeriveInput) -> TryIntoRpc {
    let rpc_path = parse_quote!(rpc);
    let tag_path = parse_quote!(tag);
    let tag_rpc_path = parse_quote!(tag_rpc);
    let rename_path = parse_quote!(rename);
    let transparent_path = parse_quote!(transparent);
    let into_boxed_path = parse_quote!(into_boxed);

    let attrs: Vec<_> = crate::attrs::get(input.attrs.iter());

    let rpc = attrs.iter().find_map(|attr| attr.parse_if(&rpc_path));
    let ident = input.ident.clone();

    let details = match input.data {
        syn::Data::Struct(struct_) => {
            let transparent = attrs.iter().any(|attr| attr.toggled(&transparent_path));

            let fields = struct_.fields;

            if transparent {
                let field = match fields {
                    syn::Fields::Named(named) => {
                        let span = named.span();
                        let mut named_it = named.named.into_iter();

                        let field = named_it.next().expect_or_abort(
                            "Expected exactly one field when #[rpc(transparent)] is used",
                        );

                        if named_it.next().is_some() {
                            abort!(
                                span,
                                "Expected exactly one field when #[rpc(transparent)] is used"
                            )
                        }

                        Box::new(field)
                    }
                    syn::Fields::Unnamed(_unnamed) => todo!("Unnamed fields"),
                    syn::Fields::Unit => abort!(
                        fields,
                        "Expected exactly one field when #[rpc(transparent)] is used"
                    ),
                };

                Details::Transparent { field }
            } else {
                Details::Struct(Struct { fields })
            }
        }
        syn::Data::Enum(enum_) => {
            let tag = attrs.iter().find_map(|attr| attr.parse_if(&tag_path));

            let tag_rpc = attrs.iter().find_map(|attr| attr.parse_if(&tag_rpc_path));

            let variants = enum_
                .variants
                .into_iter()
                .map(|variant| {
                    let attrs = crate::attrs::get(variant.attrs.iter());

                    let rename = attrs.iter().find_map(|attr| attr.parse_if(&rename_path));
                    let into_boxed = attrs.iter().any(|attr| attr.toggled(&into_boxed_path));
                    let rpc_name = rename.unwrap_or_else(|| variant.ident.clone());

                    Variant {
                        ident: variant.ident,
                        rpc_name,
                        fields: variant.fields,
                        into_boxed,
                    }
                })
                .collect();

            Details::Enum {
                tag: tag.expect_or_abort("Expected `rpc(tag = \"path\")`"),
                tag_rpc: tag_rpc.expect_or_abort("Expected `rpc(tag_rpc = \"path\")`"),
                variants,
            }
        }
        syn::Data::Union(_) => todo!("Unimplemented for the Union"),
    };

    TryIntoRpc {
        ident,
        rpc: rpc.expect_or_abort("Expected `rpc(rpc = \"path\")`"),
        details,
    }
}

impl Variant {
    fn synthesis(self) -> TokenStream2 {
        let rpc_name = self.rpc_name;
        let ident = self.ident;
        let into_boxed = self.into_boxed;
        match self.fields {
            syn::Fields::Unnamed(unnamed) => {
                let (idx, body): (Vec<_>, Vec<_>) = unnamed
                    .unnamed
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| {
                        let idx = quote::format_ident!("_{}", idx);
                        (
                            idx.clone(),
                            if into_boxed {
                                quote! { Box::new(TryIntoRpc::try_into_rpc(#idx)?) }
                            } else {
                                quote! { TryIntoRpc::try_into_rpc(#idx)? }
                            },
                        )
                    })
                    .unzip();

                quote! {
                    Self::#ident(#(#idx),*) => #rpc_name(#(#body),*)
                }
            }
            syn::Fields::Named(_) => todo!("Named enum variant"),
            syn::Fields::Unit => todo!("Unit enum variant"),
        }
    }
}

impl Struct {
    fn synthesis(self) -> TokenStream2 {
        let into_boxed_path = parse_quote!(into_boxed);
        let boxed_path = parse_quote!(boxed);

        match self.fields {
            syn::Fields::Named(named) => {
                let named = named.named.iter().map(|field| {
                    let attrs = crate::attrs::get(field.attrs.iter());

                    let name = field.ident.as_ref().unwrap();

                    let inner = if attrs.iter().any(|attr| attr.toggled(&boxed_path)) {
                        quote! {
                            TryIntoRpc::try_into_rpc(*self.#name)?
                        }
                    } else {
                        quote! {
                            TryIntoRpc::try_into_rpc(self.#name)?
                        }
                    };

                    if attrs.iter().any(|attr| attr.toggled(&into_boxed_path)) {
                        quote! {
                            #name: Box::new(#inner)
                        }
                    } else {
                        quote! {
                            #name: #inner
                        }
                    }
                });

                quote! {
                    Ok(Self::Rpc {
                        #(#named),*
                    })
                }
            }
            syn::Fields::Unnamed(_) => todo!("Unnamed struct"),
            syn::Fields::Unit => todo!("Unit"),
        }
    }
}

impl TryIntoRpc {
    pub fn synthesis(self) -> TokenStream {
        let rpc = self.rpc;
        let item_name = self.ident;

        let body = match self.details {
            Details::Enum {
                tag,
                tag_rpc,
                variants,
            } => {
                let variants = variants.into_iter().map(|v| v.synthesis());
                quote! {
                    use #tag_rpc::*;
                    Ok(Self::Rpc {
                        #tag: Some(match self {
                            #(#variants),*
                        })
                    })
                }
            }
            Details::Transparent { field } => {
                let name = field.ident.as_ref().unwrap();
                quote! {
                    TryIntoRpc::try_into_rpc(self.#name)
                }
            }
            Details::Struct(struct_) => struct_.synthesis(),
        };

        TokenStream::from(quote! {
            impl TryIntoRpc for #item_name {
                type Rpc = #rpc;

                fn try_into_rpc(self) -> ResponseResult<Self::Rpc> {
                    #body
                }
            }
        })
    }
}
