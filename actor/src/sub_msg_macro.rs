#[macro_export]
macro_rules! from_impls {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $(
                $variant:ident($ty:ty)
            ),* $(,)?
        }
    ) => {
        // #[sub_decoders2]
        $(#[$meta])*
        $vis enum $name {
            $(
                $variant($ty),
            )*
        }

        $(
            impl From<$ty> for $name {
                fn from(v: $ty) -> Self {
                    $name::$variant(v)
                }
            }
        )*

    };
}

#[cfg(test)]
mod tests {

    use reactor_macros::sub_decoders2;

    #[derive(Default, Debug, PartialEq)]
    pub struct Foo;

    #[derive(Default, Debug, PartialEq)]
    pub struct Bar;

    from_impls! {
        #[derive(Debug, PartialEq)]
        pub enum MyEnum {
            A(Foo),
            B(Bar),
            C((usize, usize))
        }
    }

    #[test]
    fn test_from_trait() {
        let foo: MyEnum = Foo.into();
        let bar: MyEnum = Bar.into();
        let c: MyEnum = (1, 2).into();

        assert_eq!(foo, MyEnum::A(Foo));
        assert_eq!(bar, MyEnum::B(Bar));
        assert_eq!(c, MyEnum::C((1, 2)));
    }

    #[sub_decoders2]
    #[derive(Debug, PartialEq)]
    pub enum MyEnum2 {
        A(Foo),
        B(Bar),
    }
    #[test]
    fn test_from_trait2() {}
}
