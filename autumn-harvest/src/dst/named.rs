//! The `named!` macro: an enum with a fixed list of names.

/// An enum with a fixed list of names.
macro_rules! named {
    (
        $(#[$meta:meta])*
        $name:ident {
            $($(#[$vmeta:meta])* $variant:ident => $text:literal,)+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant,)+
        }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: [Self; [$(named!(@unit $variant)),+].len()] = [$(Self::$variant),+];

            /// The name that `parse` accepts.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }

            /// Parse a name from `as_str`.
            ///
            /// # Errors
            ///
            /// Returns a message that names the accepted values.
            pub fn parse(name: &str) -> Result<Self, String> {
                Self::ALL
                    .into_iter()
                    .find(|value| value.as_str() == name)
                    .ok_or_else(|| {
                        let names: Vec<&str> = Self::ALL.iter().map(|v| v.as_str()).collect();
                        format!(
                            "unknown {} {name:?}: use {}",
                            stringify!($name),
                            names.join(", ")
                        )
                    })
            }
        }
    };
    (@unit $variant:ident) => { () };
}
