//! Deterministic removal of caller-controlled spare allocation capacity.

/// Rebuilds an owned string through `Box<str>`, whose allocation metadata has
/// only a length, so the resulting `String` capacity is exactly that length.
pub(crate) fn compact_string(value: String) -> String {
    String::from(value.into_boxed_str())
}

/// Rebuilds an owned vector through `Box<[T]>`, whose allocation metadata has
/// only a length, so the resulting `Vec` capacity is exactly that length.
pub(crate) fn compact_vec<T>(value: Vec<T>) -> Vec<T> {
    Vec::from(value.into_boxed_slice())
}
