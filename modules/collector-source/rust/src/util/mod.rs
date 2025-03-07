pub use fnv::*;

mod fnv;

#[macro_export]
macro_rules! hashset {
    (@single $($x:tt)*) => (());
    (@count $($rest:expr),*) => (<[()]>::len(&[$(hashset!(@single $rest)),*]));

    ($($key:expr,)+) => { hashset!($($key),+) };
    ($($key:expr),*) => {
        {
            let _cap = hashset!(@count $($key),*);
            let mut _set = $crate::util::new_set_with_capacity(_cap);
            $(
                let _ = _set.insert($key);
            )*
            _set
        }
    };
}
