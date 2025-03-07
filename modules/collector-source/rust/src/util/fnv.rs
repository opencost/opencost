//! Map
use std::ops::{
    Deref,
    DerefMut,
};

/// The `FnvMap` is an alias for a hashbrown::HashMap that uses an
/// `FnvBuilderHasher`
pub type FnvMap<K, V> = hashbrown::HashMap<K, V, fnv::FnvBuildHasher>;

/// The `FnvSet` is an alias for a hashbrown::HashSet that uses an
/// `FnvBuilderHasher`
pub type FnvSet<T> = hashbrown::HashSet<T, fnv::FnvBuildHasher>;

/// This function creates a new `FnvMap` with the provided capacity
#[allow(unused)]
pub fn new_map_with_capacity<K, V>(capacity: usize) -> FnvMap<K, V> {
    FnvMap::with_capacity_and_hasher(capacity, Default::default())
}

/// This function creates a new `FnvSet` with the provided capacity
pub fn new_set_with_capacity<T>(capacity: usize) -> FnvSet<T> {
    FnvSet::with_capacity_and_hasher(capacity, Default::default())
}

/// A `NamedMap` is a `FnvMap` with a name, often used to debug map swaps or
/// replacements, this type derefs into the inner `FnvMap`
pub struct NamedMap<K, V> {
    pub name: String,
    map: FnvMap<K, V>,
}

#[allow(unused)]
impl<K, V> NamedMap<K, V> {
    /// Creates a new `NamedMap<K, V>` instance with the provided name.
    pub fn new(n: impl Into<String>) -> Self {
        Self {
            name: n.into(),
            map: FnvMap::default(),
        }
    }

    /// Creates a new `NamedMap<K, V>` instance with the provided name and
    /// capacity.
    pub fn with_capacity(n: impl Into<String>, capacity: usize) -> Self {
        Self {
            name: n.into(),
            map: FnvMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }
}

impl<K, V> Deref for NamedMap<K, V> {
    type Target = FnvMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K, V> DerefMut for NamedMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}
