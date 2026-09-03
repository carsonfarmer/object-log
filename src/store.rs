//! Scoped object-store operations.

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackendCapability {
    ConditionalCreate,
    ConditionalUpdate,
    ConditionalRead,
    ConsistentReadAfterWrite,
    ConsistentList,
}

/// Capabilities proven by the backend conformance probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendCapabilities {
    pub supported: BTreeSet<BackendCapability>,
}

/// A placeholder for the namespace-safe storage adapter.
#[derive(Debug)]
pub struct ScopedStore;
