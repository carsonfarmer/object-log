//! Native object-log backend for Spin's existing key-value factor.
//!
//! Register [`ObjectLogKeyValueStore`] with Spin's runtime config resolver.
//! The guest uses the standard interface; no credentials or storage transport
//! cross the component boundary. See the accompanying README for operations.

mod config;
mod store;

pub use config::{Config, HostLimits, Manager, ObjectLogKeyValueStore};
pub use store::Maintenance;
