#![feature(slice_as_chunks)]

use std::sync::atomic::{AtomicU64, Ordering};
pub mod access;
pub mod catalog;
pub mod engine;
pub mod lru;
pub mod storage;

/// First object id to assign when creating a new database cluster.
const FIRST_NORMAL_OBJECT_ID: u64 = 10000;

/// Objecct identifier.
pub type Oid = u64;

/// Atomic counter to increment when allocating new OIDs.
static OID_COUNTER: AtomicU64 = AtomicU64::new(FIRST_NORMAL_OBJECT_ID);

/// Allocate a new OID.
///
/// Duplicate OIDs is (and shouldn't) not handled here.
///
/// TODO: Deal carefully with wraparround.
pub fn new_object_id() -> Oid {
    OID_COUNTER.fetch_add(1, Ordering::SeqCst)
}
