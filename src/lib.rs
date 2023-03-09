use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod access;
pub mod backend;
pub mod catalog;
pub mod cli;
pub mod executor;
pub mod initdb;
pub mod lru;
pub mod planner;
pub mod postgres_protocol;
pub mod relation;
pub mod sql;
pub mod storage;

/// First object id to assign when creating a new database cluster.
const FIRST_NORMAL_OBJECT_ID: u64 = 10000;

/// Objecct identifier.
pub type Oid = u64;

pub const INVALID_OID: Oid = 0;

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

/// A slice of bytes that represents a value of inside a tuple.
///
/// A reference of datum is always read-only.
#[derive(Debug, Default)]
pub struct Datum(Vec<u8>);

impl From<Vec<u8>> for Datum {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl Deref for Datum {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// An alias for a Option<Datum>.
pub type NullableDatum = Option<Datum>;
