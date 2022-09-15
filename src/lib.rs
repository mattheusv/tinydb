use std::slice::Iter;

pub mod initdb;

use std::sync::atomic::{AtomicU64, Ordering};
pub mod access;
pub mod catalog;
pub mod engine;
pub mod errors;
pub mod lru;
pub mod sql;
pub mod storage;

/// First object id to assign when creating a new database cluster.
const FIRST_NORMAL_OBJECT_ID: u64 = 10000;

/// Objecct identifier.
pub type Oid = u64;

pub const INVALID_OID: Oid = 0;

/// A slice of bytes that represents a value of inside a tuple.
pub type Datum = Vec<u8>;

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

/// A slice of nullable Datums.
///
/// This splice represents a single tuple row with all posible values.
///
/// The values are aligned with the same index of tuple attributes. If the
/// attribute index is represents by an None it means that this attribute
/// has a NULL value associated.
#[derive(Default, Debug)]
pub struct Datums(Vec<Option<Datum>>);

impl Datums {
    /// Appends an Option<Datum> to the back of a collection of datums.
    pub fn push(&mut self, datum: Option<Datum>) {
        self.0.push(datum);
    }

    /// Returns an iterator over a slice of Option<Datum>
    pub fn iter(&self) -> Iter<Option<Datum>> {
        self.0.iter()
    }
}

impl std::io::Write for Datums {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.push(Some(buf.to_vec()));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
