use anyhow::{bail, Result};

use crate::{
    access::{self, heap::HeapScanner, heaptuple::TupleDesc},
    new_object_id,
    storage::{relation_locator::relation_path, BufferPool},
    Oid,
};

use self::{pg_attribute::PgAttribute, pg_class::PgClass};

pub mod heap;
pub mod pg_attribute;
pub mod pg_class;
pub mod pg_database;
pub mod pg_tablespace;
pub mod pg_type;

/// Errors related with system catalog relation operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("relation {0} does not exist")]
    RelationNotFound(String),
}

/// Return the tuple description of the given relation name.
pub fn tuple_desc_from_relation(
    buffer_pool: BufferPool,
    db_oid: &Oid,
    rel_name: &str,
) -> Result<TupleDesc> {
    let pg_attribute = access::open_pg_attribute_relation(db_oid);

    let pg_class_rel = get_pg_class_relation(buffer_pool.clone(), db_oid, rel_name)?;

    let mut attributes = Vec::new();

    let heap = HeapScanner::new(buffer_pool, &pg_attribute)?;
    for tuple in heap {
        let tuple = tuple?;
        let attr = bincode::deserialize::<PgAttribute>(&tuple.data)?;
        if attr.attrelid == pg_class_rel.oid {
            attributes.push(attr);
        }
    }

    Ok(TupleDesc { attrs: attributes })
}

/// Return the pg class tuple from the given relation name.
pub fn get_pg_class_relation(
    buffer_pool: BufferPool,
    db_oid: &Oid,
    rel_name: &str,
) -> Result<PgClass> {
    let pg_class_rel = access::open_pg_class_relation(db_oid);

    let mut pg_class_tuple = None;

    let heap = HeapScanner::new(buffer_pool, &pg_class_rel)?;
    for tuple in heap {
        let tuple = tuple?;
        // Do nothing if the oid is already founded.
        if pg_class_tuple.is_none() {
            let pg_class = bincode::deserialize::<PgClass>(&tuple.data)?;
            if pg_class.relname == rel_name {
                pg_class_tuple = Some(pg_class);
            }
        }
    }

    match pg_class_tuple {
        Some(tuple) => Ok(tuple),
        None => bail!(Error::RelationNotFound(rel_name.to_string())),
    }
}

/// Genereate a new relation oid that is unique to the given the database.
///
/// Note that the current working directory is expected to be the data directory.
pub fn new_relation_oid(tablespace: &Oid, db_oid: &Oid) -> Result<Oid> {
    loop {
        let rel_oid = new_object_id();
        let relpath = relation_path(tablespace, db_oid, &rel_oid)?;
        if !relpath.exists() {
            return Ok(rel_oid);
        }
    }
}
