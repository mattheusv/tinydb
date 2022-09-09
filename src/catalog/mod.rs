use anyhow::{bail, Result};
use std::path::Path;

use crate::{
    access::{heap::heap_iter, tuple::TupleDesc},
    new_object_id,
    storage::BufferPool,
    Oid,
};

use self::{pg_attribute::PgAttribute, pg_class::PgClass};

pub mod heap;
pub mod pg_attribute;
pub mod pg_class;
pub mod pg_database;
pub mod pg_tablespace;
pub mod pg_type;

/// Genereate a new relation oid that is unique within the database of the given db data.
pub fn new_relation_oid(db_data: &str, db_oid: &Oid) -> Oid {
    let dbpath = Path::new(db_data).join(db_oid.to_string());

    loop {
        let oid = new_object_id();
        if !dbpath.join(oid.to_string()).exists() {
            return oid;
        }
    }
}

/// Errors related with system catalog relation operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("relation {0} does not exist")]
    RelationNotFound(String),
}

/// Return the tuple description of the given relation name.
pub fn tuple_desc_from_relation(
    buffer_pool: &mut BufferPool,
    db_data: &str,
    db_oid: &Oid,
    rel_name: &str,
) -> Result<TupleDesc> {
    let pg_attribute = PgAttribute::relation(db_data, db_oid);

    let pg_class_rel = get_pg_class_relation(buffer_pool, db_data, db_oid, rel_name)?;

    let mut attributes = Vec::new();

    heap_iter(buffer_pool, &pg_attribute, |tuple| -> Result<()> {
        let attr = bincode::deserialize::<PgAttribute>(&tuple.data)?;
        if attr.attrelid == pg_class_rel.oid {
            attributes.push(attr);
        }

        Ok(())
    })?;

    Ok(TupleDesc { attrs: attributes })
}

/// Return the oid of the given relation name.
pub fn get_pg_class_relation(
    buffer_pool: &mut BufferPool,
    db_data: &str,
    db_oid: &Oid,
    rel_name: &str,
) -> Result<PgClass> {
    let pg_class_rel = PgClass::relation(db_data, db_oid);

    let mut pg_class_tuple = None;

    heap_iter(buffer_pool, &pg_class_rel, |tuple| -> Result<()> {
        // Do nothing if the oid is already founded.
        if pg_class_tuple.is_none() {
            let pg_class = bincode::deserialize::<PgClass>(&tuple.data)?;
            if pg_class.relname == rel_name {
                pg_class_tuple = Some(pg_class);
            }
        }
        Ok(())
    })?;

    match pg_class_tuple {
        Some(tuple) => Ok(tuple),
        None => bail!(Error::RelationNotFound(rel_name.to_string())),
    }
}
