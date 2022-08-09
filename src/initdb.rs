use std::{fs::create_dir_all, path::Path};

use anyhow::Result;

use crate::{
    catalog::{
        heap::{self, heap_create},
        pg_attribute::{self, PgAttribute},
        pg_class::{self, PgClass},
    },
    storage::BufferPool,
};

/// Initialize a empty database at the db_data path using db_name as the database name.
pub fn init_database(buffer: &mut BufferPool, db_data: &str, db_name: &str) -> Result<()> {
    let db_path = Path::new(db_data).join(db_name);

    if !db_path.exists() {
        create_dir_all(db_path)?;
    }

    init_pg_attribute(buffer, db_data, db_name)?;
    init_pg_class(buffer, db_data, db_name)?;

    Ok(())
}

/// Initialize pg_class relation and insert default system tables.
fn init_pg_attribute(buffer: &mut BufferPool, db_data: &str, db_name: &str) -> Result<()> {
    let pg_attribute = PgAttribute::relation(db_data, db_name);
    heap::initialize_default_page_header(buffer, &pg_attribute)?;

    heap_create(
        buffer,
        db_data,
        db_name,
        pg_attribute::RELATION_NAME,
        pg_attribute::RELATION_OID,
        &PgAttribute::tuple_desc(),
    )?;

    Ok(())
}

/// Declare pg_class relation on itself.
///
/// Note that the header of pg_class is defined if needed on catalog::heap::add_new_relation_tuple
/// since this table is required to any other table and itself.
///
/// So here we just declare the pg_class on pg_class system table.
fn init_pg_class(buffer: &mut BufferPool, db_data: &str, db_name: &str) -> Result<()> {
    heap_create(
        buffer,
        db_data,
        db_name,
        pg_class::RELATION_NAME,
        pg_class::RELATION_OID,
        &PgClass::tuple_desc(),
    )?;

    Ok(())
}
