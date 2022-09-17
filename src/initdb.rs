use std::{fs::create_dir_all, path::Path};

use anyhow::Result;

use crate::{
    access::{self, heap::heap_insert, heaptuple::HeapTuple},
    catalog::{
        heap::{self, heap_create},
        pg_attribute::{self, PgAttribute},
        pg_class::{self, PgClass},
        pg_database::{self, PgDatabase, TINYDB_OID},
        pg_tablespace::{self, PgTablespace, DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID},
    },
    storage::BufferPool,
    Oid,
};

/// Initialize a empty database at the data_dir path using db_name as the database name.
pub fn init_database(buffer: &mut BufferPool, data_dir: &Path) -> Result<()> {
    let db_path = data_dir
        .join("base")
        .join(pg_database::TINYDB_OID.to_string());
    let global_path = Path::new(data_dir).join("global");

    if !db_path.exists() {
        create_dir_all(&db_path)?;
    }

    if !global_path.exists() {
        create_dir_all(&global_path)?;
    }

    // Init per database relations
    init_pg_attribute(buffer, &pg_database::TINYDB_OID)?;
    init_pg_class(buffer, &pg_database::TINYDB_OID)?;

    // Init global relations
    init_pg_tablespace(buffer, &pg_database::TINYDB_OID)?;
    init_pg_database(buffer, &pg_database::TINYDB_OID)?;

    Ok(())
}

/// Initialize pg_database relation and insert default system database.
fn init_pg_database(buffer: &mut BufferPool, db_oid: &Oid) -> Result<()> {
    let pg_database = access::open_pg_database_relation();

    heap_create(
        buffer,
        GLOBALTABLESPACE_OID,
        db_oid,
        pg_database::RELATION_NAME,
        pg_database::RELATION_OID,
        &PgDatabase::tuple_desc(),
    )?;

    heap_insert(
        buffer,
        &pg_database,
        &HeapTuple::with_default_header(&PgDatabase {
            oid: TINYDB_OID,
            datname: String::from("tinydb"),
            dattablespace: DEFAULTTABLESPACE_OID,
        })?,
    )?;

    Ok(())
}

/// Initialize pg_class relation and insert default system tables.
fn init_pg_attribute(buffer: &mut BufferPool, db_oid: &Oid) -> Result<()> {
    // We need to init the page header before call heap_create because heap_create
    // actually store the heap attributes on pg_attribute, so the header relation
    // should already be filled.
    let pg_attribute = access::open_pg_attribute_relation(db_oid);
    heap::initialize_default_page_header(buffer, &pg_attribute)?;

    heap_create(
        buffer,
        DEFAULTTABLESPACE_OID,
        db_oid,
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
fn init_pg_class(buffer: &mut BufferPool, db_oid: &Oid) -> Result<()> {
    heap_create(
        buffer,
        DEFAULTTABLESPACE_OID,
        db_oid,
        pg_class::RELATION_NAME,
        pg_class::RELATION_OID,
        &PgClass::tuple_desc(),
    )?;

    Ok(())
}

/// Initialize pg_tablespace relation and insert default tablespace.
fn init_pg_tablespace(buffer: &mut BufferPool, db_oid: &Oid) -> Result<()> {
    let pg_tablespace = access::open_pg_tablespace_relation();

    heap_create(
        buffer,
        GLOBALTABLESPACE_OID,
        db_oid,
        pg_tablespace::RELATION_NAME,
        pg_tablespace::RELATION_OID,
        &PgTablespace::tuple_desc(),
    )?;

    let pg_default = PgTablespace {
        oid: DEFAULTTABLESPACE_OID,
        spcname: String::from("pg_default"),
    };

    heap_insert(
        buffer,
        &pg_tablespace,
        &HeapTuple::with_default_header(&pg_default)?,
    )?;

    let pg_global = PgTablespace {
        oid: GLOBALTABLESPACE_OID,
        spcname: String::from("pg_global"),
    };

    heap_insert(
        buffer,
        &pg_tablespace,
        &HeapTuple::with_default_header(&pg_global)?,
    )?;
    Ok(())
}
