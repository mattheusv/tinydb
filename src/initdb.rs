use std::{fs::create_dir_all, path::Path};

use anyhow::Result;

use crate::{
    access::{
        heap::heap_insert,
        heaptuple::{HeapTuple, HeapTupleHeader},
    },
    catalog::{
        heap::{self, heap_create},
        pg_attribute::{self, PgAttribute},
        pg_class::{self, PgClass},
        pg_database::{self, PgDatabase},
        pg_tablespace::{self, PgTablespace},
    },
    new_object_id,
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
    init_pg_tablespace(buffer, db_data, db_name)?;
    init_pg_database(buffer, db_data, db_name)?;

    Ok(())
}

/// Initialize pg_database relation and insert default system database.
fn init_pg_database(buffer: &mut BufferPool, db_data: &str, db_name: &str) -> Result<()> {
    let pg_database = PgDatabase::relation(db_data, db_name);
    heap::initialize_default_page_header(buffer, &pg_database)?;

    heap_create(
        buffer,
        db_data,
        db_name,
        pg_database::RELATION_NAME,
        pg_database::RELATION_OID,
        &PgDatabase::tuple_desc(),
    )?;

    heap_insert(
        buffer,
        &pg_database,
        &mut HeapTuple {
            header: HeapTupleHeader::default(),
            data: bincode::serialize(&PgDatabase {
                oid: new_object_id(),
                datname: db_name.to_string(),
                dattablespace: pg_tablespace::GLOBALTABLESPACE_OID,
            })?,
        },
    )?;

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

/// Initialize pg_tablespace relation and insert default tablespace.
fn init_pg_tablespace(buffer: &mut BufferPool, db_data: &str, db_name: &str) -> Result<()> {
    let pg_tablespace = PgTablespace::relation(db_data, db_name);
    heap::initialize_default_page_header(buffer, &pg_tablespace)?;

    heap_create(
        buffer,
        db_data,
        db_name,
        pg_tablespace::RELATION_NAME,
        pg_tablespace::RELATION_OID,
        &PgTablespace::tuple_desc(),
    )?;

    let pg_default = PgTablespace {
        oid: pg_tablespace::DEFAULTTABLESPACE_OID,
        spcname: String::from("pg_default"),
    };

    heap_insert(
        buffer,
        &pg_tablespace,
        &mut HeapTuple {
            header: HeapTupleHeader::default(),
            data: bincode::serialize(&pg_default)?,
        },
    )?;

    let pg_global = PgTablespace {
        oid: pg_tablespace::GLOBALTABLESPACE_OID,
        spcname: String::from("pg_global"),
    };

    heap_insert(
        buffer,
        &pg_tablespace,
        &mut HeapTuple {
            header: HeapTupleHeader::default(),
            data: bincode::serialize(&pg_global)?,
        },
    )?;
    Ok(())
}
