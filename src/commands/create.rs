use std::{fs, mem::size_of, path::Path};

use anyhow::Result;
use sqlparser::ast::{self, ObjectName};

use crate::{
    access::tuple::TupleDesc,
    catalog::{heap, new_relation_oid, pg_attribute::PgAttribute},
    storage::BufferPool,
};

pub fn create_database(db_data: &str, name: ObjectName) -> Result<()> {
    let table_path = Path::new(db_data).join(name.0[0].to_string());
    fs::create_dir(table_path)?;
    Ok(())
}

pub fn create_table(
    buffer_pool: &mut BufferPool,
    db_data: &str,
    db_name: &str,
    name: ObjectName,
    columns: Vec<ast::ColumnDef>,
) -> Result<()> {
    // Create a new unique oid to the new heap relation.
    let new_oid = new_relation_oid(db_data, db_name);

    let mut tupledesc = TupleDesc::default();
    for (i, attr) in columns.iter().enumerate() {
        tupledesc.attrs.push(PgAttribute {
            attrelid: new_oid,
            attname: attr.name.to_string(),
            attnum: i + 1,            // Attributes numbers start at 1
            attlen: size_of::<i32>(), // TODO: Add support for multiple types
        })
    }

    heap::heap_create(
        buffer_pool,
        &db_data,
        db_name,
        &name.0[0].to_string(),
        new_oid,
        &tupledesc,
    )?;
    Ok(())
}
