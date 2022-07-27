use std::{fs, path::Path};

use anyhow::Result;
use sqlparser::ast::{self, ObjectName};

use crate::{catalog::heap, storage::BufferPool};

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
    heap::heap_create(
        buffer_pool,
        &db_data,
        db_name,
        &name.0[0].to_string(),
        columns,
    )?;
    Ok(())
}
