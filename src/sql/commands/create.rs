use std::{cell::RefCell, fs, mem::size_of, rc::Rc};

use anyhow::{bail, Result};
use sqlparser::ast::{self, ColumnDef, DataType, ObjectName};

use crate::{
    access::tuple::TupleDesc,
    catalog::{
        self, heap, pg_attribute::PgAttribute, pg_tablespace::DEFAULTTABLESPACE_OID, pg_type,
    },
    storage::BufferPool,
    Oid,
};

pub fn create_database(name: ObjectName) -> Result<()> {
    fs::create_dir(name.0[0].to_string())?;
    Ok(())
}

pub fn create_table(
    buffer_pool: Rc<RefCell<BufferPool>>,
    db_oid: &Oid,
    name: ObjectName,
    columns: Vec<ast::ColumnDef>,
) -> Result<()> {
    // Create a new unique oid to the new heap relation.
    let new_oid = catalog::new_relation_oid(&DEFAULTTABLESPACE_OID, db_oid)?;

    let mut tupledesc = TupleDesc::default();
    for (i, attr) in columns.iter().enumerate() {
        // Attributes numbers start at 1
        tupledesc
            .attrs
            .push(new_pg_attribute(new_oid, attr, i + 1)?)
    }

    heap::heap_create(
        &mut buffer_pool.borrow_mut(),
        DEFAULTTABLESPACE_OID,
        db_oid,
        &name.0[0].to_string(),
        new_oid,
        &tupledesc,
    )?;
    Ok(())
}

fn new_pg_attribute(attrelid: Oid, columndef: &ColumnDef, attnum: usize) -> Result<PgAttribute> {
    let (atttypid, attlen) = oid_type_and_size(&columndef.data_type)?;
    Ok(PgAttribute {
        attrelid,
        attname: columndef.name.to_string(),
        attnum,
        attlen,
        atttypid,
    })
}

/// Return the oid and the lenght of the given data type.
fn oid_type_and_size(typ: &DataType) -> Result<(Oid, i64)> {
    match typ {
        DataType::Int(len) => Ok((
            pg_type::INT_OID,
            (len.unwrap_or(size_of::<i32>() as u64)) as i64,
        )),
        DataType::Varchar(len) => match len {
            Some(len) => Ok((pg_type::VARCHAR_OID, *len as i64)),
            None => Ok((pg_type::VARCHAR_OID, -1)),
        },
        DataType::Boolean => Ok((pg_type::BOOL_OID, size_of::<bool>() as i64)),
        _ => bail!("Not supported data type: {}", typ),
    }
}
