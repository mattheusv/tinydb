use std::mem::size_of;

use crate::{
    access::{
        heap::{heap_insert, HeapTuple},
        tuple::TupleDesc,
    },
    storage::{
        bufpage::PageHeader,
        pager::PAGE_SIZE,
        rel::{Relation, RelationData},
        BufferPool,
    },
};
use anyhow::Result;
use sqlparser::ast::ColumnDef;

use super::{new_relation_oid, pg_attribute::PgAttribute, pg_class::PgClass};

/// Create a new cataloged heap relation.
pub fn heap_create(
    buffer: &mut BufferPool,
    db_data: &str,
    db_name: &str,
    rel_name: &str,
    attrs: Vec<ColumnDef>,
) -> Result<()> {
    // Create a new unique oid to the new heap relation.
    let new_oid = new_relation_oid(db_data, db_name);

    // Create a new relation and initialize a empty pager handle.
    let new_rel = RelationData::open(new_oid, db_data, db_name, rel_name)?;

    let mut tupledesc = TupleDesc::default();
    for (i, attr) in attrs.iter().enumerate() {
        tupledesc.attrs.push(PgAttribute {
            attrelid: new_oid,
            attname: attr.name.to_string(),
            attnum: i,
            attlen: size_of::<i32>(),
        })
    }

    // Now add tuples to pg_attribute for the attributes in our new relation.
    add_new_attribute_tuples(buffer, &new_rel, &tupledesc)?;

    // Open pg_class relation to store the new relation
    let pg_class = PgClass::get_relation(db_data, db_name)?;

    // Now create an entry in pg_class for the relation.
    add_new_relation_tuple(buffer, &pg_class, &new_rel)?;

    // Now that the new relation is already stored on pg_class, initialize the default page header
    // data
    initialize_default_page_header(buffer, &new_rel)?;

    Ok(())
}

/// Registers the new relation's schema by adding tuples to pg_attribute.
fn add_new_attribute_tuples(
    buffer: &mut BufferPool,
    rel: &Relation,
    tupledesc: &TupleDesc,
) -> Result<()> {
    let rel = rel.borrow();

    // Open pg_attribute relation to store the new relation attributes.
    let pg_attribute = PgAttribute::get_relation(&rel.db_data, &rel.db_name)?;

    // Initialize the pg_attribute page header if its new.
    // TODO: All catalog tables shoulb be bootstrapped at  inidbb process.
    if pg_attribute.borrow().pager.size()? == 0 {
        initialize_default_page_header(buffer, &pg_attribute)?;
    }

    // Now insert a new tuple on pg_attribute containing the new attributes information.
    for attr in &tupledesc.attrs {
        heap_insert(
            buffer,
            &pg_attribute,
            &HeapTuple {
                data: bincode::serialize(&attr)?,
            },
        )?;
    }

    Ok(())
}

/// Registers the new relation in the catalogs by adding a tuple to pg_class. If the pg_class is
/// empty the buffer pool is used to alloc a new page on pg_class file and initialize the default
/// header values.
fn add_new_relation_tuple(
    buffer: &mut BufferPool,
    pg_class: &Relation,
    new_rel: &Relation,
) -> Result<()> {
    // Initialize the pg_class page header if its new.
    // TODO: All catalog tables shoulb be bootstrapped at  inidbb process.
    if pg_class.borrow().pager.size()? == 0 {
        initialize_default_page_header(buffer, pg_class)?;
    }

    let new_rel = new_rel.borrow();

    // Now insert a new tuple on pg_class containing the new relation information.
    heap_insert(
        buffer,
        pg_class,
        &HeapTuple {
            data: bincode::serialize(&PgClass {
                oid: new_rel.oid,
                relname: new_rel.rel_name.clone(),
            })?,
        },
    )?;

    Ok(())
}

/// Initialize the default page header values on the given relation. The buffer pool is used to
/// alloc a new page on relation.
fn initialize_default_page_header(buffer: &mut BufferPool, rel: &Relation) -> Result<()> {
    let buf_id = buffer.alloc_buffer(rel)?;

    let mut data = bincode::serialize(&PageHeader::default()).unwrap();
    data.resize(PAGE_SIZE, u8::default());

    let page = buffer.get_page(&buf_id);
    page.borrow_mut().write_from_vec(data);

    buffer.unpin_buffer(buf_id, true)?;

    Ok(())
}
