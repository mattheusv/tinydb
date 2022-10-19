use crate::{
    access::{
        self,
        heap::heap_insert,
        heaptuple::{HeapTuple, TupleDesc},
    },
    relation::Relation,
    storage::{page::PageHeader, BufferPool},
    Oid,
};
use anyhow::Result;
use log::debug;

use super::{pg_class::PgClass, pg_tablespace::GLOBALTABLESPACE_OID};

/// Create a new cataloged heap relation.
pub fn heap_create(
    buffer: &BufferPool,
    tablespace: Oid,
    db_oid: &Oid,
    rel_name: &str,
    new_rel_oid: Oid,
    tupledesc: &TupleDesc,
) -> Result<()> {
    // Create a new relation object for the new heap relation.
    let new_rel = access::open_relation(new_rel_oid, tablespace, db_oid, rel_name);

    // Now add tuples to pg_attribute for the attributes in our new relation.
    add_new_attribute_tuples(buffer, &new_rel, &tupledesc)?;

    // Open pg_class relation to store the new relation
    let pg_class = access::open_pg_class_relation(db_oid);

    // Now create an entry in pg_class for the relation.
    add_new_relation_tuple(buffer, &pg_class, &new_rel)?;

    // Now that the new relation is already stored on pg_class, initialize the default page header
    // data
    initialize_default_page_header(buffer, &new_rel)?;

    Ok(())
}

/// Registers the new relation's schema by adding tuples to pg_attribute.
fn add_new_attribute_tuples(
    buffer: &BufferPool,
    rel: &Relation,
    tupledesc: &TupleDesc,
) -> Result<()> {
    let rel = rel.borrow();

    // Open pg_attribute relation to store the new relation attributes.
    let pg_attribute = access::open_pg_attribute_relation(&rel.locator.database);

    // Now insert a new tuple on pg_attribute containing the new attributes information.
    for attr in &tupledesc.attrs {
        heap_insert(
            buffer,
            &pg_attribute,
            &HeapTuple::with_default_header(&attr)?,
        )?;
    }

    Ok(())
}

/// Registers the new relation in the catalogs by adding a tuple to pg_class. If the pg_class is
/// empty the buffer pool is used to alloc a new page on pg_class file and initialize the default
/// header values.
fn add_new_relation_tuple(
    buffer: &BufferPool,
    pg_class: &Relation,
    new_rel: &Relation,
) -> Result<()> {
    let new_rel = new_rel.borrow();

    // Initialize default page header of pg_class relation if needed.
    if buffer.size_of_relation(pg_class)? == 0 {
        initialize_default_page_header(buffer, pg_class)?;
    }

    // Now insert a new tuple on pg_class containing the new relation information.
    heap_insert(
        buffer,
        pg_class,
        &HeapTuple::with_default_header(&PgClass {
            oid: new_rel.locator.oid,
            relname: new_rel.rel_name.clone(),
            reltablespace: new_rel.locator.tablespace,
            relisshared: new_rel.locator.tablespace == GLOBALTABLESPACE_OID,
        })?,
    )?;

    Ok(())
}

/// Initialize the default page header values on the given relation. The buffer pool is used to
/// alloc a new page on relation.
pub fn initialize_default_page_header(buffer: &BufferPool, rel: &Relation) -> Result<()> {
    if buffer.size_of_relation(rel)? > 0 {
        // Page header already initialized.
        return Ok(());
    }

    let buf_id = buffer.alloc_buffer(rel)?;
    let mut page = buffer.get_page(&buf_id)?;

    bincode::serialize_into(&mut page.writer(), &PageHeader::default())?;

    buffer.flush_buffer(&buf_id)?;
    buffer.unpin_buffer(buf_id, true)?;

    debug!(
        "Initialized default pager header data for relation: {}",
        rel.borrow().rel_name
    );

    Ok(())
}
