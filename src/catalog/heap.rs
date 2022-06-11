use crate::{
    access::heap::{heap_insert, HeapTuple},
    storage::{
        bufpage::PageHeader,
        pager::PAGE_SIZE,
        rel::{Relation, RelationData},
        BufferPool,
    },
};
use anyhow::Result;

use super::pg_class::PgClass;

/// Create a new cataloged heap relation.
pub fn heap_create(
    buffer: &mut BufferPool,
    db_data: &str,
    db_name: &str,
    rel_name: &str,
) -> Result<()> {
    // Create a new relation and initialize a empty pager handle.
    let new_rel = RelationData::open(db_data, db_name, rel_name)?;

    // Open pg_class relation to store the new relation
    let pg_class = RelationData::open(db_data, db_name, "pg_class")?;

    // Now create an entry in pg_class for the relation.
    add_new_relation_tuple(buffer, &pg_class, &new_rel)?;

    // Now that the new relation is already stored on pg_class, initialize the default page header
    // data
    initialize_default_page_header(buffer, &new_rel)?;

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

    // Now insert a new tuple on pg_class containing the new relation information.
    heap_insert(
        buffer,
        pg_class,
        &HeapTuple {
            data: bincode::serialize(&PgClass {
                relname: new_rel.borrow().rel_name.clone(),
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
