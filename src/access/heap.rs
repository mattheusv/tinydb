use std::io::{Cursor, Read};

use crate::{
    relation::Relation,
    storage::{
        freespace,
        page::{page_add_item, ItemId, PageHeader, ITEM_ID_SIZE, PAGE_HEADER_SIZE},
        BufferPool,
    },
};
use anyhow::Result;

use super::heaptuple::HeapTuple;

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(buffer_pool: &mut BufferPool, rel: &Relation, tuple: &HeapTuple) -> Result<()> {
    let buffer = freespace::get_page_with_free_space(buffer_pool, rel)?;
    let page = buffer_pool.get_page(&buffer)?;

    page_add_item(&page, &tuple.encode()?)?;

    buffer_pool.unpin_buffer(buffer, true)?;

    Ok(())
}

pub fn heap_scan(buffer_pool: &mut BufferPool, rel: &Relation) -> Result<Vec<HeapTuple>> {
    let mut tuples = Vec::new();
    heap_iter(buffer_pool, rel, |tuple| -> Result<()> {
        tuples.push(tuple);
        Ok(())
    })?;
    Ok(tuples)
}

/// Iterate over all heap pages and heap tuples to the given relation calling function f to each
/// tuple in a page.
pub fn heap_iter<F>(buffer_pool: &mut BufferPool, rel: &Relation, mut f: F) -> Result<()>
where
    F: FnMut(HeapTuple) -> Result<()>,
{
    // TODO: Iterate over all pages on relation
    let buffer = buffer_pool.fetch_buffer(rel, 1)?;
    let page = buffer_pool.get_page(&buffer)?;
    let page_header = PageHeader::new(&page)?;

    let page_data = page.borrow().bytes();

    // Get a reference to the raw data of item_id_data.
    let item_id_data = &page_data[PAGE_HEADER_SIZE..page_header.start_free_space as usize];

    let mut item_id_data_cursor = Cursor::new(item_id_data);
    let mut item_id_data = vec![0; ITEM_ID_SIZE];
    loop {
        let size = item_id_data_cursor.read(&mut item_id_data)?;
        if size == 0 {
            // EOF
            break;
        }

        // Deserialize a single ItemId from the list item_id_data.
        let item_id = bincode::deserialize::<ItemId>(&item_id_data)?;

        // Slice the raw page to get a refenrece to a tuple inside the page.
        let data = &page_data[item_id.offset as usize..(item_id.offset + item_id.length) as usize];
        let tuple = HeapTuple::decode(data)?;

        f(tuple)?;

        item_id_data = vec![0; ITEM_ID_SIZE];
    }

    buffer_pool.unpin_buffer(buffer, false)?;

    Ok(())
}
