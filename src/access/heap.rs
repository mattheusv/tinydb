use crate::storage::{
    bufpage::{page_add_item, ItemId, PageHeader, ITEM_ID_SIZE, PAGE_HEADER_SIZE},
    rel::Relation,
    BufferPool,
};
use anyhow::Result;

/// HeapTuple is an in-memory data structure that points to a tuple on some page.
pub struct HeapTuple {
    pub data: Vec<u8>,
}

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(buffer_pool: &mut BufferPool, rel: &Relation, tuple: &HeapTuple) -> Result<()> {
    // TODO: Search for a free page to add the new tuple.
    let buffer = buffer_pool.fetch_buffer(rel, 1)?;
    let page = buffer_pool.get_page(&buffer);

    page_add_item(&page, &tuple.data)?;

    buffer_pool.unpin_buffer(buffer, true)?;

    Ok(())
}

pub fn heap_scan(buffer_pool: &mut BufferPool, rel: &Relation) -> Result<()> {
    // TODO: Iterate over all pages on relation
    let buffer = buffer_pool.fetch_buffer(rel, 1)?;
    let page = buffer_pool.get_page(&buffer);
    let page_header = PageHeader::new(&page)?;

    let page_data = page.borrow().bytes();

    let item_id_data = &page_data[PAGE_HEADER_SIZE..page_header.start_free_space as usize];

    let (item_id_data, _) = item_id_data.as_chunks::<ITEM_ID_SIZE>();
    let mut result = Vec::with_capacity(item_id_data.len());

    for data in item_id_data {
        result.push(bincode::deserialize::<ItemId>(&data.to_vec())?);
    }

    for item_id in result {
        let data = &page_data[item_id.offset as usize..(item_id.offset + item_id.length) as usize];
        let value = bincode::deserialize::<i32>(&data)?;
        println!("-> value: {}", value);
    }

    buffer_pool.unpin_buffer(buffer, false)?;

    Ok(())
}
