use crate::{
    catalog::{pg_attribute::PgAttribute, pg_class::PgClass},
    storage::{
        bufpage::{page_add_item, ItemId, PageHeader, ITEM_ID_SIZE, PAGE_HEADER_SIZE},
        freespace,
        rel::Relation,
        BufferPool,
    },
};
use anyhow::Result;

/// HeapTuple is an in-memory data structure that points to a tuple on some page.
pub struct HeapTuple {
    pub data: Vec<u8>,
}

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(buffer_pool: &mut BufferPool, rel: &Relation, tuple: &HeapTuple) -> Result<()> {
    let buffer = freespace::get_page_with_free_space(buffer_pool, rel)?;
    let page = buffer_pool.get_page(&buffer);

    page_add_item(&page, &tuple.data)?;

    buffer_pool.unpin_buffer(buffer, true)?;

    Ok(())
}

pub fn heap_scan(buffer_pool: &mut BufferPool, rel: &Relation) -> Result<()> {
    heap_iter(buffer_pool, rel, |tuple| -> Result<()> {
        match rel.borrow().rel_name.as_str() {
            "pg_class" => {
                let value = bincode::deserialize::<PgClass>(&tuple)?;
                println!("-> value: {:?}", value);
            }
            "pg_attribute" => {
                let value = bincode::deserialize::<Vec<PgAttribute>>(&tuple)?;
                println!("-> value: {:?}", value);
            }
            _ => {
                let value = bincode::deserialize::<i32>(&tuple)?;
                println!("-> value: {}", value);
            }
        }

        Ok(())
    })
}

/// Iterate over all heap pages and heap tuples to the given relation calling function f to each
/// tuple in a page.
pub fn heap_iter<F>(buffer_pool: &mut BufferPool, rel: &Relation, mut f: F) -> Result<()>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    // TODO: Iterate over all pages on relation
    let buffer = buffer_pool.fetch_buffer(rel, 1)?;
    let page = buffer_pool.get_page(&buffer);
    let page_header = PageHeader::new(&page)?;

    let page_data = page.borrow().bytes();

    let item_id_data = &page_data[PAGE_HEADER_SIZE..page_header.start_free_space as usize];

    let (item_id_data, _) = item_id_data.as_chunks::<ITEM_ID_SIZE>();

    for data in item_id_data {
        let item_id = bincode::deserialize::<ItemId>(&data.to_vec())?;
        let data = &page_data[item_id.offset as usize..(item_id.offset + item_id.length) as usize];
        f(data)?;
    }

    buffer_pool.unpin_buffer(buffer, false)?;

    Ok(())
}
