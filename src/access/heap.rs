use std::mem::size_of;

use crate::{
    catalog::pg_attribute::PgAttribute,
    storage::{
        bufpage::{page_add_item, ItemId, PageHeader, ITEM_ID_SIZE, PAGE_HEADER_SIZE},
        freespace,
        rel::Relation,
        BufferPool,
    },
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Represents the size of a heap header tuple.
pub const HEAP_TUPLE_HEADER_SIZE: usize = size_of::<HeapTupleHeader>();

#[derive(Serialize, Deserialize)]
pub struct HeapTupleHeader {}

/// HeapTuple is an in-memory data structure that points to a tuple on some page.
pub struct HeapTuple {
    /// Heap tuple header fields.
    pub header: HeapTupleHeader,

    /// Actual heap tuple data (header NOT included).
    pub data: Vec<u8>,
}

impl HeapTuple {
    /// Create a new heap tuple from raw tuple bytes.
    pub fn from_raw_tuple(tuple: &[u8]) -> Result<Self> {
        Ok(Self {
            header: bincode::deserialize(&tuple[0..HEAP_TUPLE_HEADER_SIZE])?,
            data: tuple[HEAP_TUPLE_HEADER_SIZE..].to_vec(),
        })
    }

    /// Return the heap tuple representation in raw bytes.
    pub fn to_raw_tuple(&self) -> Result<Vec<u8>> {
        // TODO: Try to avoid allocation here.
        let mut tuple = bincode::serialize(&self.header)?.to_vec();
        tuple.append(&mut self.data.clone());
        Ok(tuple)
    }
}

/// Describe tuple attributes of single relation.
pub struct TupleDesc {
    /// List of attributes of a single tuple from a relation.
    pub attrs: Vec<PgAttribute>,
}

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(buffer_pool: &mut BufferPool, rel: &Relation, tuple: &HeapTuple) -> Result<()> {
    let buffer = freespace::get_page_with_free_space(buffer_pool, rel)?;
    let page = buffer_pool.get_page(&buffer);

    page_add_item(&page, &tuple.to_raw_tuple()?)?;

    buffer_pool.unpin_buffer(buffer, true)?;

    Ok(())
}

pub fn heap_scan(buffer_pool: &mut BufferPool, rel: &Relation) -> Result<Vec<HeapTuple>> {
    let mut tuples = Vec::new();
    heap_iter(buffer_pool, rel, |tuple| -> Result<()> {
        tuples.push(HeapTuple::from_raw_tuple(&tuple)?);
        Ok(())
    })?;
    Ok(tuples)
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

    // Get a reference to the raw data of item_id_data .
    let item_id_data = &page_data[PAGE_HEADER_SIZE..page_header.start_free_space as usize];

    // Split the raw item_id_data to a list of ItemId.
    let (item_id_data, _) = item_id_data.as_chunks::<ITEM_ID_SIZE>();

    for data in item_id_data {
        // Deserialize a single ItemId from the list item_id_data.
        let item_id = bincode::deserialize::<ItemId>(&data.to_vec())?;

        // Slice the raw page to get a refenrece to a tuple inside the page.
        let data = &page_data[item_id.offset as usize..(item_id.offset + item_id.length) as usize];
        f(data)?;
    }

    buffer_pool.unpin_buffer(buffer, false)?;

    Ok(())
}
