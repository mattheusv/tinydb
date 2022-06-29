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

/// Describe tuple attributes of single relation.
pub struct TupleDesc {
    /// List of attributes of a single tuple from a relation.
    pub attrs: Vec<PgAttribute>,
}

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(buffer_pool: &mut BufferPool, rel: &Relation, tuple: &HeapTuple) -> Result<()> {
    let buffer = freespace::get_page_with_free_space(buffer_pool, rel)?;
    let page = buffer_pool.get_page(&buffer);

    page_add_item(&page, &tuple.data)?;

    buffer_pool.unpin_buffer(buffer, true)?;

    Ok(())
}

pub fn heap_scan(
    buffer_pool: &mut BufferPool,
    rel: &Relation,
    tuple_desc: &TupleDesc,
) -> Result<()> {
    heap_iter(buffer_pool, rel, |mut tuple| -> Result<()> {
        match rel.borrow().rel_name.as_str() {
            "pg_class" => {
                let value = bincode::deserialize::<PgClass>(&tuple)?;
                println!("-> value: {:?}", value);
            }
            "pg_attribute" => {
                let value = bincode::deserialize::<PgAttribute>(&tuple)?;
                println!("-> value: {:?}", value);
            }
            _ => {
                for (i, attr) in tuple_desc.attrs.iter().enumerate() {
                    assert_eq!(
                        attr.attnum, i,
                        "Expected equal tuple desc attr num to be equal loop index"
                    );

                    if tuple.len() < attr.attlen {
                        // Means that the value does not exist on tuple.
                        println!("{} : NULL", attr.attname);
                    } else {
                        // Value exists on tuple, so deserialize it.
                        let attr_value = &tuple[..attr.attlen];
                        let value = bincode::deserialize::<i32>(&attr_value)?;
                        println!("{} : {}", attr.attname, value);

                        tuple = &tuple[attr.attlen..];
                    }
                }
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
