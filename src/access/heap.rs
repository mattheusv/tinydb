use std::mem::size_of;

use crate::{
    catalog::pg_attribute::PgAttribute,
    storage::{
        bufpage::{page_add_item, ItemId, PageHeader, ITEM_ID_SIZE, PAGE_HEADER_SIZE},
        freespace,
        rel::Relation,
        BufferPool,
    },
    Dataum,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Represents the size of a heap header tuple.
pub const HEAP_TUPLE_HEADER_SIZE: usize = size_of::<HeapTupleHeaderFields>();

/// Bit flag stored on t_infomask informing if a tuple has null values.
const HEAP_HASNULL: u16 = 0x0001;

/// Hold all fields that is writen on heap tuple header section on disk.
#[derive(Serialize, Deserialize)]
pub struct HeapTupleHeaderFields {
    /// Varios bit flags.
    pub t_infomask: u16,

    /// Number of attributes.
    pub t_nattrs: u16,

    /// Offset to user data.
    pub t_hoff: u16,
}

impl Default for HeapTupleHeaderFields {
    fn default() -> Self {
        Self {
            t_infomask: u16::default(),
            t_nattrs: u16::default(),
            t_hoff: HEAP_TUPLE_HEADER_SIZE as u16,
        }
    }
}

/// Hold the fixed header fields and optinal fields that are written on heap tuple data
/// section on disk.
#[derive(Default)]
pub struct HeapTupleHeader {
    /// Fixed heap tuple fields.
    pub fields: HeapTupleHeaderFields,

    /// Bitmap of NULLs.
    ///
    /// The bitmap is *not* stored if t_infomask shows that there
    /// are no nulls in the tuple.
    pub t_bits: Vec<bool>,
}

/// HeapTuple is an in-memory data structure that points to a tuple on some page.
#[derive(Default)]
pub struct HeapTuple {
    /// Heap tuple header fields.
    pub header: HeapTupleHeader,

    /// Actual heap tuple data (header NOT included).
    pub data: Vec<u8>,
}

impl HeapTupleHeader {
    /// Return true if heap tuple has null values.
    pub fn has_nulls(&self) -> bool {
        self.fields.t_infomask & HEAP_HASNULL != 0
    }
}

impl HeapTuple {
    /// Create a new heap tuple from raw tuple bytes.
    pub fn from_raw_tuple(tuple: &[u8]) -> Result<Self> {
        let mut header = HeapTupleHeader {
            fields: bincode::deserialize(&tuple[0..HEAP_TUPLE_HEADER_SIZE])?,
            t_bits: Vec::new(),
        };
        let t_hoff = header.fields.t_hoff as usize;

        if header.has_nulls() {
            header.t_bits = bincode::deserialize(&tuple[HEAP_TUPLE_HEADER_SIZE..t_hoff])?;
        }

        Ok(Self {
            header,
            data: tuple[t_hoff..].to_vec(),
        })
    }

    /// Return the heap tuple representation in raw bytes.
    ///
    // TODO: Try to compute t_hoff outside of this function to avoid require a
    // mutable reference to self.
    pub fn to_raw_tuple(&mut self) -> Result<Vec<u8>> {
        let mut t_bits = None;

        if self.has_nulls() {
            let t_bits_data = bincode::serialize(&self.header.t_bits)?;
            self.header.fields.t_hoff += t_bits_data.len() as u16;
            t_bits = Some(t_bits_data);
        }

        // TODO: Try to avoid allocation here.
        let mut tuple = bincode::serialize(&self.header.fields)?.to_vec();

        if self.has_nulls() {
            tuple.append(&mut t_bits.unwrap().to_vec());
        }

        tuple.append(&mut self.data.clone());
        Ok(tuple)
    }

    /// Add a new attribute value on tuple.
    pub fn append_data(&mut self, data: &mut Vec<u8>) {
        self.data.append(data);
        self.header.t_bits.push(false);
        self.header.fields.t_nattrs += 1;
    }

    /// Extract an attribute of a heap tuple and return it as a Datum.
    ///
    /// This works for either system or user attributes. The given attnum
    /// is properly range-checked.
    ///  
    ///  If the field in question has a NULL value, we return None. Otherwise return
    ///  Some<Dataum> where Dataum represents the actual attribute value on heap.
    pub fn get_attr(&self, attnum: usize, tuple_desc: &TupleDesc) -> Option<Dataum> {
        if attnum > tuple_desc.attrs.len() || self.attr_is_null(attnum) {
            // Attribute does not exists on tuple.
            return None;
        }

        let attr = &tuple_desc.attrs[attnum - 1];

        // Iterate over all tuple attributes to get the correclty offset of the required attribute.
        let mut offset = 0;
        for attr in &tuple_desc.attrs {
            if attr.attnum == attnum {
                break;
            }
            if self.attr_is_null(attr.attnum) {
                // Skip NULL values.
                continue;
            }

            offset += attr.attlen;
        }

        Some(self.data[offset..offset + attr.attlen].to_vec())
    }

    /// Return true if heap tuple has null values.
    pub fn has_nulls(&self) -> bool {
        self.header.fields.t_infomask & HEAP_HASNULL != 0
    }

    /// Add HEAP_HASNULL bit flag on heap header.
    pub fn add_has_nulls_flag(&mut self) {
        self.header.t_bits.push(true);
        self.header.fields.t_infomask |= HEAP_HASNULL;
    }

    /// Return true if the given attnum on tuple has a NULL value.
    fn attr_is_null(&self, attnum: usize) -> bool {
        self.has_nulls() && attnum <= self.header.t_bits.len() && self.header.t_bits[attnum - 1]
    }
}

/// Describe tuple attributes of single relation.
pub struct TupleDesc {
    /// List of attributes of a single tuple from a relation.
    pub attrs: Vec<PgAttribute>,
}

/// Insert a new tuple into a heap page of the given relation.
pub fn heap_insert(
    buffer_pool: &mut BufferPool,
    rel: &Relation,
    tuple: &mut HeapTuple,
) -> Result<()> {
    let buffer = freespace::get_page_with_free_space(buffer_pool, rel)?;
    let page = buffer_pool.get_page(&buffer);

    page_add_item(&page, &tuple.to_raw_tuple()?)?;

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
        let tuple = HeapTuple::from_raw_tuple(data)?;

        f(tuple)?;
    }

    buffer_pool.unpin_buffer(buffer, false)?;

    Ok(())
}
