use anyhow::Result;
use std::mem::size_of;

use serde::{Deserialize, Serialize};

use crate::{Datum, Datums};

use super::tuple::TupleDesc;

/// Represents the size of a heap header tuple.
pub const HEAP_TUPLE_HEADER_SIZE: usize = size_of::<HeapTupleHeaderFields>();

/// Bit flag stored on t_infomask informing if a tuple has null values.
const HEAP_HASNULL: u16 = 0x0001;

/// Hold all fields that is writen on heap tuple header section on disk.
#[derive(Serialize, Deserialize, Debug)]
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
#[derive(Default, Debug)]
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
#[derive(Default, Debug)]
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
    /// Construct a heap tuple for the given vector of possible datum values.
    pub fn from_datums(values: Datums) -> Result<Self> {
        let mut heaptuple = Self::default();
        for datum in values.iter() {
            match datum {
                Some(datum) => {
                    heaptuple.header.t_bits.push(false);
                    heaptuple.header.fields.t_nattrs += 1;

                    heaptuple.data.extend_from_slice(datum);
                }
                None => {
                    // Add HEAP_HASNULL bit flag on heap header and add true on t_bits
                    // informing that the value of the attr is NULL.
                    heaptuple.header.fields.t_infomask |= HEAP_HASNULL;
                    heaptuple.header.t_bits.push(true);
                }
            }
        }

        if heaptuple.has_nulls() {
            // TODO: Find a better way to compute t_hoff
            let t_bits_data = bincode::serialize(&heaptuple.header.t_bits)?;
            heaptuple.header.fields.t_hoff += t_bits_data.len() as u16;
        }
        Ok(heaptuple)
    }

    /// Create a new heap tuple from raw tuple bytes.
    pub fn decode(tuple: &[u8]) -> Result<Self> {
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
    pub fn encode(&mut self) -> Result<Vec<u8>> {
        let mut tuple = bincode::serialize(&self.header.fields)?.to_vec();
        if self.has_nulls() {
            bincode::serialize_into(&mut tuple, &self.header.t_bits)?;
        }

        tuple.append(&mut self.data.clone());
        Ok(tuple)
    }

    /// Extract an attribute of a heap tuple and return it as a Datum.
    ///
    /// This works for either system or user attributes. The given attnum
    /// is properly range-checked.
    ///  
    ///  If the field in question has a NULL value, we return None. Otherwise return
    ///  Some<Datum> where Dataum represents the actual attribute value on heap.
    pub fn get_attr(&self, attnum: usize, tuple_desc: &TupleDesc) -> Option<Datum> {
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
        self.header.has_nulls()
    }

    /// Return true if the given attnum on tuple has a NULL value.
    fn attr_is_null(&self, attnum: usize) -> bool {
        self.has_nulls() && attnum <= self.header.t_bits.len() && self.header.t_bits[attnum - 1]
    }
}
