use anyhow::{anyhow, Result};
use std::mem::size_of;

use serde::{Deserialize, Serialize};

use crate::{catalog::pg_attribute::PgAttribute, sql::encode::Varlena, Datum, Datums};

/// Represents the size of a heap header tuple.
pub const HEAP_TUPLE_HEADER_SIZE: usize = size_of::<HeapTupleHeaderFields>();

/// Bit flag stored on t_infomask informing if a tuple has null values.
const HEAP_HASNULL: u16 = 0x0001;

/// Bit flag stored on t_infomask informing if a tuple has variable-width attribute(s).
const HEAP_HASVARWIDTH: u16 = 0x0002;

/// Describe the structure of tuples. Basically it holds the columns of tables.
pub struct TupleDesc {
    /// Columns of table.
    pub attrs: Vec<PgAttribute>,
}

impl Default for TupleDesc {
    fn default() -> Self {
        Self { attrs: Vec::new() }
    }
}

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

    /// Return true if heap tuple has varlena values.
    pub fn has_var_width(&self) -> bool {
        self.fields.t_infomask & HEAP_HASVARWIDTH != 0
    }
}

impl HeapTuple {
    /// Create a new heap tuple with the given data and default header values.
    pub fn with_default_header<T>(data: T) -> Result<Self>
    where
        T: serde::Serialize,
    {
        Ok(Self {
            header: HeapTupleHeader::default(),
            data: bincode::serialize(&data)?,
        })
    }
    /// Construct a heap tuple for the given vector of possible datum values.
    ///
    /// The tuple desc attributes should be aligned with datum values index, wich
    /// means that values[i] should references tuple_desc.attrs[i].
    pub fn from_datums(values: Datums, tuple_desc: &TupleDesc) -> Result<Self> {
        let mut heaptuple = Self::default();
        for (attrnum, datum) in values.iter().enumerate() {
            let attr = tuple_desc
                .attrs
                .get(attrnum)
                .ok_or_else(|| anyhow!("Can not get pg attribute from {}", attrnum))?;

            match datum {
                Some(datum) => {
                    if attr.attlen < 0 {
                        // Add HEAP_HASVARWIDTH flag on tuple header to inform that
                        // the tuple has varlena fields.
                        heaptuple.header.fields.t_infomask |= HEAP_HASVARWIDTH;
                    }

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

        if heaptuple.header.has_nulls() {
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
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut tuple = bincode::serialize(&self.header.fields)?.to_vec();
        if self.header.has_nulls() {
            bincode::serialize_into(&mut tuple, &self.header.t_bits)?;
        }

        tuple.extend_from_slice(&self.data);
        Ok(tuple)
    }

    /// Extract an attribute of a heap tuple and return it as a Datum.
    ///
    /// This works for either system or user attributes. The given attnum
    /// is properly range-checked.
    ///  
    ///  If the field in question has a NULL value, we return None. Otherwise return
    ///  Some<Datum> where Dataum represents the actual attribute value on heap.
    pub fn get_attr(&self, attnum: usize, tuple_desc: &TupleDesc) -> Result<Option<Datum>> {
        if attnum > tuple_desc.attrs.len() || self.attr_is_null(attnum) {
            // Attribute does not exists on tuple.
            return Ok(None);
        }

        // Iterate over all tuple attributes to get the correclty offset of the required attribute.
        let mut off_start = 0;
        let mut off_end = 0;
        for attr in &tuple_desc.attrs {
            if self.attr_is_null(attr.attnum) && attr.attnum != attnum {
                // Skip NULL values.
                continue;
            }

            if attr.attlen > 0 {
                off_end += attr.attlen as usize;
            } else {
                // If we don't know the size of attribute value we
                // decode a varlena struct to get the actual size of
                // field.
                let varlena = bincode::deserialize::<Varlena>(&self.data[off_start..])?;

                // Return the varlena value if its the field that was fetched
                if attr.attnum == attnum {
                    return Ok(Some(varlena.v_data));
                }

                // Otherwise, just sum the total size of varlena tuple field.
                off_end += varlena.len();
            }

            if attr.attnum == attnum {
                if self.attr_is_null(attr.attnum) {
                    return Ok(None);
                }
                return Ok(Some(self.data[off_start..off_end].to_vec()));
            }

            off_start = off_end;
        }

        Ok(None)
    }

    /// Return true if the given attnum on tuple has a NULL value.
    fn attr_is_null(&self, attnum: usize) -> bool {
        self.header.has_nulls()
            && attnum <= self.header.t_bits.len()
            && self.header.t_bits[attnum - 1]
    }
}
