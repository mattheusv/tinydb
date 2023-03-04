use bytes::{Buf, BufMut};
use std::marker::PhantomData;

use crate::catalog::pg_attribute::PgAttribute;
use anyhow::Result;

pub struct RelationEncoder<'a, R> {
    relation: &'a R,
}

impl<'a, R> RelationEncoder<'a, R> {
    pub fn new(relation: &'a R) -> Self {
        Self { relation }
    }
}

impl RelationEncoder<'_, PgAttribute> {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encode_to = Vec::new();
        encode_to.put_u64(self.relation.attrelid);

        let attname = self.relation.attname.as_bytes();
        encode_to.put_u32(attname.len() as u32);
        encode_to.put_slice(attname);

        encode_to.put_i32(self.relation.attnum as i32);
        encode_to.put_i64(self.relation.attlen);
        encode_to.put_u64(self.relation.atttypid);

        Ok(encode_to)
    }
}

// impl RelationEncoder<'_, PgClass> {
//     pub fn encode(&self) -> Result<Vec<u8>> {
//         todo!()
//     }
// }

pub struct RelationDecoder<R> {
    _rel: PhantomData<R>,
}

impl RelationDecoder<PgAttribute> {
    pub fn decode(tuple: &Vec<u8>) -> Result<PgAttribute> {
        let mut tuple = &tuple[..];

        let attrelid = tuple.get_u64();

        let attname_size = tuple.get_u32();
        let attname = &tuple.chunk()[..attname_size as usize];
        let attname = String::from_utf8(attname.to_vec())?; // TODO: Avoid allocation
        tuple.advance(attname_size as usize);

        let attnum = tuple.get_i32() as usize;
        let attlen = tuple.get_i64();
        let atttypid = tuple.get_u64();

        Ok(PgAttribute {
            attrelid,
            attname,
            attnum,
            attlen,
            atttypid,
        })
    }
}

// impl RelationDecoder<PgClass> {
//     pub fn decode(tuple: &Vec<u8>) -> Result<PgClass> {
//         todo!()
//     }
// }
