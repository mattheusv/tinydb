use anyhow::Result;
use sqlparser::ast::{self, ObjectName};

use crate::{
    access::{heap::heap_insert, heaptuple::HeapTuple},
    catalog::Catalog,
    encode::encode,
    storage::{rel::RelationData, BufferPool},
    Datums,
};

pub fn insert_into(
    buffer_pool: &mut BufferPool,
    catalog: &Catalog,
    db_data: &str,
    db_name: &str,
    table_name: ObjectName,
    columns: Vec<ast::Ident>,
    source: Box<ast::Query>,
) -> Result<()> {
    let rel_name = table_name.0[0].to_string();
    let oid = catalog.get_oid_relation(buffer_pool, db_name, &rel_name)?;

    let rel = RelationData::open(oid, db_data, db_name, &rel_name);

    match source.body {
        ast::SetExpr::Values(values) => {
            let tuple_desc = catalog.tuple_desc_from_relation(buffer_pool, db_name, &rel_name)?;

            let mut heap_values = Datums::default();

            // Iterate over all rows on insert to write new tuples.
            for row in &values.0 {
                assert_eq!(
                    row.len(),
                    columns.len(),
                    "INSERT has more expressions than target columns"
                );

                // Iterate over relation attrs and try to find the value that is being inserted
                // for each attr. If the value does not exists a NULL value should be inserted
                // on tuple header t_bits array.
                for attr in &tuple_desc.attrs {
                    // TODO: Find a better way to lookup the attr value that is being inserted
                    let index = columns.iter().position(|ident| ident.value == attr.attname);
                    match index {
                        Some(index) => {
                            let value = &row[index];
                            match value {
                                ast::Expr::Value(value) => {
                                    encode(&mut heap_values, &value)?;
                                }
                                _ => todo!(),
                            }
                        }
                        None => {
                            heap_values.push(None);
                        }
                    }
                }
            }

            heap_insert(buffer_pool, &rel, &mut HeapTuple::from_datums(heap_values)?)?;
        }
        _ => todo!(),
    }

    Ok(())
}
