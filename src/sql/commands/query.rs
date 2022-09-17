use anyhow::{bail, Result};
use sqlparser::ast;
use std::{cell::RefCell, io, rc::Rc};

use crate::{
    access::{self, heap::heap_scan, heaptuple::HeapTuple, tuple::TupleDesc},
    catalog,
    sql::{commands::SQLError, encode::decode},
    storage::BufferPool,
    Oid, INVALID_OID,
};

pub fn select(
    buffer_pool: Rc<RefCell<BufferPool>>,
    output: &mut dyn io::Write,
    db_oid: &Oid,
    query: Box<ast::Query>,
) -> Result<()> {
    match query.body {
        ast::SetExpr::Select(select) => {
            for table in select.from {
                match table.relation {
                    ast::TableFactor::Table { name, .. } => {
                        let rel_name = name.0[0].to_string();
                        let pg_class_rel = catalog::get_pg_class_relation(
                            &mut buffer_pool.borrow_mut(),
                            db_oid,
                            &rel_name,
                        )?;

                        let tuple_desc = catalog::tuple_desc_from_relation(
                            &mut buffer_pool.borrow_mut(),
                            db_oid,
                            &rel_name,
                        )?;

                        let rel = access::open_relation(
                            pg_class_rel.oid,
                            pg_class_rel.reltablespace,
                            if pg_class_rel.relisshared {
                                &INVALID_OID
                            } else {
                                db_oid
                            },
                            &rel_name,
                        );
                        let tuples = heap_scan(&mut buffer_pool.borrow_mut(), &rel)?;
                        print_relation_tuples(output, tuples, &tuple_desc)?;
                    }
                    _ => bail!(SQLError::Unsupported(table.relation.to_string())),
                }
            }
        }
        _ => bail!(SQLError::Unsupported(query.body.to_string())),
    }
    Ok(())
}

fn print_relation_tuples(
    output: &mut dyn io::Write,
    tuples: Vec<HeapTuple>,
    tuple_desc: &TupleDesc,
) -> Result<()> {
    let mut columns = Vec::new();
    let mut records = Vec::new();

    for attr in &tuple_desc.attrs {
        columns.push(attr.attname.clone());
    }

    for tuple in tuples {
        let mut tuple_values = Vec::new();
        for attr in tuple_desc.attrs.iter() {
            let datum = tuple.get_attr(attr.attnum, tuple_desc)?;
            match datum {
                Some(datum) => {
                    tuple_values.push(decode(&datum, attr.atttypid)?);
                }
                None => {
                    tuple_values.push(String::from("NULL"));
                }
            }
        }
        records.push(tuple_values);
    }

    let mut table = tabled::builder::Builder::default().set_columns(columns);

    for record in records {
        table = table.add_record(record);
    }

    let table = table.build().with(tabled::Style::psql());

    writeln!(output, "{}", table)?;

    Ok(())
}
