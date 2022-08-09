use anyhow::Result;
use sqlparser::ast;
use std::io;

use crate::{
    access::{
        heap::{heap_scan, HeapTuple},
        tuple::TupleDesc,
    },
    catalog::{pg_attribute::PgAttribute, pg_class::PgClass, Catalog},
    storage::{
        rel::{Relation, RelationData},
        BufferPool,
    },
};

pub fn select(
    buffer_pool: &mut BufferPool,
    catalog: &Catalog,
    db_data: &str,
    output: &mut dyn io::Write,
    db_name: &str,
    query: Box<ast::Query>,
) -> Result<()> {
    match query.body {
        ast::SetExpr::Select(select) => {
            for table in select.from {
                match table.relation {
                    ast::TableFactor::Table { name, .. } => {
                        let rel_name = name.0[0].to_string();
                        let oid = catalog.get_oid_relation(buffer_pool, db_name, &rel_name)?;

                        let rel_attrs = catalog.get_attributes_from_relation(
                            buffer_pool,
                            db_name,
                            &rel_name,
                        )?;

                        let tuple_desc = TupleDesc { attrs: rel_attrs };

                        let rel = RelationData::open(oid, db_data, db_name, &rel_name);
                        let tuples = heap_scan(buffer_pool, &rel)?;
                        print_relation_tuples(output, &rel, tuples, &tuple_desc)?;
                    }
                    _ => todo!(),
                }
            }
        }
        _ => todo!(),
    }
    Ok(())
}

fn print_relation_tuples(
    output: &mut dyn io::Write,
    rel: &Relation,
    tuples: Vec<HeapTuple>,
    tuple_desc: &TupleDesc,
) -> Result<()> {
    let mut columns = Vec::new();
    let mut records = Vec::new();

    match rel.borrow().rel_name.as_str() {
        "pg_class" => {
            columns.append(&mut vec![String::from("oid"), String::from("relname")]);
            for tuple in tuples {
                let value = bincode::deserialize::<PgClass>(&tuple.data)?;
                records.push(vec![value.oid.to_string(), value.relname]);
            }
        }
        "pg_attribute" => {
            columns.append(&mut vec![
                String::from("attrelid"),
                String::from("attname"),
                String::from("attnum"),
                String::from("attlen"),
            ]);
            for tuple in tuples {
                let value = bincode::deserialize::<PgAttribute>(&tuple.data)?;
                records.push(vec![
                    value.attrelid.to_string(),
                    value.attname,
                    value.attnum.to_string(),
                    value.attlen.to_string(),
                ]);
            }
        }
        _ => {
            for attr in &tuple_desc.attrs {
                columns.push(attr.attname.clone());
            }

            for tuple in tuples {
                let mut tuple_values = Vec::new();
                for attr in tuple_desc.attrs.iter() {
                    let dataum = tuple.get_attr(attr.attnum, tuple_desc);
                    match dataum {
                        Some(dataum) => {
                            let value = bincode::deserialize::<i32>(&dataum)?;
                            tuple_values.push(value.to_string());
                        }
                        None => {
                            tuple_values.push(String::from("NULL"));
                        }
                    }
                }
                records.push(tuple_values);
            }
        }
    }

    let mut table = tabled::builder::Builder::default().set_columns(columns);

    for record in records {
        table = table.add_record(record);
    }

    let table = table.build().with(tabled::Style::psql());

    writeln!(output, "{}", table)?;

    Ok(())
}
