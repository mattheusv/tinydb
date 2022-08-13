use anyhow::{bail, Result};
use sqlparser::ast;
use std::io;

use crate::{
    access::{heap::heap_scan, heaptuple::HeapTuple, tuple::TupleDesc},
    catalog::{
        pg_attribute::{self, PgAttribute},
        pg_class::{self, PgClass},
        pg_database::{self, PgDatabase},
        pg_tablespace::{self, PgTablespace},
        Catalog,
    },
    encode::decode,
    errors::Error,
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

                        let tuple_desc =
                            catalog.tuple_desc_from_relation(buffer_pool, db_name, &rel_name)?;

                        let rel = RelationData::open(oid, db_data, db_name, &rel_name);
                        let tuples = heap_scan(buffer_pool, &rel)?;
                        print_relation_tuples(output, &rel, tuples, &tuple_desc)?;
                    }
                    _ => bail!(Error::UnsupportedOperation(table.relation.to_string())),
                }
            }
        }
        _ => bail!(Error::UnsupportedOperation(query.body.to_string())),
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
        pg_class::RELATION_NAME => {
            columns.append(&mut vec![String::from("oid"), String::from("relname")]);
            for tuple in tuples {
                let value = bincode::deserialize::<PgClass>(&tuple.data)?;
                records.push(vec![value.oid.to_string(), value.relname]);
            }
        }
        pg_attribute::RELATION_NAME => {
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
        pg_tablespace::RELATION_NAME => {
            columns.append(&mut vec![String::from("oid"), String::from("spcname")]);
            for tuple in tuples {
                let value = bincode::deserialize::<PgTablespace>(&tuple.data)?;
                records.push(vec![value.oid.to_string(), value.spcname]);
            }
        }
        pg_database::RELATION_NAME => {
            columns.extend_from_slice(&[
                String::from("oid"),
                String::from("datname"),
                String::from("dattablespace"),
            ]);
            for tuple in tuples {
                let value = bincode::deserialize::<PgDatabase>(&tuple.data)?;
                records.push(vec![
                    value.oid.to_string(),
                    value.datname,
                    value.dattablespace.to_string(),
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
                    let datum = tuple.get_attr(attr.attnum, tuple_desc);
                    match datum {
                        Some(datum) => {
                            tuple_values.push(decode(&datum)?);
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
