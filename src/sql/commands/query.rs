use anyhow::Result;
use sqlparser::ast;
use std::{cell::RefCell, io, rc::Rc};

use crate::{
    access::heaptuple::{HeapTuple, TupleDesc},
    executor::Executor,
    planner::Plan,
    sql::encode::decode,
    storage::BufferPool,
    Oid,
};

pub fn select(
    buffer_pool: Rc<RefCell<BufferPool>>,
    output: &mut dyn io::Write,
    db_oid: &Oid,
    query: Box<ast::Query>,
) -> Result<()> {
    let plan = Plan::create(buffer_pool.clone(), db_oid, query)?;
    let executor = Executor::new(buffer_pool.clone());
    let tuple_table = executor.exec(&plan)?;
    print_relation_tuples(output, tuple_table.tuples, &tuple_table.tuple_desc)?;
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
