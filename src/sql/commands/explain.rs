use anyhow::{bail, Result};
use sqlparser::ast::Statement;
use std::{cell::RefCell, io, rc::Rc};

use crate::{
    planner::{Plan, PlanNodeType},
    sql::commands::SQLError,
    storage::BufferPool,
    Oid,
};

pub fn explain(
    buffer_pool: Rc<RefCell<BufferPool>>,
    output: &mut dyn io::Write,
    db_oid: &Oid,
    stmt: Statement,
) -> Result<()> {
    match stmt {
        Statement::Query(query) => {
            let plan = Plan::create(buffer_pool.clone(), db_oid, query)?;
            print_explain(&plan, output)
        }
        _ => bail!(SQLError::Unsupported(stmt.to_string())),
    }
}

fn print_explain(plan: &Plan, output: &mut dyn io::Write) -> Result<()> {
    write!(
        output,
        "        QUERY PLAN
-----------------------------
"
    )?;
    match plan.node_type {
        PlanNodeType::SeqScan => {
            write!(output, "Seq Scan on {}\n", plan.relation.borrow().rel_name)?;
        }
    };
    write!(output, "\n")?;
    Ok(())
}
