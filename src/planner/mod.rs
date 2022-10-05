use anyhow::{bail, Result};
use sqlparser::ast::{self, SetExpr, TableFactor};
use std::{cell::RefCell, rc::Rc};

use crate::{
    access::{self, heaptuple::TupleDesc},
    catalog,
    relation::Relation,
    sql::SQLError,
    storage::BufferPool,
    Oid, INVALID_OID,
};

/// Types of a plan node on plan tree.
pub enum PlanNodeType {
    /// Sequential scan plan node.
    SeqScan,
}

/// A tree of nodes to be executed.
pub struct Plan {
    /// Type of plan node.
    pub node_type: PlanNodeType,

    /// Tuple description of relation being used by planner executor.
    pub tuple_desc: Rc<TupleDesc>,

    /// Current relation used by planner executor to operate.
    pub relation: Relation,
}

impl Plan {
    /// Create a new plan for the given parsed query.
    pub fn create(
        buffer_pool: Rc<RefCell<BufferPool>>,
        db_oid: &Oid,
        query: Box<ast::Query>,
    ) -> Result<Rc<Plan>> {
        match query.body {
            SetExpr::Select(select) => create_plan_from_select(buffer_pool, db_oid, &select),
            _ => bail!(SQLError::Unsupported(query.body.to_string())),
        }
    }
}

fn create_plan_from_select(
    buffer_pool: Rc<RefCell<BufferPool>>,
    db_oid: &Oid,
    select: &ast::Select,
) -> Result<Rc<Plan>> {
    if select.from.len() > 1 {
        bail!(SQLError::Unsupported(
            "Can not use multiple expressions on FROM".to_string(),
        ));
    }
    create_plan_from_table_with_join(buffer_pool, db_oid, &select.from[0])
}

fn create_plan_from_table_with_join(
    buffer_pool: Rc<RefCell<BufferPool>>,
    db_oid: &Oid,
    from: &ast::TableWithJoins,
) -> Result<Rc<Plan>> {
    match &from.relation {
        TableFactor::Table { name, .. } => {
            create_seq_scan(buffer_pool.clone(), db_oid, &name.0[0].to_string())
        }
        _ => bail!(SQLError::Unsupported(from.relation.to_string())),
    }
}

fn create_seq_scan(
    buffer_pool: Rc<RefCell<BufferPool>>,
    db_oid: &Oid,
    rel_name: &str,
) -> Result<Rc<Plan>> {
    let pg_class_rel = catalog::get_pg_class_relation(buffer_pool.clone(), db_oid, &rel_name)?;

    let tuple_desc = catalog::tuple_desc_from_relation(buffer_pool, db_oid, &rel_name)?;

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
    Ok(Rc::new(Plan {
        node_type: PlanNodeType::SeqScan,
        relation: rel,
        tuple_desc: Rc::new(tuple_desc),
    }))
}
