use anyhow::{bail, Result};
use sqlparser::ast::{self, SetExpr, TableFactor};
use std::{cell::RefCell, rc::Rc};

use crate::{
    access::{self, heap::HeapScanner, heaptuple::TupleDesc},
    catalog::{self, pg_class::PgClass},
    relation::Relation,
    sql::SQLError,
    storage::BufferPool,
    Oid, INVALID_OID,
};

/// Sequential scan information needed by executor.
pub struct SeqScanState {
    /// Tuple description of relation being used by planner executor.
    pub tuple_desc: Rc<TupleDesc>,

    /// Current relation used by executor to operate.
    pub relation: Relation,

    /// Iterator used to fetch tuples.
    pub heap_scanner: HeapScanner,
}

/// Types of a plan node on plan tree.
pub enum PlanNodeType {
    /// Sequential scan plan node.
    SeqScan { state: SeqScanState },
}

/// A tree of nodes to be executed.
pub struct Plan {
    /// Type of plan node.
    pub node_type: PlanNodeType,
}

impl Plan {
    /// Create a new plan for the given parsed query.
    pub fn create(
        buffer_pool: Rc<RefCell<BufferPool>>,
        db_oid: &Oid,
        query: Box<ast::Query>,
    ) -> Result<Plan> {
        let plan = match query.body {
            SetExpr::Select(select) => create_plan_from_select(buffer_pool, db_oid, &select)?,
            _ => bail!(SQLError::Unsupported(query.body.to_string())),
        };
        Ok(plan)
    }
}

fn create_plan_from_select(
    buffer_pool: Rc<RefCell<BufferPool>>,
    db_oid: &Oid,
    select: &ast::Select,
) -> Result<Plan> {
    if select.from.len() > 1 {
        bail!(SQLError::Unsupported(
            "Can not use multiple expressions on FROM".to_string(),
        ));
    }

    let from = &select.from[0];

    match &from.relation {
        TableFactor::Table { name, .. } => {
            let rel_name = name.0[0].to_string();
            let pg_class = catalog::get_pg_class_relation(buffer_pool.clone(), db_oid, &rel_name)?;

            let tuple_desc = Rc::new(catalog::tuple_desc_from_relation(
                buffer_pool.clone(),
                db_oid,
                &rel_name,
            )?);

            Ok(create_seq_scan(
                buffer_pool,
                db_oid,
                &rel_name,
                &pg_class,
                tuple_desc,
            )?)
        }
        _ => bail!(SQLError::Unsupported(from.relation.to_string())),
    }
}

fn create_seq_scan(
    buffer_pool: Rc<RefCell<BufferPool>>,
    db_oid: &Oid,
    rel_name: &str,
    pg_class_rel: &PgClass,
    tuple_desc: Rc<TupleDesc>,
) -> Result<Plan> {
    let relation = access::open_relation(
        pg_class_rel.oid,
        pg_class_rel.reltablespace,
        if pg_class_rel.relisshared {
            &INVALID_OID
        } else {
            db_oid
        },
        &rel_name,
    );
    Ok(Plan {
        node_type: PlanNodeType::SeqScan {
            state: SeqScanState {
                tuple_desc,
                relation: relation.clone(),
                heap_scanner: HeapScanner::new(buffer_pool, &relation)?,
            },
        },
    })
}
