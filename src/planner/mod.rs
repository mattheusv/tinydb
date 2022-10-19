use anyhow::{bail, Result};
use core::fmt;
use sqlparser::ast::{self, SetExpr, TableFactor};
use std::rc::Rc;

use crate::{
    access::{self, heap::HeapScanner, heaptuple::TupleDesc},
    catalog::{self, pg_attribute::PgAttribute, pg_class::PgClass},
    relation::Relation,
    sql::SQLError,
    storage::BufferPool,
    Oid, INVALID_OID,
};

/// Information needed to project a query output.
pub struct ProjectionState {
    /// Projection output attributes of query.
    ///
    /// Note that could be in a different order that is stored on
    /// page, since it represents the output from a query. use the
    /// tuple_desc_ field if the attributes order on page is required.
    pub projection: Vec<PgAttribute>,

    /// Tuple descriptor from a relation heap tuple. The tuple descriptor
    /// attributes is in the same order that is stored on page tuple.
    pub tuple_desc: Rc<TupleDesc>,

    pub child: Plan,
}

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
    /// Projection plan node.
    Projection { state: Box<ProjectionState> },

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
    pub fn create(buffer_pool: BufferPool, db_oid: &Oid, query: Box<ast::Query>) -> Result<Plan> {
        let plan = match query.body {
            SetExpr::Select(select) => create_plan_from_select(buffer_pool, db_oid, &select)?,
            _ => bail!(SQLError::Unsupported(query.body.to_string())),
        };
        Ok(plan)
    }
}

fn create_plan_from_select(
    buffer_pool: BufferPool,
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

            let mut projection = Vec::with_capacity(select.projection.len());

            for item in &select.projection {
                match item {
                    ast::SelectItem::UnnamedExpr(expr) => match expr {
                        ast::Expr::Identifier(ident) => {
                            match tuple_desc
                                .attrs
                                .iter()
                                .find(|attr| attr.attname == ident.value)
                            {
                                Some(attr) => projection.push(attr.clone()),
                                None => bail!(
                                    "Attribute {} does not exists on relation {}",
                                    ident.value,
                                    rel_name
                                ),
                            }
                        }
                        _ => bail!(SQLError::Unsupported(from.relation.to_string())),
                    },
                    ast::SelectItem::Wildcard => {
                        projection.extend_from_slice(&tuple_desc.attrs);
                    }
                    _ => bail!(SQLError::Unsupported(from.relation.to_string())),
                }
            }

            Ok(Plan {
                node_type: PlanNodeType::Projection {
                    state: Box::new(ProjectionState {
                        projection,
                        tuple_desc: tuple_desc.clone(),
                        child: create_seq_scan(
                            buffer_pool,
                            db_oid,
                            &rel_name,
                            &pg_class,
                            tuple_desc,
                        )?,
                    }),
                },
            })
        }
        _ => bail!(SQLError::Unsupported(from.relation.to_string())),
    }
}

fn create_seq_scan(
    buffer_pool: BufferPool,
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

impl fmt::Display for PlanNodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanNodeType::Projection { .. } => write!(f, "Projection"),
            PlanNodeType::SeqScan { .. } => write!(f, "SeqScan"),
        }
    }
}
