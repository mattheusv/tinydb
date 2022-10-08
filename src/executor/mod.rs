use std::rc::Rc;

use anyhow::Result;

use crate::{
    access::heaptuple::{HeapTuple, TupleDesc},
    planner::{Plan, PlanNodeType},
};

/// A plan tree executor. Contains function to execute each type of PlanNodeType.
pub struct Executor {}

impl Executor {
    /// Create a new executor using the given buffer pool to fetch page buffers.
    pub fn new() -> Self {
        Self {}
    }

    /// Main entrypoint of a planner executor, it recursivily exec all nodes
    /// for the planer and return a tuple table result with all operations
    /// of the planner performed.
    pub fn exec(&self, node: &mut Plan) -> Result<TupleTable> {
        match &mut node.node_type {
            PlanNodeType::SeqScan { ref mut state } => {
                let mut tuple_table = TupleTable {
                    tuple_desc: state.tuple_desc.clone(),
                    tuples: Vec::new(),
                };

                // Iterate over all tuples until consume the entire page.
                loop {
                    match state.heap_scanner.next_tuple()? {
                        Some(tuple) => {
                            tuple_table.tuples.push(tuple);
                        }
                        None => break,
                    }
                }

                Ok(tuple_table)
            }
        }
    }
}

/// The planner executor store tuples in a tuple table which is essentially a list of independent
/// tuples.
#[derive(Default)]
pub struct TupleTable {
    /// Tuple description for a list of tuples.
    pub tuple_desc: Rc<TupleDesc>,

    /// Per row tuple values.
    pub tuples: Vec<HeapTuple>,
}
