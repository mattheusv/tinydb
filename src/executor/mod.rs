use std::sync::Arc;

use anyhow::{bail, Result};

use crate::{
    access::heaptuple::{HeapTuple, TupleDesc},
    planner::{Plan, PlanNodeType},
    NullableDatum,
};

/// A plan tree executor. Contains function to execute each type of PlanNodeType.
pub struct Executor;

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
            PlanNodeType::Projection { state } => {
                let mut tuple_table = TupleTable {
                    tuple_desc: Arc::new(TupleDesc {
                        attrs: state.projection.clone(),
                    }),
                    values: Vec::new(),
                };

                loop {
                    match self.fetch_next_tuple(&mut state.child)? {
                        Some(tuple) => {
                            let mut slot = Vec::new();

                            for attr in &tuple_table.tuple_desc.attrs {
                                // Use the tuple descriptor from projection state since
                                // it is in the same order that is stored on disk page.
                                let datum = tuple.get_attr(attr.attnum, &state.tuple_desc)?;
                                slot.push(datum);
                            }
                            tuple_table.values.push(slot);
                        }
                        None => break,
                    }
                }

                Ok(tuple_table)
            }

            _ => bail!("Unexpected root plan node of type {}", node.node_type),
        }
    }

    fn fetch_next_tuple(&self, node: &mut Plan) -> Result<Option<HeapTuple>> {
        match &mut node.node_type {
            PlanNodeType::SeqScan { ref mut state } => state.heap_scanner.next_tuple(),
            _ => bail!(
                "Unsupported plan node type {} to fetch next page",
                node.node_type
            ),
        }
    }
}

/// The planner executor store tuples in a tuple table which is essentially a list of independent
/// tuple table slots.
#[derive(Default)]
pub struct TupleTable {
    /// Tuple descriptor of tuple table output values.
    pub tuple_desc: Arc<TupleDesc>,

    /// Per row attribute values. Each Datums store the all attributes of a single
    /// row on the same order from tuple_desc.attrs.
    pub values: Vec<Vec<NullableDatum>>,
}
