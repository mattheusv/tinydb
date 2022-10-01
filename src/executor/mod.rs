use std::{cell::RefCell, rc::Rc};

use anyhow::Result;

use crate::{
    access::{
        heap::heap_scan,
        heaptuple::{HeapTuple, TupleDesc},
    },
    planner::{Plan, PlanNodeType},
    storage::BufferPool,
};

/// A plan tree executor. Contains function to execute each type of PlanNodeType.
pub struct Executor {
    buffer_pool: Rc<RefCell<BufferPool>>,
}

impl Executor {
    /// Create a new executor using the given buffer pool to fetch page buffers.
    pub fn new(buffer_pool: Rc<RefCell<BufferPool>>) -> Self {
        Self { buffer_pool }
    }

    /// Main entrypoint of a planner executor, it recursivily exec all nodes
    /// for the planer and return a tuple table result with all operations
    /// of the planner performed.
    pub fn exec(&self, node: &Plan) -> Result<TupleTable> {
        match &node.node_type {
            PlanNodeType::SeqScan => self.exec_seq_scan(&node),
        }
    }

    /// Planner executor for a plan node of SeqScan type.
    fn exec_seq_scan(&self, node: &Plan) -> Result<TupleTable> {
        let tuples = heap_scan(self.buffer_pool.clone(), &node.relation)?;
        Ok(TupleTable {
            tuple_desc: node.tuple_desc.clone(),
            tuples,
        })
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
