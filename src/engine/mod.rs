use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use crate::sql::commands::explain::explain;
use crate::sql::commands::SQLError;
use crate::sql::commands::{create::create_table, insert::insert_into, query::select};
use crate::storage::BufferPool;
use crate::Oid;
use anyhow::{bail, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

pub struct Engine {
    buffer_pool: Rc<RefCell<BufferPool>>,
}

impl Engine {
    pub fn new(buffer_pool: Rc<RefCell<BufferPool>>) -> Self {
        Self { buffer_pool }
    }

    pub fn exec(&mut self, output: &mut dyn io::Write, command: &str, db_oid: &Oid) -> Result<()> {
        let ast = Parser::parse_sql(&DIALECT, command)?;

        for stmt in ast {
            self.exec_stmt(output, db_oid, stmt)?;
        }

        Ok(())
    }

    fn exec_stmt(
        &mut self,
        output: &mut dyn io::Write,
        db_oid: &Oid,
        stmt: Statement,
    ) -> Result<()> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                create_table(self.buffer_pool.clone(), db_oid, name, columns)
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => insert_into(
                self.buffer_pool.clone(),
                db_oid,
                table_name,
                columns,
                source,
            ),
            Statement::Query(query) => select(self.buffer_pool.clone(), output, db_oid, query),
            Statement::Explain { statement, .. } => {
                explain(self.buffer_pool.clone(), output, db_oid, *statement)
            }
            _ => bail!(SQLError::Unsupported(stmt.to_string())),
        }
    }
}
