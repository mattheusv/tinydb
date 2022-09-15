use std::io;

use crate::errors::Error;
use crate::sql::commands::{
    create::create_database, create::create_table, insert::insert_into, query::select,
};
use crate::storage::BufferPool;
use crate::Oid;
use anyhow::{bail, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

pub struct Engine {
    buffer_pool: BufferPool,
    db_data: String,
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.buffer_pool
            .flush_all_buffers()
            .expect("failed to flush all buffers to disk");
    }
}

impl Engine {
    pub fn new(buffer_pool: BufferPool, db_data: &str) -> Self {
        Self {
            buffer_pool,
            db_data: db_data.to_string(),
        }
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
            Statement::CreateDatabase { db_name, .. } => create_database(&self.db_data, db_name),
            Statement::CreateTable { name, columns, .. } => {
                create_table(&mut self.buffer_pool, &self.db_data, db_oid, name, columns)
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => insert_into(
                &mut self.buffer_pool,
                &self.db_data,
                db_oid,
                table_name,
                columns,
                source,
            ),
            Statement::Query(query) => {
                select(&mut self.buffer_pool, &self.db_data, output, db_oid, query)
            }
            _ => bail!(Error::UnsupportedOperation(stmt.to_string())),
        }
    }
}
