use std::fs;
use std::path::Path;

use crate::access::heap::{heap_insert, heap_scan, HeapTuple};
use crate::catalog::heap;
use crate::storage::rel::RelationData;
use crate::storage::BufferPool;
use anyhow::Result;
use sqlparser::ast::{self, ColumnDef, ObjectName, Statement};
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

    pub fn exec(&mut self, command: &str, db_name: &str) -> Result<()> {
        let ast = Parser::parse_sql(&DIALECT, command)?;

        for stmt in ast {
            self.exec_stmt(db_name, stmt)?;
        }

        Ok(())
    }

    fn exec_stmt(&mut self, db_name: &str, stmt: Statement) -> Result<()> {
        match stmt {
            Statement::CreateDatabase { db_name, .. } => self.create_database(db_name),
            Statement::CreateTable { name, columns, .. } => {
                self.create_table(db_name, name, columns)
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.insert_into(db_name, table_name, columns, source),
            Statement::Query(query) => self.query(db_name, query),
            _ => {
                todo!()
            }
        }
    }

    fn query(&mut self, db_name: &str, query: Box<ast::Query>) -> Result<()> {
        match query.body {
            ast::SetExpr::Select(select) => {
                for table in select.from {
                    match table.relation {
                        ast::TableFactor::Table { name, .. } => {
                            let rel = RelationData::open(&self.db_data, db_name, &name.0[0].value)?;
                            heap_scan(&mut self.buffer_pool, &rel)?;
                        }
                        _ => todo!(),
                    }
                }
            }
            _ => todo!(),
        }
        Ok(())
    }

    fn insert_into(
        &mut self,
        db_name: &str,
        table_name: ObjectName,
        _: Vec<ast::Ident>,
        source: Box<ast::Query>,
    ) -> Result<()> {
        let rel = RelationData::open(&self.db_data, db_name, &table_name.0[0].to_string())?;

        match source.body {
            ast::SetExpr::Values(values) => {
                for row in values.0 {
                    for value in row {
                        match value {
                            ast::Expr::Value(value) => match value {
                                ast::Value::Number(value, _) => {
                                    let value = value.parse::<i32>().unwrap();
                                    let value = bincode::serialize(&value).unwrap();

                                    heap_insert(
                                        &mut self.buffer_pool,
                                        &rel,
                                        &HeapTuple { data: value },
                                    )?;
                                }
                                _ => todo!(),
                            },
                            _ => todo!(),
                        }
                    }
                }
            }
            _ => todo!(),
        }

        Ok(())
    }

    fn create_table(&mut self, db_name: &str, name: ObjectName, _: Vec<ColumnDef>) -> Result<()> {
        let rel = RelationData::open(&self.db_data, db_name, &name.0[0].to_string())?;
        heap::heap_create(&mut self.buffer_pool, &rel)?;
        Ok(())
    }

    fn create_database(&self, name: ObjectName) -> Result<()> {
        let table_path = Path::new(&self.db_data).join(name.0[0].to_string());
        fs::create_dir(table_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_engine() -> Result<()> {
        {
            let buffer = BufferPool::new(120);
            let mut engine = Engine::new(buffer, "data");

            engine.exec("CREATE DATABASE testing;", "")?;
            engine.exec("CREATE TABLE t(a int);", "testing")?;
            engine.exec("INSERT INTO t(a) VALUES(87);", "testing")?;
            engine.exec("SELECT * FROM t;", "testing")?;
        }

        Ok(())
    }
}
