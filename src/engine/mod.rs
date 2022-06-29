use std::fs;
use std::path::Path;

use crate::access::heap::{heap_insert, heap_scan, HeapTuple, TupleDesc};
use crate::catalog::{heap, Catalog};
use crate::storage::rel::RelationData;
use crate::storage::BufferPool;
use anyhow::Result;
use sqlparser::ast::{self, ColumnDef, ObjectName, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

pub struct Engine {
    buffer_pool: BufferPool,
    catalog: Catalog,
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
            catalog: Catalog::new(db_data),
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
                            let rel_name = name.0[0].to_string();
                            let oid = self.catalog.get_oid_relation(
                                &mut self.buffer_pool,
                                db_name,
                                &rel_name,
                            )?;

                            let rel_attrs = self.catalog.get_attributes_from_relation(
                                &mut self.buffer_pool,
                                db_name,
                                &rel_name,
                            )?;

                            let tuple_desc = TupleDesc { attrs: rel_attrs };

                            let rel = RelationData::open(oid, &self.db_data, db_name, &rel_name)?;
                            heap_scan(&mut self.buffer_pool, &rel, &tuple_desc)?;
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
        columns: Vec<ast::Ident>,
        source: Box<ast::Query>,
    ) -> Result<()> {
        let rel_name = table_name.0[0].to_string();
        let oid = self
            .catalog
            .get_oid_relation(&mut self.buffer_pool, db_name, &rel_name)?;

        let rel = RelationData::open(oid, &self.db_data, db_name, &rel_name)?;

        match source.body {
            ast::SetExpr::Values(values) => {
                let mut heap_data = Vec::new();
                for (idx, _) in columns.iter().enumerate() {
                    for row in &values.0 {
                        assert_eq!(
                            columns.len(),
                            row.len(),
                            "Incompatible columns and values to insert"
                        );
                        let value = &row[idx];
                        match value {
                            ast::Expr::Value(value) => match value {
                                ast::Value::Number(value, _) => {
                                    let value = value.parse::<i32>().unwrap();
                                    heap_data.append(&mut bincode::serialize(&value).unwrap());
                                }
                                _ => todo!(),
                            },
                            _ => todo!(),
                        }
                    }
                }

                heap_insert(&mut self.buffer_pool, &rel, &HeapTuple { data: heap_data })?;
            }
            _ => todo!(),
        }

        Ok(())
    }

    fn create_table(
        &mut self,
        db_name: &str,
        name: ObjectName,
        columns: Vec<ColumnDef>,
    ) -> Result<()> {
        heap::heap_create(
            &mut self.buffer_pool,
            &self.db_data,
            db_name,
            &name.0[0].to_string(),
            columns,
        )?;
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
    use crate::initdb::init_database;
    use tempfile::tempdir;

    #[test]
    fn test_engine() -> Result<()> {
        {
            let db_data = tempdir()?;
            let db_name = "test_engine";

            init_database(&db_data.path().to_path_buf(), db_name)?;

            let buffer = BufferPool::new(120);
            let mut engine = Engine::new(buffer, &db_data.path().to_string_lossy().to_string());

            engine.exec("CREATE TABLE t(a int);", db_name)?;
            engine.exec("INSERT INTO t(a) VALUES(87);", db_name)?;
            engine.exec("SELECT * FROM t;", db_name)?;
        }

        Ok(())
    }
}
