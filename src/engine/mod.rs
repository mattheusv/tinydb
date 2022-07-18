use std::fs;
use std::path::Path;

use crate::access::heap::{heap_insert, heap_scan, HeapTuple, TupleDesc};
use crate::catalog::pg_attribute::PgAttribute;
use crate::catalog::pg_class::PgClass;
use crate::catalog::{heap, Catalog};
use crate::storage::rel::{Relation, RelationData};
use crate::storage::BufferPool;
use anyhow::Result;
use sqlparser::ast::{self, ColumnDef, ObjectName, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tabled::builder::Builder;
use tabled::Style;

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
                            let tuples = heap_scan(&mut self.buffer_pool, &rel)?;
                            self.print_relation_tuples(&rel, tuples, &tuple_desc)?;
                        }
                        _ => todo!(),
                    }
                }
            }
            _ => todo!(),
        }
        Ok(())
    }

    fn print_relation_tuples(
        &self,
        rel: &Relation,
        tuples: Vec<HeapTuple>,
        tuple_desc: &TupleDesc,
    ) -> Result<()> {
        let mut columns = Vec::new();
        let mut records = Vec::new();

        match rel.borrow().rel_name.as_str() {
            "pg_class" => {
                columns.append(&mut vec![String::from("oid"), String::from("relname")]);
                for tuple in tuples {
                    let value = bincode::deserialize::<PgClass>(&tuple.data)?;
                    records.push(vec![value.oid.to_string(), value.relname]);
                }
            }
            "pg_attribute" => {
                columns.append(&mut vec![
                    String::from("attrelid"),
                    String::from("attname"),
                    String::from("attnum"),
                    String::from("attlen"),
                ]);
                for tuple in tuples {
                    let value = bincode::deserialize::<PgAttribute>(&tuple.data)?;
                    records.push(vec![
                        value.attrelid.to_string(),
                        value.attname,
                        value.attnum.to_string(),
                        value.attlen.to_string(),
                    ]);
                }
            }
            _ => {
                for attr in &tuple_desc.attrs {
                    columns.push(attr.attname.clone());
                }

                for tuple in tuples {
                    let mut tuple_values = Vec::new();
                    for attr in tuple_desc.attrs.iter() {
                        let dataum = tuple.get_attr(attr.attnum, tuple_desc);
                        match dataum {
                            Some(dataum) => {
                                let value = bincode::deserialize::<i32>(&dataum)?;
                                tuple_values.push(value.to_string());
                            }
                            None => {
                                tuple_values.push(String::from("NULL"));
                            }
                        }
                    }
                    records.push(tuple_values);
                }
            }
        }

        let mut table = Builder::default().set_columns(columns);

        for record in records {
            table = table.add_record(record);
        }

        let table = table.build().with(Style::psql());

        println!("{}", table);

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
                let rel_attrs = self.catalog.get_attributes_from_relation(
                    &mut self.buffer_pool,
                    db_name,
                    &rel_name,
                )?;

                let mut heap_tuple = HeapTuple::default();

                // Iterate over all rows on insert to write new tuples.
                for row in &values.0 {
                    assert_eq!(
                        row.len(),
                        columns.len(),
                        "INSERT has more expressions than target columns"
                    );

                    // Iterate over relation attrs and try to find the value that is being inserted
                    // for each attr. If the value does not exists a NULL value should be inserted
                    // on tuple header t_bits array.
                    //
                    // TODO: Add null bit on NULL attr values on tuple header t_bits.
                    for attr in &rel_attrs {
                        // TODO: Find a better way to lookup the attr value that is being inserted
                        let index = columns.iter().position(|ident| ident.value == attr.attname);
                        match index {
                            Some(index) => {
                                let value = &row[index];
                                match value {
                                    ast::Expr::Value(value) => match value {
                                        ast::Value::Number(value, _) => {
                                            let value = value.parse::<i32>().unwrap();
                                            heap_tuple.append_data(
                                                &mut bincode::serialize(&value).unwrap(),
                                            );
                                        }
                                        _ => todo!(),
                                    },
                                    _ => todo!(),
                                }
                            }
                            None => {
                                heap_tuple.add_has_nulls_flag();
                            }
                        }
                    }
                }

                heap_insert(&mut self.buffer_pool, &rel, &mut heap_tuple)?;
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
