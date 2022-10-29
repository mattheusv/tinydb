pub mod encode;

use encode::encode;

use std::{io, mem::size_of};

use anyhow::{bail, Result};
use sqlparser::{
    ast::{self, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::{
    access::{
        self,
        heap::heap_insert,
        heaptuple::{HeapTuple, TupleDesc},
    },
    catalog::{
        self, heap::heap_create, pg_attribute::PgAttribute, pg_tablespace::DEFAULTTABLESPACE_OID,
        pg_type,
    },
    executor::{Executor, TupleTable},
    planner::{Plan, PlanNodeType},
    storage::BufferPool,
    Datums, Oid,
};

use self::encode::decode;

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

/// Errors related with a SQL command
#[derive(Debug, thiserror::Error)]
pub enum SQLError {
    /// Unsupported SQL operation.
    #[error("unsuported operation {0}")]
    Unsupported(String),
}

/// An executor config holds per connection configuration values.
pub struct ExecutorConfig {
    /// Oid of database that is connect in.
    pub database: Oid,
}

/// A connection executor is in charge of executing queries on a give database connection.
pub struct ConnectionExecutor {
    config: ExecutorConfig,

    buffer_pool: BufferPool,
}

impl ConnectionExecutor {
    pub fn new(config: ExecutorConfig, buffer_pool: BufferPool) -> Self {
        Self {
            config,
            buffer_pool,
        }
    }

    /// Run the give SQL command sending the output to the given output writer.
    pub fn run(&mut self, output: &mut dyn io::Write, command: &str) -> Result<()> {
        let ast = Parser::parse_sql(&DIALECT, command)?;

        for stmt in ast {
            self.exec_stmt(output, stmt)?;
        }

        Ok(())
    }

    fn exec_stmt(&mut self, output: &mut dyn io::Write, stmt: Statement) -> Result<()> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => self.exec_create_table(name, columns),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.exec_insert(table_name, columns, source),
            Statement::Query(query) => self.exec_select(output, query),
            Statement::Explain { statement, .. } => self.exec_explain(output, *statement),
            _ => bail!(SQLError::Unsupported(stmt.to_string())),
        }
    }

    fn exec_select(&mut self, output: &mut dyn io::Write, query: Box<ast::Query>) -> Result<()> {
        let mut plan = Plan::create(self.buffer_pool.clone(), &self.config.database, query)?;
        let executor = Executor::new();
        let tuple_table = executor.exec(&mut plan)?;
        self.print_relation_tuples(output, tuple_table)?;
        Ok(())
    }

    fn exec_insert(
        &mut self,
        table_name: ast::ObjectName,
        columns: Vec<ast::Ident>,
        source: Box<ast::Query>,
    ) -> Result<()> {
        let rel_name = table_name.0[0].to_string();
        let pg_class_rel = catalog::get_pg_class_relation(
            self.buffer_pool.clone(),
            &self.config.database,
            &rel_name,
        )?;

        let rel = access::open_relation(
            pg_class_rel.oid,
            pg_class_rel.reltablespace,
            &self.config.database,
            &rel_name,
        );

        match source.body {
            ast::SetExpr::Values(values) => {
                let tuple_desc = catalog::tuple_desc_from_relation(
                    self.buffer_pool.clone(),
                    &self.config.database,
                    &rel_name,
                )?;

                let mut heap_values = Datums::default();

                // Iterate over all rows on insert to write new tuples.
                for row in &values.0 {
                    // INSERT statement don't specify the columns
                    if columns.len() == 0 {
                        for attr in &tuple_desc.attrs {
                            match row.get(attr.attnum - 1) {
                                Some(value) => match value {
                                    ast::Expr::Value(value) => {
                                        encode(&mut heap_values, &value, attr)?;
                                    }
                                    _ => bail!(SQLError::Unsupported(value.to_string())),
                                },
                                None => heap_values.push(None),
                            }
                        }
                    } else {
                        if row.len() != columns.len() {
                            bail!("INSERT has more expressions than target columns");
                        }

                        // Iterate over relation attrs and try to find the value that is being inserted
                        // for each attr. If the value does not exists a NULL value should be inserted
                        // on tuple header t_bits array.
                        for attr in &tuple_desc.attrs {
                            // TODO: Find a better way to lookup the attr value that is being inserted
                            let index =
                                columns.iter().position(|ident| ident.value == attr.attname);
                            match index {
                                Some(index) => {
                                    let value = &row[index];
                                    match value {
                                        ast::Expr::Value(value) => {
                                            encode(&mut heap_values, &value, attr)?;
                                        }
                                        _ => bail!(SQLError::Unsupported(value.to_string())),
                                    }
                                }
                                None => {
                                    heap_values.push(None);
                                }
                            }
                        }
                    }
                }

                heap_insert(
                    &self.buffer_pool,
                    &rel,
                    &mut HeapTuple::from_datums(heap_values, &tuple_desc)?,
                )?;
            }
            _ => bail!(SQLError::Unsupported(source.to_string())),
        }

        Ok(())
    }

    fn exec_create_table(
        &mut self,
        name: ast::ObjectName,
        columns: Vec<ast::ColumnDef>,
    ) -> Result<()> {
        // Create a new unique oid to the new heap relation.
        let new_oid = catalog::new_relation_oid(&DEFAULTTABLESPACE_OID, &self.config.database)?;

        let mut tupledesc = TupleDesc::default();
        for (i, attr) in columns.iter().enumerate() {
            // Attributes numbers start at 1
            tupledesc
                .attrs
                .push(self.new_pg_attribute(new_oid, attr, i + 1)?)
        }

        heap_create(
            &mut self.buffer_pool,
            DEFAULTTABLESPACE_OID,
            &self.config.database,
            &name.0[0].to_string(),
            new_oid,
            &tupledesc,
        )?;
        Ok(())
    }

    fn new_pg_attribute(
        &self,
        attrelid: Oid,
        columndef: &ast::ColumnDef,
        attnum: usize,
    ) -> Result<PgAttribute> {
        let (atttypid, attlen) = self.oid_type_and_size(&columndef.data_type)?;
        Ok(PgAttribute {
            attrelid,
            attname: columndef.name.to_string(),
            attnum,
            attlen,
            atttypid,
        })
    }

    /// Return the oid and the lenght of the given data type.
    fn oid_type_and_size(&self, typ: &ast::DataType) -> Result<(Oid, i64)> {
        match typ {
            ast::DataType::Int(len) => Ok((
                pg_type::INT_OID,
                (len.unwrap_or(size_of::<i32>() as u64)) as i64,
            )),
            ast::DataType::Varchar(len) => match len {
                Some(len) => Ok((pg_type::VARCHAR_OID, *len as i64)),
                None => Ok((pg_type::VARCHAR_OID, -1)),
            },
            ast::DataType::Boolean => Ok((pg_type::BOOL_OID, size_of::<bool>() as i64)),
            _ => bail!("Not supported data type: {}", typ),
        }
    }

    fn exec_explain(&mut self, output: &mut dyn io::Write, stmt: Statement) -> Result<()> {
        match stmt {
            Statement::Query(query) => {
                let plan = Plan::create(self.buffer_pool.clone(), &self.config.database, query)?;
                self.print_explain(&plan, output)
            }
            _ => bail!(SQLError::Unsupported(stmt.to_string())),
        }
    }

    fn print_explain(&self, plan: &Plan, output: &mut dyn io::Write) -> Result<()> {
        write!(
            output,
            "        QUERY PLAN
-----------------------------
"
        )?;
        match &plan.node_type {
            PlanNodeType::SeqScan { state } => {
                write!(output, "Seq Scan on {}\n", state.relation.rel_name)?;
            }
            PlanNodeType::Projection { .. } => {} // Don't show projection plan node
        };
        write!(output, "\n")?;
        Ok(())
    }

    fn print_relation_tuples(
        &self,
        output: &mut dyn io::Write,
        tuple_table: TupleTable,
    ) -> Result<()> {
        let mut columns = Vec::new();
        let mut records = Vec::new();

        for attr in &tuple_table.tuple_desc.attrs {
            columns.push(attr.attname.clone());
        }

        for slot in tuple_table.values {
            let mut tuple_values = Vec::new();
            for (attidx, attr) in tuple_table.tuple_desc.attrs.iter().enumerate() {
                let datum = &slot[attidx];
                match datum {
                    Some(datum) => {
                        tuple_values.push(decode(&datum, attr.atttypid)?);
                    }
                    None => {
                        tuple_values.push(String::from("NULL"));
                    }
                }
            }
            records.push(tuple_values);
        }

        let mut table = tabled::builder::Builder::default().set_columns(columns);

        for record in records {
            table = table.add_record(record);
        }

        let table = table.build().with(tabled::Style::psql());

        writeln!(output, "{}", table)?;

        Ok(())
    }
}
