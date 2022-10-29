pub mod encode;

use encode::encode;

use std::{io::Write, mem::size_of};

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
    pub fn run<W>(&self, output: &mut W, command: &str) -> Result<()>
    where
        W: Write,
    {
        let ast = Parser::parse_sql(&DIALECT, command)?;

        for stmt in ast {
            self.exec_stmt(output, &stmt)?;
        }

        Ok(())
    }

    pub fn run_pg(&self, command: &str) -> Result<RowDescriptor> {
        let ast = Parser::parse_sql(&DIALECT, command)?;
        if ast.len() > 1 {
            bail!("Can not execute multiple statements in a single command");
        }

        let stmt = &ast[0];
        match stmt {
            Statement::Query(query) => self.exec_pg_select(query),
            _ => bail!(SQLError::Unsupported(stmt.to_string())),
        }
    }

    fn exec_stmt<W>(&self, output: &mut W, stmt: &Statement) -> Result<()>
    where
        W: Write,
    {
        match stmt {
            Statement::CreateTable { name, columns, .. } => self.exec_create_table(name, columns),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.exec_insert(table_name, columns, source),
            Statement::Query(query) => self.exec_select(output, query),
            Statement::Explain { statement, .. } => self.exec_explain(output, statement),
            _ => bail!(SQLError::Unsupported(stmt.to_string())),
        }
    }

    fn exec_select<W>(&self, output: &mut W, query: &Box<ast::Query>) -> Result<()>
    where
        W: Write,
    {
        let mut plan = Plan::create(self.buffer_pool.clone(), &self.config.database, query)?;
        let executor = Executor::new();
        let tuple_table = executor.exec(&mut plan)?;
        self.print_relation_tuples(output, tuple_table)?;
        Ok(())
    }

    fn exec_pg_select(&self, query: &Box<ast::Query>) -> Result<RowDescriptor> {
        let mut plan = Plan::create(self.buffer_pool.clone(), &self.config.database, query)?;
        let executor = Executor::new();
        let tuple_table = executor.exec(&mut plan)?;
        Ok(RowDescriptor::from(tuple_table))
    }

    fn exec_insert(
        &self,
        table_name: &ast::ObjectName,
        columns: &Vec<ast::Ident>,
        source: &Box<ast::Query>,
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

        match &source.body {
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
        &self,
        name: &ast::ObjectName,
        columns: &Vec<ast::ColumnDef>,
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
            &self.buffer_pool,
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

    fn exec_explain<W>(&self, output: &mut W, stmt: &Statement) -> Result<()>
    where
        W: Write,
    {
        match stmt {
            Statement::Query(query) => {
                let plan = Plan::create(self.buffer_pool.clone(), &self.config.database, query)?;
                self.print_explain(output, &plan)
            }
            _ => bail!(SQLError::Unsupported(stmt.to_string())),
        }
    }

    fn print_explain<W>(&self, output: &mut W, plan: &Plan) -> Result<()>
    where
        W: Write,
    {
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

    fn print_relation_tuples<W>(&self, output: &mut W, tuple_table: TupleTable) -> Result<()>
    where
        W: Write,
    {
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

pub struct FieldDescription {
    pub name: Vec<u8>,
    pub table_oid: u32,
    pub table_attribute_number: u16,
    pub data_type_oid: u32,
    pub data_type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

pub struct RowDescriptor {
    pub fields: Vec<FieldDescription>,
}

impl From<TupleTable> for RowDescriptor {
    fn from(table: TupleTable) -> Self {
        let mut fields = Vec::with_capacity(table.tuple_desc.attrs.len());
        for attr in &table.tuple_desc.attrs {
            fields.push(FieldDescription {
                name: attr.attname.as_bytes().to_vec(),
                table_oid: attr.attrelid as u32,
                table_attribute_number: attr.attnum as u16,
                data_type_oid: attr.atttypid as u32,
                data_type_size: attr.attlen as i16,
                type_modifier: -1,
                format: 0,
            })
        }

        Self { fields }
    }
}
