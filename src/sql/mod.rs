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
    planner::Plan,
    storage::BufferPool,
    NullableDatum, Oid,
};
use anyhow::{bail, Result};
use encode::encode;
use sqlparser::ast::{self, Expr, Value};
use std::mem::size_of;

pub mod encode;

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
    /// Configuration options for a database connection.
    config: ExecutorConfig,

    /// Buffer pool shared with the query planner and executor.
    buffer_pool: BufferPool,
}

impl ConnectionExecutor {
    pub fn new(config: ExecutorConfig, buffer_pool: BufferPool) -> Self {
        Self {
            config,
            buffer_pool,
        }
    }

    pub fn exec_query(&self, query: &Box<ast::Query>) -> Result<PGResult> {
        let mut plan = Plan::create(&self.buffer_pool, &self.config.database, query)?;
        let executor = Executor::new();
        let tuple_table = executor.exec(&mut plan)?;
        Ok(PGResult::from(tuple_table))
    }

    pub fn exec_insert(
        &self,
        table_name: &ast::ObjectName,
        columns: &Vec<ast::Ident>,
        source: &Box<ast::Query>,
    ) -> Result<()> {
        let rel_name = table_name.0[0].to_string();
        let pg_class_rel =
            catalog::get_pg_class_relation(&self.buffer_pool, &self.config.database, &rel_name)?;

        let rel = access::open_relation(
            pg_class_rel.oid,
            pg_class_rel.reltablespace,
            &self.config.database,
            &rel_name,
        );

        match &source.body {
            ast::SetExpr::Values(values) => {
                let tuple_desc = catalog::tuple_desc_from_relation(
                    &self.buffer_pool,
                    &self.config.database,
                    &rel_name,
                )?;

                let mut heap_values = Vec::new();

                // Iterate over all rows on insert to write new tuples.
                for row in &values.0 {
                    let attr_values = tuple_values_from_insert_row(columns, row, &tuple_desc)?;
                    for (attr, value) in attr_values.iter() {
                        encode(&mut heap_values, value, attr)?;
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

    pub fn exec_create_table(
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
}

/// Return a Vector of tuples, wich each tuple contains a attribute and their respective value on a
/// row from insert statetment.
///
/// The attribute and value returned is a reference on the given tuple desc attributes and vector
/// of rows.
fn tuple_values_from_insert_row<'a>(
    columns: &Vec<ast::Ident>,
    row: &'a Vec<Expr>,
    tuple_desc: &'a TupleDesc,
) -> Result<Vec<(&'a PgAttribute, &'a Value)>> {
    let mut map = Vec::with_capacity(tuple_desc.attrs.len());

    if columns.len() == 0 {
        // INSERT statement don't specify the columns, so iterate over all attributes of the tuple
        // and try to get the value on insert statment. If the value is not present, set the attr
        // value to null.
        for attr in &tuple_desc.attrs {
            match row.get(attr.attnum - 1) {
                Some(value) => match value {
                    ast::Expr::Value(value) => {
                        map.push((attr, value));
                    }
                    _ => bail!(SQLError::Unsupported(value.to_string())),
                },
                None => {
                    map.push((attr, &Value::Null));
                }
            };
        }
    } else if row.len() != columns.len() {
        bail!("INSERT has more expressions than target columns");
    } else {
        // Iterate over relation attrs and try to find the value that is being inserted for each
        // attr. If the value does not exists on statment the value of attr is set to NULL
        for attr in &tuple_desc.attrs {
            // TODO: Find a better way to lookup the attr value that is being inserted
            let index = columns.iter().position(|ident| ident.value == attr.attname);
            match index {
                Some(index) => {
                    let value = &row[index];
                    match value {
                        ast::Expr::Value(value) => {
                            map.push((attr, value));
                        }
                        _ => bail!(SQLError::Unsupported(value.to_string())),
                    }
                }
                None => {
                    map.push((attr, &Value::Null));
                }
            }
        }
    }

    Ok(map)
}

/// Describe an attribute in a row.
#[derive(Debug, Clone)]
pub struct FieldDescription {
    /// Name of field.
    pub name: Vec<u8>,

    /// Oid of the table the field belongs to.
    pub table_oid: u32,

    /// Number of attribute in a row.
    pub table_attribute_number: u16,

    /// Oid of the type of attribute.
    pub data_type_oid: u32,

    /// Fixed size of atribute value.
    pub data_type_size: i16,

    // Fields required by postgres protocol, usually set to 0.
    pub type_modifier: i32,

    pub format: i16,
}

/// A descriptor for all attributes in a pg result row.
#[derive(Debug, Clone)]
pub struct RowDescriptor {
    pub fields: Vec<FieldDescription>,
}

/// A query result contaning the data for all rows an a descriptor for each attribute in a row.
#[derive(Debug)]
pub struct PGResult {
    /// Row attributes descriptor
    pub desc: RowDescriptor,

    /// All values returned from a query.
    pub tuples: Vec<Vec<NullableDatum>>,
}

impl From<TupleTable> for PGResult {
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

        Self {
            desc: RowDescriptor { fields },
            tuples: table.values,
        }
    }
}
