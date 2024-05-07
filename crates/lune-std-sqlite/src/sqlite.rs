use lune_utils::TableBuilder;
use mlua::prelude::{LuaUserData, LuaValue};
use mlua::{ExternalResult, LuaSerdeExt, UserDataMethods};
use rusqlite::types::{FromSqlError, FromSqlResult, ValueRef};
use rusqlite::{params_from_iter, Connection, Result};
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::str;

fn convert_to_lua_compatible_type(value: ValueRef<'_>) -> FromSqlResult<Value> {
    match value {
        ValueRef::Text(s) => Ok(serde_json::from_slice(s)
            .unwrap_or(Value::String(str::from_utf8(s).unwrap().to_string()))), // KO for b"text"
        ValueRef::Blob(b) => Ok(serde_json::from_slice(b)
            .unwrap_or(Value::String(str::from_utf8(b).unwrap().to_string()))),
        ValueRef::Integer(i) => Ok(Value::Number(Number::from(i))),
        ValueRef::Real(f) => {
            match Number::from_f64(f) {
                Some(n) => Ok(Value::Number(n)),
                _ => return Err(FromSqlError::InvalidType), // FIXME
            }
        }
        ValueRef::Null => Ok(Value::Null),
    }
}

#[derive(Debug)]
pub struct SQLite {
    inner: Connection,
}

impl SQLite {
    pub fn connect(path: String) -> Result<SQLite> {
        let inner = Connection::open(&path)?;
        Ok(Self { inner })
    }

    pub fn execute(&self, sql: Option<String>, parameters: Option<Vec<Value>>) -> Result<usize> {
        self.inner.execute(
            &sql.unwrap(),
            params_from_iter(parameters.unwrap_or(Vec::new())),
        )
    }

    pub fn execute_batch(&self, sql: Option<String>) -> Result<()> {
        self.inner.execute_batch(
            &sql.unwrap(),
        )
    }

    pub fn query(
        &self,
        sql: Option<String>,
        parameters: Option<Vec<Value>>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut stmt = self.inner.prepare(&sql.unwrap())?;
        let mut column_names: Vec<String> = Vec::new();
        for column_name in stmt.column_names() {
            column_names.push(column_name.to_string());
        }
        let mut rows = stmt
            .query(params_from_iter(parameters.unwrap_or(Vec::new())))?;
        let mut data = Vec::new();

        while let Some(row) = rows.next()? {
            let mut row_data = HashMap::new();
            for (i, column_name) in column_names.iter().enumerate() {
                let value_ref = row.get_ref(i)?;
                let value = convert_to_lua_compatible_type(value_ref)?;
                row_data.insert(column_name.to_string(), value);
            }
            data.push(row_data);
        }

        Ok(data)
    }
}

impl LuaUserData for SQLite {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method(
            "execute",
            |lua, this, (sql, params): (Option<String>, Option<Vec<LuaValue>>)| {
                let mut sql_params: Vec<Value> = Vec::new();
                for param in params.unwrap_or(Vec::new()) {
                    let value: Value = lua.from_value(param).into_lua_err()?;
                    sql_params.push(value);
                }
                let rows_modified: f64 = this.execute(sql, Some(sql_params)).into_lua_err()? as f64;
                Ok(rows_modified)
            },
        );
        
        methods.add_method(
            "executeBatch",
            |lua, this, (sql): (Option<String>)| {
                this.execute_batch(sql).into_lua_err()?;
                Ok(())
            },
        );

        methods.add_method(
            "query",
            |lua, this, (sql, params): (Option<String>, Option<Vec<LuaValue>>)| {
                let mut sql_params: Vec<Value> = Vec::new();
                for param in params.unwrap_or(Vec::new()) {
                    let value: Value = lua.from_value(param).into_lua_err()?;
                    sql_params.push(value);
                }
                let data = this.query(sql, Some(sql_params)).into_lua_err()?;
                let mut table_builder = TableBuilder::new(lua)?;
                for row in data {
                    let mut row_builder = TableBuilder::new(lua)?;
                    for column_name in row.keys() {
                        let column_value = row.get(column_name).unwrap();
                        let lua_value = lua.to_value(&column_value)?;
                        row_builder = row_builder.with_value(column_name.to_string(), lua_value)?;
                    }
                    table_builder = table_builder.with_sequential_value(row_builder.build()?)?;
                }
                Ok(table_builder.build())
            },
        );
    }
}
