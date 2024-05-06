use lune_utils::TableBuilder;
use mlua::prelude::{LuaUserData};
use mlua::{LuaSerdeExt, UserDataMethods};
use rusqlite::types::{FromSqlError, FromSqlResult, ValueRef};
use rusqlite::{params_from_iter, Connection, Result};
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::str;

fn convert_to_lua_compatible_type(value: ValueRef<'_>) -> FromSqlResult<Value> {
    match value {
        ValueRef::Text(s) => Ok(serde_json::from_slice(s)
            .unwrap_or(Value::String(str::from_utf8(s).unwrap().to_string()))), // KO for b"text"
        ValueRef::Blob(b) => serde_json::from_slice(b),
        ValueRef::Integer(i) => Ok(Value::Number(Number::from(i))),
        ValueRef::Real(f) => {
            match Number::from_f64(f) {
                Some(n) => Ok(Value::Number(n)),
                _ => return Err(FromSqlError::InvalidType), // FIXME
            }
        }
        ValueRef::Null => Ok(Value::Null),
    }
    .map_err(|err| FromSqlError::Other(Box::new(err)))
}

#[derive(Debug)]
pub struct SQLite {
    // NOTE: We store this as the UTC time zone since it is the most commonly
    // used and getting the generics right for TimeZone is somewhat tricky,
    // but none of the method implementations below should rely on this tz
    inner: Connection,
}

impl SQLite {
    pub fn connect(path: String) -> Result<SQLite> {
        let inner = Connection::open(&path)?;
        Ok(Self { inner })
    }

    pub fn execute(&self, sql: Option<String>, parameters: Option<Vec<String>>) -> Result<usize> {
        self.inner.execute(
            &*sql.unwrap(),
            params_from_iter(parameters.unwrap_or(Vec::new())),
        )
    }

    pub fn query(
        &self,
        sql: Option<String>,
        parameters: Option<Vec<String>>,
    ) -> Vec<HashMap<String, Value>> {
        let mut stmt = self.inner.prepare(&*sql.unwrap()).unwrap();
        let mut column_names: Vec<String> = Vec::new();
        for column_name in stmt.column_names() {
            column_names.push(column_name.to_string());
        }
        let mut rows = stmt
            .query(params_from_iter(parameters.unwrap_or(Vec::new())))
            .unwrap();
        let mut data = Vec::new();

        while let Some(row) = rows.next().unwrap() {
            let mut row_data = HashMap::new();
            for (i, column_name) in column_names.iter().enumerate() {
                let value_ref = row.get_ref_unwrap(i);
                let value = convert_to_lua_compatible_type(value_ref).unwrap();
                row_data.insert(column_name.to_string(), value);
            }
            data.push(row_data)
        }

        data
    }
}

impl LuaUserData for SQLite {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method(
            "execute",
            |_, this, (sql, params): (Option<String>, Option<Vec<String>>)| {
                let rows_modified: f64 = this.execute(sql, params).unwrap() as f64;
                Ok(rows_modified)
            },
        );

        methods.add_method(
            "query",
            |lua, this, (sql, params): (Option<String>, Option<Vec<String>>)| {
                let data = this.query(sql, params);
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
                table_builder.build()
            },
        );
    }
}
