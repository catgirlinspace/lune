use std::collections::HashMap;
use std::path::Path;
use mlua::prelude::{LuaResult, LuaUserData, LuaValue};
use mlua::{ExternalResult, IntoLua, Lua, LuaSerdeExt, UserDataMethods};
use rusqlite::{Connection, params_from_iter, Result, Rows};
use rusqlite::ffi::sqlite3_last_insert_rowid;
use serde_json::Value;
use lune_utils::TableBuilder;

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
        self.inner.execute(&*sql.unwrap(), params_from_iter(parameters.unwrap()))
    }

    pub fn query(&self, sql: Option<String>, parameters: Option<Vec<String>>) -> Result<(Rows<'_>, Vec<&str>)> {
        let mut stmt = self.inner.prepare(&*sql.unwrap())?;
        let rows = stmt.query(params_from_iter(parameters.unwrap()))?;
        let column_names = stmt.column_names();

        Ok((rows, column_names))
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
                let Ok((mut rows, columns)) = this.query(sql, params).into_lua_err();
                let mut table_builder = TableBuilder::new(lua)?;
                while let Some(row) = rows.next().unwrap() {
                    let mut row_builder = TableBuilder::new(lua)?;
                    for (i, column) in columns.iter().enumerate() {
                        let column_value = row.get_unwrap::<_, Value>(i);
                        let lua_value = lua.to_value(&column_value)?;
                        row_builder = row_builder.with_value(column.to_string(), lua_value)?;
                    }
                    table_builder = table_builder.with_sequential_value(row_builder.build()?)?;
                }
                table_builder.build()
            },
        );
    }
}