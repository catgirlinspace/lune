use std::collections::HashMap;
use rusqlite::{Connection, params_from_iter, Result};
use lune_utils::TableBuilder;

#[derive(Debug)]
pub struct SQLite {
    // NOTE: We store this as the UTC time zone since it is the most commonly
    // used and getting the generics right for TimeZone is somewhat tricky,
    // but none of the method implementations below should rely on this tz
    inner: Connection,
}

impl SQLite {
    pub fn connect(path: impl AsRef<str>) -> Result<SQLite> {
        let inner = Connection::open(path)?;
        Ok(Self { inner })
    }

    pub fn execute(&self, sql: Option<&str>, parameters: Option<&Vec<str>>) -> Result<usize> {
        self.inner.execute(sql.unwrap(), params_from_iter(parameters.unwrap()))
    }

    pub fn query(&self, sql: Option<&str>, parameters: Option<&Vec<str>>) -> mlua::Result<Table<'lua>> {
        let mut stmt = self.inner.prepare(sql.unwrap())?;
        let mut rows = stmt.query(params_from_iter(parameters.unwrap()))?;
        let mut lua_rows = TableBuilder::new()?; // needs a lua... idk how to do
        let column_names = stmt.column_names();

        while let Some(row) = rows.next()? {
            let mut data = HashMap::new();
            for (i, column_name) in column_names.iter().enumerate() {
                data.insert(column_name, row.get_unwrap(i));
            }
            lua_rows.with_sequential_value(data)?;
        };
        lua_rows.build()
    }
}