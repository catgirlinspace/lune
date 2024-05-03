#![allow(clippy::cargo_common_metadata)]

mod sqlite;

use mlua::prelude::*;

use lune_utils::TableBuilder;

use rusqlite::{Connection, Result};

/**
Creates the `datetime` standard library module.

# Errors

Errors when out of memory.
 */
pub fn module(lua: &Lua) -> LuaResult<LuaTable> {
    TableBuilder::new(lua)?
        .with_function("new", |_, path: String| {
            Ok(SQLite::connect(path)?)
        })?
        .build_readonly()
}
