use mlua::prelude::LuaUserData;

#[derive(Debug)]
pub struct NullUserdata {}

impl LuaUserData for NullUserdata {}
