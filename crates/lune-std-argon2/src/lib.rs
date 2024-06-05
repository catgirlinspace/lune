#![allow(clippy::cargo_common_metadata)]

use mlua::prelude::*;

use lune_utils::TableBuilder;

use argon2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher, PasswordVerifier, SaltString
    },
    Argon2
};
use mlua::{ExternalResult};

use Result::Err;

/**
Creates the `datetime` standard library module.

# Errors

Errors when out of memory.
 */
pub fn module(lua: &Lua) -> LuaResult<LuaTable> {
    TableBuilder::new(lua)?
        .with_function("hash", |_, input: String| {
            let salt = SaltString::generate(&mut OsRng);
            match Argon2::default().hash_password(input.as_ref(), &salt) {
                Ok(result) => {
                    Ok(result.to_string())
                }
                Err(err) => {
                    Err(LuaError::RuntimeError(format!("failed to hash: {err}")))
                }
            }
        })?
        .with_function("verify", |_, (input, hash): (String, String)| {
            match PasswordHash::new(&hash) {
                Ok(parsed) => {
                    let matches = Argon2::default().verify_password(input.as_ref(), &parsed).is_ok();
                    Ok(matches)
                }
                Err(err) => { Err(LuaError::RuntimeError(format!("failed to parse hash: {err}"))) }
            }
        })?
        .build_readonly()
}
