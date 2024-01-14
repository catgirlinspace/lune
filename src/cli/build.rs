use std::{
    env::{self, consts::EXE_EXTENSION},
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Context, Result};
use clap::Parser;
use console::style;
use mlua::Compiler as LuaCompiler;
use tokio::{fs, io::AsyncWriteExt as _};

use crate::executor::MetaChunk;

/// Build a standalone executable
#[derive(Debug, Clone, Parser)]
pub struct BuildCommand {
    /// The path to the input file
    pub input: PathBuf,

    /// The path to the output file - defaults to the
    /// input file path with an executable extension
    #[clap(short, long)]
    pub output: Option<PathBuf>,
}

impl BuildCommand {
    pub async fn run(self) -> Result<ExitCode> {
        let output_path = self
            .output
            .unwrap_or_else(|| self.input.with_extension(EXE_EXTENSION));

        let input_path_displayed = self.input.display();
        let output_path_displayed = output_path.display();

        // Try to read the input file
        let source_code = fs::read(&self.input)
            .await
            .context("failed to read input file")?;

        // Read the contents of the lune interpreter as our starting point
        println!(
            "Creating standalone binary using {}",
            style(input_path_displayed).green()
        );
        let mut patched_bin = fs::read(env::current_exe()?).await?;

        // Compile luau input into bytecode
        let bytecode = LuaCompiler::new()
            .set_optimization_level(2)
            .set_coverage_level(0)
            .set_debug_level(1)
            .compile(source_code);

        // Append the bytecode / metadata to the end
        let meta = MetaChunk { bytecode };
        patched_bin.extend_from_slice(&meta.to_bytes());

        // And finally write the patched binary to the output file
        println!(
            "Writing standalone binary to {}",
            style(output_path_displayed).blue()
        );
        write_executable_file_to(output_path, patched_bin).await?;

        Ok(ExitCode::SUCCESS)
    }
}

async fn write_executable_file_to(path: impl AsRef<Path>, bytes: impl AsRef<[u8]>) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);

    #[cfg(unix)]
    {
        options.mode(0o755); // Read & execute for all, write for owner
    }

    let mut file = options.open(path).await?;
    file.write_all(bytes.as_ref()).await?;

    Ok(())
}
