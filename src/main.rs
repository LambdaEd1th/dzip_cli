use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod compression;
mod constants;
mod pack;
mod types;
mod unpack;
mod utils;

#[derive(Parser)]
#[command(
    name = "dzip-cli",
    author = "Ed1th",
    version,
    about = "Marmalade SDK .dz Archive Tool",
    long_about = "A CLI tool to unpack and pack Marmalade SDK .dz archives."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Unpack a .dz archive
    Unpack {
        /// Input .dz file
        input: PathBuf,
        /// Output directory (optional)
        #[arg(short, long)]
        outdir: Option<PathBuf>,
    },
    /// Pack a directory based on a TOML config
    Pack {
        /// Input config.toml file
        config: PathBuf,
    },
}

fn main() {
    let args = Cli::parse();

    let res = match args.command {
        Commands::Unpack { input, outdir } => unpack::do_unpack(&input, outdir),
        Commands::Pack { config } => pack::do_pack(&config),
    };

    if let Err(e) = res {
        eprintln!("[Error] {:#}", e);
        std::process::exit(1);
    }
}
