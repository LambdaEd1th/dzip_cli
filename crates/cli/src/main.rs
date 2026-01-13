use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use log::{LevelFilter, info, warn};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// Import do_list
use dzip_core::{
    DzipObserver, StdFileSystem, create_default_registry, do_list, do_pack, do_unpack,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Enable verbose logging (Debug level) for troubleshooting.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Unpack a .dz archive
    Unpack {
        /// Input .dz file
        input: PathBuf,

        /// Optional output directory
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Keep raw data if decompression fails
        #[arg(short, long)]
        keep_raw: bool,
    },
    /// Pack a directory into a .dz archive based on a .toml config
    Pack {
        /// Input .toml configuration file
        config: PathBuf,
    },
    /// List contents of a .dz archive without unpacking
    List {
        /// Input .dz file
        input: PathBuf,
    },
}

struct CliProgressObserver {
    pb: Arc<Mutex<Option<ProgressBar>>>,
}

impl CliProgressObserver {
    fn new() -> Self {
        Self {
            pb: Arc::new(Mutex::new(None)),
        }
    }
}

impl DzipObserver for CliProgressObserver {
    fn info(&self, message: &str) {
        info!("{}", message);
    }

    fn warn(&self, message: &str) {
        warn!("{}", message);
    }

    fn progress_start(&self, total: u64) {
        let pb = ProgressBar::new(total);
        let style = ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("#>-");
        pb.set_style(style);

        let mut lock = self.pb.lock().unwrap();
        *lock = Some(pb);
    }

    fn progress_inc(&self, delta: u64) {
        let lock = self.pb.lock().unwrap();
        if let Some(pb) = lock.as_ref() {
            pb.inc(delta);
        }
    }

    fn progress_finish(&self, message: &str) {
        let lock = self.pb.lock().unwrap();
        if let Some(pb) = lock.as_ref() {
            pb.finish_with_message(message.to_string());
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let mut builder = env_logger::Builder::from_default_env();
    if std::env::var("RUST_LOG").is_err() {
        builder.filter(None, LevelFilter::Info);
    }
    if cli.verbose {
        builder.filter(None, LevelFilter::Debug);
    }
    builder.init();

    let registry = create_default_registry();
    let observer = CliProgressObserver::new();
    let fs = StdFileSystem;

    let result = match &cli.command {
        Commands::Unpack {
            input,
            output,
            keep_raw,
        } => do_unpack(input, output.clone(), *keep_raw, &registry, &observer, &fs),

        Commands::Pack { config } => do_pack(config, &registry, &observer, &fs),

        Commands::List { input } => {
            match do_list(input, &observer, &fs) {
                Ok(entries) => {
                    // Display results in a tabular format
                    println!();
                    // [Fix] Removed arguments for literal strings as per clippy suggestions
                    println!("{:<15} | {:<8} | Path", "Size (Bytes)", "Chunks");
                    println!("{:-<15}-|-{:-<8}-|--------------------------------", "", "");

                    for entry in &entries {
                        println!(
                            "{:<15} | {:<8} | {}",
                            entry.original_size, entry.chunk_count, entry.path
                        );
                    }
                    println!();
                    println!("Total files: {}", entries.len());
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
    };

    if let Err(e) = result {
        eprintln!("\x1b[31mError:\x1b[0m {:#}", e);
        std::process::exit(1);
    }
}
