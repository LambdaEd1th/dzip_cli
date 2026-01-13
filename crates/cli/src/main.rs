use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn, LevelFilter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// Import core types and the Observer trait
use dzip_core::{create_default_registry, do_pack, do_unpack, DzipObserver};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Enable verbose logging (Debug level) for troubleshooting.
    /// This is a global argument usable with any subcommand.
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

        /// Optional output directory (default: same as input filename)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Keep raw data if decompression fails or for proprietary chunks
        #[arg(short, long)]
        keep_raw: bool,
    },
    /// Pack a directory into a .dz archive based on a .toml config
    Pack {
        /// Input .toml configuration file
        config: PathBuf,
    },
}

/// A concrete implementation of DzipObserver for the CLI.
/// Handles printing logs to stdout/stderr and updating the Indicatif progress bar.
struct CliProgressObserver {
    // ProgressBar is wrapped in Mutex because DzipObserver requires Sync.
    // Option allows us to initialize it only when needed.
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
    // 1. Parse CLI arguments first to check for the --verbose flag
    let cli = Cli::parse();

    // 2. Initialize the logger builder
    let mut builder = env_logger::Builder::from_default_env();

    // Set the default log level to 'Info' if RUST_LOG environment variable is not set.
    if std::env::var("RUST_LOG").is_err() {
        builder.filter(None, LevelFilter::Info);
    }

    // If --verbose is passed, force the log level to 'Debug'.
    if cli.verbose {
        builder.filter(None, LevelFilter::Debug);
    }

    builder.init();

    // 3. Create the codec registry
    let registry = create_default_registry();

    // 4. Create the observer for UI feedback
    let observer = CliProgressObserver::new();

    // 5. Execute the command logic
    let result = match &cli.command {
        Commands::Unpack {
            input,
            output,
            keep_raw,
        } => do_unpack(input, output.clone(), *keep_raw, &registry, &observer),
        Commands::Pack { config } => do_pack(config, &registry, &observer),
    };

    // 6. Handle errors gracefully (Optimized Error Reporting)
    if let Err(e) = result {
        // Print errors in red using ANSI escape codes for better visibility
        eprintln!("\x1b[31mError:\x1b[0m {:#}", e);
        std::process::exit(1);
    }
}
