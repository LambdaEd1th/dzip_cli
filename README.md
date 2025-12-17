# dzip-cli

**dzip-cli** is a high-performance command-line tool written in Rust for unpacking and packing **Marmalade SDK** resource archives (`.dz` / `.dzip`).

It is designed to provide robust and accurate parsing capabilities, specifically addressing complex issues found in legacy archives such as compression header correction, implicit directory structure restoration, and split (multi-volume) archive handling.

## ✨ Features

  * **Full Unpacking**: Restores original directory structures and files from `.dz` archives to your local disk.
  * **High-Precision Packing**: Repacks resources into compliant `.dz` files based on auto-generated TOML configuration files.
  * **Extensive Compression Support**:
      * ✅ **LZMA** (Legacy format of standard 13-byte headers)
      * ✅ **ZLIB** (Deflate)
      * ✅ **BZIP2**
      * ✅ **COPY** (Store / No compression)
      * ✅ **ZERO** (Zero-block generation)
  * **Split Archives**: Automatically identifies, reads, and writes multi-volume archives (e.g., `data.dz`, `data.d01`, `data.d02`...).
  * **Smart Fixes**:
      * **ZSIZE Correction**: Automatically calculates real compressed sizes from offsets, fixing issues where the header reports incorrect sizes.
      * **Directory Restoration**: Handles the implicit root directory (`.`) logic specific to Marmalade archives.
      * **Cross-Platform Compatibility**: Automatically converts between Windows (`\`) and Unix (`/`) path separators during unpack/pack operations.

## 🛠️ Installation & Build

Ensure you have [Rust and Cargo](https://rustup.rs/) installed on your system.

1.  **Clone the repository**:

    ```bash
    git clone https://github.com/your-username/dzip-cli.git
    cd dzip-cli
    ```

2.  **Build release version**:

    ```bash
    cargo build --release
    ```

3.  **Run**:
    The compiled binary will be located at `./target/release/dzip-cli` (or `dzip-cli.exe` on Windows).

## 📖 Usage

### 1\. Unpacking

Reads a `.dz` file, extracts its content to a folder, and generates a `.toml` configuration file for repacking.

```bash
# Basic usage (extracts to a folder named after the input file)
dzip-cli unpack sample.dz

# Specify a custom output directory
dzip-cli unpack sample.dz --outdir my_output_folder
```

**Output artifacts:**

  * `sample/` (Folder): Contains all extracted raw resource files (images, JSONs, etc.).
  * `sample.toml` (File): Contains archive metadata, chunk mapping, and compression parameters.

### 2\. Packing

Reads a `.toml` configuration file, reads source files from the corresponding resource folder, and generates a new `.dz` archive.

```bash
# Just provide the config file
dzip-cli pack sample.toml
```

**Note**: The packer automatically looks for a resource folder with the same name as the config file in the same directory (e.g., `sample.toml` corresponds to the `sample/` folder).

**Output artifact:**

  * `sample_packed.dz`: The newly generated archive file (and potentially `.d01`, `.d02` if split).

-----

## ⚙️ Configuration Structure (TOML)

The generated TOML file is crucial for repacking. Here is an explanation of its structure:

```toml
[archive]
version = 0
total_files = 12
total_directories = 4
total_chunks = 20

# List of split archive filenames (if applicable)
archive_files = [] 

# File Mapping: Defines the relationship between logical paths and physical chunks
[[files]]
path = "textures/background.png"  # Logical path (automatically normalized)
directory = "textures"            # Parent directory
filename = "background.png"       # Filename
chunks = [0, 1]                   # This file consists of Chunk 0 and Chunk 1 stitched together

# Chunk Definitions: Physical properties of data blocks
[[chunks]]
id = 0
offset = 96                       # Offset in the .dz file (auto-calculated during pack)
size_compressed = 34812           # Compressed size
size_decompressed = 65536         # Uncompressed size
flags = ["LZMA"]                  # Compression algorithm flag
archive_file_index = 0            # Which split file this chunk belongs to (0 is the main file)
```

## ⚠️ Known Limitations

  * **Proprietary DZ Algorithm**: Does not support the proprietary compression algorithm flagged as `CHUNK_DZ (0x04)` (internal Marmalade format). If encountered during unpacking, the tool will report an error, but raw data will be preserved if possible.
  * **Encryption**: Does not support archives with DRM or custom encryption.

## 📄 License

This project is licensed under the **GNU General Public License v3.0 (GPLv3)**.

You may copy, distribute and modify the software as long as you track changes/dates in source files. Any modifications to or software including (via compiler) GPL-licensed code must also be made available under the GPL along with build & install instructions.

*Marmalade SDK is a trademark of its respective owners.*