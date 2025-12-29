use crate::constants::ChunkFlags; // [Fixed]: Removed unused DEFAULT_BUFFER_SIZE
use crate::error::DzipError;
use anyhow::{Context, Result};
use std::io::{self, Read, Write};
use std::sync::Arc;

pub trait Decompressor: Send + Sync {
    fn decompress(&self, input: &mut dyn Read, output: &mut dyn Write, len: u32) -> Result<()>;
}

pub trait Compressor: Send + Sync {
    fn compress(&self, input: &mut dyn Read, output: &mut dyn Write) -> Result<()>;
}

#[derive(Clone)]
pub struct CodecRegistry {
    decompressors: Vec<(ChunkFlags, Arc<dyn Decompressor>)>,
    compressors: Vec<(ChunkFlags, Arc<dyn Compressor>)>,
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecRegistry {
    pub fn new() -> Self {
        Self {
            decompressors: Vec::new(),
            compressors: Vec::new(),
        }
    }

    pub fn register_decompressor<D: Decompressor + 'static>(
        &mut self,
        mask: ChunkFlags,
        decompressor: D,
    ) {
        self.decompressors.push((mask, Arc::new(decompressor)));
    }

    pub fn register_compressor<C: Compressor + 'static>(
        &mut self,
        mask: ChunkFlags,
        compressor: C,
    ) {
        self.compressors.push((mask, Arc::new(compressor)));
    }

    pub fn decompress(
        &self,
        input: &mut dyn Read,
        output: &mut dyn Write,
        flags_raw: u16,
        len: u32,
    ) -> Result<()> {
        let flags = ChunkFlags::from_bits_truncate(flags_raw);
        for (mask, decoder) in &self.decompressors {
            if flags.intersects(*mask) {
                return decoder.decompress(input, output, len);
            }
        }
        // Fallback: copy directly (e.g. for COPY or unknown flags processed as raw)
        io::copy(input, output)?;
        Ok(())
    }

    pub fn compress(
        &self,
        input: &mut dyn Read,
        output: &mut dyn Write,
        flags_raw: u16,
    ) -> Result<()> {
        let flags = ChunkFlags::from_bits_truncate(flags_raw);
        for (mask, encoder) in &self.compressors {
            if flags.intersects(*mask) {
                return encoder.compress(input, output);
            }
        }
        // Fallback: copy directly
        io::copy(input, output)?;
        Ok(())
    }
}

// --- Implementations ---

struct ZeroDecompressor;
impl Decompressor for ZeroDecompressor {
    fn decompress(&self, _input: &mut dyn Read, output: &mut dyn Write, len: u32) -> Result<()> {
        // [Optimization] Use std::io::repeat to avoid allocating a large vector of zeros.
        let mut zero_reader = std::io::repeat(0).take(len as u64);
        io::copy(&mut zero_reader, output)?;
        Ok(())
    }
}

struct LzmaDecompressor;
impl Decompressor for LzmaDecompressor {
    fn decompress(&self, input: &mut dyn Read, output: &mut dyn Write, _len: u32) -> Result<()> {
        let mut lzma_reader = lzma_rust2::LzmaReader::new_mem_limit(input, u32::MAX, None)
            .map_err(|e| DzipError::Decompression(format!("LZMA init: {}", e)))?;
        io::copy(&mut lzma_reader, output)
            .map_err(|e| DzipError::Decompression(format!("LZMA read: {}", e)))?;
        Ok(())
    }
}

struct ZlibDecompressor;
impl Decompressor for ZlibDecompressor {
    fn decompress(&self, input: &mut dyn Read, output: &mut dyn Write, _len: u32) -> Result<()> {
        let mut d = flate2::read::ZlibDecoder::new(input);
        io::copy(&mut d, output)
            .map_err(|e| DzipError::Decompression(format!("ZLIB read: {}", e)))?;
        Ok(())
    }
}

struct Bzip2Decompressor;
impl Decompressor for Bzip2Decompressor {
    fn decompress(&self, input: &mut dyn Read, output: &mut dyn Write, _len: u32) -> Result<()> {
        let mut d = bzip2::read::BzDecoder::new(input);
        io::copy(&mut d, output)
            .map_err(|e| DzipError::Decompression(format!("BZIP2 read: {}", e)))?;
        Ok(())
    }
}

struct PassThroughDecompressor;
impl Decompressor for PassThroughDecompressor {
    fn decompress(&self, input: &mut dyn Read, output: &mut dyn Write, _len: u32) -> Result<()> {
        // Direct copy for DZ_RANGE or COPY
        io::copy(input, output)?;
        Ok(())
    }
}

// --- Compressors ---

struct LzmaCompressor;
impl Compressor for LzmaCompressor {
    fn compress(&self, input: &mut dyn Read, output: &mut dyn Write) -> Result<()> {
        let options = lzma_rust2::LzmaOptions::default();
        let mut w = lzma_rust2::LzmaWriter::new_use_header(output, &options, None)
            .context("Failed to initialize LZMA writer")?;
        io::copy(input, &mut w)?;
        w.finish()?;
        Ok(())
    }
}

struct ZlibCompressor;
impl Compressor for ZlibCompressor {
    fn compress(&self, input: &mut dyn Read, output: &mut dyn Write) -> Result<()> {
        let mut e = flate2::write::ZlibEncoder::new(output, flate2::Compression::default());
        io::copy(input, &mut e)?;
        e.finish()?;
        Ok(())
    }
}

struct Bzip2Compressor;
impl Compressor for Bzip2Compressor {
    fn compress(&self, input: &mut dyn Read, output: &mut dyn Write) -> Result<()> {
        let mut e = bzip2::write::BzEncoder::new(output, bzip2::Compression::default());
        io::copy(input, &mut e)?;
        e.finish()?;
        Ok(())
    }
}

struct PassThroughCompressor;
impl Compressor for PassThroughCompressor {
    fn compress(&self, input: &mut dyn Read, output: &mut dyn Write) -> Result<()> {
        io::copy(input, output)?;
        Ok(())
    }
}

pub fn create_default_registry() -> CodecRegistry {
    let mut reg = CodecRegistry::new();
    reg.register_decompressor(ChunkFlags::ZERO, ZeroDecompressor);
    reg.register_decompressor(ChunkFlags::DZ_RANGE, PassThroughDecompressor);
    reg.register_decompressor(ChunkFlags::LZMA, LzmaDecompressor);
    reg.register_decompressor(ChunkFlags::ZLIB, ZlibDecompressor);
    reg.register_decompressor(ChunkFlags::BZIP, Bzip2Decompressor);

    reg.register_compressor(ChunkFlags::LZMA, LzmaCompressor);
    reg.register_compressor(ChunkFlags::ZLIB, ZlibCompressor);
    reg.register_compressor(ChunkFlags::BZIP, Bzip2Compressor);
    // Register PassThroughCompressor for DZ_RANGE (Proprietary format)
    reg.register_compressor(ChunkFlags::DZ_RANGE, PassThroughCompressor);
    reg
}
