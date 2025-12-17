use crate::constants::*;
use anyhow::{Context, Result, anyhow};
use std::io::{Cursor, Read, Write};

pub fn decompress_chunk(data: &[u8], flags: u16, expected_len: u32) -> Result<Vec<u8>> {
    if flags & CHUNK_ZERO != 0 {
        return Ok(vec![0u8; expected_len as usize]);
    }
    if flags & CHUNK_DZ != 0 {
        return Err(anyhow!(
            "Proprietary Marmalade Range Encoding (CHUNK_DZ) is not supported."
        ));
    }

    if flags & CHUNK_LZMA != 0 {
        let mut output = Vec::with_capacity(expected_len as usize);
        let mut reader = Cursor::new(data);
        let mut lzma_reader = lzma_rust2::LzmaReader::new_mem_limit(&mut reader, u32::MAX, None)
            .map_err(|e| anyhow!("Failed to initialize LZMA reader: {}", e))?;
        lzma_reader
            .read_to_end(&mut output)
            .context("LZMA decompress failed")?;
        return Ok(output);
    }

    if flags & CHUNK_ZLIB != 0 {
        let mut d = flate2::read::ZlibDecoder::new(data);
        let mut buf = Vec::new();
        d.read_to_end(&mut buf)?;
        return Ok(buf);
    }

    if flags & CHUNK_BZIP != 0 {
        let mut d = bzip2::read::BzDecoder::new(data);
        let mut buf = Vec::new();
        d.read_to_end(&mut buf).context("BZIP2 decompress failed")?;
        return Ok(buf);
    }

    // Default COPY
    Ok(data.to_vec())
}

pub fn compress_data(data: &[u8], flags: u16) -> Result<Vec<u8>> {
    if flags & CHUNK_ZERO != 0 {
        return Ok(Vec::new());
    }
    if flags & CHUNK_DZ != 0 {
        return Err(anyhow!(
            "Proprietary Marmalade Range Encoding (CHUNK_DZ) compression is not supported."
        ));
    }

    if flags & CHUNK_LZMA != 0 {
        let mut output = Vec::new();
        let writer = Cursor::new(&mut output);
        let options = lzma_rust2::LzmaOptions::default();
        let mut lzma_writer: lzma_rust2::LzmaWriter<Cursor<&mut Vec<u8>>> =
            lzma_rust2::LzmaWriter::new_use_header(writer, &options, None)
                .context("Failed to initialize LZMA writer")?;
        lzma_writer
            .write_all(data)
            .context("LZMA compress write failed")?;
        lzma_writer
            .finish()
            .context("LZMA compress finish failed")?;
        return Ok(output);
    }

    if flags & CHUNK_ZLIB != 0 {
        let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(data)?;
        return Ok(e.finish()?);
    }

    if flags & CHUNK_BZIP != 0 {
        let mut e = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
        e.write_all(data)?;
        return Ok(e.finish()?);
    }

    Ok(data.to_vec())
}
