use crate::constants::*;
use anyhow::Result;
use std::io::Read;

pub fn decode_flags(flags: u16) -> Vec<String> {
    let mut list = Vec::new();
    if flags == 0 {
        list.push("COPY".to_string());
        return list;
    }

    if flags & CHUNK_COMBUF != 0 {
        list.push("COMBUF".to_string());
    }
    if flags & CHUNK_DZ != 0 {
        list.push("DZ_RANGE".to_string());
    }
    if flags & CHUNK_ZLIB != 0 {
        list.push("ZLIB".to_string());
    }
    if flags & CHUNK_BZIP != 0 {
        list.push("BZIP".to_string());
    }
    if flags & CHUNK_MP3 != 0 {
        list.push("MP3".to_string());
    }
    if flags & CHUNK_JPEG != 0 {
        list.push("JPEG".to_string());
    }
    if flags & CHUNK_ZERO != 0 {
        list.push("ZERO".to_string());
    }
    if flags & CHUNK_COPYCOMP != 0 {
        list.push("COPY".to_string());
    }
    if flags & CHUNK_LZMA != 0 {
        list.push("LZMA".to_string());
    }
    if flags & CHUNK_RANDOMACCESS != 0 {
        list.push("RANDOM_ACCESS".to_string());
    }

    list
}

pub fn encode_flags(flags_vec: &[String]) -> u16 {
    let mut res = 0;
    if flags_vec.is_empty() {
        return 0;
    }
    for f in flags_vec {
        match f.as_str() {
            "COMBUF" => res |= CHUNK_COMBUF,
            "DZ_RANGE" => res |= CHUNK_DZ,
            "ZLIB" => res |= CHUNK_ZLIB,
            "BZIP" => res |= CHUNK_BZIP,
            "MP3" => res |= CHUNK_MP3,
            "JPEG" => res |= CHUNK_JPEG,
            "ZERO" => res |= CHUNK_ZERO,
            "COPY" => res |= CHUNK_COPYCOMP,
            "LZMA" => res |= CHUNK_LZMA,
            "RANDOM_ACCESS" => res |= CHUNK_RANDOMACCESS,
            _ => {}
        }
    }
    if res == 0 && flags_vec.contains(&"COPY".to_string()) {
        res |= CHUNK_COPYCOMP;
    }
    res
}

pub fn read_null_term_string<R: Read>(reader: &mut R) -> Result<String> {
    let mut bytes = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        reader.read_exact(&mut byte)?;
        if byte[0] == 0 {
            break;
        }
        bytes.push(byte[0]);
    }
    Ok(String::from_utf8_lossy(&bytes).to_string())
}
