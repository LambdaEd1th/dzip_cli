pub const MAGIC: u32 = 0x5A525444; // 'DTRZ' in Little Endian

// Chunk Flags
pub const CHUNK_COMBUF: u16 = 0x1;
pub const CHUNK_DZ: u16 = 0x4;
pub const CHUNK_ZLIB: u16 = 0x8;
pub const CHUNK_BZIP: u16 = 0x10;
pub const CHUNK_MP3: u16 = 0x20;
pub const CHUNK_JPEG: u16 = 0x40;
pub const CHUNK_ZERO: u16 = 0x80;
pub const CHUNK_COPYCOMP: u16 = 0x100;
pub const CHUNK_LZMA: u16 = 0x200;
pub const CHUNK_RANDOMACCESS: u16 = 0x400;