use bitflags::bitflags;

pub const MAGIC: u32 = 0x5A525444; // 'DTRZ' in Little Endian
pub const CHUNK_LIST_TERMINATOR: u16 = 0xFFFF;

pub const CURRENT_DIR_STR: &str = ".";

pub const DEFAULT_BUFFER_SIZE: usize = 128 * 1024;

// This acts as the Single Source of Truth for flag handling.
pub const FLAG_MAPPINGS: &[(ChunkFlags, &str)] = &[
    (ChunkFlags::COMBUF, "COMBUF"),
    (ChunkFlags::DZ_RANGE, "DZ_RANGE"),
    (ChunkFlags::ZLIB, "ZLIB"),
    (ChunkFlags::BZIP, "BZIP"),
    (ChunkFlags::MP3, "MP3"),
    (ChunkFlags::JPEG, "JPEG"),
    (ChunkFlags::ZERO, "ZERO"),
    (ChunkFlags::COPYCOMP, "COPY"), // Maps COPYCOMP bit to "COPY" string
    (ChunkFlags::LZMA, "LZMA"),
    (ChunkFlags::RANDOMACCESS, "RANDOM_ACCESS"),
];

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ChunkFlags: u16 {
        const COMBUF       = 0x1;
        const DZ_RANGE     = 0x4;
        const ZLIB         = 0x8;
        const BZIP         = 0x10;
        const MP3          = 0x20;
        const JPEG         = 0x40;
        const ZERO         = 0x80;
        const COPYCOMP     = 0x100;
        const LZMA         = 0x200;
        const RANDOMACCESS = 0x400;
    }
}
