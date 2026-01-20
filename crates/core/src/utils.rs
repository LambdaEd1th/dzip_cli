use crate::format::{ChunkFlags, FLAG_MAPPINGS};
use std::borrow::Cow;
use std::io::BufRead;
use std::path::{Component, MAIN_SEPARATOR_STR, Path};

/// Reads a null-terminated string from a reader.
pub fn read_null_term_string<R: BufRead>(reader: &mut R) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    reader.read_until(0, &mut bytes)?;
    if bytes.last() == Some(&0) {
        bytes.pop();
    }
    String::from_utf8(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Encodes a list of string flags into a u16 bitmask using FLAG_MAPPINGS.
pub fn encode_flags(flags: &[Cow<'_, str>]) -> u16 {
    let mut mask = ChunkFlags::empty();

    for input_flag in flags {
        for (flag_bit, flag_str) in FLAG_MAPPINGS {
            if input_flag.as_ref() == *flag_str {
                mask.insert(*flag_bit);
            }
        }
    }

    mask.bits()
}

/// Decodes a u16 bitmask into a list of string flags using FLAG_MAPPINGS.
pub fn decode_flags(bits: u16) -> Vec<Cow<'static, str>> {
    let flags = ChunkFlags::from_bits_truncate(bits);
    let mut list = Vec::new();

    for (flag_bit, flag_str) in FLAG_MAPPINGS {
        if flags.contains(*flag_bit) {
            list.push(Cow::Borrowed(*flag_str));
        }
    }

    list
}

/// Helper to parse path components robustly, handling both '/' and '\' on all platforms.
/// This ensures we can process Windows-created archives on Unix, and vice versa.
fn get_robust_components(path: &Path) -> Vec<String> {
    let mut parts = Vec::new();
    for c in path.components() {
        if let Component::Normal(os) = c {
            let s = os.to_string_lossy();

            // Handle Windows paths even on Unix by manually splitting backslashes.
            // On Windows, Path::components does this automatically.
            // On macOS/Linux, '\' is a valid char in filenames, so we must split manually.
            let segments = if s.contains('\\') {
                s.split('\\').collect::<Vec<_>>()
            } else {
                vec![s.as_ref()]
            };

            for segment in segments {
                // Security: Filter out ".." and "." to prevent directory traversal
                if segment == ".." || segment == "." || segment.is_empty() {
                    continue;
                }
                parts.push(segment.to_string());
            }
        }
    }
    parts
}

/// Converts a path to the OS-specific format.
/// Unix: "a/b/c", Windows: "a\b\c"
/// Used for: Unpacking files to disk, creating TOML configs, CLI output.
pub fn to_native_path(path: &Path) -> String {
    let parts = get_robust_components(path);
    if parts.is_empty() {
        return ".".to_string();
    }
    // Joins using the current OS separator
    parts.join(MAIN_SEPARATOR_STR)
}

/// Converts a path to the standard Archive format.
/// Always: "a\b\c" (Windows Backslash)
/// Used for: Internal archive headers (writing to .dz files), Pack logic.
pub fn to_archive_path(path: &Path) -> String {
    let parts = get_robust_components(path);
    if parts.is_empty() {
        return ".".to_string();
    }
    // regardless of the OS this tool is running on.
    parts.join("\\")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn test_encode_flags_single() {
        let flags = vec![Cow::Borrowed("LZMA")];
        assert_eq!(encode_flags(&flags), ChunkFlags::LZMA.bits());
    }

    #[test]
    fn test_encode_flags_multiple() {
        let flags = vec![Cow::Borrowed("ZLIB"), Cow::Borrowed("COMBUF")];
        let expected = ChunkFlags::ZLIB.bits() | ChunkFlags::COMBUF.bits();
        assert_eq!(encode_flags(&flags), expected);
    }

    #[test]
    fn test_encode_flags_empty() {
        let flags: Vec<Cow<'_, str>> = vec![];
        assert_eq!(encode_flags(&flags), 0);
    }

    #[test]
    fn test_decode_flags_single() {
        let bits = ChunkFlags::LZMA.bits();
        let decoded = decode_flags(bits);
        assert_eq!(decoded, vec![Cow::Borrowed("LZMA")]);
    }

    #[test]
    fn test_decode_flags_multiple() {
        let bits = ChunkFlags::ZLIB.bits() | ChunkFlags::BZIP.bits();
        let decoded = decode_flags(bits);
        assert!(decoded.contains(&Cow::Borrowed("ZLIB")));
        assert!(decoded.contains(&Cow::Borrowed("BZIP")));
    }

    #[test]
    fn test_to_native_path_simple() {
        let path = Path::new("a/b/c");
        let result = to_native_path(path);
        #[cfg(unix)]
        assert_eq!(result, "a/b/c");
        #[cfg(windows)]
        assert_eq!(result, "a\\b\\c");
    }

    #[test]
    fn test_to_archive_path_simple() {
        let path = Path::new("a/b/c");
        let result = to_archive_path(path);
        assert_eq!(result, "a\\b\\c");
    }

    #[test]
    fn test_roundtrip_flags() {
        let original = vec![Cow::Borrowed("LZMA"), Cow::Borrowed("COMBUF")];
        let encoded = encode_flags(&original);
        let decoded = decode_flags(encoded);
        for flag in &original {
            assert!(decoded.contains(flag));
        }
    }
}
