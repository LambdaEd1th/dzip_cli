use crate::constants::{ChunkFlags, FLAG_MAPPINGS};
use crate::error::DzipError;
use std::borrow::Cow;
use std::io::{self, BufRead};
use std::path::{Component, Path, PathBuf};

use crate::Result as LibResult;

pub fn decode_flags(bits: u16) -> Vec<Cow<'static, str>> {
    let flags = ChunkFlags::from_bits_truncate(bits);
    let mut list = Vec::new();

    if flags.is_empty() {
        list.push(Cow::Borrowed("COPY"));
        return list;
    }

    for (flag, name) in FLAG_MAPPINGS {
        if flags.contains(*flag) {
            list.push(Cow::Borrowed(*name));
        }
    }

    list
}

pub fn encode_flags<S: AsRef<str>>(flags_vec: &[S]) -> u16 {
    let mut res = ChunkFlags::empty();

    if flags_vec.is_empty() {
        return res.bits();
    }

    for f in flags_vec {
        let s = f.as_ref();
        if let Some((flag, _)) = FLAG_MAPPINGS.iter().find(|(_, name)| *name == s) {
            res.insert(*flag);
        }
    }

    if res.is_empty() && flags_vec.iter().any(|f| f.as_ref() == "COPY") {
        res.insert(ChunkFlags::COPYCOMP);
    }

    res.bits()
}

// [Fixed]: Return std::io::Result directly
pub fn read_null_term_string<R: BufRead>(reader: &mut R) -> io::Result<String> {
    let mut bytes = Vec::new();
    reader.read_until(0, &mut bytes)?;
    if bytes.last() == Some(&0) {
        bytes.pop();
    }
    // lossy conversion is usually acceptable here, or map to InvalidData
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

// [Fixed]: Return library Result type (DzipError) directly
pub fn sanitize_path(base: &Path, rel_path_str: &str) -> LibResult<PathBuf> {
    let normalized = rel_path_str.replace('\\', "/");
    let rel_path = Path::new(&normalized);
    let mut safe_path = PathBuf::new();

    for component in rel_path.components() {
        match component {
            Component::Normal(os_str) => safe_path.push(os_str),
            Component::ParentDir => {
                return Err(DzipError::Security(format!(
                    "Directory traversal (..) detected in path: {}",
                    rel_path_str
                )));
            }
            Component::RootDir => continue,
            Component::Prefix(_) => {
                return Err(DzipError::Security(format!(
                    "Absolute path or drive letter detected: {}",
                    rel_path_str
                )));
            }
            Component::CurDir => continue,
        }
    }

    if safe_path.as_os_str().is_empty() {
        return Err(DzipError::Security(format!(
            "Invalid empty path resolution: {}",
            rel_path_str
        )));
    }

    Ok(base.join(safe_path))
}

pub fn normalize_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(..) => {}
            Component::RootDir => {
                result.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            Component::Normal(c) => {
                result.push(c);
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        let p = Path::new("foo/bar/../baz");
        assert_eq!(normalize_path(p), PathBuf::from("foo/baz"));

        let p = Path::new("./foo/./bar");
        assert_eq!(normalize_path(p), PathBuf::from("foo/bar"));
    }
}
