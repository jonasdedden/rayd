//! Local-filesystem `SpillBackend`.
//!
//! Stores each spilled object as one file under a base directory.
//! On-disk layout:
//!
//! ```text
//! [u32 LE: metadata_len]
//! [metadata bytes]
//! [u64 LE: data_len]
//! [data bytes]
//! ```
//!
//! Filenames are `<hex object_id>.spill` so `ls`-ing the directory is
//! human-readable when debugging. The `SpillUrl` returned is
//! `file:///absolute/path/to/<hex>.spill`.

use std::fs::{self, File};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

/// Per-process counter for unique temp filenames. Two concurrent
/// spills of the same `object_id` would otherwise collide on the
/// `.spill.tmp` path; the counter gives each writer a fresh name so
/// neither truncates the other's in-flight bytes before the rename.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

use super::{ObjectIdBytes, RestoredObject, SpillBackend, SpillError, SpillUrl};

/// Spill backend that writes to a local directory.
#[derive(Debug, Clone)]
pub struct LocalFsBackend {
    root: PathBuf,
}

impl LocalFsBackend {
    /// Create a backend rooted at `root`. Creates the directory if it
    /// doesn't exist; existing contents are left alone (so a restart
    /// can rediscover already-spilled objects).
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, SpillError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let canonical = fs::canonicalize(&root)?;
        Ok(Self { root: canonical })
    }

    fn path_for(&self, object_id: ObjectIdBytes) -> PathBuf {
        let mut name = String::with_capacity(object_id.len() * 2 + ".spill".len());
        for b in object_id {
            use std::fmt::Write as _;
            let _ = write!(name, "{b:02x}");
        }
        name.push_str(".spill");
        self.root.join(name)
    }

    fn url_for(path: &Path) -> SpillUrl {
        SpillUrl(format!("file://{}", path.display()))
    }

    fn parse_url<'a>(&self, url: &'a SpillUrl) -> Result<&'a Path, SpillError> {
        let stripped = url.0.strip_prefix("file://").ok_or_else(|| SpillError::Corrupt {
            url: url.0.clone(),
            reason: "url missing file:// prefix".to_owned(),
        })?;
        let path = Path::new(stripped);
        // Reject urls that escape the configured root — defends
        // against a buggy caller (or future remote operator) handing
        // us a path that isn't ours to touch.
        if !path.starts_with(&self.root) {
            return Err(SpillError::Corrupt {
                url: url.0.clone(),
                reason: format!(
                    "path is outside spill root {}",
                    self.root.display()
                ),
            });
        }
        Ok(path)
    }
}

impl SpillBackend for LocalFsBackend {
    fn spill(
        &self,
        object_id: ObjectIdBytes,
        metadata: Bytes,
        data: Bytes,
    ) -> Result<SpillUrl, SpillError> {
        let path = self.path_for(object_id);

        // Write to a unique temp file first, then atomically rename —
        // keeps a partial file from looking like a successful spill
        // if the process crashes mid-write, AND avoids two concurrent
        // re-spills of the same id from clobbering each other's
        // in-flight bytes (each writer gets its own temp path; the
        // rename is the linearization point).
        let seq = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = path.with_extension(format!("spill.tmp.{seq}"));
        {
            let mut file = File::create(&tmp_path)?;
            let metadata_len = u32::try_from(metadata.len()).map_err(|_| SpillError::Corrupt {
                url: tmp_path.display().to_string(),
                reason: format!("metadata too large: {} bytes", metadata.len()),
            })?;
            file.write_all(&metadata_len.to_le_bytes())?;
            file.write_all(&metadata)?;
            file.write_all(&u64::try_from(data.len()).unwrap_or(u64::MAX).to_le_bytes())?;
            file.write_all(&data)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &path)?;
        Ok(Self::url_for(&path))
    }

    fn restore(&self, url: &SpillUrl) -> Result<RestoredObject, SpillError> {
        let path = self.parse_url(url)?;
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(SpillError::NotFound { url: url.0.clone() });
            }
            Err(e) => return Err(e.into()),
        };

        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf).map_err(|e| corrupt_io(url, e))?;
        let metadata_len = u32::from_le_bytes(len_buf) as usize;
        let mut metadata = vec![0u8; metadata_len];
        file.read_exact(&mut metadata).map_err(|e| corrupt_io(url, e))?;

        let mut data_len_buf = [0u8; 8];
        file.read_exact(&mut data_len_buf).map_err(|e| corrupt_io(url, e))?;
        let data_len = usize::try_from(u64::from_le_bytes(data_len_buf)).map_err(|_| {
            SpillError::Corrupt {
                url: url.0.clone(),
                reason: "data length doesn't fit in usize".to_owned(),
            }
        })?;
        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data).map_err(|e| corrupt_io(url, e))?;

        Ok(RestoredObject {
            metadata: Bytes::from(metadata),
            data: Bytes::from(data),
        })
    }

    fn remove(&self, url: &SpillUrl) -> Result<(), SpillError> {
        let path = self.parse_url(url)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

fn corrupt_io(url: &SpillUrl, e: std::io::Error) -> SpillError {
    SpillError::Corrupt {
        url: url.0.clone(),
        reason: format!("truncated or unreadable: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn obj_id(seed: u8) -> ObjectIdBytes {
        let mut id = [0u8; 28];
        id[0] = seed;
        id
    }

    #[test]
    fn spill_then_restore_round_trips() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();

        let metadata = Bytes::from_static(b"meta-bytes");
        let data = Bytes::from_static(b"hello, plasma!");
        let url = backend.spill(obj_id(1), metadata.clone(), data.clone()).unwrap();
        assert!(url.as_str().starts_with("file://"));

        let restored = backend.restore(&url).unwrap();
        assert_eq!(restored.metadata, metadata);
        assert_eq!(restored.data, data);
    }

    #[test]
    fn spill_handles_empty_payloads() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();

        let url = backend.spill(obj_id(2), Bytes::new(), Bytes::new()).unwrap();
        let restored = backend.restore(&url).unwrap();
        assert!(restored.metadata.is_empty());
        assert!(restored.data.is_empty());
    }

    #[test]
    fn re_spill_overwrites() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();

        let url1 = backend
            .spill(obj_id(3), Bytes::from_static(b"m1"), Bytes::from_static(b"d1"))
            .unwrap();
        let url2 = backend
            .spill(obj_id(3), Bytes::from_static(b"m2"), Bytes::from_static(b"d2"))
            .unwrap();
        assert_eq!(url1, url2, "same object_id maps to the same url");

        let restored = backend.restore(&url1).unwrap();
        assert_eq!(restored.metadata, Bytes::from_static(b"m2"));
        assert_eq!(restored.data, Bytes::from_static(b"d2"));
    }

    #[test]
    fn remove_drops_the_file() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();

        let url = backend
            .spill(obj_id(4), Bytes::from_static(b"m"), Bytes::from_static(b"d"))
            .unwrap();
        backend.remove(&url).unwrap();
        let err = backend.restore(&url).unwrap_err();
        assert!(matches!(err, SpillError::NotFound { .. }), "got {err:?}");
    }

    #[test]
    fn remove_is_idempotent_on_missing() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();

        // Manufacture a url that points to a non-existent file under
        // the spill root.
        let path = backend.path_for(obj_id(5));
        let url = LocalFsBackend::url_for(&path);
        backend.remove(&url).expect("removing an absent file is a no-op");
    }

    #[test]
    fn restore_unknown_url_is_not_found() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();
        let path = backend.path_for(obj_id(6));
        let url = LocalFsBackend::url_for(&path);
        let err = backend.restore(&url).unwrap_err();
        assert!(matches!(err, SpillError::NotFound { .. }), "got {err:?}");
    }

    #[test]
    fn url_outside_root_is_corrupt() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();
        let url = SpillUrl("file:///etc/passwd".to_owned());
        let err = backend.restore(&url).unwrap_err();
        assert!(matches!(err, SpillError::Corrupt { .. }), "got {err:?}");
    }

    #[test]
    fn truncated_file_is_corrupt() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(tmp.path()).unwrap();

        // Write a deliberately too-short file at a valid spill path.
        let path = backend.path_for(obj_id(7));
        fs::write(&path, [0u8; 2]).unwrap(); // less than the 4-byte header

        let url = LocalFsBackend::url_for(&path);
        let err = backend.restore(&url).unwrap_err();
        assert!(matches!(err, SpillError::Corrupt { .. }), "got {err:?}");
    }

    #[test]
    fn re_open_finds_existing_spilled_objects() {
        let tmp = TempDir::new().unwrap();

        let url = {
            let backend = LocalFsBackend::new(tmp.path()).unwrap();
            backend
                .spill(obj_id(8), Bytes::from_static(b"meta"), Bytes::from_static(b"data"))
                .unwrap()
        };
        // Drop and re-open the backend at the same root.
        let backend2 = LocalFsBackend::new(tmp.path()).unwrap();
        let restored = backend2.restore(&url).unwrap();
        assert_eq!(restored.data, Bytes::from_static(b"data"));
    }
}
