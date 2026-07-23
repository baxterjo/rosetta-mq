use std::path::{Path, PathBuf};

/// Resolves `path` against `base_dir` unless `path` is already absolute. Used to keep
/// file paths in config (proto files, cert/key files, etc.) relative to the config file's
/// directory, not the process's CWD.
pub fn resolve_path(base_dir: &Path, path: &str) -> PathBuf {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}
