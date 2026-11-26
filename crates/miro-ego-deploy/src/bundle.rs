//! Native Miro-Ego release bundle verification (`miro-ego.rdk-x5-bundle.v1`), AArch64 ELF64
//! validation, SHA-256 manifest verification, path-traversal/symlink rejection, and Backlog-ready
//! CAS store (`StoreLayer`) & `/data/updates/` offline package (`OfflinePackageHook`) traits.

use crate::{DeployError, DeployResult};
use miro_ego_core::ids::{hex_encode, is_valid_sha256, new_uuid_v4};
use miro_ego_core::sys::fs::{fsync_directory, open_readonly_safe, read_bounded_json};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

pub const BUNDLE_SCHEMA: &str = "miro-ego.rdk-x5-bundle.v1";
pub const TARGET_PLATFORM: &str = "linux_aarch64_rdk_x5_v1";
pub const REQUIRED_BINARIES: &[&str] = &["bin/miro-ego", "bin/miro-ego-encoder"];
pub const MAX_MANIFEST_BYTES: usize = 128 * 1024;
pub const MAX_BUNDLE_FILES: usize = 256;
pub const MAX_BINARY_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_TOTAL_BUNDLE_BYTES: u64 = 512 * 1024 * 1024;

const ELF_MAGIC: [u8; 4] = [0x7f, b'E', b'L', b'F'];
const ELFCLASS64: u8 = 2;
const ELFDATA2LSB: u8 = 1;
const ET_EXEC: u16 = 2;
const ET_DYN: u16 = 3;
const EM_AARCH64: u16 = 183;

/// Single verified file entry declared inside `bundle.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleFileEntry {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
    #[serde(default)]
    pub executable: bool,
}

/// Native Miro-Ego release bundle manifest (`bundle.json`).
///
/// Pure native binaries + deploy assets only — zero CPython runtime and zero Python wheels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleManifest {
    pub schema: String,
    pub platform: String,
    pub version: String,
    pub commit: String,
    pub binaries: Vec<BundleFileEntry>,
    #[serde(default)]
    pub assets: Vec<BundleFileEntry>,
}

/// Verified native Miro-Ego release bundle ready for atomic A/B staging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedBundle {
    pub root: PathBuf,
    pub manifest: BundleManifest,
    pub manifest_sha256: String,
    pub total_bytes: u64,
}

impl VerifiedBundle {
    pub fn commit(&self) -> &str {
        &self.manifest.commit
    }

    pub fn version(&self) -> &str {
        &self.manifest.version
    }

    pub fn all_entries(&self) -> impl Iterator<Item = &BundleFileEntry> {
        self.manifest
            .binaries
            .iter()
            .chain(self.manifest.assets.iter())
    }
}

/// Validate canonical 40-character lowercase hexadecimal Git commit SHA.
pub fn validate_commit_sha(commit: &str) -> DeployResult<()> {
    if commit.len() != 40
        || !commit
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(DeployError::InvalidBundle {
            message: format!("commit must be a 40-character lowercase hex SHA, got {commit:?}"),
        });
    }
    Ok(())
}

/// Validate a relative POSIX path inside a bundle, rejecting traversal, absolute paths,
/// backslashes, control chars, `.whl` wheels, and CPython runtime payloads.
pub fn validate_bundle_relative_path(rel: &str) -> DeployResult<PathBuf> {
    if rel.is_empty()
        || rel.starts_with('/')
        || rel.contains('\\')
        || rel.bytes().any(|b| b < 0x20 || b == 0x7f)
    {
        return Err(DeployError::InvalidBundle {
            message: format!("unsafe bundle entry path: {rel:?}"),
        });
    }

    let lower = rel.to_ascii_lowercase();
    if lower.ends_with(".whl")
        || lower.starts_with("runtime/")
        || lower.contains("/site-packages/")
        || lower.ends_with("/python3")
    {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "legacy Python wheel or CPython runtime payload is forbidden in native bundle: {rel}"
            ),
        });
    }

    let mut clean = PathBuf::new();
    for segment in rel.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(DeployError::InvalidBundle {
                message: format!("path traversal or empty segment in bundle entry: {rel:?}"),
            });
        }
        clean.push(segment);
    }

    if clean
        .components()
        .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(DeployError::InvalidBundle {
            message: format!("non-normal path component in bundle entry: {rel:?}"),
        });
    }
    Ok(clean)
}

/// Verify that `root / rel` has no symlinks in any path component and points to a regular file.
pub fn resolve_no_symlinks(root: &Path, rel: &Path) -> DeployResult<PathBuf> {
    let mut current = root.to_path_buf();
    let components: Vec<_> = rel.components().collect();
    for (idx, comp) in components.iter().enumerate() {
        let Component::Normal(seg) = comp else {
            return Err(DeployError::InvalidBundle {
                message: format!("invalid component in path {}", rel.display()),
            });
        };
        current.push(seg);
        let meta = fs::symlink_metadata(&current).map_err(|err| DeployError::InvalidBundle {
            message: format!(
                "missing or unreadable bundle entry {}: {err}",
                rel.display()
            ),
        })?;
        if meta.file_type().is_symlink() {
            return Err(DeployError::InvalidBundle {
                message: format!("symlink rejected in bundle path: {}", rel.display()),
            });
        }
        let is_last = idx + 1 == components.len();
        if is_last {
            if !meta.file_type().is_file() {
                return Err(DeployError::InvalidBundle {
                    message: format!("bundle entry must be a regular file: {}", rel.display()),
                });
            }
        } else if !meta.file_type().is_dir() {
            return Err(DeployError::InvalidBundle {
                message: format!(
                    "intermediate bundle path component is not a directory: {}",
                    current.display()
                ),
            });
        }
    }
    Ok(current)
}

/// Validate an AArch64 ELF64 executable/PIE binary header (`ET_EXEC` or `ET_DYN`, `EM_AARCH64`).
pub fn validate_aarch64_elf64_header(header: &[u8], label: &str) -> DeployResult<()> {
    if header.len() < 64 {
        return Err(DeployError::InvalidBundle {
            message: format!("{label} is too small to be a valid ELF64 binary"),
        });
    }
    if header[0..4] != ELF_MAGIC {
        return Err(DeployError::InvalidBundle {
            message: format!("{label} is not an ELF binary"),
        });
    }
    if header[4] != ELFCLASS64 || header[5] != ELFDATA2LSB {
        return Err(DeployError::InvalidBundle {
            message: format!("{label} must be 64-bit little-endian ELF64"),
        });
    }
    let e_type = u16::from_le_bytes([header[16], header[17]]);
    if e_type != ET_EXEC && e_type != ET_DYN {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "{label} has unsupported ELF type {e_type} (expected ET_EXEC or ET_DYN)"
            ),
        });
    }
    let e_machine = u16::from_le_bytes([header[18], header[19]]);
    if e_machine != EM_AARCH64 {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "{label} is not an AArch64 binary (e_machine={e_machine}, expected {EM_AARCH64})"
            ),
        });
    }
    Ok(())
}

/// Stream-hash a regular file (rejecting symlinks/hardlinks) and return `(bytes, sha256, header_64)`.
pub fn hash_file_safe(path: &Path) -> DeployResult<(u64, String, Vec<u8>)> {
    let mut file = open_readonly_safe(path).map_err(|err| DeployError::InvalidBundle {
        message: format!("cannot safely open {}: {err}", path.display()),
    })?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    let mut header = Vec::with_capacity(64);

    loop {
        let n = file
            .read(&mut buf)
            .map_err(|err| DeployError::InvalidBundle {
                message: format!("failed reading {}: {err}", path.display()),
            })?;
        if n == 0 {
            break;
        }
        if header.len() < 64 {
            let need = (64 - header.len()).min(n);
            header.extend_from_slice(&buf[..need]);
        }
        hasher.update(&buf[..n]);
        total = total
            .checked_add(n as u64)
            .ok_or_else(|| DeployError::InvalidBundle {
                message: "file byte length overflow".to_owned(),
            })?;
    }

    Ok((total, hex_encode(hasher.finalize().as_slice()), header))
}

/// Strictly verify a native Miro-Ego release bundle directory (`bundle.json`, binaries, assets).
pub fn verify_bundle_dir(bundle_dir: &Path) -> DeployResult<VerifiedBundle> {
    let root_meta = fs::symlink_metadata(bundle_dir).map_err(|err| DeployError::InvalidBundle {
        message: format!("bundle path unavailable {}: {err}", bundle_dir.display()),
    })?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(DeployError::InvalidBundle {
            message: "bundle path must be a real directory, not a symlink".to_owned(),
        });
    }

    let manifest_path = bundle_dir.join("bundle.json");
    let (_, manifest_sha256, _) = hash_file_safe(&manifest_path)?;
    let manifest: BundleManifest = read_bounded_json(&manifest_path, MAX_MANIFEST_BYTES, 16)
        .map_err(|err| DeployError::InvalidBundle {
            message: format!("invalid bundle.json: {err}"),
        })?;

    if manifest.schema != BUNDLE_SCHEMA {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "unsupported bundle schema {:?} (expected {BUNDLE_SCHEMA})",
                manifest.schema
            ),
        });
    }
    if manifest.platform != TARGET_PLATFORM {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "bundle platform mismatch {:?} (expected {TARGET_PLATFORM})",
                manifest.platform
            ),
        });
    }
    if manifest.version.is_empty()
        || manifest.version.len() > 64
        || manifest.version.bytes().any(|b| b < 0x20 || b == 0x7f)
    {
        return Err(DeployError::InvalidBundle {
            message: "bundle version string is invalid".to_owned(),
        });
    }
    validate_commit_sha(&manifest.commit)?;

    let total_entries = manifest
        .binaries
        .len()
        .saturating_add(manifest.assets.len());
    if manifest.binaries.is_empty() || total_entries > MAX_BUNDLE_FILES {
        return Err(DeployError::InvalidBundle {
            message: format!("bundle entry count {total_entries} is out of bounds"),
        });
    }

    let mut seen_paths = HashSet::new();
    let mut seen_binaries = HashSet::new();
    let mut total_bytes: u64 = 0;

    for entry in &manifest.binaries {
        verify_single_entry(bundle_dir, entry, true, &mut seen_paths, &mut total_bytes)?;
        seen_binaries.insert(entry.path.as_str());
    }
    for required in REQUIRED_BINARIES {
        if !seen_binaries.contains(required) {
            return Err(DeployError::InvalidBundle {
                message: format!("bundle missing required native AArch64 binary: {required}"),
            });
        }
    }
    for entry in &manifest.assets {
        if !is_allowed_bundle_asset_path(&entry.path) {
            return Err(DeployError::InvalidBundle {
                message: format!(
                    "bundle asset must live under deploy/miro-ego/ or share/voice/*.wav: {}",
                    entry.path
                ),
            });
        }
        verify_single_entry(bundle_dir, entry, false, &mut seen_paths, &mut total_bytes)?;
    }

    Ok(VerifiedBundle {
        root: bundle_dir.to_path_buf(),
        manifest,
        manifest_sha256,
        total_bytes,
    })
}

/// Check whether a bundle asset relative path is in an allowed namespace:
/// `deploy/miro-ego/<asset>` or `share/voice/<cue>.wav`.
pub fn is_allowed_bundle_asset_path(path: &str) -> bool {
    if let Some(rest) = path.strip_prefix("deploy/miro-ego/") {
        return !rest.is_empty();
    }
    if let Some(wav_file) = path.strip_prefix("share/voice/") {
        if let Some(stem) = wav_file.strip_suffix(".wav") {
            return !stem.is_empty()
                && !stem.contains('/')
                && stem
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'));
        }
    }
    false
}

/// Stream-copy a regular file opened with `O_NOFOLLOW` into an exclusive temporary file under
/// `dst`'s parent directory, verifying byte length, SHA-256, and optional AArch64 ELF64 header
/// on the exact copied stream BEFORE atomic rename + parent directory fsync.
pub fn copy_and_verify_safe(
    bundle_root: Option<&Path>,
    src_path_or_rel: &Path,
    dst: &Path,
    mode: u32,
    expected_bytes: Option<u64>,
    expected_sha256: &str,
    require_aarch64_elf: bool,
) -> DeployResult<u64> {
    if !is_valid_sha256(expected_sha256) {
        return Err(DeployError::InvalidBundle {
            message: format!("invalid expected sha256: {expected_sha256}"),
        });
    }
    let resolved_src = match bundle_root {
        Some(root) => resolve_no_symlinks(root, src_path_or_rel)?,
        None => src_path_or_rel.to_path_buf(),
    };
    let mut src_file =
        open_readonly_safe(&resolved_src).map_err(|err| DeployError::InvalidBundle {
            message: format!("cannot safely open {}: {err}", resolved_src.display()),
        })?;

    let parent = dst.parent().ok_or_else(|| DeployError::Failed {
        message: format!("destination path has no parent: {}", dst.display()),
    })?;
    fs::create_dir_all(parent).map_err(|err| DeployError::Failed {
        message: format!(
            "failed creating destination dir {}: {err}",
            parent.display()
        ),
    })?;

    let file_name = dst
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_else(|| "artifact".to_owned());
    let tmp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        new_uuid_v4().replace('-', "")
    ));

    let copy_res = (|| -> DeployResult<u64> {
        let mut out_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&tmp_path)
            .map_err(|err| DeployError::Failed {
                message: format!(
                    "failed creating staging temp file {}: {err}",
                    tmp_path.display()
                ),
            })?;

        let mut hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        let mut total: u64 = 0;
        let mut header = Vec::with_capacity(64);

        loop {
            let n = src_file
                .read(&mut buf)
                .map_err(|err| DeployError::InvalidBundle {
                    message: format!("failed reading {}: {err}", resolved_src.display()),
                })?;
            if n == 0 {
                break;
            }
            if header.len() < 64 {
                let need = (64 - header.len()).min(n);
                header.extend_from_slice(&buf[..need]);
            }
            hasher.update(&buf[..n]);
            out_file
                .write_all(&buf[..n])
                .map_err(|err| DeployError::Failed {
                    message: format!("failed writing {}: {err}", tmp_path.display()),
                })?;
            total = total
                .checked_add(n as u64)
                .ok_or_else(|| DeployError::InvalidBundle {
                    message: "copied file byte length overflow".to_owned(),
                })?;
            if total > MAX_BINARY_BYTES {
                return Err(DeployError::InvalidBundle {
                    message: format!(
                        "copied file {} exceeded max size {MAX_BINARY_BYTES}",
                        resolved_src.display()
                    ),
                });
            }
        }

        if let Some(exp_len) = expected_bytes {
            if total != exp_len {
                return Err(DeployError::InvalidBundle {
                    message: format!(
                        "size mismatch while staging {} (expected {exp_len}, got {total})",
                        resolved_src.display()
                    ),
                });
            }
        }
        let actual_sha = hex_encode(hasher.finalize().as_slice());
        if actual_sha != expected_sha256 {
            return Err(DeployError::InvalidBundle {
                message: format!(
                    "SHA-256 mismatch while staging {} (expected {expected_sha256}, got {actual_sha})",
                    resolved_src.display()
                ),
            });
        }
        if require_aarch64_elf {
            validate_aarch64_elf64_header(&header, &resolved_src.display().to_string())?;
        }

        out_file
            .set_permissions(fs::Permissions::from_mode(mode))
            .map_err(|err| DeployError::Failed {
                message: format!("failed chmod on {}: {err}", tmp_path.display()),
            })?;
        out_file.sync_all().map_err(|err| DeployError::Failed {
            message: format!("failed fsync on {}: {err}", tmp_path.display()),
        })?;
        drop(out_file);

        fs::rename(&tmp_path, dst).map_err(|err| DeployError::Failed {
            message: format!(
                "failed atomically promoting {} to {}: {err}",
                tmp_path.display(),
                dst.display()
            ),
        })?;
        let _ = fsync_directory(parent);
        Ok(total)
    })();

    if copy_res.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    copy_res
}

fn verify_single_entry(
    bundle_dir: &Path,
    entry: &BundleFileEntry,
    require_aarch64_elf: bool,
    seen_paths: &mut HashSet<String>,
    total_bytes: &mut u64,
) -> DeployResult<()> {
    let rel = validate_bundle_relative_path(&entry.path)?;
    if !seen_paths.insert(entry.path.clone()) {
        return Err(DeployError::InvalidBundle {
            message: format!("duplicate bundle entry: {}", entry.path),
        });
    }
    if entry.bytes == 0 || entry.bytes > MAX_BINARY_BYTES {
        return Err(DeployError::InvalidBundle {
            message: format!("invalid byte length for {}: {}", entry.path, entry.bytes),
        });
    }
    if !is_valid_sha256(&entry.sha256) {
        return Err(DeployError::InvalidBundle {
            message: format!("invalid sha256 format for {}", entry.path),
        });
    }

    let full_path = resolve_no_symlinks(bundle_dir, &rel)?;
    let (actual_bytes, actual_sha256, header) = hash_file_safe(&full_path)?;
    if actual_bytes != entry.bytes || actual_sha256 != entry.sha256 {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "digest or size mismatch for {} (expected {} bytes {}, got {} bytes {})",
                entry.path, entry.bytes, entry.sha256, actual_bytes, actual_sha256
            ),
        });
    }

    if require_aarch64_elf {
        if !entry.path.starts_with("bin/") {
            return Err(DeployError::InvalidBundle {
                message: format!("binary entry must live under bin/: {}", entry.path),
            });
        }
        validate_aarch64_elf64_header(&header, &entry.path)?;
    }

    *total_bytes =
        total_bytes
            .checked_add(actual_bytes)
            .ok_or_else(|| DeployError::InvalidBundle {
                message: "bundle total byte length overflow".to_owned(),
            })?;
    if *total_bytes > MAX_TOTAL_BUNDLE_BYTES {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "bundle total bytes {} exceeds cap {MAX_TOTAL_BUNDLE_BYTES}",
                *total_bytes
            ),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Backlog-Ready Extension Traits (CAS Incremental Store & /data/updates/ Hooks)
// ---------------------------------------------------------------------------

/// Content-addressed store (CAS) layer trait reserved for Backlog incremental OTA layers.
pub trait StoreLayer: Send + Sync {
    /// Return `true` if the object identified by lowercase hex `sha256` exists in this store.
    fn has_object(&self, sha256: &str) -> DeployResult<bool>;

    /// Materialize the verified object `sha256` to `dest`, returning written byte count.
    fn fetch_object(&self, sha256: &str, dest: &Path) -> DeployResult<u64>;

    /// Ingest `src` into the store after verifying its SHA-256 equals `expected_sha256`.
    fn put_object(&self, src: &Path, expected_sha256: &str) -> DeployResult<()>;
}

/// Simple filesystem-backed content-addressed store (`<root>/<sha[0..2]>/<sha>`).
#[derive(Debug, Clone)]
pub struct LocalCasStore {
    root: PathBuf,
}

impl LocalCasStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn object_path(&self, sha256: &str) -> DeployResult<PathBuf> {
        if !is_valid_sha256(sha256) {
            return Err(DeployError::InvalidBundle {
                message: format!("invalid CAS sha256 key: {sha256}"),
            });
        }
        Ok(self.root.join(&sha256[..2]).join(sha256))
    }
}

impl StoreLayer for LocalCasStore {
    fn has_object(&self, sha256: &str) -> DeployResult<bool> {
        let path = self.object_path(sha256)?;
        Ok(fs::symlink_metadata(&path)
            .map(|m| m.file_type().is_file())
            .unwrap_or(false))
    }

    fn fetch_object(&self, sha256: &str, dest: &Path) -> DeployResult<u64> {
        let path = self.object_path(sha256)?;
        copy_and_verify_safe(None, &path, dest, 0o644, None, sha256, false)
    }

    fn put_object(&self, src: &Path, expected_sha256: &str) -> DeployResult<()> {
        let dest = self.object_path(expected_sha256)?;
        let _ = copy_and_verify_safe(None, src, &dest, 0o644, None, expected_sha256, false)?;
        Ok(())
    }
}

/// Descriptor for an offline update bundle discovered under `/data/updates/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfflinePackageDescriptor {
    pub path: PathBuf,
    pub version: String,
    pub commit: String,
    pub total_bytes: u64,
}

/// Extension hook reserved for Backlog `/data/updates/` offline package discovery & policy checks.
pub trait OfflinePackageHook: Send + Sync {
    /// Scan `updates_dir` (e.g. `/data/updates/`) for valid native Miro-Ego bundles.
    fn discover_packages(&self, updates_dir: &Path) -> DeployResult<Vec<OfflinePackageDescriptor>>;

    /// Optional pre-install verification gate (e.g. detached signature verification in Backlog).
    fn pre_install_verify(&self, bundle: &VerifiedBundle) -> DeployResult<()>;
}

/// Default offline package hook scanning `/data/updates/` subdirectories for verified bundles.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultOfflinePackageHook;

impl OfflinePackageHook for DefaultOfflinePackageHook {
    fn discover_packages(&self, updates_dir: &Path) -> DeployResult<Vec<OfflinePackageDescriptor>> {
        if !updates_dir.is_dir() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(updates_dir).map_err(|err| DeployError::Failed {
            message: format!(
                "failed to scan updates directory {}: {err}",
                updates_dir.display()
            ),
        })?;
        let mut found = Vec::new();
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if fs::symlink_metadata(&path)
                .map(|m| m.file_type().is_dir())
                .unwrap_or(false)
                && path.join("bundle.json").is_file()
            {
                if let Ok(verified) = verify_bundle_dir(&path) {
                    found.push(OfflinePackageDescriptor {
                        path,
                        version: verified.manifest.version,
                        commit: verified.manifest.commit,
                        total_bytes: verified.total_bytes,
                    });
                }
            }
        }
        found.sort_by(|a, b| a.commit.cmp(&b.commit));
        Ok(found)
    }

    fn pre_install_verify(&self, _bundle: &VerifiedBundle) -> DeployResult<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// Build a minimal valid 128-byte AArch64 ELF64 PIE binary header + tag bytes.
    pub fn fake_aarch64_elf(tag: &[u8]) -> Vec<u8> {
        let mut buf = vec![0u8; 128];
        buf[0..4].copy_from_slice(&ELF_MAGIC);
        buf[4] = ELFCLASS64;
        buf[5] = ELFDATA2LSB;
        buf[6] = 1; // EV_CURRENT
        buf[16..18].copy_from_slice(&ET_DYN.to_le_bytes());
        buf[18..20].copy_from_slice(&EM_AARCH64.to_le_bytes());
        let copy_len = tag.len().min(64);
        buf[64..64 + copy_len].copy_from_slice(&tag[..copy_len]);
        buf
    }

    pub fn build_valid_bundle(
        dir: &Path,
        version: &str,
        commit: &str,
        assets: &[(&str, &[u8], bool)],
    ) -> BundleManifest {
        fs::create_dir_all(dir.join("bin")).unwrap();
        let miro_bin = fake_aarch64_elf(format!("miro-ego-{version}-{commit}").as_bytes());
        let enc_bin = fake_aarch64_elf(format!("encoder-{version}-{commit}").as_bytes());
        fs::write(dir.join("bin/miro-ego"), &miro_bin).unwrap();
        fs::write(dir.join("bin/miro-ego-encoder"), &enc_bin).unwrap();

        let binaries = vec![
            BundleFileEntry {
                path: "bin/miro-ego".to_owned(),
                bytes: miro_bin.len() as u64,
                sha256: hex_encode(Sha256::digest(&miro_bin).as_slice()),
                executable: true,
            },
            BundleFileEntry {
                path: "bin/miro-ego-encoder".to_owned(),
                bytes: enc_bin.len() as u64,
                sha256: hex_encode(Sha256::digest(&enc_bin).as_slice()),
                executable: true,
            },
        ];

        let mut asset_entries = Vec::new();
        for &(name, content, exec) in assets {
            let rel = if name.contains('/') {
                name.to_owned()
            } else {
                format!("deploy/miro-ego/{name}")
            };
            let full = dir.join(&rel);
            fs::create_dir_all(full.parent().unwrap()).unwrap();
            fs::write(&full, content).unwrap();
            asset_entries.push(BundleFileEntry {
                path: rel,
                bytes: content.len() as u64,
                sha256: hex_encode(Sha256::digest(content).as_slice()),
                executable: exec,
            });
        }

        let manifest = BundleManifest {
            schema: BUNDLE_SCHEMA.to_owned(),
            platform: TARGET_PLATFORM.to_owned(),
            version: version.to_owned(),
            commit: commit.to_owned(),
            binaries,
            assets: asset_entries,
        };
        let raw = serde_json::to_vec_pretty(&manifest).unwrap();
        fs::write(dir.join("bundle.json"), raw).unwrap();
        manifest
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{build_valid_bundle, fake_aarch64_elf};
    use super::*;
    use std::os::unix::fs::symlink;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-bundle-{tag}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn verifies_valid_aarch64_native_bundle_and_discovers_via_offline_hook() {
        let root = temp_dir("valid");
        let bundle_dir = root.join("release-0.3.0");
        let commit = "a".repeat(40);
        build_valid_bundle(
            &bundle_dir,
            "0.3.0",
            &commit,
            &[
                ("miro-ego.service", b"[Unit]\nDescription=Miro-Ego\n", false),
                (
                    "share/voice/device_ready.wav",
                    b"RIFF$\x00\x00\x00WAVE",
                    false,
                ),
            ],
        );

        let verified = verify_bundle_dir(&bundle_dir).unwrap();
        assert_eq!(verified.commit(), commit);
        assert_eq!(verified.version(), "0.3.0");

        // OfflinePackageHook discovers it cleanly under /data/updates style directory.
        let hook = DefaultOfflinePackageHook;
        let discovered = hook.discover_packages(&root).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].commit, commit);

        // LocalCasStore roundtrip.
        let cas = LocalCasStore::new(root.join("cas"));
        let bin_entry = &verified.manifest.binaries[0];
        assert!(!cas.has_object(&bin_entry.sha256).unwrap());
        cas.put_object(&bundle_dir.join(&bin_entry.path), &bin_entry.sha256)
            .unwrap();
        assert!(cas.has_object(&bin_entry.sha256).unwrap());
        let fetched = root.join("fetched-bin");
        let n = cas.fetch_object(&bin_entry.sha256, &fetched).unwrap();
        assert_eq!(n, bin_entry.bytes);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_non_aarch64_elf_tampered_sha_symlinks_traversal_and_wheels() {
        let root = temp_dir("rejects");
        let commit = "b".repeat(40);

        // 1. x86_64 ELF binary (e_machine = 62) must be rejected.
        let x86_dir = root.join("x86");
        build_valid_bundle(&x86_dir, "0.3.0", &commit, &[]);
        let mut x86_bin = fake_aarch64_elf(b"x86");
        x86_bin[18..20].copy_from_slice(&62u16.to_le_bytes());
        fs::write(x86_dir.join("bin/miro-ego"), &x86_bin).unwrap();
        let mut m: BundleManifest =
            serde_json::from_slice(&fs::read(x86_dir.join("bundle.json")).unwrap()).unwrap();
        m.binaries[0].sha256 = hex_encode(Sha256::digest(&x86_bin).as_slice());
        m.binaries[0].bytes = x86_bin.len() as u64;
        fs::write(x86_dir.join("bundle.json"), serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(verify_bundle_dir(&x86_dir).is_err());

        // 2. Tampered binary (SHA mismatch) must be rejected.
        let tamper_dir = root.join("tamper");
        build_valid_bundle(&tamper_dir, "0.3.0", &commit, &[]);
        let mut corrupted = fs::read(tamper_dir.join("bin/miro-ego-encoder")).unwrap();
        corrupted[70] ^= 0xff;
        fs::write(tamper_dir.join("bin/miro-ego-encoder"), &corrupted).unwrap();
        assert!(verify_bundle_dir(&tamper_dir).is_err());

        // 3. Symlink entry must be rejected.
        let sym_dir = root.join("symlink");
        build_valid_bundle(&sym_dir, "0.3.0", &commit, &[]);
        let external = root.join("outside.bin");
        fs::write(&external, fs::read(sym_dir.join("bin/miro-ego")).unwrap()).unwrap();
        fs::remove_file(sym_dir.join("bin/miro-ego")).unwrap();
        symlink(&external, sym_dir.join("bin/miro-ego")).unwrap();
        assert!(verify_bundle_dir(&sym_dir).is_err());

        // 4. Path traversal and Python wheels/runtime forbidden.
        assert!(validate_bundle_relative_path("../etc/passwd").is_err());
        assert!(validate_bundle_relative_path("/bin/miro-ego").is_err());
        assert!(validate_bundle_relative_path("wheels/app-0.1-py3-none-any.whl").is_err());
        assert!(validate_bundle_relative_path("runtime/bin/python3").is_err());

        fs::remove_dir_all(root).unwrap();
    }
}
