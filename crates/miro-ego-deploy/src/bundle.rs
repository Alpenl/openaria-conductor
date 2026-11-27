//! Native Miro-Ego release bundle verification (`miro-ego.rdk-x5-bundle.v1`), detached Ed25519
//! manifest signature verification (`bundle.json.sig`), AArch64 ELF64 validation, SHA-256
//! manifest verification, path-traversal/symlink rejection, and Backlog-ready CAS store
//! (`StoreLayer`) & `/data/updates/` offline package (`OfflinePackageHook`) traits.

use crate::{DeployError, DeployResult};
use miro_ego_core::ids::{hex_encode, is_valid_sha256, new_uuid_v4};
use miro_ego_core::sys::fs::{
    fsync_directory, open_directory_safe, open_readonly_safe, openat_retry,
    parse_strict_json_value, read_bounded_bytes,
};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

pub const BUNDLE_SCHEMA: &str = "miro-ego.rdk-x5-bundle.v1";
pub const BUNDLE_SIGNATURE_FILENAME: &str = "bundle.json.sig";
pub const BUNDLE_SIGNATURE_DOMAIN: &[u8] = b"miro-ego.rdk-x5-bundle-sig.v1\n";
pub const DEFAULT_RELEASE_SIGNING_SEED_LABEL: &[u8] =
    b"miro-ego-0.3.0-default-release-signing-seed";
pub const TARGET_PLATFORM: &str = "linux_aarch64_rdk_x5_v1";
pub const REQUIRED_BINARIES: &[&str] = &["bin/miro-ego", "bin/miro-ego-encoder"];
pub const MAX_MANIFEST_BYTES: usize = 128 * 1024;
pub const MAX_SIGNATURE_FILE_BYTES: usize = 256;
pub const MAX_BUNDLE_FILES: usize = 256;
pub const MAX_UPDATES_DIR_ENTRIES: usize = 256;
pub const MAX_BINARY_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_TOTAL_BUNDLE_BYTES: u64 = 512 * 1024 * 1024;

const ELF_MAGIC: [u8; 4] = [0x7f, b'E', b'L', b'F'];
const ELFCLASS64: u8 = 2;
const ELFDATA2LSB: u8 = 1;
const ET_EXEC: u16 = 2;
const ET_DYN: u16 = 3;
const EM_AARCH64: u16 = 183;

const fn hex_nibble_const(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

const fn parse_compile_time_pubkey_hex(raw: Option<&str>) -> Option<[u8; 32]> {
    let Some(s) = raw else {
        return None;
    };
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if start == end {
        return None;
    }
    if end - start != 64 {
        panic!("MIRO_EGO_RELEASE_PUBLIC_KEY_HEX must be exactly 64 hex characters");
    }
    let mut out = [0u8; 32];
    let mut idx = 0;
    while idx < 32 {
        let Some(hi) = hex_nibble_const(bytes[start + idx * 2]) else {
            panic!("MIRO_EGO_RELEASE_PUBLIC_KEY_HEX contains non-hex character");
        };
        let Some(lo) = hex_nibble_const(bytes[start + idx * 2 + 1]) else {
            panic!("MIRO_EGO_RELEASE_PUBLIC_KEY_HEX contains non-hex character");
        };
        out[idx] = (hi << 4) | lo;
        idx += 1;
    }
    Some(out)
}

const COMPILED_RELEASE_PUBLIC_KEY: Option<[u8; 32]> =
    parse_compile_time_pubkey_hex(option_env!("MIRO_EGO_RELEASE_PUBLIC_KEY_HEX"));

/// Derive the default deterministic 32-byte Ed25519 signing seed used when
/// `MIRO_EGO_RELEASE_PUBLIC_KEY_HEX` is not overridden at compile time.
pub fn default_release_signing_seed() -> [u8; 32] {
    let digest = Sha256::digest(DEFAULT_RELEASE_SIGNING_SEED_LABEL);
    let mut seed = [0u8; 32];
    seed.copy_from_slice(digest.as_slice());
    seed
}

/// Return the compiled-in 32-byte Ed25519 trusted release public key.
///
/// Honors compile-time `MIRO_EGO_RELEASE_PUBLIC_KEY_HEX` when set; otherwise derives the public
/// key from `Sha256::digest(b"miro-ego-0.3.0-default-release-signing-seed")`.
pub fn default_release_public_key() -> [u8; 32] {
    if let Some(pinned) = COMPILED_RELEASE_PUBLIC_KEY {
        return pinned;
    }
    let seed = default_release_signing_seed();
    let mut pubkey = [0u8; 32];
    if let Ok(kp) = Ed25519KeyPair::from_seed_unchecked(&seed) {
        let raw = kp.public_key().as_ref();
        if raw.len() == 32 {
            pubkey.copy_from_slice(raw);
        }
    }
    pubkey
}

/// Sign exact `bundle.json` manifest bytes under domain `b"miro-ego.rdk-x5-bundle-sig.v1\n"`
/// using a 32-byte Ed25519 seed, returning the 128-character lowercase hex signature string.
pub fn sign_bundle_manifest_with_seed(manifest_bytes: &[u8], seed: &[u8; 32]) -> String {
    let mut msg = Vec::with_capacity(BUNDLE_SIGNATURE_DOMAIN.len() + manifest_bytes.len());
    msg.extend_from_slice(BUNDLE_SIGNATURE_DOMAIN);
    msg.extend_from_slice(manifest_bytes);
    match Ed25519KeyPair::from_seed_unchecked(seed) {
        Ok(kp) => {
            let sig = kp.sign(&msg);
            hex_encode(sig.as_ref())
        }
        Err(_) => String::new(),
    }
}

fn decode_signature_hex_strict(raw_sig_file: &[u8]) -> DeployResult<[u8; 64]> {
    let text = std::str::from_utf8(raw_sig_file).map_err(|_| DeployError::InvalidBundle {
        message: "bundle.json.sig is not valid ASCII/UTF-8".to_owned(),
    })?;
    let trimmed = text.trim_end_matches([' ', '\t', '\r', '\n']);
    if trimmed.len() != 128
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(DeployError::InvalidBundle {
            message: "bundle.json.sig must contain exactly 128 lowercase hex characters".to_owned(),
        });
    }
    let hex_bytes = trimmed.as_bytes();
    let mut sig = [0u8; 64];
    for (idx, slot) in sig.iter_mut().enumerate() {
        let hi = match hex_bytes[idx * 2] {
            b @ b'0'..=b'9' => b - b'0',
            b @ b'a'..=b'f' => b - b'a' + 10,
            _ => {
                return Err(DeployError::InvalidBundle {
                    message: "invalid hex digit in bundle.json.sig".to_owned(),
                });
            }
        };
        let lo = match hex_bytes[idx * 2 + 1] {
            b @ b'0'..=b'9' => b - b'0',
            b @ b'a'..=b'f' => b - b'a' + 10,
            _ => {
                return Err(DeployError::InvalidBundle {
                    message: "invalid hex digit in bundle.json.sig".to_owned(),
                });
            }
        };
        *slot = (hi << 4) | lo;
    }
    Ok(sig)
}

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
    pub(crate) payloads_verified: bool,
}

impl VerifiedBundle {
    pub fn commit(&self) -> &str {
        &self.manifest.commit
    }

    pub fn version(&self) -> &str {
        &self.manifest.version
    }

    pub fn payloads_verified(&self) -> bool {
        self.payloads_verified
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

/// Fast synchronous preflight verification of `bundle.json`, detached Ed25519 `bundle.json.sig`,
/// non-symlink regular-file existence, declared byte sizes, and 64-byte AArch64 ELF64 binary
/// headers against the compiled-in trusted release public key (deferring full-file SHA-256 reads
/// to single-pass `stage_release` / `copy_and_verify_safe`).
pub fn verify_bundle_manifest(bundle_dir: &Path) -> DeployResult<VerifiedBundle> {
    let trusted_pubkey = default_release_public_key();
    verify_bundle_manifest_with_key(bundle_dir, &trusted_pubkey)
}

/// Fast synchronous preflight verification of `bundle.json`, detached Ed25519 `bundle.json.sig`,
/// non-symlink regular-file existence, declared byte sizes, and 64-byte AArch64 ELF64 binary
/// headers against an explicit 32-byte Ed25519 `trusted_pubkey`.
pub fn verify_bundle_manifest_with_key(
    bundle_dir: &Path,
    trusted_pubkey: &[u8; 32],
) -> DeployResult<VerifiedBundle> {
    let root_meta = fs::symlink_metadata(bundle_dir).map_err(|err| DeployError::InvalidBundle {
        message: format!("bundle path unavailable {}: {err}", bundle_dir.display()),
    })?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(DeployError::InvalidBundle {
            message: "bundle path must be a real directory, not a symlink".to_owned(),
        });
    }

    // Verify detached Ed25519 signature (`bundle.json.sig`) and read `bundle.json` ONCE into memory.
    let sig_path = resolve_no_symlinks(bundle_dir, Path::new(BUNDLE_SIGNATURE_FILENAME))?;
    let sig_raw = read_bounded_bytes(&sig_path, MAX_SIGNATURE_FILE_BYTES).map_err(|err| {
        DeployError::InvalidBundle {
            message: format!("invalid or unreadable {BUNDLE_SIGNATURE_FILENAME}: {err}"),
        }
    })?;
    let signature_bytes = decode_signature_hex_strict(&sig_raw)?;

    let manifest_path = resolve_no_symlinks(bundle_dir, Path::new("bundle.json"))?;
    let manifest_bytes = read_bounded_bytes(&manifest_path, MAX_MANIFEST_BYTES).map_err(|err| {
        DeployError::InvalidBundle {
            message: format!("invalid or unreadable bundle.json: {err}"),
        }
    })?;

    let mut signed_message =
        Vec::with_capacity(BUNDLE_SIGNATURE_DOMAIN.len() + manifest_bytes.len());
    signed_message.extend_from_slice(BUNDLE_SIGNATURE_DOMAIN);
    signed_message.extend_from_slice(&manifest_bytes);

    let verifier = UnparsedPublicKey::new(&ED25519, trusted_pubkey);
    verifier
        .verify(&signed_message, &signature_bytes)
        .map_err(|_| DeployError::InvalidBundle {
            message: "bundle.json.sig Ed25519 signature verification failed".to_owned(),
        })?;

    let manifest_sha256 = hex_encode(Sha256::digest(&manifest_bytes).as_slice());
    let manifest_val =
        parse_strict_json_value(&manifest_bytes, 16).map_err(|err| DeployError::InvalidBundle {
            message: format!("invalid bundle.json: {err}"),
        })?;
    let manifest: BundleManifest =
        serde_json::from_value(manifest_val).map_err(|err| DeployError::InvalidBundle {
            message: format!("invalid bundle.json schema: {err}"),
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
        preflight_single_entry(bundle_dir, entry, true, &mut seen_paths, &mut total_bytes)?;
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
        preflight_single_entry(bundle_dir, entry, false, &mut seen_paths, &mut total_bytes)?;
    }

    Ok(VerifiedBundle {
        root: bundle_dir.to_path_buf(),
        manifest,
        manifest_sha256,
        total_bytes,
        payloads_verified: false,
    })
}

/// Strictly verify a native Miro-Ego release bundle directory against the compiled-in trusted
/// Ed25519 release public key (`bundle.json.sig`, `bundle.json`, binaries, and assets).
pub fn verify_bundle_dir(bundle_dir: &Path) -> DeployResult<VerifiedBundle> {
    let trusted_pubkey = default_release_public_key();
    verify_bundle_dir_with_key(bundle_dir, &trusted_pubkey)
}

/// Strictly verify a native Miro-Ego release bundle directory against an explicit 32-byte
/// Ed25519 `trusted_pubkey`.
pub fn verify_bundle_dir_with_key(
    bundle_dir: &Path,
    trusted_pubkey: &[u8; 32],
) -> DeployResult<VerifiedBundle> {
    let mut verified = verify_bundle_manifest_with_key(bundle_dir, trusted_pubkey)?;
    for entry in &verified.manifest.binaries {
        verify_entry_digest(bundle_dir, entry, true)?;
    }
    for entry in &verified.manifest.assets {
        verify_entry_digest(bundle_dir, entry, false)?;
    }
    verified.payloads_verified = true;
    Ok(verified)
}

/// Explicit allowlist of non-`miro-ego-*` systemd drop-in directories permitted inside a bundle.
pub const ALLOWED_SYSTEMD_DROPIN_DIRS: &[&str] = &[
    "miro-ego.service.d",
    "fstrim.service.d",
    "logrotate.timer.d",
];

pub(crate) fn is_allowed_systemd_dropin_dir(name: &str) -> bool {
    name.ends_with(".d")
        && !name.contains('/')
        && (name.starts_with("miro-ego-") || ALLOWED_SYSTEMD_DROPIN_DIRS.contains(&name))
}

/// Check whether a bundle asset relative path is in an allowed namespace:
/// `deploy/miro-ego/<asset>`, `deploy/miro-ego/<allowed-unit>.d/<name>.conf`,
/// or `share/voice/<cue>.wav`.
pub fn is_allowed_bundle_asset_path(path: &str) -> bool {
    if let Some(rest) = path.strip_prefix("deploy/miro-ego/") {
        if rest.is_empty() {
            return false;
        }
        if let Some((dir, file)) = rest.split_once('/') {
            return is_allowed_systemd_dropin_dir(dir)
                && !file.is_empty()
                && !file.contains('/')
                && file.ends_with(".conf");
        }
        return true;
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

fn open_or_create_dir_tree_nofollow(parent: &Path) -> DeployResult<File> {
    let anchor = if parent.is_absolute() {
        Path::new("/")
    } else {
        Path::new(".")
    };
    let mut cur_dir = open_directory_safe(anchor).map_err(|err| DeployError::Failed {
        message: format!(
            "failed opening root directory {} safely: {err}",
            anchor.display()
        ),
    })?;
    for comp in parent.components() {
        match comp {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(seg) => {
                let seg_c = CString::new(seg.as_bytes()).map_err(|_| DeployError::Failed {
                    message: format!("path component contains NUL byte: {}", parent.display()),
                })?;
                let dir_flags =
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
                cur_dir = match openat_retry(cur_dir.as_raw_fd(), &seg_c, dir_flags) {
                    Ok(d) => d,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        // SAFETY: `cur_dir` is an open directory fd and `seg_c` is NUL-terminated.
                        let rc =
                            unsafe { libc::mkdirat(cur_dir.as_raw_fd(), seg_c.as_ptr(), 0o755) };
                        if rc != 0 {
                            let mk_err = std::io::Error::last_os_error();
                            if mk_err.kind() != std::io::ErrorKind::AlreadyExists {
                                return Err(DeployError::Failed {
                                    message: format!(
                                        "failed creating destination dir {}: {mk_err}",
                                        parent.display()
                                    ),
                                });
                            }
                        }
                        openat_retry(cur_dir.as_raw_fd(), &seg_c, dir_flags).map_err(
                            |open_err| DeployError::Failed {
                                message: format!(
                                    "destination dir {} is unsafe or a symlink: {open_err}",
                                    parent.display()
                                ),
                            },
                        )?
                    }
                    Err(err) => {
                        return Err(DeployError::Failed {
                            message: format!(
                                "destination dir {} component is unsafe or not a directory: {err}",
                                parent.display()
                            ),
                        });
                    }
                };
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(DeployError::Failed {
                    message: format!(
                        "forbidden path traversal component in destination {}",
                        parent.display()
                    ),
                });
            }
        }
    }
    Ok(cur_dir)
}

/// Stream-copy a regular file opened with `O_NOFOLLOW` into an exclusive temporary file under
/// `dst`'s parent directory (walking components with `O_DIRECTORY | O_NOFOLLOW`), verifying byte
/// length, SHA-256, and optional AArch64 ELF64 header on the exact copied stream BEFORE atomic
/// `renameat` + parent directory fsync.
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
    let parent_dir = open_or_create_dir_tree_nofollow(parent)?;

    let file_name = dst
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_else(|| "artifact".to_owned());
    let dst_name_c = CString::new(file_name.as_bytes()).map_err(|_| DeployError::Failed {
        message: format!("destination file name contains NUL byte: {}", dst.display()),
    })?;
    let tmp_name = format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        new_uuid_v4().replace('-', "")
    );
    let tmp_name_c = CString::new(tmp_name.as_bytes()).map_err(|_| DeployError::Failed {
        message: "temp file name contains NUL byte".to_owned(),
    })?;
    let tmp_path = parent.join(&tmp_name);

    let copy_res = (|| -> DeployResult<u64> {
        let out_fd = loop {
            // SAFETY: `parent_dir` is an open `O_DIRECTORY` fd and `tmp_name_c` is NUL-terminated.
            let fd = unsafe {
                libc::openat(
                    parent_dir.as_raw_fd(),
                    tmp_name_c.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC,
                    mode as libc::mode_t,
                )
            };
            if fd >= 0 {
                break fd;
            }
            let err = std::io::Error::last_os_error();
            if err.kind() != std::io::ErrorKind::Interrupted {
                return Err(DeployError::Failed {
                    message: format!(
                        "failed creating staging temp file {}: {err}",
                        tmp_path.display()
                    ),
                });
            }
        };
        // SAFETY: `out_fd` is a newly opened owned descriptor.
        let mut out_file = unsafe { File::from_raw_fd(out_fd) };

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

        // SAFETY: `parent_dir` is an open directory fd and both C strings are NUL-terminated.
        if unsafe {
            libc::renameat(
                parent_dir.as_raw_fd(),
                tmp_name_c.as_ptr(),
                parent_dir.as_raw_fd(),
                dst_name_c.as_ptr(),
            )
        } != 0
        {
            let err = std::io::Error::last_os_error();
            return Err(DeployError::Failed {
                message: format!(
                    "failed atomically promoting {} to {}: {err}",
                    tmp_path.display(),
                    dst.display()
                ),
            });
        }
        let _ = fsync_directory(parent);
        Ok(total)
    })();

    if copy_res.is_err() {
        // SAFETY: `parent_dir` is an open directory fd and `tmp_name_c` is NUL-terminated.
        unsafe {
            let _ = libc::unlinkat(parent_dir.as_raw_fd(), tmp_name_c.as_ptr(), 0);
        }
    }
    copy_res
}

fn read_prefix_safe(path: &Path, max_len: usize) -> DeployResult<Vec<u8>> {
    let mut file = open_readonly_safe(path).map_err(|err| DeployError::InvalidBundle {
        message: format!("cannot safely open {}: {err}", path.display()),
    })?;
    let mut out = vec![0u8; max_len];
    let mut filled = 0usize;
    while filled < max_len {
        let n = file
            .read(&mut out[filled..])
            .map_err(|err| DeployError::InvalidBundle {
                message: format!("failed reading {}: {err}", path.display()),
            })?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    out.truncate(filled);
    Ok(out)
}

fn preflight_single_entry(
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
    let meta = fs::symlink_metadata(&full_path).map_err(|err| DeployError::InvalidBundle {
        message: format!("cannot stat bundle entry {}: {err}", full_path.display()),
    })?;
    if meta.len() != entry.bytes {
        return Err(DeployError::InvalidBundle {
            message: format!(
                "size mismatch for {} (expected {} bytes, got {} bytes)",
                entry.path,
                entry.bytes,
                meta.len()
            ),
        });
    }

    if require_aarch64_elf {
        if !entry.path.starts_with("bin/") {
            return Err(DeployError::InvalidBundle {
                message: format!("binary entry must live under bin/: {}", entry.path),
            });
        }
        let header = read_prefix_safe(&full_path, 64)?;
        validate_aarch64_elf64_header(&header, &entry.path)?;
    }

    *total_bytes =
        total_bytes
            .checked_add(entry.bytes)
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

fn verify_entry_digest(
    bundle_dir: &Path,
    entry: &BundleFileEntry,
    require_aarch64_elf: bool,
) -> DeployResult<()> {
    let rel = validate_bundle_relative_path(&entry.path)?;
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
        validate_aarch64_elf64_header(&header, &entry.path)?;
    }
    Ok(())
}

/// Parse a `major.minor.patch` numeric prefix (tolerating `v` prefix and `-pre`/`+build` suffixes)
/// for deterministic semantic version ordering.
fn parse_semver_triplet(version: &str) -> Option<(u64, u64, u64, bool)> {
    let trimmed = version.trim().strip_prefix('v').unwrap_or(version.trim());
    let without_build = trimmed.split('+').next().unwrap_or(trimmed);
    let (core, prerelease) = match without_build.split_once('-') {
        Some((c, pre)) => (c, !pre.is_empty()),
        None => (without_build, false),
    };
    let mut parts = core.split('.');
    let major: u64 = parts.next()?.parse().ok()?;
    let minor: u64 = parts.next()?.parse().ok()?;
    let patch: u64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    // Stable release (`!prerelease` -> `true`) ranks above pre-release of the same triplet.
    Some((major, minor, patch, !prerelease))
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
        let Ok(meta) = fs::symlink_metadata(updates_dir) else {
            return Ok(Vec::new());
        };
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(updates_dir).map_err(|err| DeployError::Failed {
            message: format!(
                "failed to scan updates directory {}: {err}",
                updates_dir.display()
            ),
        })?;
        let mut candidate_paths: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .take(MAX_UPDATES_DIR_ENTRIES)
            .map(|e| e.path())
            .collect();
        candidate_paths.sort();

        let mut found = Vec::new();
        for path in candidate_paths {
            if fs::symlink_metadata(&path)
                .map(|m| !m.file_type().is_symlink() && m.file_type().is_dir())
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
        // Sort highest parsed semver `(major, minor, patch)` first so callers selecting the
        // first non-current candidate get the newest release, breaking ties by commit SHA.
        found.sort_by(|a, b| {
            let sem_a = parse_semver_triplet(&a.version);
            let sem_b = parse_semver_triplet(&b.version);
            sem_b
                .cmp(&sem_a)
                .then_with(|| b.version.cmp(&a.version))
                .then_with(|| a.commit.cmp(&b.commit))
        });
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

    pub fn write_signed_manifest(dir: &Path, manifest: &BundleManifest) {
        let mut raw = serde_json::to_vec_pretty(manifest).unwrap();
        raw.push(b'\n');
        let seed = default_release_signing_seed();
        let sig_hex = sign_bundle_manifest_with_seed(&raw, &seed);
        fs::write(dir.join("bundle.json"), &raw).unwrap();
        fs::write(dir.join(BUNDLE_SIGNATURE_FILENAME), format!("{sig_hex}\n")).unwrap();
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
        write_signed_manifest(dir, &manifest);
        manifest
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{build_valid_bundle, fake_aarch64_elf, write_signed_manifest};
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

        // 1. x86_64 ELF binary (e_machine = 62) must be rejected even with valid signature.
        let x86_dir = root.join("x86");
        build_valid_bundle(&x86_dir, "0.3.0", &commit, &[]);
        let mut x86_bin = fake_aarch64_elf(b"x86");
        x86_bin[18..20].copy_from_slice(&62u16.to_le_bytes());
        fs::write(x86_dir.join("bin/miro-ego"), &x86_bin).unwrap();
        let mut m: BundleManifest =
            serde_json::from_slice(&fs::read(x86_dir.join("bundle.json")).unwrap()).unwrap();
        m.binaries[0].sha256 = hex_encode(Sha256::digest(&x86_bin).as_slice());
        m.binaries[0].bytes = x86_bin.len() as u64;
        write_signed_manifest(&x86_dir, &m);
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

    #[test]
    fn enforces_detached_ed25519_signature_and_semver_package_sorting() {
        let root = temp_dir("ed25519-sig");
        let commit_1 = "1".repeat(40);
        let commit_2 = "2".repeat(40);
        let commit_10 = "0".repeat(40); // lexicographically smallest commit, highest semver 0.3.10!

        let b_031 = root.join("pkg-0.3.1");
        let b_032 = root.join("pkg-0.3.2");
        let b_0310 = root.join("pkg-0.3.10");
        build_valid_bundle(&b_031, "0.3.1", &commit_1, &[]);
        build_valid_bundle(&b_032, "0.3.2", &commit_2, &[]);
        build_valid_bundle(&b_0310, "0.3.10", &commit_10, &[]);

        // Semver sorting puts 0.3.10 first, then 0.3.2, then 0.3.1 regardless of commit SHA!
        let discovered = DefaultOfflinePackageHook.discover_packages(&root).unwrap();
        let versions: Vec<&str> = discovered.iter().map(|d| d.version.as_str()).collect();
        assert_eq!(versions, vec!["0.3.10", "0.3.2", "0.3.1"]);

        // 1. Missing bundle.json.sig fails closed with InvalidBundle.
        let unsigned_dir = root.join("unsigned");
        build_valid_bundle(&unsigned_dir, "0.3.0", &commit_1, &[]);
        fs::remove_file(unsigned_dir.join(BUNDLE_SIGNATURE_FILENAME)).unwrap();
        assert!(matches!(
            verify_bundle_dir(&unsigned_dir),
            Err(DeployError::InvalidBundle { .. })
        ));

        // 2. Tampered bundle.json (trailing space added) fails signature verification.
        let tampered_manifest = root.join("tampered-manifest");
        build_valid_bundle(&tampered_manifest, "0.3.0", &commit_1, &[]);
        let mut raw_json = fs::read(tampered_manifest.join("bundle.json")).unwrap();
        raw_json.push(b' ');
        fs::write(tampered_manifest.join("bundle.json"), raw_json).unwrap();
        assert!(matches!(
            verify_bundle_dir(&tampered_manifest),
            Err(DeployError::InvalidBundle { .. })
        ));

        // 3. Wrong public key fails verify_bundle_dir_with_key; custom seed + matching pubkey succeeds!
        let custom_dir = root.join("custom-key");
        build_valid_bundle(&custom_dir, "0.3.5", &commit_1, &[]);
        let alt_seed: [u8; 32] = Sha256::digest(b"alternate-release-seed").into();
        let alt_kp = Ed25519KeyPair::from_seed_unchecked(&alt_seed).unwrap();
        let mut alt_pub = [0u8; 32];
        alt_pub.copy_from_slice(alt_kp.public_key().as_ref());

        // Default-signed bundle rejected under alt_pub:
        assert!(verify_bundle_dir_with_key(&custom_dir, &alt_pub).is_err());

        // Re-sign under alt_seed -> accepted under alt_pub, rejected under default key!
        let raw_custom = fs::read(custom_dir.join("bundle.json")).unwrap();
        let alt_sig = sign_bundle_manifest_with_seed(&raw_custom, &alt_seed);
        assert_eq!(alt_sig.len(), 128);
        fs::write(
            custom_dir.join(BUNDLE_SIGNATURE_FILENAME),
            format!("{alt_sig}\n"),
        )
        .unwrap();
        assert!(verify_bundle_dir_with_key(&custom_dir, &alt_pub).is_ok());
        assert!(verify_bundle_dir(&custom_dir).is_err());

        // 4. Symlinked or >256B or uppercase signature files are rejected.
        let bad_sig_dir = root.join("bad-sig");
        build_valid_bundle(&bad_sig_dir, "0.3.0", &commit_1, &[]);
        let valid_sig = fs::read_to_string(bad_sig_dir.join(BUNDLE_SIGNATURE_FILENAME)).unwrap();
        fs::write(
            bad_sig_dir.join(BUNDLE_SIGNATURE_FILENAME),
            valid_sig.to_ascii_uppercase(),
        )
        .unwrap();
        assert!(verify_bundle_dir(&bad_sig_dir).is_err());

        fs::write(
            bad_sig_dir.join(BUNDLE_SIGNATURE_FILENAME),
            format!("{}{}", valid_sig.trim(), " ".repeat(200)),
        )
        .unwrap();
        assert!(verify_bundle_dir(&bad_sig_dir).is_err());

        let ext_sig = root.join("external.sig");
        fs::write(&ext_sig, &valid_sig).unwrap();
        fs::remove_file(bad_sig_dir.join(BUNDLE_SIGNATURE_FILENAME)).unwrap();
        symlink(&ext_sig, bad_sig_dir.join(BUNDLE_SIGNATURE_FILENAME)).unwrap();
        assert!(verify_bundle_dir(&bad_sig_dir).is_err());

        fs::remove_dir_all(root).unwrap();
    }
}
