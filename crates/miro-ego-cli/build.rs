//! Build script populating `MIRO_EGO_GIT_COMMIT` when unset in the build environment.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn is_valid_git_commit_hex(value: &str) -> bool {
    (40..=64).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn watch_git_head(repo_root: &Path) {
    let dot_git = repo_root.join(".git");
    if dot_git.is_dir() {
        let head = dot_git.join("HEAD");
        if head.is_file() {
            println!("cargo:rerun-if-changed={}", head.display());
            if let Ok(raw) = fs::read_to_string(&head)
                && let Some(rel_ref) = raw.trim().strip_prefix("ref:")
            {
                let ref_path = dot_git.join(rel_ref.trim());
                if ref_path.is_file() {
                    println!("cargo:rerun-if-changed={}", ref_path.display());
                }
            }
        }
    } else if dot_git.is_file() {
        println!("cargo:rerun-if-changed={}", dot_git.display());
    }
}

fn main() {
    println!("cargo:rerun-if-env-changed=MIRO_EGO_GIT_COMMIT");

    if let Some(manifest_dir) = env::var_os("CARGO_MANIFEST_DIR") {
        let manifest_path = Path::new(&manifest_dir);
        if let Some(repo_root) = manifest_path.parent().and_then(Path::parent) {
            watch_git_head(repo_root);
        }
    }

    if let Ok(existing) = env::var("MIRO_EGO_GIT_COMMIT") {
        let trimmed = existing.trim();
        if is_valid_git_commit_hex(trimmed) {
            let lower = trimmed.to_ascii_lowercase();
            if lower != existing {
                println!("cargo:rustc-env=MIRO_EGO_GIT_COMMIT={lower}");
            }
            return;
        }
    }

    if let Ok(output) = Command::new("git").args(["rev-parse", "HEAD"]).output()
        && output.status.success()
        && let Ok(text) = String::from_utf8(output.stdout)
    {
        let sha = text.trim().to_ascii_lowercase();
        if is_valid_git_commit_hex(&sha) {
            println!("cargo:rustc-env=MIRO_EGO_GIT_COMMIT={sha}");
        }
    }
}
