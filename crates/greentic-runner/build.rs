use std::process::Command;

fn main() {
    let ts = chrono::Utc::now().to_rfc3339();
    println!("cargo:rustc-env=BUILD_TIMESTAMP_UTC={ts}");

    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".into());
    println!("cargo:rustc-env=BUILD_PROFILE={profile}");

    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".into());
    println!("cargo:rustc-env=TARGET={target}");

    // Best-effort git SHA; absent if not a git repo or git missing.
    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        && output.status.success()
    {
        let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !sha.is_empty() {
            println!("cargo:rustc-env=GIT_SHA={sha}");
        }
    }

    println!("cargo:rerun-if-changed=build.rs");
    emit_git_rerun_hints();
}

/// Ask git where its metadata lives instead of assuming `./.git/HEAD`: in a
/// worktree `.git` is a file and the crate is not at the repo root.
fn git_path(arg: &str) -> Option<String> {
    let output = Command::new("git").args(["rev-parse", arg]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!path.is_empty()).then_some(path)
}

fn emit_git_rerun_hints() {
    if let Some(git_dir) = git_path("--absolute-git-dir") {
        let head = std::path::Path::new(&git_dir).join("HEAD");
        if head.exists() {
            println!("cargo:rerun-if-changed={}", head.display());
        }
    }
    // Branch tips and packed refs live in the common dir (shared by worktrees).
    if let Some(common) = git_path("--git-common-dir") {
        let base = std::path::Path::new(&common);
        let base = if base.is_absolute() {
            base.to_path_buf()
        } else if let Ok(cwd) = std::env::current_dir() {
            cwd.join(base)
        } else {
            return;
        };
        // A missing path makes cargo rerun on every build, so only emit what exists.
        for rel in ["packed-refs", "refs/heads"] {
            let path = base.join(rel);
            if path.exists() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
}
