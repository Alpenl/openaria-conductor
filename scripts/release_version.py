"""Miro-Ego 0.3.x release version and annotated tag guard."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"0\.3\.[0-9]+\Z")
EXPECTED_SUPPORT_SCHEMA = "miro-ego.device-api-consumer-support.v1"
_TAG_SIGNATURE_RE = re.compile(
    r"(?:\r?\n)*-----BEGIN (?:PGP|SSH) SIGNATURE-----[\s\S]*?"
    r"-----END (?:PGP|SSH) SIGNATURE-----\s*\Z"
)


def strip_tag_signature(text: str) -> str:
    """Strip trailing PGP or SSH signature armor from annotated Git tag notes."""
    return _TAG_SIGNATURE_RE.sub("", text).strip()


def project_version(root: Path = ROOT) -> str:
    """Validate and return the unified Miro-Ego 0.3.x version across repo manifests."""
    cargo_data = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    workspace = cargo_data.get("workspace", {})
    workspace_version = workspace.get("package", {}).get("version")
    if not isinstance(workspace_version, str) or not VERSION.fullmatch(workspace_version):
        raise ValueError(
            f"Cargo.toml workspace.package.version must match 0.3.x, got {workspace_version!r}"
        )

    pyproject_data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    pyproject_version = pyproject_data.get("project", {}).get("version")
    if pyproject_version != workspace_version:
        raise ValueError(
            "Version mismatch: "
            f"Cargo.toml={workspace_version!r} vs pyproject.toml={pyproject_version!r}"
        )

    members = workspace.get("members", [])
    if not members:
        raise ValueError("Cargo.toml workspace.members must not be empty")
    for member in members:
        member_manifest = root / member / "Cargo.toml"
        if not member_manifest.is_file():
            raise ValueError(f"Missing workspace member manifest: {member_manifest}")
        member_pkg = tomllib.loads(member_manifest.read_text(encoding="utf-8")).get("package", {})
        member_ver = member_pkg.get("version")
        if member_ver != {"workspace": True} and member_ver != workspace_version:
            raise ValueError(
                f"Workspace member {member} version mismatch: "
                f"{member_ver!r} != {workspace_version!r}"
            )

    support_path = root / "contracts" / "miro-ego-api-support.json"
    if not support_path.is_file():
        raise ValueError(f"Missing API contract support manifest: {support_path}")
    support = json.loads(support_path.read_text(encoding="utf-8"))
    if support.get("schema") != EXPECTED_SUPPORT_SCHEMA or support.get("consumer") != "miro-ego":
        raise ValueError("contracts/miro-ego-api-support.json has unexpected schema or consumer")
    if support.get("supported_device_api_majors") != [1]:
        raise ValueError(
            "contracts/miro-ego-api-support.json must pin supported_device_api_majors=[1]"
        )
    for version_key in ("version", "software_version"):
        if version_key in support and support[version_key] != workspace_version:
            raise ValueError(
                f"contracts/miro-ego-api-support.json {version_key}={support[version_key]!r} "
                f"!= {workspace_version!r}"
            )

    return workspace_version


def release_tag(tag: str, root: Path = ROOT) -> tuple[str, str]:
    """Verify an annotated v0.3.x Git release tag points cleanly to HEAD."""
    version = project_version(root)
    expected_tag = f"v{version}"
    if tag != expected_tag:
        raise ValueError(f"Release tag {tag!r} does not match source version {expected_tag!r}")

    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    if git("cat-file", "-t", f"refs/tags/{tag}") != "tag":
        raise ValueError("Release tag must be an annotated tag (git tag -a)")
    if git("rev-parse", f"refs/tags/{tag}^{{commit}}") != git("rev-parse", "HEAD"):
        raise ValueError("Release tag does not point to HEAD")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Working tree has uncommitted changes; refusing release tag verification")
    subject = git("for-each-ref", "--format=%(contents:subject)", f"refs/tags/{tag}")
    body = git("for-each-ref", "--format=%(contents:body)", f"refs/tags/{tag}")
    combined = f"{subject}\n\n{body}" if body else subject
    notes = strip_tag_signature(combined)
    if not notes or len(notes) > 16000:
        raise ValueError("Annotated release tag must contain non-empty notes (<= 16000 chars)")
    return version, notes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate unified Miro-Ego 0.3.x version")
    parser.add_argument("--tag", help="Optional annotated Git tag v0.3.x to verify")
    parser.add_argument("--notes-file", type=Path, help="Optional path to write tag release notes")
    args = parser.parse_args(argv)
    version = project_version()
    if args.tag:
        version, notes = release_tag(args.tag)
        if args.notes_file is not None:
            args.notes_file.write_text(notes + "\n", encoding="utf-8")
    print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
