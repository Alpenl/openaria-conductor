"""Single release guard: only annotated v0.2.N tags matching all package identities."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"0\.2\.[1-9][0-9]*\Z")


def project_version(root: Path = ROOT) -> str:
    value = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    if not VERSION.fullmatch(value):
        raise ValueError("版本仅允许 0.2.x（x >= 1）；改变系列须用户明确授权")
    native = tomllib.loads((root / "native/Cargo.toml").read_text())["package"]["version"]
    web = json.loads((root / "src/rp_ylx/web/assets.json").read_text())["version"]
    if native != value or web != value:
        raise ValueError("Conductor、Rust 与嵌入 Echo Web 必须使用相同版本")
    return value


def release_tag(tag: str, root: Path = ROOT) -> tuple[str, str]:
    version = project_version(root)
    if tag != "v" + version:
        raise ValueError("发布标签与源码版本不一致")

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    if git("cat-file", "-t", f"refs/tags/{tag}") != "tag":
        raise ValueError("正式发布必须使用附注标签（git tag -a）")
    if git("rev-parse", f"refs/tags/{tag}^{{commit}}") != git("rev-parse", "HEAD"):
        raise ValueError("标签没有指向当前构建提交")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise ValueError("正式发布工作树存在未提交修改，拒绝混用标签与本地文件")
    notes = git("for-each-ref", "--format=%(contents)", f"refs/tags/{tag}")
    if not notes or len(notes) > 16000:
        raise ValueError("附注标签必须包含不超过 16000 字符的发布说明")
    return version, notes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag")
    parser.add_argument("--notes-file", type=Path)
    args = parser.parse_args()
    version = project_version()
    if args.tag:
        version, notes = release_tag(args.tag)
        if args.notes_file:
            args.notes_file.write_text(notes + "\n")
    print(version)


if __name__ == "__main__":
    main()
