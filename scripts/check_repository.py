#!/usr/bin/env python3
"""Fail-closed checks for repository docs, file hygiene, and likely secrets."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parent.parent
MAX_FILE_BYTES = 1_048_576
REQUIRED_TEXT = {
    "README.md": ("canonical Historian data model", "cargo nextest"),
    "CONTRIBUTING.md": ("cargo nextest", "test --workspace --doc --locked"),
    "AGENTS.md": ("_roadmap/", "M00-PR03"),
    "docs/architecture.md": ("native", "adapter", "tooling", "canonical model"),
    "docs/dependency-policy.md": ("package identity", "default-members"),
    "docs/implementation-brief.md": ("M00-PR01", "foundation"),
    "docs/implementation-brief-m00-pr02.md": ("M00-PR02", "canonical model"),
    "docs/model-contract.md": ("UUIDv7", "CollectionEnvelope", "256", "64"),
    "docs/baseline.md": ("Idle RSS", "N/A"),
    "docs/continuation-m00-pr02.md": ("M00-PR02", "semantic"),
    "docs/continuation-m00-pr03.md": ("M00-PR03", "independent oracle"),
}
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "GitHub token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    "Slack token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
}
MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
REMOTE_SCHEMES = ("http://", "https://", "mailto:")
EXCLUDED_PREFIXES = (".git/", "_roadmap/", "target/")


def repository_files() -> list[Path]:
    """Return tracked and non-ignored untracked files without entering ignored trees."""
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    paths = []
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8", errors="strict")
        if relative.startswith(EXCLUDED_PREFIXES):
            raise ValueError(f"excluded path unexpectedly entered repository scan: {relative}")
        paths.append(ROOT / relative)
    return sorted(paths)


def text_contents(path: Path, errors: list[str]) -> str | None:
    """Read a small UTF-8 source file, rejecting opaque or unsafe-to-scan inputs."""
    relative = path.relative_to(ROOT).as_posix()
    if path.is_symlink():
        errors.append(f"symlinks are not accepted by repository checks: {relative}")
        return None
    if path.suffix.lower() == ".pdf":
        errors.append(f"PDF files are opaque to the no-secret check: {relative}")
        return None
    size = path.stat().st_size
    if size > MAX_FILE_BYTES:
        errors.append(f"file exceeds {MAX_FILE_BYTES} bytes: {relative} ({size} bytes)")
        return None
    data = path.read_bytes()
    if b"\0" in data:
        errors.append(f"binary file is opaque to the no-secret check: {relative}")
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        errors.append(f"file is not valid UTF-8 and cannot be scanned: {relative}")
        return None


def check_markdown_links(path: Path, text: str, errors: list[str]) -> None:
    """Check local Markdown link targets; remote links are intentionally not fetched."""
    for match in MARKDOWN_LINK.finditer(text):
        raw_target = match.group(1).strip()
        if raw_target.startswith("<") and raw_target.endswith(">"):
            raw_target = raw_target[1:-1]
        if not raw_target or raw_target.startswith("#") or raw_target.startswith(REMOTE_SCHEMES):
            continue
        target_without_fragment = unquote(raw_target.split("#", 1)[0])
        if not target_without_fragment:
            continue
        target = (path.parent / target_without_fragment).resolve()
        try:
            target.relative_to(ROOT)
        except ValueError:
            errors.append(
                f"local link escapes the repository: {path.relative_to(ROOT)} -> {raw_target}"
            )
            continue
        if not target.exists():
            errors.append(
                f"broken local link: {path.relative_to(ROOT)} -> {raw_target}"
            )


def check_text_file(path: Path, text: str, errors: list[str]) -> None:
    """Check high-confidence secrets and whitespace in one source file."""
    relative = path.relative_to(ROOT).as_posix()
    for label, pattern in SECRET_PATTERNS.items():
        if pattern.search(text):
            errors.append(f"possible {label} in {relative}")
    for line_number, line in enumerate(text.splitlines(), start=1):
        if line.endswith((" ", "\t")):
            errors.append(f"trailing whitespace: {relative}:{line_number}")
    if path.suffix.lower() in {".md", ".markdown"}:
        check_markdown_links(path, text, errors)


def check_required_instructions(contents: dict[str, str], errors: list[str]) -> None:
    """Keep the entry-point documentation present and explicit."""
    for relative, required_phrases in REQUIRED_TEXT.items():
        text = contents.get(relative)
        if text is None:
            errors.append(f"required repository document is missing or unreadable: {relative}")
            continue
        for phrase in required_phrases:
            if phrase not in text:
                errors.append(f"{relative} must contain `{phrase}`")


def main() -> int:
    """Run all checks and return a process status suitable for CI."""
    errors: list[str] = []
    contents: dict[str, str] = {}
    try:
        paths = repository_files()
    except (OSError, subprocess.CalledProcessError, UnicodeError, ValueError) as error:
        print(f"repository check could not enumerate files: {error}", file=sys.stderr)
        return 1

    for path in paths:
        try:
            text = text_contents(path, errors)
        except OSError as error:
            errors.append(f"could not inspect {path.relative_to(ROOT)}: {error}")
            continue
        if text is None:
            continue
        contents[path.relative_to(ROOT).as_posix()] = text
        check_text_file(path, text, errors)

    check_required_instructions(contents, errors)
    if errors:
        print("repository checks failed:", file=sys.stderr)
        for error in sorted(set(errors)):
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"repository checks passed for {len(paths)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
