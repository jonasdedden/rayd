"""Set the Cargo workspace version to match a release tag.

Run before `maturin build` in the release workflow so the produced
wheel filename (`rayd-X.Y.Z-...whl`) matches the git tag (`vX.Y.Z`).

Two coupled edits in `Cargo.toml`:

1. `[workspace.package].version` — the version every member crate
   inherits via `version.workspace = true`. Stamped into the wheel
   metadata by maturin.
2. Each `[workspace.dependencies].rayd-*.version` — the cross-crate
   path-dep pin. Cargo refuses to resolve when this disagrees with
   the actual crate version, so they must move together.

`pyproject.toml` declares `version` as `dynamic` and maturin pulls
it from the Cargo workspace at build time, so the Python side stays
in sync automatically.

Usage:
    python3 tools/set_version_from_tag.py vX.Y.Z
    python3 tools/set_version_from_tag.py refs/tags/vX.Y.Z

Validates the tag matches `vMAJOR.MINOR.PATCH[-prerelease]` and
exits non-zero otherwise so a CI step that calls this fails loudly
rather than silently shipping the wrong version.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

TAG_RE = re.compile(r"^v(\d+\.\d+\.\d+(?:-[A-Za-z0-9.]+)?)$")


def replace_in_section(
    path: pathlib.Path,
    section: str,
    key: str,
    value: str,
) -> None:
    """Replace `key = "..."` inside the `[section]` block of a TOML file.

    Uses regex (not a TOML parser) so quoting + comments survive a
    round-trip — important because we don't want the script to reformat
    the file while doing one targeted substitution.
    """
    text = path.read_text()
    pattern = re.compile(
        r"(\[" + re.escape(section) + r"\][^\[]*?)\b"
        + re.escape(key)
        + r'\s*=\s*"[^"]*"',
        re.DOTALL,
    )
    new_text, count = pattern.subn(
        lambda m: m.group(1) + f'{key} = "{value}"',
        text,
        count=1,
    )
    if count != 1:
        msg = (
            f"failed to find {key!r} in [{section}] block of {path} "
            f"(matched {count} times; expected 1)"
        )
        raise SystemExit(msg)
    path.write_text(new_text)


def bump_path_dep_version(
    path: pathlib.Path,
    dep_name: str,
    value: str,
) -> None:
    """Update the `version = "..."` field inside a `[workspace.dependencies]`
    inline-table entry of the form `dep_name = { path = "...", version = "..." }`.

    These pins must match `[workspace.package].version` or `cargo metadata`
    refuses to resolve. Targeted regex so adjacent entries with similar
    names (e.g. `rayd-core` vs `rayd-core-extra`) aren't accidentally
    matched — anchored to the start of the line.
    """
    text = path.read_text()
    pattern = re.compile(
        r"^(" + re.escape(dep_name) + r"\s*=\s*\{[^}]*?\bversion\s*=\s*)\"[^\"]*\"",
        re.MULTILINE,
    )
    new_text, count = pattern.subn(
        lambda m: m.group(1) + f'"{value}"',
        text,
        count=1,
    )
    if count != 1:
        msg = (
            f"failed to find version pin for {dep_name!r} in {path} "
            f"(matched {count} times; expected 1)"
        )
        raise SystemExit(msg)
    path.write_text(new_text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "tag",
        help="git tag like 'v1.2.3' or full ref 'refs/tags/v1.2.3'",
    )
    args = parser.parse_args()

    tag = args.tag.removeprefix("refs/tags/")
    match = TAG_RE.match(tag)
    if match is None:
        msg = f"tag {tag!r} does not match vMAJOR.MINOR.PATCH[-prerelease]"
        raise SystemExit(msg)
    version = match.group(1)

    repo = pathlib.Path(__file__).resolve().parent.parent
    cargo = repo / "Cargo.toml"
    replace_in_section(cargo, "workspace.package", "version", version)
    # Cross-crate version pins under `[workspace.dependencies]` must
    # move together with the workspace.package version.
    for dep in ("rayd-core", "rayd-gcs", "rayd-plasma", "rayd-raylet"):
        bump_path_dep_version(cargo, dep, version)

    # Echo the rewritten lines so the CI log records the actual
    # values that maturin will pick up — handy for after-the-fact
    # debugging if a wheel ever ships with the wrong version.
    print(f"--- Cargo.toml after rewrite (version-bearing lines) ---")
    for line in cargo.read_text().splitlines():
        stripped = line.strip()
        if "version" in stripped and (
            stripped.startswith("version = ") or stripped.startswith("rayd-")
        ):
            print(f"  {stripped}")

    print(f"set version to {version} (from tag {tag})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
