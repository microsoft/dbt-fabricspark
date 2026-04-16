#!/usr/bin/env python3
"""Download dbt seed CSV files from GitHub Gist.

Reads a seed manifest YAML and idempotently downloads any missing files.
Uses only Python stdlib — no extra dependencies required.

Usage:
    python download_seeds.py                                      # defaults
    python download_seeds.py /path/to/project                     # explicit project dir
    python download_seeds.py --manifest my_seeds.yaml /path/to/project  # custom manifest
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_MANIFEST = "ci_seeds.yaml"


def _project_dir_default() -> Path:
    return Path(__file__).resolve().parent.parent / "dbt-adventureworks"


def _load_manifest(project_dir: Path, manifest_name: str) -> dict:
    path = project_dir / manifest_name
    if not path.exists():
        raise SystemExit(f"ERROR: manifest not found at {path}")
    text = path.read_text()

    gist_id_m = re.search(r'^gist_id:\s*"(.+?)"', text, re.MULTILINE)
    gist_owner_m = re.search(r'^gist_owner:\s*"(.+?)"', text, re.MULTILINE)
    if not gist_id_m or not gist_owner_m:
        raise SystemExit("ERROR: could not parse gist_id / gist_owner from manifest")

    files = [
        {"path": m.group(1).strip(), "filename": m.group(2).strip()}
        for m in re.finditer(r"-\s+path:\s*(.+?)\n\s+filename:\s*(.+?)(?:\n|$)", text)
    ]
    return {"gist_id": gist_id_m.group(1), "gist_owner": gist_owner_m.group(1), "files": files}


def main(project_dir: Path | None = None, manifest: str = DEFAULT_MANIFEST) -> int:
    project_dir = (project_dir or _project_dir_default()).resolve()
    manifest_data = _load_manifest(project_dir, manifest)
    base_url = f"https://gist.githubusercontent.com/{manifest_data['gist_owner']}/{manifest_data['gist_id']}/raw"

    downloaded = skipped = failed = 0
    for entry in manifest_data["files"]:
        dest = project_dir / entry["path"]
        if dest.exists():
            skipped += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            urllib.request.urlretrieve(f"{base_url}/{entry['filename']}", dest)
            print(f"  Downloaded {entry['path']}")
            downloaded += 1
        except urllib.error.URLError as exc:
            print(f"  FAILED {entry['path']}: {exc}", file=sys.stderr)
            failed += 1

    total = len(manifest_data["files"])
    print(f"seed-ci: {downloaded} downloaded, {skipped} already present, {failed} failed (total: {total} files)")
    return 1 if failed else 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download dbt seed CSV files from GitHub Gist.",
    )
    parser.add_argument(
        "project_dir",
        nargs="?",
        default=None,
        help="Path to the dbt project directory (default: auto-detect dbt-adventureworks)",
    )
    parser.add_argument(
        "--manifest",
        default=DEFAULT_MANIFEST,
        help=f"Seed manifest filename inside the project dir (default: {DEFAULT_MANIFEST})",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    project_dir = Path(args.project_dir) if args.project_dir else None
    raise SystemExit(main(project_dir=project_dir, manifest=args.manifest))
