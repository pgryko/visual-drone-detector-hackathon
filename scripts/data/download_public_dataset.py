#!/usr/bin/env python3
"""Download datasets using presigned public manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Callable, Dict, Optional
from urllib import request


class DownloadError(Exception):
    """Base error for download failures."""


class ChecksumMismatchError(DownloadError):
    """Raised when a downloaded file fails checksum verification."""


Fetcher = Callable[[str], bytes]


def load_public_manifest(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _http_fetch(url: str) -> bytes:
    with request.urlopen(url) as resp:
        return resp.read()


def _has_expected_hash(value: Optional[str]) -> bool:
    return value not in (None, "", "pending")


def _checksum(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def download_dataset_from_manifest(
    manifest_path: Path,
    output_root: Path,
    verify: bool = True,
    fetcher: Optional[Fetcher] = None,
) -> Dict[str, int]:
    manifest = load_public_manifest(manifest_path)
    fetch = fetcher or _http_fetch
    summary = {"downloaded": 0, "skipped": 0}

    for file_info in manifest.get("files", []):
        rel_path = file_info.get("local_path") or file_info["r2_key"]
        target_path = output_root / rel_path
        target_path.parent.mkdir(parents=True, exist_ok=True)

        expected_sha = file_info.get("sha256")
        expected_size = file_info.get("size_bytes")

        if target_path.exists():
            if verify and _has_expected_hash(expected_sha):
                current_sha = _checksum(target_path.read_bytes())
                if current_sha == expected_sha and (
                    expected_size is None or target_path.stat().st_size == expected_size
                ):
                    summary["skipped"] += 1
                    continue
            elif not verify:
                summary["skipped"] += 1
                continue

        content = fetch(file_info["presigned_url"])
        target_path.write_bytes(content)

        if verify and _has_expected_hash(expected_sha):
            downloaded_sha = _checksum(content)
            if downloaded_sha != expected_sha:
                raise ChecksumMismatchError(
                    f"Checksum mismatch for {target_path}: expected {expected_sha} got {downloaded_sha}"
                )
        if verify and expected_size is not None and len(content) != expected_size:
            raise ChecksumMismatchError(
                f"Size mismatch for {target_path}: expected {expected_size} bytes got {len(content)}"
            )

        summary["downloaded"] += 1

    return summary


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="Path to public manifest JSON")
    parser.add_argument(
        "--output-dir",
        default="datasets",
        help="Directory to place downloaded files (default: datasets)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip checksum and size verification",
    )
    args = parser.parse_args(argv)

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}", file=sys.stderr)
        return 1

    output_root = Path(args.output_dir)

    try:
        summary = download_dataset_from_manifest(
            manifest_path=manifest_path,
            output_root=output_root,
            verify=not args.no_verify,
        )
    except ChecksumMismatchError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except DownloadError as exc:
        print(str(exc), file=sys.stderr)
        return 3

    print(
        f"Download complete: {summary['downloaded']} files downloaded, {summary['skipped']} skipped"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
