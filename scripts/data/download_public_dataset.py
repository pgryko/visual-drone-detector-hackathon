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

CHUNK_SIZE = 1024 * 1024  # 1 MiB


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


def _stream_download(
    url: str,
    destination: Path,
    expected_sha: Optional[str],
    expected_size: Optional[int],
    verify: bool,
) -> Dict[str, Optional[str | int]]:
    """Download ``url`` to ``destination`` in chunks and return metadata."""

    hasher = hashlib.sha256() if verify and _has_expected_hash(expected_sha) else None

    downloaded_size = 0
    with request.urlopen(url) as resp, destination.open("wb") as out_file:
        while True:
            chunk = resp.read(CHUNK_SIZE)
            if not chunk:
                break
            out_file.write(chunk)
            downloaded_size += len(chunk)
            if hasher is not None:
                hasher.update(chunk)

    downloaded_sha = hasher.hexdigest() if hasher is not None else None

    return {"size": downloaded_size, "sha": downloaded_sha}


def download_dataset_from_manifest(
    manifest_path: Path,
    output_root: Path,
    verify: bool = True,
    fetcher: Optional[Fetcher] = None,
) -> Dict[str, int]:
    manifest = load_public_manifest(manifest_path)
    summary = {"downloaded": 0, "skipped": 0}

    files = manifest.get("files", [])
    total_files = len(files)
    if total_files == 0:
        print("Manifest contained no files", flush=True)

    for index, file_info in enumerate(files, start=1):
        rel_path = file_info.get("local_path") or file_info["r2_key"]
        target_path = output_root / rel_path
        target_path.parent.mkdir(parents=True, exist_ok=True)

        expected_sha = file_info.get("sha256")
        expected_size = file_info.get("size_bytes")

        if target_path.exists():
            if verify and _has_expected_hash(expected_sha):
                current_sha = _checksum(target_path.read_bytes())
                current_size = target_path.stat().st_size
                if current_sha == expected_sha and (
                    expected_size is None or current_size == expected_size
                ):
                    summary["skipped"] += 1
                    continue
            elif not verify:
                summary["skipped"] += 1
                continue

        presigned_url = file_info["presigned_url"]

        if fetcher is None:
            meta = _stream_download(
                url=presigned_url,
                destination=target_path,
                expected_sha=expected_sha,
                expected_size=expected_size,
                verify=verify,
            )
            downloaded_sha = meta["sha"]
            downloaded_size = meta["size"]
        else:
            content = fetcher(presigned_url)
            target_path.write_bytes(content)
            downloaded_size = len(content)
            downloaded_sha = (
                _checksum(content)
                if verify and _has_expected_hash(expected_sha)
                else None
            )

        if verify and _has_expected_hash(expected_sha):
            if downloaded_sha != expected_sha:
                raise ChecksumMismatchError(
                    f"Checksum mismatch for {target_path}: expected {expected_sha} got {downloaded_sha}"
                )
        if verify and expected_size is not None and downloaded_size != expected_size:
            raise ChecksumMismatchError(
                f"Size mismatch for {target_path}: expected {expected_size} bytes got {downloaded_size}"
            )

        summary["downloaded"] += 1

        if total_files:
            should_report = (
                total_files <= 100
                or index == 1
                or index == total_files
                or index % 100 == 0
            )
            if should_report:
                print(f"[{index}/{total_files}] downloaded {rel_path}", flush=True)

    return summary


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", required=True, help="Path to public manifest JSON"
    )
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
