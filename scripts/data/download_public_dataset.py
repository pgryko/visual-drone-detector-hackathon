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
from urllib.error import HTTPError, URLError

from tqdm import tqdm

CHUNK_SIZE = 1024 * 1024  # 1 MiB


class DownloadError(Exception):
    """Base error for download failures."""


class ChecksumMismatchError(DownloadError):
    """Raised when a downloaded file fails checksum verification."""


class HTTPDownloadError(DownloadError):
    """Raised when a file cannot be downloaded due to HTTP errors."""


Fetcher = Callable[[str], bytes]


def load_public_manifest(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _has_expected_hash(value: Optional[str]) -> bool:
    return value not in (None, "", "pending")


def _checksum(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _checksum_file(file_path: Path) -> str:
    """Compute SHA256 checksum of a file by reading it in chunks."""
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            hasher.update(chunk)
    return hasher.hexdigest()


def _stream_download(
    url: str,
    destination: Path,
    expected_sha: Optional[str],
    expected_size: Optional[int],
    verify: bool,
    progress: Optional[tqdm] = None,
) -> Dict[str, Optional[str | int]]:
    """Download ``url`` to ``destination`` in chunks and return metadata."""

    hasher = hashlib.sha256() if verify and _has_expected_hash(expected_sha) else None

    downloaded_size = 0
    try:
        with request.urlopen(url) as resp, destination.open("wb") as out_file:
            while True:
                chunk = resp.read(CHUNK_SIZE)
                if not chunk:
                    break
                out_file.write(chunk)
                downloaded_size += len(chunk)
                if hasher is not None:
                    hasher.update(chunk)
                if progress is not None:
                    progress.update(len(chunk))
    except (HTTPError, URLError) as e:
        # Clean up partial file if download failed
        if destination.exists():
            destination.unlink()
        raise HTTPDownloadError(f"Failed to download {url}: {e}")

    downloaded_sha = hasher.hexdigest() if hasher is not None else None

    return {"size": downloaded_size, "sha": downloaded_sha}


def download_dataset_from_manifest(
    manifest_path: Path,
    output_root: Path,
    verify: bool = True,
    fetcher: Optional[Fetcher] = None,
    fast_resume: bool = False,
    start_from: int = 0,
) -> Dict[str, int]:
    manifest = load_public_manifest(manifest_path)
    summary = {"downloaded": 0, "skipped": 0, "failed": 0}

    # Create a log file for failed downloads
    failed_downloads_log = output_root / "failed_downloads.log"
    failed_downloads_log.parent.mkdir(parents=True, exist_ok=True)

    files = manifest.get("files", [])
    total_files = len(files)
    if total_files == 0:
        print("Manifest contained no files", flush=True)

    # Apply start_from offset
    if start_from > 0:
        if start_from >= total_files:
            print(f"Start index {start_from} is beyond total files {total_files}")
            return summary
        files = files[start_from:]
        print(f"Starting from file {start_from + 1} of {total_files}")

    files_to_process = len(files)

    show_progress = fetcher is None and files_to_process > 0
    file_progress: Optional[tqdm] = None
    if show_progress:
        file_progress = tqdm(
            total=files_to_process,
            unit="file",
            desc="Files",
            dynamic_ncols=True,
        )
        if start_from > 0:
            file_progress.set_description(f"Files (from {start_from + 1})")

    try:
        manifest_dataset = manifest.get("dataset") or manifest.get("bundle")

        for file_info in files:
            local_path = file_info.get("local_path")
            rel_path = Path(local_path) if local_path else Path(file_info["r2_key"])

            dataset_name = file_info.get("dataset") or manifest_dataset
            if dataset_name:
                dataset_prefix = Path(dataset_name)
                if not rel_path.parts or rel_path.parts[0] != dataset_name:
                    rel_path = dataset_prefix / rel_path

            target_path = output_root / rel_path
            target_path.parent.mkdir(parents=True, exist_ok=True)

            expected_sha = file_info.get("sha256")
            expected_size = file_info.get("size_bytes")

            if target_path.exists():
                if fast_resume:
                    # Fast resume: only check file size, skip checksum verification
                    if expected_size is not None:
                        current_size = target_path.stat().st_size
                        if current_size == expected_size:
                            summary["skipped"] += 1
                            if file_progress is not None:
                                file_progress.update(1)
                            continue
                    else:
                        # No expected size, assume file is complete if it exists
                        summary["skipped"] += 1
                        if file_progress is not None:
                            file_progress.update(1)
                        continue
                elif verify and _has_expected_hash(expected_sha):
                    # Full verification: check both size and checksum
                    current_sha = _checksum_file(target_path)
                    current_size = target_path.stat().st_size
                    if current_sha == expected_sha and (
                        expected_size is None or current_size == expected_size
                    ):
                        summary["skipped"] += 1
                        if file_progress is not None:
                            file_progress.update(1)
                        continue
                elif not verify:
                    # No verification: skip if file exists
                    summary["skipped"] += 1
                    if file_progress is not None:
                        file_progress.update(1)
                    continue

            presigned_url = file_info["presigned_url"]

            if fetcher is None:
                byte_progress: Optional[tqdm] = None
                if show_progress:
                    # Only show a byte-level bar for sizeable downloads to avoid
                    # spamming logs with short-lived progress bars.
                    size_hint = expected_size or 0
                    if size_hint >= 1_000_000 or expected_size is None:
                        byte_progress = tqdm(
                            total=expected_size,
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1024,
                            desc=str(rel_path),
                            leave=False,
                            dynamic_ncols=True,
                        )
                try:
                    meta = _stream_download(
                        url=presigned_url,
                        destination=target_path,
                        expected_sha=expected_sha,
                        expected_size=expected_size,
                        verify=verify,
                        progress=byte_progress,
                    )
                except HTTPDownloadError as e:
                    # Log the failed download and continue with next file
                    error_msg = f"FAILED: {rel_path} - {str(e)}\n"
                    with failed_downloads_log.open("a", encoding="utf-8") as log_file:
                        log_file.write(error_msg)
                    print(f"Failed to download {rel_path}: {e}", file=sys.stderr)
                    summary["failed"] += 1
                    if file_progress is not None:
                        file_progress.update(1)
                    continue
                finally:
                    if byte_progress is not None:
                        byte_progress.close()

                downloaded_sha = meta["sha"]
                downloaded_size = meta["size"]
            else:
                try:
                    content = fetcher(presigned_url)
                    target_path.write_bytes(content)
                    downloaded_size = len(content)
                    downloaded_sha = (
                        _checksum(content)
                        if verify and _has_expected_hash(expected_sha)
                        else None
                    )
                except Exception as e:
                    # Log the failed download and continue with next file
                    error_msg = f"FAILED: {rel_path} - Fetcher error: {str(e)}\n"
                    with failed_downloads_log.open("a", encoding="utf-8") as log_file:
                        log_file.write(error_msg)
                    print(f"Failed to download {rel_path}: {e}", file=sys.stderr)
                    summary["failed"] += 1
                    if file_progress is not None:
                        file_progress.update(1)
                    continue

            if verify and _has_expected_hash(expected_sha):
                if downloaded_sha != expected_sha:
                    raise ChecksumMismatchError(
                        f"Checksum mismatch for {target_path}: expected {expected_sha} got {downloaded_sha}"
                    )
            if (
                verify
                and expected_size is not None
                and downloaded_size != expected_size
            ):
                raise ChecksumMismatchError(
                    f"Size mismatch for {target_path}: expected {expected_size} bytes got {downloaded_size}"
                )

            summary["downloaded"] += 1
            if file_progress is not None:
                file_progress.update(1)

    finally:
        if file_progress is not None:
            file_progress.close()

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
    parser.add_argument(
        "--fast-resume",
        action="store_true",
        help="Fast resume: only check file size for existing files, skip checksum verification",
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Start downloading from a specific file index (0-based)",
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
            fast_resume=args.fast_resume,
            start_from=args.start_from,
        )
    except ChecksumMismatchError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except HTTPDownloadError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except DownloadError as exc:
        print(str(exc), file=sys.stderr)
        return 4

    failed_count = summary.get('failed', 0)
    if failed_count > 0:
        print(
            f"Download complete: {summary['downloaded']} files downloaded, {summary['skipped']} skipped, {failed_count} failed"
        )
        print(f"Failed downloads logged to: {Path(args.output_dir) / 'failed_downloads.log'}")
    else:
        print(
            f"Download complete: {summary['downloaded']} files downloaded, {summary['skipped']} skipped"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
