#!/usr/bin/env python3
"""Generate presigned download manifests for datasets stored in Cloudflare R2."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from scripts.data.r2_manager import R2Manager


def load_manifest(manifest_path: Path) -> Dict[str, Any]:
    with manifest_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def generate_presigned_manifest(
    dataset_name: str,
    manifest_path: Path,
    s3_client,
    bucket_name: str,
    expires_in: int,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    if now is None:
        now = datetime.now(timezone.utc)
    base_manifest = load_manifest(manifest_path)
    expires_at = now + timedelta(seconds=expires_in)

    files_payload: List[Dict[str, Any]] = []
    for file_info in base_manifest.get("files", []):
        r2_key = file_info["r2_key"]
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": r2_key},
            ExpiresIn=expires_in,
        )
        entry = {
            "local_path": file_info.get("local_path"),
            "r2_key": r2_key,
            "size_bytes": file_info.get("size_bytes"),
            "sha256": file_info.get("sha256"),
            "presigned_url": url,
            "expires_at": expires_at.isoformat(),
            "dataset": dataset_name,
        }
        if "md5" in file_info:
            entry["md5"] = file_info["md5"]
        files_payload.append(entry)

    payload: Dict[str, Any] = {
        "dataset": dataset_name,
        "generated_at": now.isoformat(),
        "expires_in": expires_in,
        "expires_at": expires_at.isoformat(),
        "files": files_payload,
    }

    if "summary" in base_manifest:
        payload["summary"] = base_manifest["summary"]

    return payload


def write_manifest(output_path: Path, payload: Dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def resolve_dataset_names(args, manager: R2Manager) -> List[str]:
    if args.all:
        datasets = manager.list_datasets()
        if not datasets:
            print("No datasets found in manifests directory", file=sys.stderr)
        return datasets
    if args.dataset:
        return [args.dataset]
    raise SystemExit("Please specify --dataset NAME or --all")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", help="Dataset name to generate presigned URLs for")
    parser.add_argument(
        "--all", action="store_true", help="Generate manifests for all datasets"
    )
    parser.add_argument(
        "--expires-in",
        type=int,
        default=24 * 60 * 60,
        help="Expiry in seconds for each presigned URL (default: 86400)",
    )
    parser.add_argument(
        "--output",
        help="Output path (only valid when generating a single dataset)",
    )
    parser.add_argument(
        "--bundle",
        help="Optional name for an aggregated manifest that includes every dataset",
    )
    parser.add_argument(
        "--bundle-output",
        help="Override path for the aggregated manifest (defaults to presigned/<bundle>.public.json)",
    )
    args = parser.parse_args(argv)

    manager = R2Manager()
    if manager.s3_client is None:
        print("R2 client not configured. Please set credentials in .env", file=sys.stderr)
        return 2

    dataset_names = resolve_dataset_names(args, manager)
    if not dataset_names:
        return 1

    manifests_dir = Path("datasets/manifests")
    output_paths: List[Path] = []
    dataset_payloads: List[Dict[str, Any]] = []
    shared_now = datetime.now(timezone.utc)

    for dataset_name in dataset_names:
        manifest_path = manifests_dir / f"{dataset_name}.json"
        if not manifest_path.exists():
            print(f"Manifest not found: {manifest_path}", file=sys.stderr)
            continue

        payload = generate_presigned_manifest(
            dataset_name=dataset_name,
            manifest_path=manifest_path,
            s3_client=manager.s3_client,
            bucket_name=manager.bucket_name,
            expires_in=args.expires_in,
            now=shared_now,
        )
        dataset_payloads.append(payload)

        if args.output and len(dataset_names) == 1:
            output_path = Path(args.output)
        else:
            output_path = manifests_dir / "presigned" / f"{dataset_name}.public.json"
        write_manifest(output_path, payload)
        output_paths.append(output_path)

    if args.bundle and dataset_payloads:
        if args.bundle_output:
            bundle_path = Path(args.bundle_output)
        else:
            bundle_path = manifests_dir / "presigned" / f"{args.bundle}.public.json"

        bundle_payload = build_bundle_manifest(
            bundle_name=args.bundle,
            payloads=dataset_payloads,
            generated_at=shared_now,
            expires_in=args.expires_in,
        )
        write_manifest(bundle_path, bundle_payload)
        output_paths.append(bundle_path)

    if not output_paths:
        return 1

    print(
        "Generated presigned manifests:\n" +
        "\n".join(f" - {path}" for path in output_paths)
    )
    return 0


def build_bundle_manifest(
    bundle_name: str,
    payloads: List[Dict[str, Any]],
    generated_at: datetime,
    expires_in: int,
) -> Dict[str, Any]:
    files: List[Dict[str, Any]] = []
    earliest_expiry: Optional[datetime] = None

    for payload in payloads:
        payload_expiry = datetime.fromisoformat(payload["expires_at"])
        if earliest_expiry is None or payload_expiry < earliest_expiry:
            earliest_expiry = payload_expiry
        files.extend(payload["files"])

    if earliest_expiry is None:
        earliest_expiry = generated_at + timedelta(seconds=expires_in)

    return {
        "bundle": bundle_name,
        "generated_at": generated_at.isoformat(),
        "expires_in": expires_in,
        "expires_at": earliest_expiry.isoformat(),
        "files": files,
    }


if __name__ == "__main__":
    raise SystemExit(main())
