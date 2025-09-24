#!/usr/bin/env python3
"""
Cloudflare R2 Dataset Manager
Handles upload, download, and synchronization of datasets with R2 storage
"""

import os
import json
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from dataclasses import dataclass
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    """Information about a dataset file"""

    local_path: str
    r2_key: str
    size_bytes: int
    md5: str
    sha256: str
    description: str


class R2Manager:
    """Manages dataset storage in Cloudflare R2"""

    def __init__(self):
        """Initialize R2 client with credentials from environment"""
        self.endpoint = os.getenv("CLOUDFLARE_R2_ENDPOINT_URL") or os.getenv(
            "CLOUDFLARE_R2_ENDPOINT"
        )
        self.access_key_id = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY")
        self.bucket_name = os.getenv("CLOUDFLARE_R2_BUCKET_NAME", "drone-datasets")
        self.public_url = os.getenv("CLOUDFLARE_R2_PUBLIC_URL", "")

        if not all([self.endpoint, self.access_key_id, self.secret_access_key]):
            logger.warning("R2 credentials not found. Please configure .env file")
            self.s3_client = None
        else:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name="auto",
            )

    def calculate_checksums(self, file_path: Path) -> Dict[str, str]:
        """Calculate MD5 and SHA256 checksums for a file"""
        md5_hash = hashlib.md5()
        sha256_hash = hashlib.sha256()

        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
                sha256_hash.update(chunk)

        return {"md5": md5_hash.hexdigest(), "sha256": sha256_hash.hexdigest()}

    def _ensure_bucket_exists(self) -> bool:
        """Ensure the bucket exists, create if it doesn't"""
        if not self.s3_client:
            return False

        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ Bucket {self.bucket_name} exists")
            return True
        except Exception as e:
            if "404" in str(e) or "NoSuchBucket" in str(e) or "Not Found" in str(e):
                try:
                    logger.info(f"🔨 Creating bucket: {self.bucket_name}")
                    # For R2, we need to create bucket without LocationConstraint
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"✅ Created bucket: {self.bucket_name}")
                    return True
                except Exception as create_error:
                    logger.error(
                        f"❌ Failed to create bucket {self.bucket_name}: {create_error}"
                    )
                    logger.error(
                        f"💡 Please create the bucket manually in Cloudflare R2 dashboard:"
                    )
                    logger.error(f"   1. Go to https://dash.cloudflare.com/")
                    logger.error(f"   2. Navigate to R2 Object Storage")
                    logger.error(f"   3. Click 'Create bucket'")
                    logger.error(f"   4. Name it '{self.bucket_name}'")
                    return False
            else:
                logger.error(f"❌ Error checking bucket {self.bucket_name}: {e}")
                return False

    def upload_file(
        self, local_path: Path, r2_key: str, show_progress: bool = True
    ) -> bool:
        """Upload a single file to R2"""
        if not self.s3_client:
            logger.error("R2 client not initialized. Check credentials.")
            return False

        # Ensure bucket exists before uploading
        if not self._ensure_bucket_exists():
            logger.error(
                f"Cannot upload: bucket {self.bucket_name} doesn't exist and couldn't be created"
            )
            return False

        try:
            file_size = local_path.stat().st_size

            if show_progress:
                with tqdm(
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Uploading {local_path.name}",
                ) as pbar:

                    def upload_callback(bytes_amount):
                        pbar.update(bytes_amount)

                    self.s3_client.upload_file(
                        str(local_path),
                        self.bucket_name,
                        r2_key,
                        Callback=upload_callback,
                    )
            else:
                self.s3_client.upload_file(str(local_path), self.bucket_name, r2_key)

            logger.info(f"✅ Uploaded {local_path} to {r2_key}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to upload {local_path}: {e}")
            return False

    def download_file(
        self, r2_key: str, local_path: Path, show_progress: bool = True
    ) -> bool:
        """Download a single file from R2"""
        if not self.s3_client:
            # Try public URL if available
            if self.public_url:
                return self._download_public(r2_key, local_path, show_progress)
            logger.error("R2 client not initialized and no public URL available.")
            return False

        try:
            # Create parent directory if needed
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Get file size for progress bar
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=r2_key)
            file_size = response["ContentLength"]

            if show_progress:
                with tqdm(
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Downloading {local_path.name}",
                ) as pbar:

                    def download_callback(bytes_amount):
                        pbar.update(bytes_amount)

                    self.s3_client.download_file(
                        self.bucket_name,
                        r2_key,
                        str(local_path),
                        Callback=download_callback,
                    )
            else:
                self.s3_client.download_file(self.bucket_name, r2_key, str(local_path))

            logger.info(f"✅ Downloaded {r2_key} to {local_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to download {r2_key}: {e}")
            return False

    def _file_exists_in_r2(
        self, r2_key: str, expected_size: Optional[int] = None
    ) -> bool:
        """Check if file exists in R2 and optionally verify size"""
        if not self.s3_client:
            return False

        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=r2_key)
            if expected_size:
                return response["ContentLength"] == expected_size
            return True
        except Exception:
            return False

    def _download_public(
        self, r2_key: str, local_path: Path, show_progress: bool = True
    ) -> bool:
        """Download from public URL (no auth required)"""
        import requests

        url = f"{self.public_url}/{r2_key}"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            with open(local_path, "wb") as f:
                if show_progress:
                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        desc=f"Downloading {local_path.name}",
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))
                else:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

            logger.info(f"✅ Downloaded {url} to {local_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to download from {url}: {e}")
            return False

    def upload_dataset(
        self,
        dataset_name: str,
        update_manifest: bool = True,
        max_workers: int = 1,
    ):
        """Upload an entire dataset based on its manifest"""
        manifest_path = Path(f"datasets/manifests/{dataset_name}.json")
        if not manifest_path.exists():
            logger.error(f"Manifest not found: {manifest_path}")
            return

        with open(manifest_path) as f:
            manifest = json.load(f)

        dataset_dir = Path(f"datasets/{dataset_name}")
        if not dataset_dir.exists():
            logger.error(f"Dataset directory not found: {dataset_dir}")
            return

        logger.info(f"📦 Uploading {dataset_name} dataset...")

        # Update manifest with checksums if needed
        pending_uploads = []
        for file_info in manifest["files"]:
            local_path = dataset_dir / file_info["local_path"]

            if not local_path.exists():
                logger.warning(f"File not found: {local_path}")
                continue

            # Calculate and update checksums
            if update_manifest and (
                file_info.get("md5") == "pending"
                or file_info.get("sha256") == "pending"
            ):
                logger.info(f"Calculating checksums for {local_path.name}...")
                checksums = self.calculate_checksums(local_path)
                file_info["md5"] = checksums["md5"]
                file_info["sha256"] = checksums["sha256"]
                file_info["size_bytes"] = local_path.stat().st_size

            # Check if file already exists in R2
            if self._file_exists_in_r2(
                file_info["r2_key"], file_info.get("size_bytes")
            ):
                logger.info(f"⏭️  {local_path.name} already exists in R2, skipping...")
                continue

            pending_uploads.append((local_path, file_info))

        if pending_uploads:
            show_progress = max_workers == 1
            if max_workers <= 1:
                for local_path, file_info in pending_uploads:
                    self.upload_file(
                        local_path, file_info["r2_key"], show_progress=show_progress
                    )
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_file = {
                        executor.submit(
                            self.upload_file,
                            local_path,
                            file_info["r2_key"],
                            False,
                        ): (local_path, file_info)
                        for local_path, file_info in pending_uploads
                    }
                    for future in as_completed(future_to_file):
                        local_path, file_info = future_to_file[future]
                        success = future.result()
                        if not success:
                            logger.error(
                                f"❌ Failed to upload {local_path} to {file_info['r2_key']}"
                            )

        # Save updated manifest
        if update_manifest:
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=2)
            logger.info(f"✅ Updated manifest with checksums")

        logger.info(f"✅ Dataset {dataset_name} uploaded successfully")

    def download_dataset(
        self,
        dataset_name: str,
        validate: bool = True,
        max_workers: int = 1,
    ):
        """Download an entire dataset based on its manifest"""
        manifest_path = Path(f"datasets/manifests/{dataset_name}.json")
        if not manifest_path.exists():
            logger.error(f"Manifest not found: {manifest_path}")
            return

        with open(manifest_path) as f:
            manifest = json.load(f)

        dataset_dir = Path(f"datasets/{dataset_name}")
        dataset_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📦 Downloading {dataset_name} dataset...")

        to_download = []
        for file_info in manifest["files"]:
            local_path = dataset_dir / file_info["local_path"]
            expected_sha = file_info.get("sha256")
            has_expected_hash = expected_sha not in (None, "", "pending")

            # Skip if file exists and checksum matches
            if local_path.exists():
                if validate and has_expected_hash:
                    logger.info(f"Validating {local_path.name}...")
                    checksums = self.calculate_checksums(local_path)

                    if checksums[
                        "sha256"
                    ] == expected_sha and local_path.stat().st_size == file_info.get(
                        "size_bytes"
                    ):
                        logger.info(f"✓ {local_path.name} already exists and is valid")
                        continue
                    else:
                        logger.warning(
                            f"Checksum mismatch for {local_path.name}, re-downloading..."
                        )
                else:
                    if not has_expected_hash and validate:
                        logger.info(
                            f"Skipping validation for {local_path.name}; manifest hash pending"
                        )
                    # File already present; no validation needed
                    continue

            to_download.append((file_info, local_path))

        if to_download:
            show_progress = max_workers == 1
            if max_workers <= 1:
                for file_info, local_path in to_download:
                    if not self._download_and_validate(
                        file_info, local_path, validate, show_progress
                    ):
                        return
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_file = {
                        executor.submit(
                            self._download_and_validate,
                            file_info,
                            local_path,
                            validate,
                            False,
                        ): (file_info, local_path)
                        for file_info, local_path in to_download
                    }
                    for future in as_completed(future_to_file):
                        _, local_path = future_to_file[future]
                        success = future.result()
                        if not success:
                            logger.error(
                                f"❌ Download or validation failed for {local_path.name}"
                            )
                            return

        logger.info(f"✅ Dataset {dataset_name} downloaded successfully")

    def list_datasets(self) -> List[str]:
        """List all available datasets from manifests"""
        manifests_dir = Path("datasets/manifests")
        if not manifests_dir.exists():
            return []

        datasets = []
        for manifest_file in manifests_dir.glob("*.json"):
            if manifest_file.name.endswith(".public.json"):
                continue
            dataset_name = manifest_file.stem
            datasets.append(dataset_name)

        return datasets

    def sync_all_datasets(self, direction: str = "download", max_workers: int = 1):
        """Sync all datasets either up or down"""
        datasets = self.list_datasets()

        for dataset in datasets:
            if direction == "download":
                self.download_dataset(dataset, max_workers=max_workers)
            elif direction == "upload":
                self.upload_dataset(dataset, max_workers=max_workers)
            else:
                logger.error(
                    f"Invalid direction: {direction}. Use 'upload' or 'download'"
                )
                return

    def _download_and_validate(
        self, file_info: dict, local_path: Path, validate: bool, show_progress: bool
    ) -> bool:
        success = self.download_file(
            file_info["r2_key"], local_path, show_progress=show_progress
        )
        if not success:
            return False

        if validate and file_info.get("sha256") not in (None, "", "pending"):
            checksums = self.calculate_checksums(local_path)
            if checksums["sha256"] != file_info["sha256"]:
                logger.error(f"❌ Checksum verification failed for {local_path.name}")
                return False
        if (
            validate
            and file_info.get("size_bytes")
            and local_path.stat().st_size != file_info["size_bytes"]
        ):
            logger.error(
                f"❌ Size verification failed for {local_path.name}: "
                f"expected {file_info['size_bytes']} got {local_path.stat().st_size}"
            )
            return False
        return True


def main():
    """CLI interface for R2 dataset manager"""
    import argparse

    parser = argparse.ArgumentParser(description="Manage datasets in Cloudflare R2")
    parser.add_argument(
        "action",
        choices=["upload", "download", "sync", "list"],
        help="Action to perform",
    )
    parser.add_argument("--dataset", help="Dataset name (for upload/download)")
    parser.add_argument("--all", action="store_true", help="Process all datasets")
    parser.add_argument(
        "--no-validate", action="store_true", help="Skip checksum validation"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel workers for upload/download (default: 1)",
    )

    args = parser.parse_args()

    manager = R2Manager()

    if args.action == "list":
        datasets = manager.list_datasets()
        print("📊 Available datasets:")
        for dataset in datasets:
            print(f"  - {dataset}")

    elif args.action in ["upload", "download"]:
        if args.all:
            manager.sync_all_datasets(direction=args.action, max_workers=args.workers)
        elif args.dataset:
            if args.action == "upload":
                manager.upload_dataset(args.dataset, max_workers=args.workers)
            else:
                manager.download_dataset(
                    args.dataset,
                    validate=not args.no_validate,
                    max_workers=args.workers,
                )
        else:
            print("Please specify --dataset or --all")

    elif args.action == "sync":
        manager.sync_all_datasets(direction="download", max_workers=args.workers)


if __name__ == "__main__":
    main()
