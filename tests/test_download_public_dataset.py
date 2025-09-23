import hashlib
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import scripts.data.download_public_dataset as downloader


class DownloadPublicDatasetTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.temp_dir.name)
        self.output_dir = self.repo_root / "datasets"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.repo_root / "toyset.public.json"

    def tearDown(self):
        shutil.rmtree(self.repo_root, ignore_errors=True)
        self.temp_dir.cleanup()

    def _write_manifest(self, sha_value: str) -> dict:
        manifest = {
            "dataset": "toyset",
            "files": [
                {
                    "local_path": "toyset/file.bin",
                    "r2_key": "toyset/file.bin",
                    "size_bytes": 11,
                    "sha256": sha_value,
                    "presigned_url": "https://example.com/file.bin",
                }
            ],
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest

    def test_downloads_file_and_validates_checksum(self):
        content = b"hello world"
        sha_value = hashlib.sha256(content).hexdigest()
        self._write_manifest(sha_value)

        fetch_calls = []

        def fake_fetch(url: str) -> bytes:
            fetch_calls.append(url)
            return content

        summary = downloader.download_dataset_from_manifest(
            manifest_path=self.manifest_path,
            output_root=self.output_dir,
            verify=True,
            fetcher=fake_fetch,
        )

        target_path = self.output_dir / "toyset" / "file.bin"
        self.assertTrue(target_path.exists())
        self.assertEqual(target_path.read_bytes(), content)
        self.assertEqual(summary["downloaded"], 1)
        self.assertEqual(fetch_calls, ["https://example.com/file.bin"])

    def test_skips_existing_file_when_hash_matches(self):
        content = b"hello world"
        sha_value = hashlib.sha256(content).hexdigest()
        self._write_manifest(sha_value)
        target_path = self.output_dir / "toyset" / "file.bin"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(content)

        def fake_fetch(url: str) -> bytes:
            raise AssertionError("fetch should not be called when file is valid")

        summary = downloader.download_dataset_from_manifest(
            manifest_path=self.manifest_path,
            output_root=self.output_dir,
            verify=True,
            fetcher=fake_fetch,
        )

        self.assertEqual(summary["skipped"], 1)

    def test_raises_on_checksum_mismatch(self):
        content = b"hello world"
        sha_value = hashlib.sha256(b"not matching").hexdigest()
        self._write_manifest(sha_value)

        def fake_fetch(url: str) -> bytes:
            return content

        with self.assertRaises(downloader.ChecksumMismatchError):
            downloader.download_dataset_from_manifest(
                manifest_path=self.manifest_path,
                output_root=self.output_dir,
                verify=True,
                fetcher=fake_fetch,
            )


if __name__ == "__main__":
    unittest.main()
