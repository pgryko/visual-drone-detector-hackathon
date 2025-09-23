import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import scripts.data.generate_presigned_urls as presign


class StubS3Client:
    def __init__(self):
        self.calls = []

    def generate_presigned_url(self, operation_name, Params=None, ExpiresIn=None):
        self.calls.append((operation_name, Params, ExpiresIn))
        key = Params["Key"]
        return f"https://example.com/{key}?expires={ExpiresIn}"


class PresignGenerationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.temp_dir.name)
        self.datasets_dir = self.repo_root / "datasets"
        self.manifests_dir = self.datasets_dir / "manifests"
        self.datasets_dir.mkdir(parents=True, exist_ok=True)
        self.manifests_dir.mkdir(parents=True, exist_ok=True)
        self.cwd = os.getcwd()
        os.chdir(self.repo_root)

    def tearDown(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.repo_root, ignore_errors=True)
        self.temp_dir.cleanup()

    def _write_manifest(self, name: str, size: int = 123, sha: str = "abc") -> Path:
        manifest = {
            "dataset": name,
            "files": [
                {
                    "local_path": f"{name}/file.zip",
                    "r2_key": f"{name}/file.zip",
                    "size_bytes": size,
                    "sha256": sha,
                }
            ],
        }
        path = self.manifests_dir / f"{name}.json"
        path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return path

    def test_generate_manifest_payload(self):
        manifest_path = self._write_manifest("toyset")
        s3 = StubS3Client()
        base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)

        payload = presign.generate_presigned_manifest(
            dataset_name="toyset",
            manifest_path=manifest_path,
            s3_client=s3,
            bucket_name="test-bucket",
            expires_in=3600,
            now=base_time,
        )

        self.assertEqual(payload["dataset"], "toyset")
        self.assertEqual(payload["generated_at"], base_time.isoformat())
        self.assertEqual(
            payload["expires_at"], (base_time + timedelta(seconds=3600)).isoformat()
        )
        self.assertEqual(payload["expires_in"], 3600)
        self.assertEqual(len(payload["files"]), 1)
        file_entry = payload["files"][0]
        self.assertEqual(
            file_entry["presigned_url"], "https://example.com/toyset/file.zip?expires=3600"
        )
        self.assertEqual(file_entry["size_bytes"], 123)
        self.assertEqual(file_entry["r2_key"], "toyset/file.zip")
        self.assertEqual(file_entry["dataset"], "toyset")

    def test_cli_writes_output_manifest(self):
        self._write_manifest("toyset")

        stub_manager = mock.MagicMock()
        stub_manager.bucket_name = "test-bucket"
        stub_manager.s3_client = StubS3Client()

        with mock.patch.object(presign, "R2Manager", return_value=stub_manager):
            output_path = self.manifests_dir / "toyset.public.json"
            exit_code = presign.main(
                [
                    "--dataset",
                    "toyset",
                    "--expires-in",
                    "600",
                    "--output",
                    str(output_path),
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertTrue(output_path.exists())
        data = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(data["dataset"], "toyset")
        self.assertEqual(len(data["files"]), 1)
        self.assertIn("presigned_url", data["files"][0])
        self.assertEqual(
            stub_manager.s3_client.calls,
            [("get_object", {"Bucket": "test-bucket", "Key": "toyset/file.zip"}, 600)],
        )

    def test_cli_bundle_creates_aggregate_manifest(self):
        self._write_manifest("toyset")
        self._write_manifest("otherset", size=456)

        stub_manager = mock.MagicMock()
        stub_manager.bucket_name = "test-bucket"
        stub_manager.s3_client = StubS3Client()
        stub_manager.list_datasets.return_value = ["toyset", "otherset"]

        with mock.patch.object(presign, "R2Manager", return_value=stub_manager):
            exit_code = presign.main(
                ["--all", "--expires-in", "600", "--bundle", "all-datasets"]
            )

        self.assertEqual(exit_code, 0)
        bundle_path = self.manifests_dir / "presigned" / "all-datasets.public.json"
        self.assertTrue(bundle_path.exists())
        data = json.loads(bundle_path.read_text(encoding="utf-8"))
        self.assertEqual(data["bundle"], "all-datasets")
        self.assertEqual(len(data["files"]), 2)
        datasets = {entry["dataset"] for entry in data["files"]}
        self.assertEqual(datasets, {"toyset", "otherset"})


if __name__ == "__main__":
    unittest.main()
