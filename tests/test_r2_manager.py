import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from scripts.data.r2_manager import R2Manager


class R2ManagerManifestTests(unittest.TestCase):
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

    def _write_manifest(self, dataset_name: str, files: list[dict]) -> None:
        manifest = {
            "dataset": dataset_name,
            "files": files,
            "summary": {"file_count": len(files), "total_bytes": sum(f.get("size_bytes", 0) for f in files)},
        }
        (self.manifests_dir / f"{dataset_name}.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )

    def test_list_datasets_skips_public_manifests(self):
        self._write_manifest(
            "toyset",
            [
                {
                    "local_path": "toyset/file.zip",
                    "r2_key": "toyset/file.zip",
                    "size_bytes": 1,
                    "sha256": "pending",
                }
            ],
        )
        # Public manifest should be ignored
        (self.manifests_dir / "toyset.public.json").write_text("{}", encoding="utf-8")

        manager = R2Manager()
        datasets = manager.list_datasets()
        self.assertEqual(datasets, ["toyset"])

    def test_upload_dataset_updates_pending_hashes(self):
        dataset_name = "toyset"
        dataset_dir = self.datasets_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        file_path = dataset_dir / "images/frame_0001.jpg"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(b"example-bytes")

        self._write_manifest(
            dataset_name,
            [
                {
                    "local_path": "images/frame_0001.jpg",
                    "r2_key": "toyset/images/frame_0001.jpg",
                    "size_bytes": 0,
                    "sha256": "pending",
                }
            ],
        )

        manager = R2Manager()
        manager._file_exists_in_r2 = lambda *args, **kwargs: False

        upload_calls = []

        def fake_upload(local_path, r2_key, show_progress=True):
            upload_calls.append((Path(local_path), r2_key, show_progress))
            return True

        manager.upload_file = fake_upload

        computed_hashes = {"md5": "md5hash", "sha256": "sha256hash"}

        def fake_checksums(local_path):
            self.assertEqual(Path(local_path).resolve(), file_path.resolve())
            return computed_hashes

        manager.calculate_checksums = fake_checksums

        manager.upload_dataset(dataset_name, max_workers=1)

        manifest_path = self.manifests_dir / f"{dataset_name}.json"
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
        file_entry = manifest_data["files"][0]
        self.assertEqual(file_entry["sha256"], "sha256hash")
        self.assertEqual(file_entry["md5"], "md5hash")
        self.assertEqual(file_entry["size_bytes"], file_path.stat().st_size)

        self.assertEqual(len(upload_calls), 1)

    def test_download_skips_validation_when_hash_pending(self):
        dataset_name = "toyset"
        dataset_dir = self.datasets_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        file_path = dataset_dir / "images/frame_0001.jpg"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(b"example-bytes")

        self._write_manifest(
            dataset_name,
            [
                {
                    "local_path": "images/frame_0001.jpg",
                    "r2_key": "toyset/images/frame_0001.jpg",
                    "size_bytes": file_path.stat().st_size,
                    "sha256": "pending",
                }
            ],
        )

        manager = R2Manager()

        checksum_calls = []

        def fake_checksums(local_path):
            checksum_calls.append(Path(local_path))
            return {"md5": "md5hash", "sha256": "sha256hash"}

        download_calls = []

        def fake_download(r2_key, local_path):
            download_calls.append((r2_key, Path(local_path)))
            return True

        manager.calculate_checksums = fake_checksums
        manager.download_file = fake_download

        manager.download_dataset(dataset_name, validate=True, max_workers=1)

    def test_upload_dataset_parallel_invokes_upload(self):
        dataset_name = "toyset"
        dataset_dir = self.datasets_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        paths = []
        for idx in range(2):
            img_path = dataset_dir / f"images/frame_{idx}.jpg"
            img_path.parent.mkdir(parents=True, exist_ok=True)
            img_path.write_bytes(b"example-bytes" + bytes([idx]))
            paths.append(img_path)

        files = []
        for idx, path in enumerate(paths):
            files.append(
                {
                    "local_path": path.relative_to(dataset_dir).as_posix(),
                    "r2_key": f"toyset/{path.relative_to(dataset_dir).as_posix()}",
                    "size_bytes": 0,
                    "sha256": "pending",
                }
            )

        self._write_manifest(dataset_name, files)

        manager = R2Manager()
        manager._file_exists_in_r2 = lambda *args, **kwargs: False

        upload_calls = []

        def fake_upload(local_path, r2_key, show_progress=True):
            upload_calls.append((Path(local_path), r2_key, show_progress))
            return True

        def fake_checksums(local_path):
            return {"md5": "md5", "sha256": f"sha_{Path(local_path).name}"}

        manager.upload_file = fake_upload
        manager.calculate_checksums = fake_checksums

        manager.upload_dataset(dataset_name, max_workers=2)

        self.assertEqual(len(upload_calls), 2)
        self.assertTrue(all(not call[2] for call in upload_calls))

    def test_download_dataset_parallel_invokes_download(self):
        dataset_name = "toyset"
        dataset_dir = self.datasets_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        files = [
            {
                "local_path": "images/frame_0001.jpg",
                "r2_key": "toyset/images/frame_0001.jpg",
                "size_bytes": 12,
                "sha256": "pending",
            },
            {
                "local_path": "images/frame_0002.jpg",
                "r2_key": "toyset/images/frame_0002.jpg",
                "size_bytes": 12,
                "sha256": "pending",
            },
        ]

        self._write_manifest(dataset_name, files)

        manager = R2Manager()

        download_calls = []

        def fake_download(r2_key, local_path, show_progress=True):
            download_calls.append((r2_key, Path(local_path), show_progress))
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(local_path).write_bytes(b"content")
            return True

        manager.download_file = fake_download
        manager.calculate_checksums = lambda path: {"md5": "md5", "sha256": "sha"}

        manager.download_dataset(dataset_name, validate=False, max_workers=2)

        self.assertEqual(len(download_calls), 2)
        self.assertTrue(all(not call[2] for call in download_calls))


if __name__ == "__main__":
    unittest.main()
