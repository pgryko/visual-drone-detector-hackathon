import json
import shutil
import tempfile
import unittest
from pathlib import Path

import scripts.build_manifests as build_manifests


class BuildManifestsIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.temp_dir.name)
        self.datasets_dir = self.repo_root / "datasets"
        self.manifests_dir = self.datasets_dir / "manifests"

        # Preserve originals so we can restore in tearDown
        self._original_repo_root = build_manifests.REPO_ROOT
        self._original_datasets_dir = build_manifests.DATASETS_DIR
        self._original_manifests_dir = build_manifests.MANIFESTS_DIR

        build_manifests.REPO_ROOT = self.repo_root
        build_manifests.DATASETS_DIR = self.datasets_dir
        build_manifests.MANIFESTS_DIR = self.manifests_dir

    def tearDown(self):
        build_manifests.REPO_ROOT = self._original_repo_root
        build_manifests.DATASETS_DIR = self._original_datasets_dir
        build_manifests.MANIFESTS_DIR = self._original_manifests_dir
        shutil.rmtree(self.repo_root, ignore_errors=True)
        self.temp_dir.cleanup()

    def _create_sample_dataset(self, name: str, include_label: bool = True) -> None:
        image_dir = self.datasets_dir / name / "images"
        image_dir.mkdir(parents=True, exist_ok=True)
        (image_dir / "frame_0001.jpg").write_bytes(b"\x89jpg")

        if include_label:
            label_dir = self.datasets_dir / name / "labels"
            label_dir.mkdir(parents=True, exist_ok=True)
            (label_dir / "frame_0001.txt").write_text(
                "0 0.5 0.5 0.1 0.1", encoding="utf-8"
            )

    def test_main_generates_json_manifest(self):
        self._create_sample_dataset("toyset")

        exit_code = build_manifests.main(["--datasets", "toyset"])
        self.assertEqual(exit_code, 0)

        json_path = self.manifests_dir / "toyset.json"
        jsonl_path = self.manifests_dir / "toyset.jsonl"
        media_jsonl_path = self.manifests_dir / "toyset.media.jsonl"

        self.assertTrue(json_path.exists(), "Expected dataset JSON manifest to be created")
        self.assertTrue(jsonl_path.exists(), "Expected per-file JSONL manifest to be created")
        self.assertTrue(
            media_jsonl_path.exists(),
            "Expected curated media JSONL manifest to be created",
        )

        data = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(data["dataset"], "toyset")
        self.assertEqual(data["summary"]["file_count"], 2)

        files = {entry["local_path"]: entry for entry in data["files"]}
        self.assertIn("images/frame_0001.jpg", files)
        self.assertEqual(
            files["images/frame_0001.jpg"]["r2_key"], "toyset/images/frame_0001.jpg"
        )
        self.assertEqual(files["images/frame_0001.jpg"]["sha256"], "pending")

    def test_hash_option_populates_sha256(self):
        self._create_sample_dataset("toyset", include_label=False)

        exit_code = build_manifests.main(["--datasets", "toyset", "--hash", "sha256"])
        self.assertEqual(exit_code, 0)

        json_path = self.manifests_dir / "toyset.json"
        data = json.loads(json_path.read_text(encoding="utf-8"))
        files = data["files"]
        self.assertTrue(files, "Expected files array")
        for file_info in files:
            self.assertTrue(file_info["sha256"] and file_info["sha256"] != "pending")

    def test_media_manifest_pairs_images_and_labels(self):
        self._create_sample_dataset("toyset")

        build_manifests.main(["--datasets", "toyset"])

        media_path = self.manifests_dir / "toyset.media.jsonl"
        records = [json.loads(line) for line in media_path.read_text(encoding="utf-8").splitlines()]
        image_record = next(rec for rec in records if rec["kind"] == "image")
        label_record = next(rec for rec in records if rec["kind"] == "annotation")
        self.assertTrue(image_record["paired_with"].endswith("labels/frame_0001.txt"))
        self.assertTrue(label_record["paired_with"].endswith("images/frame_0001.jpg"))


if __name__ == "__main__":
    unittest.main()
