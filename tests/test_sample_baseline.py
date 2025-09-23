import shutil
import tempfile
from pathlib import Path
import unittest

import baselines.sample_baseline as sample_baseline


class SampleBaselineTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_root = Path(self.temp_dir.name)

    def tearDown(self):
        shutil.rmtree(self.output_root, ignore_errors=True)
        self.temp_dir.cleanup()

    def test_prepare_sample_dataset_extracts_files(self):
        dataset_dir = sample_baseline.prepare_sample_dataset(
            zip_path=Path("data/reference/sample_starter/sample-starter.zip"),
            output_root=self.output_root,
        )

        self.assertTrue((dataset_dir / "images/sample_0001.png").exists())
        self.assertTrue((dataset_dir / "labels/sample_0001.txt").exists())

    def test_iterate_annotations_pairs_images_and_labels(self):
        dataset_dir = sample_baseline.prepare_sample_dataset(
            zip_path=Path("data/reference/sample_starter/sample-starter.zip"),
            output_root=self.output_root,
        )

        pairs = list(sample_baseline.iterate_yolo_pairs(dataset_dir))
        self.assertEqual(len(pairs), 1)
        image_path, label_path = pairs[0]
        self.assertTrue(image_path.name.endswith("sample_0001.png"))
        self.assertTrue(label_path.name.endswith("sample_0001.txt"))


if __name__ == "__main__":
    unittest.main()
