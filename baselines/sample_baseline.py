"""Utility helpers for the sample starter dataset.

These functions extract the tiny sample dataset packaged with the repository and
provide a lightweight iterator that lines up YOLO image/label pairs. Participants can
use them as a jumping-off point for experimentation without touching the large R2-hosted
corpus.
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Iterable, Iterator, Tuple

DEFAULT_SAMPLE_ZIP = Path("data/reference/sample_starter/sample-starter.zip")
DEFAULT_OUTPUT_ROOT = Path("datasets")
DATASET_NAME = "sample-starter"


def prepare_sample_dataset(
    zip_path: Path | None = None, output_root: Path | None = None
) -> Path:
    """Extract the bundled sample dataset into ``output_root/sample-starter``.

    Parameters
    ----------
    zip_path: Path to the sample dataset archive. Defaults to the bundled zip.
    output_root: Root directory where datasets live. Defaults to ``datasets/``.

    Returns
    -------
    Path to the extracted dataset directory.
    """

    archive = Path(zip_path) if zip_path is not None else DEFAULT_SAMPLE_ZIP
    if not archive.exists():
        raise FileNotFoundError(f"Sample archive missing: {archive}")

    root = Path(output_root) if output_root is not None else DEFAULT_OUTPUT_ROOT
    dataset_dir = root / DATASET_NAME
    dataset_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(archive, "r") as zf:
        zf.extractall(dataset_dir)

    return dataset_dir


def iterate_yolo_pairs(dataset_dir: Path) -> Iterator[Tuple[Path, Path]]:
    """Yield (image_path, label_path) tuples for YOLO-style datasets."""

    dataset_dir = Path(dataset_dir)
    images_dir = dataset_dir / "images"
    labels_dir = dataset_dir / "labels"

    if not images_dir.is_dir() or not labels_dir.is_dir():
        return iter(())

    image_by_stem = {path.stem: path for path in images_dir.iterdir() if path.is_file()}

    def gen() -> Iterable[Tuple[Path, Path]]:
        for label_path in sorted(labels_dir.glob("*.txt")):
            image_path = image_by_stem.get(label_path.stem)
            if image_path is not None:
                yield image_path, label_path

    return iter(gen())


__all__ = ["prepare_sample_dataset", "iterate_yolo_pairs", "DATASET_NAME"]
