"""Utility helpers for the sample starter dataset.

These functions extract the tiny sample dataset packaged with the repository and
provide a lightweight iterator that lines up YOLO image/label pairs. Participants can
use them as a jumping-off point for experimentation without touching the large R2-hosted
corpus.
"""

from __future__ import annotations

import base64
import zipfile
from pathlib import Path
from typing import Iterable, Iterator, Tuple

DEFAULT_SAMPLE_ZIP = Path("data/reference/sample_starter/sample-starter.zip")
DEFAULT_OUTPUT_ROOT = Path("datasets")
DATASET_NAME = "sample-starter"

_FALLBACK_SAMPLE_FILES = {
    "images/sample_0001.png": base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/w8AAuwB9arY2bkAAAAASUVORK5CYII="
    ),
    "labels/sample_0001.txt": b"0 0.5 0.5 1.0 1.0\n",
    "classes.txt": b"drone\n",
}


def _is_default_archive(candidate: Path) -> bool:
    """Return True if ``candidate`` points at the bundled sample archive."""

    try:
        return candidate.resolve(strict=False) == DEFAULT_SAMPLE_ZIP.resolve(
            strict=False
        )
    except RuntimeError:
        # ``resolve`` can raise on deeply nested relative paths; fall back to equality.
        return candidate == DEFAULT_SAMPLE_ZIP


def _write_fallback_dataset(dataset_dir: Path) -> None:
    """Materialise the tiny starter dataset without relying on the zip archive."""

    for relative_path, payload in _FALLBACK_SAMPLE_FILES.items():
        destination = dataset_dir / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)


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

    root = Path(output_root) if output_root is not None else DEFAULT_OUTPUT_ROOT
    dataset_dir = root / DATASET_NAME
    dataset_dir.mkdir(parents=True, exist_ok=True)

    archive = Path(zip_path) if zip_path is not None else DEFAULT_SAMPLE_ZIP

    if archive.exists():
        with zipfile.ZipFile(archive, "r") as zf:
            zf.extractall(dataset_dir)
        return dataset_dir

    if zip_path is None or _is_default_archive(archive):
        _write_fallback_dataset(dataset_dir)
        return dataset_dir

    raise FileNotFoundError(f"Sample archive missing: {archive}")


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
