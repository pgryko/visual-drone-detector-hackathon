#!/usr/bin/env python3
"""
Generate per-dataset manifests for files under `datasets/`.

Outputs:
- datasets/manifests/<dataset>.jsonl         (one JSON object per file)
- datasets/manifests/<dataset>.media.jsonl   (curated media/labels with pairing)
- datasets/manifests/index.json              (summary across datasets)
- datasets/manifests/media-index.json        (summary for curated media)

Each JSONL record contains:
- path: relative path from repo root (posix style)
- dataset: top-level dataset directory name
- rel_path: path relative to the dataset directory
- size_bytes: file size in bytes
- mtime: modification time (unix epoch seconds)
- ext: lowercased file extension (without dot) or ""

Hashing can be enabled via --hash sha256, but is off by default to keep it fast.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Set


REPO_ROOT = Path(__file__).resolve().parents[1]
DATASETS_DIR = REPO_ROOT / "datasets"
MANIFESTS_DIR = DATASETS_DIR / "manifests"

EXCLUDE_DIR_NAMES: Set[str] = {".git", ".idea", "node_modules", "__pycache__"}


@dataclass
class FileRecord:
    path: str
    dataset: str
    rel_path: str
    size_bytes: int
    mtime: float
    ext: str
    sha256: Optional[str] = None


@dataclass
class MediaRecord:
    path: str
    dataset: str
    rel_path: str
    kind: str  # image | video | annotation | other
    group_id: Optional[str]
    split: Optional[str]  # train | val | test | None
    paired_with: Optional[str] = None


def iter_files(root: Path) -> Iterator[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        # prune excluded dirs in-place
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIR_NAMES]
        for fn in filenames:
            yield Path(dirpath) / fn


def sha256_file(path: Path, bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(bufsize)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def build_manifest_for_dataset(dataset_dir: Path, do_hash: bool) -> List[FileRecord]:
    dataset_name = dataset_dir.name
    records: List[FileRecord] = []
    for fpath in iter_files(dataset_dir):
        try:
            st = fpath.stat()
        except FileNotFoundError:
            # Skip transient files
            continue
        rel_to_dataset = fpath.relative_to(dataset_dir).as_posix()
        rel_to_repo = fpath.relative_to(REPO_ROOT).as_posix()
        ext = fpath.suffix.lower()[1:] if fpath.suffix else ""
        rec = FileRecord(
            path=rel_to_repo,
            dataset=device_safe(dataset_name),
            rel_path=rel_to_dataset,
            size_bytes=st.st_size,
            mtime=st.st_mtime,
            ext=ext,
            sha256=sha256_file(fpath) if do_hash else None,
        )
        records.append(rec)
    return records


def build_dataset_manifest_payload(
    dataset_name: str, records: List[FileRecord], hashed: bool
) -> dict:
    files = []
    total_bytes = 0
    for rec in sorted(records, key=lambda r: r.rel_path):
        total_bytes += rec.size_bytes
        files.append(
            {
                "local_path": rec.rel_path,
                "r2_key": f"{dataset_name}/{rec.rel_path}",
                "size_bytes": rec.size_bytes,
                "sha256": rec.sha256 if rec.sha256 else "pending",
                "ext": rec.ext,
                "mtime": rec.mtime,
            }
        )

    generated_at = datetime.now(timezone.utc).isoformat()

    manifest = {
        "dataset": dataset_name,
        "root": f"datasets/{dataset_name}",
        "generated_at": generated_at,
        "hashed": hashed,
        "summary": {
            "file_count": len(records),
            "total_bytes": total_bytes,
        },
        "files": files,
    }

    return manifest


def write_dataset_manifest(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def device_safe(name: str) -> str:
    # Keep as-is but guard against weird whitespace
    return name.strip()


def write_jsonl(path: Path, records: Iterable[FileRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            obj = asdict(rec)
            # Drop None fields for compactness
            if obj.get("sha256") is None:
                obj.pop("sha256", None)
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def write_media_jsonl(path: Path, records: Iterable[MediaRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            obj = asdict(rec)
            for k in list(obj.keys()):
                if obj[k] is None:
                    obj.pop(k)
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# ---------- Curated media helpers ----------

IMAGE_EXTS = {"jpg", "jpeg", "png", "bmp", "tif", "tiff", "webp"}
VIDEO_EXTS = {"mp4", "avi", "mov", "mkv", "m4v"}
YOLO_LABEL_EXT = "txt"


def infer_split_from_path(rel_path: str) -> Optional[str]:
    parts = [p.lower() for p in Path(rel_path).parts]
    for p in parts:
        if p == "train":
            return "train"
        if p in ("val", "valid", "validation"):
            return "val"
        if p == "test":
            return "test"
    return None


def yolo_key_from_rel(rel_path: str) -> Optional[str]:
    parts = Path(rel_path).parts
    # Find the first occurrence of 'images' or 'labels' and make a key from the tail
    try:
        idx = parts.index("images")
    except ValueError:
        try:
            idx = parts.index("labels")
        except ValueError:
            return None
    tail = Path(*parts[idx + 1 :]).as_posix()
    # Drop extension
    if "/" in tail:
        stem = "/".join(tail.split("/")[:-1] + [Path(tail).stem])
    else:
        stem = Path(tail).stem
    return stem


def classify_kind(rel_path: str, ext: str) -> str:
    lp = rel_path.lower()
    if ext in IMAGE_EXTS:
        return "image"
    if ext in VIDEO_EXTS:
        return "video"
    if ext == YOLO_LABEL_EXT and ("/labels/" in lp or lp.startswith("labels/")):
        return "annotation"
    if ext == "json" and ("/annotations/" in lp or lp.startswith("annotations/") or Path(rel_path).name.startswith("instances_")):
        return "annotation"
    if ext == "xml" and ("/annotations/" in lp or "pascal" in lp or "voc" in lp or "/annotations" in lp):
        return "annotation"
    return "other"


def build_curated_media(records: List[FileRecord], dataset_name: str) -> List[MediaRecord]:
    # Index YOLO-style images and labels by a normalized key
    image_by_key: dict[str, FileRecord] = {}
    label_by_key: dict[str, FileRecord] = {}

    for r in records:
        k = yolo_key_from_rel(r.rel_path)
        if k is None:
            continue
        if r.ext in IMAGE_EXTS:
            image_by_key[k] = r
        elif r.ext == YOLO_LABEL_EXT:
            label_by_key[k] = r

    media: List[MediaRecord] = []
    for r in records:
        kind = classify_kind(r.rel_path, r.ext)
        if kind == "other":
            # Keep curated list focused; skip non-media
            continue

        split = infer_split_from_path(r.rel_path)
        group_id = None
        paired_with = None

        if kind in ("image", "annotation"):
            key = yolo_key_from_rel(r.rel_path)
            if key is not None:
                group_id = key
                if kind == "image" and key in label_by_key:
                    paired_with = label_by_key[key].path
                elif kind == "annotation" and key in image_by_key:
                    paired_with = image_by_key[key].path

        media.append(
            MediaRecord(
                path=r.path,
                dataset=dataset_name,
                rel_path=r.rel_path,
                kind=kind,
                group_id=group_id,
                split=split,
                paired_with=paired_with,
            )
        )

    return media


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--hash",
        choices=["sha256"],
        help="Compute and include file hash; increases runtime",
    )
    ap.add_argument(
        "--datasets",
        nargs="*",
        help="Specific top-level dataset directories to process; defaults to all under datasets/",
    )
    args = ap.parse_args(argv)

    if not DATASETS_DIR.is_dir():
        print(f"Missing datasets dir: {DATASETS_DIR}", file=sys.stderr)
        return 2

    to_process: List[Path] = []
    if args.datasets:
        for name in args.datasets:
            p = DATASETS_DIR / name
            if not p.is_dir():
                print(f"Skip non-dir dataset: {name}", file=sys.stderr)
                continue
            to_process.append(p)
    else:
        for child in DATASETS_DIR.iterdir():
            if not child.is_dir():
                continue
            if child.name in {"manifests"}:
                continue
            to_process.append(child)

    do_hash = args.hash == "sha256"

    summary = []
    media_summary = []
    for ds_dir in sorted(to_process, key=lambda p: p.name.lower()):
        records = build_manifest_for_dataset(ds_dir, do_hash=do_hash)
        manifest_path = MANIFESTS_DIR / f"{ds_dir.name}.jsonl"
        write_jsonl(manifest_path, records)

        total_bytes = sum(r.size_bytes for r in records)
        summary_entry = {
            "dataset": ds_dir.name,
            "root": ds_dir.relative_to(REPO_ROOT).as_posix(),
            "manifest": manifest_path.relative_to(REPO_ROOT).as_posix(),
            "file_count": len(records),
            "total_bytes": total_bytes,
            "hashed": do_hash,
        }
        summary.append(summary_entry)

        dataset_manifest = build_dataset_manifest_payload(
            ds_dir.name, records, hashed=do_hash
        )
        dataset_manifest_path = MANIFESTS_DIR / f"{ds_dir.name}.json"
        write_dataset_manifest(dataset_manifest_path, dataset_manifest)

        # Curated media/labels manifest
        media_records = build_curated_media(records, ds_dir.name)
        media_manifest_path = MANIFESTS_DIR / f"{ds_dir.name}.media.jsonl"
        write_media_jsonl(media_manifest_path, media_records)

        # Summaries by kind and split
        by_kind: dict[str, int] = {}
        by_split: dict[str, int] = {}
        pairs = 0
        for m in media_records:
            by_kind[m.kind] = by_kind.get(m.kind, 0) + 1
            if m.split:
                by_split[m.split] = by_split.get(m.split, 0) + 1
            if m.paired_with:
                pairs += 1
        media_summary.append(
            {
                "dataset": ds_dir.name,
                "manifest": media_manifest_path.relative_to(REPO_ROOT).as_posix(),
                "counts": {"total": len(media_records), **by_kind},
                "splits": by_split,
                "paired_records": pairs,
            }
        )

    # Write index
    MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
    with (MANIFESTS_DIR / "index.json").open("w", encoding="utf-8") as f:
        json.dump({"datasets": summary}, f, indent=2)

    with (MANIFESTS_DIR / "media-index.json").open("w", encoding="utf-8") as f:
        json.dump({"datasets": media_summary}, f, indent=2)

    print(
        f"Wrote {len(summary)} manifests and curated media to {MANIFESTS_DIR.relative_to(REPO_ROOT)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
