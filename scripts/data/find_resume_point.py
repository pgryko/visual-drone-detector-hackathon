#!/usr/bin/env python3
"""Find the resume point for an interrupted download."""

import argparse
import json
import sys
from pathlib import Path


def find_resume_point(manifest_path: Path, output_root: Path) -> int:
    """Find the index of the first missing file in the manifest."""
    
    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    
    files = manifest.get("files", [])
    manifest_dataset = manifest.get("dataset") or manifest.get("bundle")
    
    for i, file_info in enumerate(files):
        local_path = file_info.get("local_path")
        rel_path = Path(local_path) if local_path else Path(file_info["r2_key"])
        
        dataset_name = file_info.get("dataset") or manifest_dataset
        if dataset_name:
            dataset_prefix = Path(dataset_name)
            if not rel_path.parts or rel_path.parts[0] != dataset_name:
                rel_path = dataset_prefix / rel_path
        
        target_path = output_root / rel_path
        
        if not target_path.exists():
            return i
    
    return len(files)  # All files exist


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", required=True, help="Path to public manifest JSON"
    )
    parser.add_argument(
        "--output-dir",
        default="datasets",
        help="Directory where files are being downloaded (default: datasets)",
    )
    
    args = parser.parse_args()
    
    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}", file=sys.stderr)
        return 1
    
    output_root = Path(args.output_dir)
    
    resume_point = find_resume_point(manifest_path, output_root)
    
    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    total_files = len(manifest.get("files", []))
    
    if resume_point >= total_files:
        print(f"All {total_files} files are already downloaded!")
        return 0
    
    print(f"Resume point: {resume_point}")
    print(f"Progress: {resume_point}/{total_files} files already downloaded ({resume_point/total_files*100:.1f}%)")
    print(f"Remaining: {total_files - resume_point} files")
    print()
    print("To resume download, use:")
    print(f"python scripts/data/download_public_dataset.py \\")
    print(f"  --manifest {args.manifest} \\")
    print(f"  --output-dir {args.output_dir} \\")
    print(f"  --fast-resume \\")
    print(f"  --start-from {resume_point}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
