# Visual Drone Detector Hackathon Starter

This repository provides a batteries-included starting point for the drone detection hackathon. It focuses on making dataset onboarding and baseline experimentation fast while keeping the repo itself lightweight.

## Quick Start

```bash
# 1. Create a virtual environment (uses uv by default)
uv sync

# 2. Activate the environment
source .venv/bin/activate

# 3. Run tests to confirm everything is wired up
python -m unittest discover -s tests
```

### Download Public Data Drops

Hackathon participants receive a public manifest with short-lived presigned URLs. Use the downloader script to fetch the files:

```bash
# Download into datasets/<dataset_name>
python scripts/data/download_public_dataset.py \
  --manifest path/to/<dataset>.public.json \
  --output-dir datasets
```

Pass `--no-verify` to skip checksum validation if you have slow storage.

### Try the Sample Starter Dataset

A miniature YOLO-style dataset lives in `data/reference/sample_starter/sample-starter.zip`.
You can exercise the download workflow without Cloudflare credentials using the sample
public manifest:

```bash
python scripts/data/download_public_dataset.py \
  --manifest datasets/manifests/sample-starter.public.json \
  --output-dir datasets
```

Update the `presigned_url` in the manifest to point at your own hosting location
(e.g., GitHub release asset or presigned R2 link) before sharing widely.

### Kick-start an Experiment

Use the helper in `baselines/sample_baseline.py` to unpack and iterate over the sample dataset:

```bash
python - <<'PY'
from baselines.sample_baseline import prepare_sample_dataset, iterate_yolo_pairs

folder = prepare_sample_dataset()
print(f"Extracted to: {folder}")
for image_path, label_path in iterate_yolo_pairs(folder):
    print(image_path.name, label_path.read_text().strip())
PY
```

Swap in your own download manifest and detection code to turn this into a full baseline.
## Dataset Operations (Maintainers)

Maintainers work with the canonical manifests tracked in git under `datasets/manifests/`.

### Generate Inventory Manifests

```bash
# Rebuild manifests for all tracked datasets (hashing optional)
python scripts/build_manifests.py
python scripts/build_manifests.py --hash sha256  # includes sha256 hashes
```

Each run creates:

- `datasets/manifests/<dataset>.jsonl` – per-file inventory
- `datasets/manifests/<dataset>.media.jsonl` – curated media/annotation pairs
- `datasets/manifests/<dataset>.json` – canonical manifest consumed by tooling

### Upload / Download via Cloudflare R2

Set credentials in `.env` (see `.env.example`), then use the R2 manager:

```bash
# Upload a dataset (computes and saves checksums before upload)
python scripts/data/r2_manager.py upload --dataset visdrone

# Download a dataset with checksum validation
python scripts/data/r2_manager.py download --dataset visdrone
```

### Publish Presigned Manifests

Maintainers can publish time-bound download manifests for participants without granting credentials:

```bash
python scripts/data/generate_presigned_urls.py \
  --dataset visdrone \
  --expires-in 86400 \
  --output datasets/manifests/visdrone.public.json
```

The generated file is safe to share publicly. Combine it with the participant downloader to provide a turnkey data drop.

## Testing

Tests use the standard library test runner:

```bash
python -m unittest discover -s tests
```

The coverage focuses on dataset tooling (manifest generation, presign flow, downloader). Extend the suite alongside new features using a test-first workflow.

## Repository Layout

```
.
├── data/                     # Local scratch space (ignored)
├── datasets/                 # Manifests tracked in git (data files stay in R2)
├── scripts/                  # Tooling for manifests and data distribution
├── tests/                    # Unit tests for tooling
├── README.md                 # This file
└── pyproject.toml            # Project metadata & dependencies
```

## Next Steps

- Add baseline notebooks and evaluation pipelines for hackathon scoring.
- Flesh out contribution guidelines, issue templates, and hackathon brief.
- Integrate CI to keep manifests and tooling healthy over time.
