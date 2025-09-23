# Dataset Management

This directory contains dataset manifests and documentation. **Actual dataset files are stored in Cloudflare R2** to keep the git repository lightweight.

## 🏗️ Architecture

```
datasets/
├── manifests/          # Dataset metadata (tracked in git)
│   └── *.json         # Manifest files with R2 paths and checksums
├── */                 # Dataset directories (contents in .gitignore)
│   └── *.zip         # Downloaded from R2, not tracked in git
└── README.md         # This file
```

## 🚀 Quick Start

### 1. Setup Environment
```bash
# Install dependencies and create .env from the template
uv sync
cp .env.example .env

# Edit .env with your Cloudflare R2 credentials (maintainers only)
nano .env
```

### 2. Download Datasets
```bash
# Participants: download using a public manifest shared by organisers
python scripts/data/download_public_dataset.py   --manifest path/to/visdrone.public.json   --output-dir datasets

# Maintainers: download directly from R2 using credentials
python scripts/data/r2_manager.py download --dataset visdrone
```

### 3. Upload Datasets (Maintainers)
```bash
# Upload a specific dataset to R2 (updates manifest with checksums)
python scripts/data/r2_manager.py upload --dataset visdrone
```

## 📊 Available Datasets

| Dataset | Size | Images | Status | Manifest |
|---------|------|--------|--------|----------|
| Sample Starter | 445B | 1 | ✅ Included | `manifests/sample-starter.json` |
| VisDrone | 17GB | 10,209 + 261K frames | ✅ Downloaded | `manifests/visdrone.json` |
| UAVDT | 6.79GB | 77,819 | ✅ Downloaded | `manifests/uavdt.json` |
| Anti-UAV410 | 6.4GB | 410 sequences | ✅ Downloaded | Create manifest |
| SeaDronesSee | 413MB | 14,227 | ✅ Downloaded | Create manifest |
| YOLO Drone | 360MB | 1,359 | ✅ Downloaded | Create manifest |
| DUT Anti-UAV | 13MB | 10,000 | ✅ Downloaded | Create manifest |
| Pawełczyk & Wojtyra | 1.3MB | 56,821 | ✅ Downloaded | Create manifest |

## 🔐 Cloudflare R2 Setup

### 1. Create R2 Bucket
1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Navigate to R2 > Create Bucket
3. Name: `drone-datasets-hackathon`
4. Region: Automatic

### 2. Generate API Credentials
1. R2 > Manage API tokens
2. Create token with:
   - Permissions: Object Read & Write
   - Specify bucket: `drone-datasets-hackathon`
3. Copy credentials to `.env`

### 3. Configure Public Access (Optional)
1. R2 > Settings > Public Access
2. Add custom domain: `datasets.yourdomain.com`
3. Update `R2_PUBLIC_URL` in `.env`

## 📦 Dataset Operations

### List Available Datasets
```bash
python scripts/data/r2_manager.py list
```

### Download Dataset
```bash
# Download with validation
python scripts/data/r2_manager.py download --dataset visdrone

# Skip validation (faster)
python scripts/data/r2_manager.py download --dataset visdrone --no-validate
```

### Upload Dataset
```bash
# Upload and update checksums
python scripts/data/r2_manager.py upload --dataset visdrone
```

### Publish Public Manifest (Maintainers)
```bash
# Generate presigned URLs valid for 24 hours and write to a shareable JSON file
python scripts/data/generate_presigned_urls.py   --dataset visdrone   --expires-in 86400   --output datasets/manifests/visdrone.public.json
```

Share the resulting `*.public.json` with participants. They can download using `scripts/data/download_public_dataset.py`.

A sample public manifest lives at `datasets/manifests/sample-starter.public.json`. Update
its `presigned_url` field with your hosting location before sharing.

## 📋 Manifest Format

Each dataset has a JSON manifest in `datasets/manifests/` with:

```json
{
  "dataset": "dataset_name",
  "generated_at": "2025-01-01T00:00:00+00:00",
  "summary": {
    "file_count": 1,
    "total_bytes": 1234567890
  },
  "files": [
    {
      "local_path": "relative/path/to/file.zip",
      "r2_key": "dataset_name/file.zip",
      "size_bytes": 1234567890,
      "md5": "pending",
      "sha256": "pending"
    }
  ]
}
```

## 🔄 Workflow

### For Contributors
1. Clone the repository.
2. If you are a maintainer, configure R2 credentials in `.env`. Participants can skip this step.
3. Download datasets using either a public manifest (`download_public_dataset.py`) or the R2 manager if you have credentials.
4. Work on code — datasets stay out of git.
5. Commit code changes only.

### For CI/CD
```yaml
- name: Download datasets
  env:
    R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
    R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
  run: python scripts/data/r2_manager.py download --dataset visdrone
```

## 🧹 Cleanup

```bash
# Remove downloaded datasets (keeps manifests)
rm -rf datasets/<dataset_name>

# Check what's tracked in git
git status
```

## ⚠️ Important Notes

1. **Never commit dataset files** - They're excluded via `.gitignore`
2. **Always use manifests** - Ensures reproducible downloads
3. **Update checksums** - When uploading new versions
4. **Version datasets** - Use `v1.0`, `v1.1` etc. in R2 paths

## 🛠️ Troubleshooting

### Connection Issues
```bash
# Check R2 credentials by listing manifests
python scripts/data/r2_manager.py list

# Attempt a lightweight download without validation
python scripts/data/r2_manager.py download --dataset visdrone --no-validate
```

### Checksum Mismatch
- Re-download with `--no-validate`
- Or update manifest with correct checksums

### Missing Credentials
- Copy `.env.example` to `.env`
- Add R2 credentials from Cloudflare dashboard

## 📚 Documentation

- `scripts/build_manifests.py` — generate inventories and canonical manifests
- `scripts/data/r2_manager.py` — upload/download datasets with Cloudflare R2
- `scripts/data/generate_presigned_urls.py` — publish presigned download manifests
- `scripts/data/download_public_dataset.py` — participant downloader for public manifests
- [Cloudflare R2 Docs](https://developers.cloudflare.com/r2/) — official R2 documentation

## ✅ TODO: Manifest Heuristics Enhancements

The curated manifests generated by `scripts/build_manifests.py` can be enriched further. Planned, but not yet implemented:

- COCO pairing: parse `annotations/*.json`, pair to images, include `ann_count`, `categories`, `has_segm`. Flag: `--parse-coco`.
- Pascal/VOC pairing: parse `Annotations/*.xml`, pair to images, include `ann_count`, `categories`. Flag: `--parse-voc`.
- YOLO data.yaml: read `data.yaml` to resolve train/val/test splits and class names; enrich media-index. Flag: `--parse-yolo-config`.
- Tracking/MOT: detect sequences (e.g., `*/gt/gt.txt`), emit sequence-level records and optional frame-level pairs. Flags: `--parse-mot`, `--include-frames`.
- Calibration/config: tag calibration YAML/JSON/TXT and link to sequences when possible. Flag: `--include-calibration`.
- Media probing: optionally read image dims and video fps/duration for `width`, `height`, `fps`, `duration`. Flags: `--probe-images`, `--probe-videos`.

These will be gated behind CLI flags to avoid heavy I/O by default.
