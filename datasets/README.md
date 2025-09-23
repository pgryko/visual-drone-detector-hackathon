# Dataset Management

Everything in `datasets/` is metadata. Real files live in Cloudflare R2.

## Maintainer Checklist

```bash
uv sync
cp .env.example .env   # add your R2 keys

# Build manifests (hash all files)
python scripts/build_manifests.py --hash sha256

# Upload every dataset mentioned in manifests
python scripts/data/r2_manager.py upload --all

# Create presigned URLs for each dataset and one big bundle
python scripts/data/generate_presigned_urls.py --all --bundle all-datasets --expires-in 86400
```

- Per-dataset manifests land in `datasets/manifests/<dataset>.json`.
- Presigned files are written to `datasets/manifests/presigned/`.
- `all-datasets.public.json` is the one you hand to participants.

## Participant Download

```bash
python scripts/data/download_public_dataset.py \
  --manifest /path/to/all-datasets.public.json \
  --output-dir datasets
```

Need a quick smoke test? Use the tiny archive committed to the repo:

- Archive: `data/reference/sample_starter/sample-starter.zip`
- Public manifest: `datasets/manifests/sample-starter.public.json`

## R2 Setup (Once)

1. Create a bucket in Cloudflare R2.
2. Generate an API token with object read & write access.
3. Drop the endpoint URL, access key ID, and secret into `.env`.

That’s the whole loop: build manifests → upload → presign → share.
