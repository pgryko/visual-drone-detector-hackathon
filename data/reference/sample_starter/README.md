# Sample Starter Dataset

A tiny YOLO-style dataset packaged as `sample-starter.zip` to let participants test the
`download_public_dataset.py` workflow without large downloads.

Contents of the archive:

```
images/sample_0001.png  # 1x1 placeholder image
labels/sample_0001.txt  # YOLO bounding box covering the whole image
classes.txt              # Class list (single "drone" class)
```

This archive is intentionally small (a few hundred bytes) so it can live in the repo while
the real datasets remain in Cloudflare R2.
