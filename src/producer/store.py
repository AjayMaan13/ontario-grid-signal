import os
from pathlib import Path

from producer.checkpoint import Checkpoint


def _blob(uri):
    from google.cloud import storage  # imported here so local runs and tests do not need it

    bucket, name = uri[len("gs://"):].split("/", 1)
    return storage.Client().bucket(bucket).blob(name)


def load(uri: str) -> Checkpoint:
    """uri is gs://bucket/path (production) or a local file path (tests)."""
    if uri.startswith("gs://"):
        blob = _blob(uri)
        return Checkpoint.from_json(blob.download_as_text()) if blob.exists() else Checkpoint()
    path = Path(uri)
    return Checkpoint.from_json(path.read_text()) if path.exists() else Checkpoint()


def save(uri: str, checkpoint: Checkpoint):
    if uri.startswith("gs://"):
        _blob(uri).upload_from_string(checkpoint.to_json())
        return
    temp = uri + ".tmp"
    Path(temp).write_text(checkpoint.to_json())
    os.replace(temp, uri)  # a crash mid-write can never leave a half-written checkpoint
