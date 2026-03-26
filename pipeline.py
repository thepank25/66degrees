"""
Generic data pipeline.

Combines any BaseParser with any destination writer to download
a dataset's files to local disk, GCS, or S3.
"""

from __future__ import annotations

import logging
from pathlib import Path

from parsers import BaseParser

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Destination writers
# ---------------------------------------------------------------------------

class LocalWriter:
    def __init__(self, dest_config: dict) -> None:
        self.base = Path(dest_config.get("local_dir", "downloads"))
        self.base.mkdir(parents=True, exist_ok=True)

    def write(self, name: str, filename: str, data: bytes) -> str:
        dest = self.base / name / filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return str(dest)


class GCSWriter:
    def __init__(self, dest_config: dict) -> None:
        try:
            from google.cloud import storage  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "google-cloud-storage is required for GCS. "
                "Install with: pip install 66degrees[gcs]"
            ) from exc
        self._client = storage.Client()
        self._bucket = self._client.bucket(dest_config["gcs_bucket"])
        self._prefix = dest_config.get("gcs_prefix", "").rstrip("/")

    def write(self, name: str, filename: str, data: bytes) -> str:
        parts = [p for p in [self._prefix, name, filename] if p]
        blob_name = "/".join(parts)
        self._bucket.blob(blob_name).upload_from_string(data)
        return f"gs://{self._bucket.name}/{blob_name}"


class S3Writer:
    def __init__(self, dest_config: dict) -> None:
        try:
            import boto3  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for S3. "
                "Install with: pip install 66degrees[s3]"
            ) from exc
        self._s3 = boto3.client("s3")
        self._bucket = dest_config["s3_bucket"]
        self._prefix = dest_config.get("s3_prefix", "").rstrip("/")

    def write(self, name: str, filename: str, data: bytes) -> str:
        parts = [p for p in [self._prefix, name, filename] if p]
        key = "/".join(parts)
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=data)
        return f"s3://{self._bucket}/{key}"


def make_writer(dest_config: dict) -> LocalWriter | GCSWriter | S3Writer:
    dest_type = dest_config.get("type", "local")
    match dest_type:
        case "local":
            return LocalWriter(dest_config)
        case "gcs":
            return GCSWriter(dest_config)
        case "s3":
            return S3Writer(dest_config)
        case _:
            raise ValueError(f"Unknown destination type: {dest_type!r}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class DataPipeline:
    """
    Downloads every file exposed by ``parser`` and writes it via ``writer``.

    Parameters
    ----------
    parser:
        Any :class:`parsers.BaseParser` subclass.
    writer:
        A ``LocalWriter``, ``GCSWriter``, or ``S3Writer`` instance.
    """

    def __init__(self, parser: BaseParser, writer: LocalWriter | GCSWriter | S3Writer) -> None:
        self.parser = parser
        self.writer = writer

    def run(self) -> list[str]:
        """
        List all files, download each one, write to destination.

        Returns a list of destination paths / URIs.
        """
        files = self.parser.list_files()
        if not files:
            logger.warning("[%s] No files found — skipping.", self.parser.name)
            return []

        destinations: list[str] = []
        total = len(files)
        for i, file_meta in enumerate(files, start=1):
            filename = file_meta["name"]
            size_mb = file_meta.get("totalBytes", 0) / 1e6
            logger.info(
                "[%s] (%d/%d) Downloading %s (%.1f MB)…",
                self.parser.name, i, total, filename, size_mb,
            )
            data = self.parser.download_file(filename)
            dest = self.writer.write(self.parser.name, filename, data)
            destinations.append(dest)
            logger.info("[%s]   -> %s", self.parser.name, dest)

        logger.info("[%s] Done — %d file(s) saved.", self.parser.name, len(destinations))
        return destinations
