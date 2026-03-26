"""
Source-system parser classes.

Each parser is responsible for:
  - Understanding its own URL scheme
  - Listing available files (and reporting the total count)
  - Downloading a single file by name

Adding a new source system:
  1. Subclass BaseParser and implement list_files() + download_file()
  2. Register it in PARSER_REGISTRY under its config "type" string
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class BaseParser(ABC):
    """
    Contract every source-system parser must fulfil.

    Parameters
    ----------
    source_system:
        The dict for this entry from config["source_systems"][type][n].
        Must contain at least ``"name"`` and ``"url"``.
    credentials:
        The dict from config["credentials"][type] for this parser's type,
        or an empty dict if no credentials are needed.
    """

    def __init__(self, source_system: dict, credentials: dict) -> None:
        self.name: str = source_system["name"]
        self.url: str = source_system["url"]
        self.credentials = credentials

    @abstractmethod
    def list_files(self) -> list[dict]:
        """
        Return metadata for every file in this dataset.

        Each dict must contain at least:
          - ``"name"``  (str)  — filename used when writing to the destination
          - ``"totalBytes"``  (int)  — 0 if unknown
        """

    @abstractmethod
    def download_file(self, filename: str) -> bytes:
        """Return the raw bytes for a single file."""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, url={self.url!r})"


# ---------------------------------------------------------------------------
# Kaggle
# ---------------------------------------------------------------------------

class KaggleParser(BaseParser):
    """
    Parses and downloads Kaggle datasets via the kaggle Python SDK.

    Required credentials keys: ``username``, ``key``.
    """

    def __init__(self, source_system: dict, credentials: dict) -> None:
        super().__init__(source_system, credentials)
        self._owner, self._dataset = self._parse_url(self.url)
        # Expose credentials to the SDK (picks up KAGGLE_USERNAME / KAGGLE_KEY)
        username = credentials.get("username", "")
        key = credentials.get("key", "")
        if not username or not key:
            raise RuntimeError(
                "Kaggle credentials missing. Set KAGGLE_USERNAME and KAGGLE_KEY "
                "environment variables, or add them to config.json credentials."
            )
        os.environ["KAGGLE_USERNAME"] = username
        os.environ["KAGGLE_KEY"] = key
        # Import after setting env vars — kaggle/__init__.py calls authenticate()
        # at import time, so credentials must be in the environment first.
        from kaggle.api.kaggle_api_extended import KaggleApi
        self._api = KaggleApi()
        self._api.authenticate()

    # ------------------------------------------------------------------
    # BaseParser implementation
    # ------------------------------------------------------------------

    def list_files(self) -> list[dict]:
        result = self._api.dataset_list_files(f"{self._owner}/{self._dataset}")
        files = result.files if result else []
        logger.info(
            "[%s] %s/%s — %d file(s) found",
            self.name,
            self._owner,
            self._dataset,
            len(files),
        )
        return [{"name": f.name, "totalBytes": f.total_bytes or 0} for f in files]

    def download_file(self, filename: str) -> bytes:
        with tempfile.TemporaryDirectory() as tmp:
            self._api.dataset_download_file(
                f"{self._owner}/{self._dataset}",
                filename,
                path=tmp,
                quiet=True,
            )
            return (Path(tmp) / filename).read_bytes()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_url(url: str) -> tuple[str, str]:
        url = url.split("?")[0].split("#")[0].rstrip("/")
        pattern = r"(?:https?://(?:www\.)?kaggle\.com/datasets/)?([^/]+)/([^/]+)$"
        m = re.match(pattern, url)
        if not m:
            raise ValueError(
                f"Cannot parse Kaggle URL: {url!r}\n"
                "Expected: https://www.kaggle.com/datasets/owner/dataset"
            )
        return m.group(1), m.group(2)


# ---------------------------------------------------------------------------
# Registry  —  maps config "type" string → parser class
# ---------------------------------------------------------------------------

PARSER_REGISTRY: dict[str, type[BaseParser]] = {
    "kaggle": KaggleParser,
}


def get_parser(source_system: dict, credentials: dict) -> BaseParser:
    """
    Instantiate the correct parser for a source_system config entry.

    Parameters
    ----------
    source_system:
        One entry from config["source_systems"][type], e.g.
        ``{"name": "supermarket_sales", "url": "https://..."}``
    credentials:
        The credentials block for this parser's type, e.g.
        ``{"username": "alice", "key": "abc123"}``

    Raises
    ------
    KeyError
        If the source system type has no registered parser.
    """
    sys_type: str = source_system["type"]
    if sys_type not in PARSER_REGISTRY:
        raise KeyError(
            f"No parser registered for source system type {sys_type!r}. "
            f"Available: {sorted(PARSER_REGISTRY)}"
        )
    return PARSER_REGISTRY[sys_type](source_system, credentials)
