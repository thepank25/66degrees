"""
Entry point.

Reads config.json, resolves ${ENV_VAR} placeholders in credentials,
then runs a DataPipeline for every source system defined in the config.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

from parsers import get_parser
from pipeline import DataPipeline, make_writer
from ingest import ingest_filesystem #, ingest_s3, ingest_gcs


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.json"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _resolve_env_vars(value: str) -> str:
    """Replace ${VAR_NAME} placeholders with the matching environment variable."""
    def replacer(match: re.Match) -> str:
        var = match.group(1)
        resolved = os.environ.get(var, "")
        if not resolved:
            logger.warning("Environment variable %r is not set.", var)
        return resolved

    return re.sub(r"\$\{([^}]+)\}", replacer, value)


def _resolve_credentials(credentials: dict) -> dict:
    """Walk the credentials dict and resolve all env var placeholders."""
    return {
        sys_type: {k: _resolve_env_vars(v) for k, v in creds.items()}
        for sys_type, creds in credentials.items()
    }


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open() as f:
        config = json.load(f)

    config["credentials"] = _resolve_credentials(config.get("credentials", {}))
    return config


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def extract_all(config: dict) -> dict[str, list[str]]:
    """
    Iterate every source system in the config and run a pipeline for each.

    Returns a mapping of source-system name → list of destination paths/URIs.
    """
    writer = make_writer(config["destination"])
    results: dict[str, list[str]] = {}

    source_systems: dict[str, list[dict]] = config.get("source_systems", {})

    for sys_type, entries in source_systems.items():
        credentials = config.get("credentials", {}).get(sys_type, {})

        for entry in entries:
            # Inject the type so get_parser can look it up in the registry
            source_system = {**entry, "type": sys_type}

            logger.info("=== Starting pipeline: %s (%s) ===", entry["name"], sys_type)
            parser = get_parser(source_system, credentials)
            pipeline = DataPipeline(parser=parser, writer=writer)
            results[entry["name"]] = pipeline.run()

    return results


def main() -> None:
    config = load_config()
    results = extract_all(config)
    ingest = ingest_filesystem("data.db", results)
    print("\n--- Pipeline summary ---")
    for name, paths in results.items():
        print(f"\n{name}: {len(paths)} file(s)")
        for p in paths:
            print(f"  {p}")


if __name__ == "__main__":
    main()
