from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

# Sidecar written next to the csv.gz files: records each gz member's byte offset
# and row count so a loader can read members in parallel.
DATA_MANIFEST_FILENAME = "_data_manifest.json"
INPUT_SIGNATURE_FILENAME = "_input_signature.json"


def output_signature_matches(
    output_dir: Path, signature: dict[str, Any], required_paths: Iterable[Path]
) -> bool:
    """True when output_dir already holds the product of exactly this input."""
    signature_path = output_dir / INPUT_SIGNATURE_FILENAME
    if not signature_path.exists():
        return False
    try:
        stored = json.loads(signature_path.read_text())
    except (OSError, ValueError):
        return False
    return stored == signature and all(path.exists() for path in required_paths)


def write_json_file(path: Path, payload: Any, *, sort_keys: bool = True, indent: int = 2) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=indent, sort_keys=sort_keys) + "\n", encoding="utf-8"
    )
    return path
