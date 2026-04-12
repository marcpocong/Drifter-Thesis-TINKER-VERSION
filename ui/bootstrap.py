"""Bootstrap helpers for Streamlit script execution."""

from __future__ import annotations

import sys
from pathlib import Path


def discover_repo_root(current_file: str | Path | None = None) -> Path:
    start = Path(current_file).resolve() if current_file else Path(__file__).resolve()
    search_root = start if start.is_dir() else start.parent
    for candidate in (search_root, *search_root.parents):
        if (candidate / "ui").is_dir() and (candidate / "src").is_dir():
            return candidate
    return Path(__file__).resolve().parents[1]


def ensure_repo_root_on_path(current_file: str | Path | None = None) -> Path:
    repo_root = discover_repo_root(current_file)
    repo_root_text = str(repo_root)
    if repo_root_text not in sys.path:
        sys.path.insert(0, repo_root_text)
    return repo_root
