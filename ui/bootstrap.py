"""Bootstrap helpers for Streamlit script execution and optional branding."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


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


def discover_branding_assets(
    current_file: str | Path | None = None,
    *,
    asset_dir: str | Path | None = None,
) -> dict[str, Any]:
    repo_root = discover_repo_root(current_file)
    assets_root = Path(asset_dir).resolve() if asset_dir else repo_root / "ui" / "assets"

    def first_existing(*filenames: str) -> Path | None:
        for filename in filenames:
            candidate = assets_root / filename
            if candidate.exists():
                return candidate.resolve()
        return None

    logo_path = first_existing("logo.svg", "logo.png")
    icon_path = first_existing("logo_icon.png", "logo_icon.svg", "logo.png")
    page_icon_path = None
    if icon_path and icon_path.suffix.lower() in {".png", ".jpg", ".jpeg"}:
        page_icon_path = icon_path
    elif logo_path and logo_path.suffix.lower() in {".png", ".jpg", ".jpeg"}:
        page_icon_path = logo_path

    return {
        "assets_root": assets_root.resolve(),
        "logo_path": logo_path,
        "icon_path": icon_path,
        "page_icon_path": page_icon_path,
        "has_logo": bool(logo_path),
        "has_icon": bool(icon_path),
    }
