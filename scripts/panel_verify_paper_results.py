"""Compatibility wrapper for the panel review checker."""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT_TEXT = str(REPO_ROOT)
if REPO_ROOT_TEXT not in sys.path:
    sys.path.insert(0, REPO_ROOT_TEXT)

from src.services.panel_review_check import main


if __name__ == "__main__":
    raise SystemExit(main())
