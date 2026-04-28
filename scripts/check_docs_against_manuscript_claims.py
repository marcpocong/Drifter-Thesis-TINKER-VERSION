from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
PANEL_QUICK_START = ROOT / "PANEL_QUICK_START.md"
PANEL_REVIEW_GUIDE = ROOT / "docs" / "PANEL_REVIEW_GUIDE.md"
PANEL_FILES = [README, PANEL_QUICK_START, PANEL_REVIEW_GUIDE]

TITLE = "# Drifter-Validated 24–72 h Oil-Spill Forecasting for Philippine Coasts: Probability Footprints and Oil-Type Fate"
README_REQUIRED_VALUES = [
    "cmems_gfs",
    "0.1075",
    "0.5568",
    "0.5389",
    "0.4966",
    "0.3612",
]
FORBIDDEN_PANEL_STRINGS = [
    "Draft 20",
    "Draft 18",
    "thesis-facing Phase 4",
    "thesis-facing Phase 5",
]
FORBIDDEN_POSITIVE_PATTERNS = [
    r"exact 1 km match",
    r"exact-grid success",
    r"PyGNOME\s+is\s+truth",
    r"PyGNOME\s+as\s+truth",
]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_markdown_section(text: str, heading: str) -> str:
    pattern = rf"(?ms)^## {re.escape(heading)}\n(.*?)(?=^## |\Z)"
    match = re.search(pattern, text)
    return match.group(1) if match else ""


def line_is_safe_negation(line: str) -> bool:
    lowered = line.lower()
    return any(token in lowered for token in ("not ", "do not", "must not", "never ", "isn't", "is not", "rather than"))


def main() -> int:
    issues: list[str] = []

    texts = {path: read_text(path) for path in PANEL_FILES}
    readme_text = texts[README]

    if TITLE not in readme_text:
        issues.append("README is missing the exact manuscript title.")

    for value in README_REQUIRED_VALUES:
        if value not in readme_text:
            issues.append(f"README is missing required manuscript value `{value}`.")

    for path, text in texts.items():
        for forbidden in FORBIDDEN_PANEL_STRINGS:
            if forbidden in text:
                issues.append(f"{path.relative_to(ROOT)} still contains forbidden wording `{forbidden}`.")

    evidence_summary = extract_markdown_section(readme_text, "Current Manuscript Alignment")
    if not evidence_summary:
        issues.append("README is missing the `Current Manuscript Alignment` section.")
    else:
        if "PyGNOME" in evidence_summary:
            comparator_ok = "comparator-only" in evidence_summary and (
                "never observational truth" in evidence_summary
                or "never the observational scoring reference" in evidence_summary
            )
            if not comparator_ok:
                issues.append(
                    "README main evidence summary mentions PyGNOME without explicit comparator-only / never-observational-truth wording."
                )

    for path, text in texts.items():
        for pattern in FORBIDDEN_POSITIVE_PATTERNS:
            if re.search(pattern, text, flags=re.IGNORECASE):
                issues.append(f"{path.relative_to(ROOT)} contains forbidden claim pattern `{pattern}`.")

        for line in text.splitlines():
            lowered = line.lower()
            if "independent day-to-day validation" in lowered or "fully independent day-to-day" in lowered:
                if not line_is_safe_negation(line):
                    issues.append(
                        f"{path.relative_to(ROOT)} contains unsafe day-to-day validation wording: `{line.strip()}`."
                    )
            if "pygnome" in lowered and "truth" in lowered and not line_is_safe_negation(line):
                issues.append(f"{path.relative_to(ROOT)} contains unsafe PyGNOME truth wording: `{line.strip()}`.")

    if issues:
        print("Docs manuscript-claims check FAILED:")
        for issue in issues:
            print(f"- {issue}")
        return 1

    print("Docs manuscript-claims check passed.")
    print(f"Checked: {README.relative_to(ROOT)}, {PANEL_QUICK_START.relative_to(ROOT)}, {PANEL_REVIEW_GUIDE.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
