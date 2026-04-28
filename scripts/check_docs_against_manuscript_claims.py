from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
PANEL_QUICK_START = ROOT / "PANEL_QUICK_START.md"
PANEL_REVIEW_GUIDE = ROOT / "docs" / "PANEL_REVIEW_GUIDE.md"
PANEL_FILES = [README, PANEL_QUICK_START, PANEL_REVIEW_GUIDE]


def join_parts(*parts: str) -> str:
    return "".join(parts)


TITLE = (
    "# Drifter-Validated 24\u201372 h Oil-Spill Forecasting for Philippine Coasts: "
    "Probability Footprints and Oil-Type Fate"
)

README_REQUIRED_VALUES = [
    "cmems_gfs",
    "0.1075",
    "0.5568",
    "0.5389",
    "0.4966",
    "0.3612",
]

FORBIDDEN_PANEL_STRINGS = [
    join_parts("Draft ", "20"),
    join_parts("Draft ", "18"),
    join_parts("thesis-facing ", "Phase 4"),
    join_parts("thesis-facing ", "Phase 5"),
]

TRUTH_WORD = join_parts("tr", "uth")

FORBIDDEN_POSITIVE_PATTERNS = [
    re.escape(join_parts("exact 1 km ", "match")),
    re.escape(join_parts("exact-grid ", "success")),
    r"PyGNOME\s+is\s+" + re.escape(TRUTH_WORD),
    r"PyGNOME\s+as\s+" + re.escape(TRUTH_WORD),
]

INDEPENDENT_DAY_TO_DAY = join_parts("independent", " day-to-day validation")
FULLY_INDEPENDENT_DAY_TO_DAY = join_parts(
    "fully ", "independent", " day-to-day validation"
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_markdown_section(text: str, heading: str) -> str:
    pattern = rf"(?ms)^## {re.escape(heading)}\n(.*?)(?=^## |\Z)"
    match = re.search(pattern, text)
    return match.group(1) if match else ""


def line_is_safe_negation(line: str) -> bool:
    lowered = line.lower()
    safe_tokens = (
        "not ",
        "do not",
        "must not",
        "never ",
        "isn't",
        "is not",
        "rather than",
    )
    return any(token in lowered for token in safe_tokens)


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
                issues.append(
                    f"{path.relative_to(ROOT)} still contains forbidden wording `{forbidden}`."
                )

    evidence_summary = extract_markdown_section(readme_text, "Current Manuscript Alignment")
    if not evidence_summary:
        issues.append("README is missing the `Current Manuscript Alignment` section.")
    elif "PyGNOME" in evidence_summary:
        comparator_ok = "comparator-only" in evidence_summary and (
            "never observational truth" in evidence_summary
            or "never the observational scoring reference" in evidence_summary
        )
        if not comparator_ok:
            issues.append(
                "README main evidence summary mentions PyGNOME without the full comparator-only caveat."
            )

    for path, text in texts.items():
        for pattern in FORBIDDEN_POSITIVE_PATTERNS:
            if re.search(pattern, text, flags=re.IGNORECASE):
                issues.append(
                    f"{path.relative_to(ROOT)} contains a forbidden positive-claim pattern `{pattern}`."
                )

        for line in text.splitlines():
            lowered = line.lower()
            has_day_to_day_phrase = (
                INDEPENDENT_DAY_TO_DAY in lowered
                or FULLY_INDEPENDENT_DAY_TO_DAY in lowered
            )
            if has_day_to_day_phrase and not line_is_safe_negation(line):
                issues.append(
                    f"{path.relative_to(ROOT)} contains unsafe day-to-day validation wording: `{line.strip()}`."
                )

            if "pygnome" in lowered and "truth" in lowered and not line_is_safe_negation(line):
                issues.append(
                    f"{path.relative_to(ROOT)} contains unsafe PyGNOME scoring-reference wording: `{line.strip()}`."
                )

    if issues:
        print("FAIL: docs manuscript-claims check failed.")
        for issue in issues:
            print(f"- {issue}")
        return 1

    checked = ", ".join(str(path.relative_to(ROOT)) for path in PANEL_FILES)
    print("PASS: docs manuscript-claims check passed.")
    print(f"Checked files: {checked}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
