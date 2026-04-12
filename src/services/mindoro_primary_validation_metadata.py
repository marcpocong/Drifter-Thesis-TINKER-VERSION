"""Shared metadata for the promoted Mindoro Phase 3B public-validation row."""

from pathlib import Path


MINDORO_BASE_CASE_CONFIG_PATH = Path("config") / "case_mindoro_retro_2023.yaml"
MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH = (
    Path("config") / "case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml"
)
MINDORO_PRIMARY_VALIDATION_MIGRATION_NOTE_PATH = (
    Path("docs") / "MINDORO_PRIMARY_VALIDATION_MIGRATION.md"
)

MINDORO_PRIMARY_VALIDATION_TRACK_ID = "B1"
MINDORO_PRIMARY_VALIDATION_TRACK_LABEL = "Mindoro March 13 -> March 14 NOAA reinit primary validation"
MINDORO_PRIMARY_VALIDATION_PHASE_OR_TRACK = "phase3b_reinit_primary"

MINDORO_LEGACY_MARCH6_TRACK_ID = "B2"
MINDORO_LEGACY_MARCH6_TRACK_LABEL = "Mindoro legacy March 6 sparse strict reference"
MINDORO_LEGACY_MARCH6_PHASE_OR_TRACK = "phase3b_legacy_strict"

MINDORO_LEGACY_SUPPORT_TRACK_ID = "B3"
MINDORO_LEGACY_SUPPORT_TRACK_LABEL = "Mindoro legacy March 3-6 broader-support reference"

MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID = "mindoro_phase3b_primary_public_validation"
MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID = "mindoro_march13_14_noaa_reinit_stress_test"

MINDORO_SHARED_IMAGERY_CAVEAT = (
    "Both NOAA/NESDIS public products cite WorldView-3 imagery acquired on 2023-03-12, so the promoted "
    "March 13 -> March 14 row is a reinitialization-based public-validation pair with shared-imagery "
    "provenance rather than a fully independent day-to-day validation."
)

