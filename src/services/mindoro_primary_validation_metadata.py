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
MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE = "Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents"
MINDORO_PRIMARY_VALIDATION_THESIS_SUBTITLE = (
    "Mindoro March 13 -> March 14 public spill-extent reinitialization validation"
)

MINDORO_LEGACY_MARCH6_TRACK_ID = "B2"
MINDORO_LEGACY_MARCH6_TRACK_LABEL = "Mindoro legacy March 6 sparse strict reference"
MINDORO_LEGACY_MARCH6_PHASE_OR_TRACK = "phase3b_legacy_strict"

MINDORO_LEGACY_SUPPORT_TRACK_ID = "B3"
MINDORO_LEGACY_SUPPORT_TRACK_LABEL = "Mindoro legacy March 3-6 broader-support reference"

MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID = "mindoro_phase3b_primary_public_validation"
MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID = "mindoro_march13_14_noaa_reinit_stress_test"
MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR = Path("output") / "Phase 3B March13-14 Final Output"
MINDORO_PHASE1_CONFIRMATION_WORKFLOW_MODE = "phase1_mindoro_focus_pre_spill_2016_2023"
MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH = (
    Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_baseline_selection_candidate.yaml"
)
MINDORO_PHASE1_CONFIRMATION_ACCEPTED_REGISTRY_PATH = (
    Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_accepted_segment_registry.csv"
)
MINDORO_PHASE1_REGIONAL_REFERENCE_CANDIDATE_BASELINE_PATH = (
    Path("output") / "phase1_production_rerun" / "phase1_baseline_selection_candidate.yaml"
)

MINDORO_SHARED_IMAGERY_CAVEAT = (
    "Both NOAA/NESDIS public products cite WorldView-3 imagery acquired on 2023-03-12, so the promoted "
    "March 13 -> March 14 row is a reinitialization-based public-validation pair with shared-imagery "
    "provenance rather than a fully independent day-to-day validation."
)

MINDORO_PHASE1_CONFIRMATION_INTERPRETATION_TEMPLATE = (
    "The separate {workflow_mode} Mindoro-focused drifter rerun selected {recipe} as the active official "
    "B1 recipe after the focused four-recipe comparison. It now serves as the active Mindoro-specific "
    "recipe-provenance lane for B1 without "
    "rewriting the original March 13 -> March 14 raw-generation history."
)
