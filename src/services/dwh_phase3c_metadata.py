"""Shared metadata for the frozen DWH Phase 3C external transfer-validation lane."""

from pathlib import Path


DWH_BASE_CASE_CONFIG_PATH = Path("config") / "case_dwh_retro_2010_72h.yaml"
DWH_PHASE3C_FINAL_NOTE_PATH = Path("docs") / "DWH_PHASE3C_FINAL.md"
DWH_PHASE3C_FINAL_OUTPUT_DIR = Path("output") / "Phase 3C DWH Final Output"

DWH_PHASE3C_TRACK_ID_DETERMINISTIC = "C1"
DWH_PHASE3C_TRACK_ID_ENSEMBLE = "C2"
DWH_PHASE3C_TRACK_ID_COMPARATOR = "C3"

DWH_PHASE3C_TRACK_LABEL_DETERMINISTIC = "DWH deterministic external transfer validation"
DWH_PHASE3C_TRACK_LABEL_ENSEMBLE = "DWH ensemble extension and deterministic-vs-ensemble comparison"
DWH_PHASE3C_TRACK_LABEL_COMPARATOR = "DWH PyGNOME comparator-only"

DWH_PHASE3C_THESIS_PHASE_TITLE = "Phase 3C External Rich-Data Spill Transfer Validation"
DWH_PHASE3C_THESIS_SUBTITLE = "Deepwater Horizon 2010 daily-mask external transfer validation"
DWH_PHASE3C_FORCING_STACK = "HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes"
DWH_PHASE3C_DATE_COMPOSITE_NOTE = (
    "Use the 2010-05-20 initialization composite and the 2010-05-21, 2010-05-22, and 2010-05-23 "
    "validation composites as date-composite truth masks only; do not invent exact sub-daily observation "
    "acquisition times."
)
DWH_PHASE3C_FINAL_RECOMMENDATION = (
    "Deterministic remains the clean baseline transfer-validation result; p50 is the preferred probabilistic "
    "extension; p90 is support/comparison only; PyGNOME is comparator-only."
)
