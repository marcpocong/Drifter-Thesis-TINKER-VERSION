# Phase 3C DWH Final Output

This folder is a read-only curated packaging layer over the canonical stored DWH Phase 3C outputs.

Governance summary:
- DWH is `Phase 3C External Rich-Data Spill Transfer Validation` and remains a separate external transfer-validation lane.
- No drifter baseline is used here and no new drifter ingestion is part of the thesis-facing DWH lane.
- Truth comes from public observation-derived daily masks and the event-corridor union, used honestly as date-composite masks only.
- Forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`.
- Deterministic is the clean baseline transfer-validation result.
- Ensemble p50 is the preferred probabilistic extension.
- Ensemble p90 is support/comparison only.
- PyGNOME is comparator-only and never truth.

C-track meanings:
- `C1` = DWH deterministic external transfer validation.
- `C2` = DWH ensemble extension and deterministic-vs-ensemble comparison.
- `C3` = DWH PyGNOME comparator-only.

Date-composite guardrail:
- Use the 2010-05-20 initialization composite and the 2010-05-21, 2010-05-22, and 2010-05-23 validation composites as date-composite truth masks only; do not invent exact sub-daily observation acquisition times.

Packaging rule:
- This folder copies or lightly regenerates presentation artifacts from stored rasters, stored publication figures, and stored summary/manifests only.
- It does not rename or replace the canonical scientific directories under `output/CASE_DWH_RETRO_2010_72H/`.
