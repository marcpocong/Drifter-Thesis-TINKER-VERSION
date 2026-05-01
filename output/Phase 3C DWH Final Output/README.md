# Phase 3C DWH Final Output

This folder is a read-only curated packaging layer over the canonical stored DWH Phase 3C outputs.

Governance summary:
- DWH is `Phase 3C External Rich-Data Spill Transfer Validation` and remains a separate external transfer-validation lane.
- No drifter baseline is used here and no new drifter ingestion is part of the thesis-facing DWH lane.
- Truth comes from public observation-derived daily masks and the event-corridor union, used honestly as date-composite masks only.
- Those official public observation-derived DWH date-composite masks remain the scoring reference for every displayed FSS value in this curated package.
- Forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`.
- Deterministic is the clean baseline transfer-validation result.
- `mask_p50` is the preferred probabilistic extension.
- `mask_p90` is support/comparison only.
- PyGNOME is comparator-only and never truth.

C-track meanings:
- `C1` = DWH deterministic external transfer validation.
- `C2` = DWH ensemble extension and deterministic-vs-ensemble comparison.
- `C3` = DWH PyGNOME comparator-only.

Frozen time labels:
- `24 h` = `2010-05-21`.
- `48 h` = `2010-05-22`.
- `72 h` = `2010-05-23`.
- `event corridor` = `2010-05-21_to_2010-05-23`.

Date-composite guardrail:
- Use the 2010-05-20 initialization composite and the 2010-05-21, 2010-05-22, and 2010-05-23 validation composites as date-composite truth masks only; do not invent exact sub-daily observation acquisition times.

Thesis-facing figure order:
- Observation truth context.
- C1 deterministic footprint overlays and overview board.
- C2 `mask_p50` overlays.
- C2 daily `mask_p50`, `mask_p90`, and exact dual-threshold overview boards.
- C2 deterministic-vs-`mask_p50`-vs-`mask_p90` comparison boards.
- C3 PyGNOME-vs-observed comparator boards.
- Daily OpenDrift-vs-PyGNOME overview boards, including the three-row `mask_p50` / `mask_p90` / PyGNOME board.
- OpenDrift-vs-PyGNOME support boards.

Curated publication groups:
- `publication/observations/`: observation truth-context figures for the three daily composites and the event-corridor union.
- `publication/opendrift_deterministic/`: deterministic footprint overlays plus the daily overview board.
- `publication/opendrift_ensemble/`: `mask_p50`, `mask_p90`, exact dual-threshold daily overview boards, and observation-plus-OpenDrift comparison boards.
- `publication/comparator_pygnome/`: PyGNOME footprint overlays, truth-comparator boards, daily OpenDrift-vs-PyGNOME overview boards including the three-row `mask_p50` / `mask_p90` / PyGNOME board, and OpenDrift-vs-PyGNOME support boards.

Packaging rule:
- This folder copies presentation artifacts from the stored publication package and writes summary derivatives from stored tables only.
- Raw `output/CASE_DWH_RETRO_2010_72H/...` paths are rerun/staging provenance and are not tracked as the reviewer first-open folder in the submitted package.
