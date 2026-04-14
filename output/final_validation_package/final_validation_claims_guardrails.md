# Final Validation Claims Guardrails

- Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents is represented by Mindoro B1, the March 13 -> March 14 NOAA reinit validation, and it should be described with the explicit March 12 WorldView-3 caveat.
- Mindoro A is the same-case March 13 -> March 14 comparator-support track attached to B1. It is never truth and never a co-primary validation row.
- The separate focused 2016-2023 Mindoro drifter rerun selected `cmems_gfs` as the active B1 recipe provenance. It does not replace the original B1 raw provenance.
- The broader 2016-2022 regional rerun is preserved as a reference/governance lane and is not the active provenance for B1.
- Mindoro B2 and B3 remain legacy/reference rows, with B2 framed as honesty-only, and they should not be silently rewritten as if they never existed.
- PyGNOME is a comparator, not truth, in both the promoted Mindoro cross-model lane and the DWH cross-model comparison.
- DWH remains a separate Phase 3C external transfer-validation lane; do not recast it as a local drifter-calibrated case or a second Phase 1 study.
- DWH uses no drifter baseline and no new thesis-facing drifter ingestion; its forcing choice is the readiness-gated historical stack rather than a drifter-ranked recipe.
- DWH observed masks are truth for Phase 3C and must stay date-composite honest rather than implying exact sub-daily acquisition times.
- DWH currently demonstrates workflow transferability and meaningful spatial skill under real historical forcing.
- On DWH, OpenDrift outperforms PyGNOME under the current case definition.
- On DWH, deterministic remains the clean baseline transfer-validation result, p50 is the preferred probabilistic extension, and p90 is support/comparison only.
- DWH Phase 3C is scientifically reportable even if some optional future extensions remain.
- Do not relabel legacy/reference or sensitivity products as if they were the new promoted primary validation row.
