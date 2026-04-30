# Final Validation Chapter 3 Sync Memo

Thesis-facing title: Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents

Recommended revised structure:

1. Phase 1 = Transport Validation and Baseline Configuration Selection
2. Phase 2 = Standardized Machine-Readable Forecast Product Generation
3. Phase 3A = Mindoro March 13 -> March 14 Same-Case Comparator Support Track
4. Phase 3B1 = Mindoro March 13 -> March 14 NOAA Reinit Primary Validation
5. Phase 3B2 = Mindoro Legacy March 6 Sparse Strict Reference
6. Phase 3B3 = Mindoro Legacy March 3-6 Broader-Support Reference
7. Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)
8. Phase 4 = Oil-Type Fate and Shoreline Impact Analysis
9. Phase 5 = Reproducibility, Packaging, and Deliverables

Packaging guidance:

- Keep Mindoro as the main Philippine case.
- Keep DWH as the separate Phase 3C external rich-data transfer-validation branch, with no new drifter ingestion in the thesis-facing lane.
- Present Phase 3A as same-case comparator-only support attached to B1, not as a co-primary claim or truth-source replacement.
- Preserve `config/case_mindoro_retro_2023.yaml` as the frozen March 3 -> March 6 case definition and carry the Phase 3B promotion through the amendment file instead.
- Present March 13 -> March 14 as the canonical Mindoro validation with March 13 as the public seed observation and March 14 as the public target observation.
- State that the separate focused 2016-2023 mindoro drifter rerun selected `cmems_gfs` as the active b1 recipe provenance. it does not replace the original b1 raw provenance.
- State that the broader 2016-2022 regional rerun is preserved as reference/governance context rather than as the active B1 provenance lane.
- Keep March 6 and March 3-6 visible as legacy/reference material rather than deleting or hiding them.
- State that DWH uses the readiness-gated historical forcing stack rather than a Phase 1 drifter-ranked recipe, and that deterministic stays the baseline while p50 is the preferred probabilistic extension.
