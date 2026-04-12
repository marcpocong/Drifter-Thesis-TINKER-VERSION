# Final Validation Chapter 3 Sync Memo

Thesis-facing title: Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents

Recommended revised structure:

1. Phase 1 = Transport Validation and Baseline Configuration Selection
2. Phase 2 = Standardized Machine-Readable Forecast Product Generation
3. Phase 3A = Mindoro March 13 -> March 14 Cross-Model Comparator
4. Phase 3B1 = Mindoro March 13 -> March 14 NOAA Reinit Primary Validation
5. Phase 3B2 = Mindoro Legacy March 6 Sparse Strict Reference
6. Phase 3B3 = Mindoro Legacy March 3-6 Broader-Support Reference
7. Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)
8. Phase 4 = Oil-Type Fate and Shoreline Impact Analysis
9. Phase 5 = Reproducibility, Packaging, and Deliverables

Packaging guidance:

- Keep Mindoro as the main Philippine case.
- Keep DWH as the rich-data external transfer-validation branch.
- Present Phase 3A as comparator-only benchmarking, not as a truth-source replacement.
- Preserve `config/case_mindoro_retro_2023.yaml` as the frozen March 3 -> March 6 case definition and carry the Phase 3B promotion through the amendment file instead.
- Present March 13 -> March 14 as the canonical Mindoro validation with the shared-imagery caveat stated explicitly.
- State that the later 2016-2023 Mindoro-focused drifter rerun confirmed the same cmems_era5 recipe used by the stored B1 run without rewriting the original run provenance.
- Keep March 6 and March 3-6 visible as legacy/reference material rather than deleting or hiding them.
