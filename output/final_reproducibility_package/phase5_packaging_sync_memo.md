# Phase 5 Packaging Sync Memo

Phase 5 reuses the existing final validation package, Phase 1 audit, Phase 2 audit, Mindoro Phase 4 bundle, and DWH Phase 3C outputs to build a synchronized reproducibility/package layer.

## What Was Reused

- Existing final validation manifest: `output/final_validation_package/final_validation_manifest.json`
- Existing Phase 1 audit: `output/phase1_finalization_audit/phase1_finalization_status.json`
- Existing Phase 2 audit: `output/phase2_finalization_audit/phase2_finalization_status.json`
- Existing Mindoro Phase 4 manifest: `output/phase4/CASE_MINDORO_RETRO_2023/phase4_run_manifest.json`
- Frozen Mindoro base case definition: `config/case_mindoro_retro_2023.yaml`
- Mindoro primary-validation amendment file: `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`
- Mindoro drifter-confirmation candidate baseline: `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_baseline_selection_candidate.yaml`
- Mindoro curated final-output export: `output/Phase 3B March13-14 Final Output`
- DWH Phase 3C final-governance note: `docs/DWH_PHASE3C_FINAL.md`
- DWH curated final-output export: `output/Phase 3C DWH Final Output`
- Existing trajectory gallery outputs under `output/trajectory_gallery/` when present.
- Existing polished panel gallery outputs under `output/trajectory_gallery_panel/` when present.
- Existing publication-grade figure package outputs under `output/figure_package_publication/` when present.
- Existing read-only dashboard source files under `ui/` and guidance in `docs/UI_GUIDE.md` when present.

## Guardrails

- No scientific score tables were recomputed here.
- No finished Mindoro or DWH scientific outputs were overwritten.
- The March 3 -> March 6 Mindoro base case YAML remains frozen; the promoted March 13 -> March 14 row is recorded as an amendment rather than a silent rewrite.
- `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents` remains tied to B1, and both noaa/nesdis public products cite worldview-3 imagery acquired on 2023-03-12, so the promoted march 13 -> march 14 row is a reinitialization-based public-validation pair with shared-imagery provenance rather than a fully independent day-to-day validation.
- DWH Phase 3C remains a separate external transfer-validation lane under `config/case_dwh_retro_2010_72h.yaml` with forcing fixed to `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes` and no thesis-facing drifter baseline.
- The separate focused 2016-2023 Mindoro drifter rerun now supplies the active B1 recipe-provenance story, not the raw generation history of the stored March 13 -> March 14 science bundle.
- The legacy `prototype_2016` lane is framed as Phase 1 / 2 / 3A / 4 / 5 support, with its dedicated curated package rooted at `output/2016 Legacy Runs FINAL Figures`; it has no thesis-facing Phase 3B or Phase 3C.
- Its historical-origin note keeps the shared first-code search box `[108.6465, 121.3655, 6.1865, 20.3515]` explicit for the first three 2016 drifters on the west coast of the Philippines, while the stored per-case local prototype extents remain operative.
- The launcher/menu is now organized around current track categories instead of the older monolithic Mindoro full-chain story.
- The first dashboard version is intentionally read-only and does not add scientific run buttons.

## Optional Future Work Still Missing

- `output/phase4/CASE_DWH_RETRO_2010_72H/phase4_run_manifest.json`
