# Output Catalog

## Read-Only Audit Outputs

### `output/phase1_finalization_audit/`

Purpose:
Read-only Phase 1 architecture audit against the stable Chapter 3 target.

Expected files:

- `phase1_finalization_status.csv`
- `phase1_finalization_status.json`
- `phase1_finalization_memo.md`
- `phase1_final_verdict.md`

Interpretation:

- use this directory to understand whether the Phase 1 architecture is in place
- do not use it as evidence that the full 2016-2022 regional rerun has already been completed

### `output/phase2_finalization_audit/`

Purpose:
Read-only Phase 2 semantics/manifests audit.

Expected files:

- `phase2_finalization_status.csv`
- `phase2_finalization_status.json`
- `phase2_finalization_memo.md`
- `phase2_output_catalog.csv`
- `phase2_final_verdict.md`

Interpretation:

- use this directory to understand whether Phase 2 is scientifically usable now
- do not use it as evidence that the upstream Phase 1 frozen baseline story is already complete

### `output/phase4_crossmodel_comparability_audit/`

Purpose:
Read-only verdict on whether the current Phase 4 OpenDrift outputs can be compared honestly against the repo's existing PyGNOME artifacts.

Expected files:

- `phase4_crossmodel_comparability_matrix.csv`
- `phase4_crossmodel_comparability_matrix.json`
- `phase4_crossmodel_comparability_report.md`
- `phase4_crossmodel_final_verdict.md`
- `phase4_crossmodel_blockers.md` when no honest comparison is available now
- `phase4_crossmodel_minimal_next_steps.md` when new matched PyGNOME Phase 4 outputs are still required

Interpretation:

- use this directory to answer whether Phase 4 OpenDrift-versus-PyGNOME comparison is scientifically available now, pilot-only, or deferred
- do not infer a valid Phase 4 cross-model comparison just from the existence of Phase 3 spatial PyGNOME comparator outputs
- `output/phase4_crossmodel_comparison/` should only appear later if the audit finds one or more quantities honestly comparable

## Phase 4 Outputs

### `output/phase4/CASE_MINDORO_RETRO_2023/`

Purpose:
Dedicated Mindoro Phase 4 oil-type and shoreline-impact bundle.

Expected files:

- `phase4_oil_budget_timeseries_<scenario>.csv`
- `phase4_oil_budget_summary.csv`
- `phase4_shoreline_arrival.csv`
- `phase4_shoreline_segments.csv`
- `phase4_oiltype_comparison.csv`
- `phase4_run_manifest.json`
- `qa_phase4_shoreline_impacts.png`
- `qa_phase4_oiltype_comparison.png`
- `phase4_methodology_sync_memo.md`
- `phase4_final_verdict.md`

Interpretation:

- this directory is separate from Phase 2 and Phase 3 baseline validation products
- the manifest records inherited provisional status from the current transport framework
- the shoreline outputs are tied to the real shoreline-aware workflow, not to a fake standalone shoreline layer

## Trajectory Gallery Outputs

### `output/trajectory_gallery/`

Purpose:
Read-only static technical gallery of trajectories, overlays, comparison maps, and Mindoro Phase 4 shoreline/oil-type figures for panel inspection before any UI exists.

Expected files:

- `trajectory_gallery_manifest.json`
- `trajectory_gallery_index.csv`
- `figures_index.md`
- `CASE_MINDORO_RETRO_2023__...*.png`
- `CASE_DWH_RETRO_2010_72H__...*.png`

Interpretation:

- this directory is built from existing outputs only and does not rerun expensive scientific branches
- figure filenames encode case, phase/track, model, run type, date/date-range, and scenario where relevant
- copied QA figures and newly generated trajectory views are both recorded in the gallery manifest so the later UI can consume them honestly

### `output/trajectory_gallery_panel/`

Purpose:
Read-only polished panel-ready figure pack derived from the raw gallery and existing stored outputs for non-technical review.

Expected files:

- `panel_figure_manifest.json`
- `panel_figure_registry.csv`
- `panel_figure_captions.md`
- `panel_figure_talking_points.md`
- `CASE_MINDORO_RETRO_2023__...__panel__*.png`
- `CASE_DWH_RETRO_2010_72H__...__panel__*.png`

Interpretation:

- this directory is the polished presentation layer, not a replacement for the raw technical gallery
- board filenames remain machine-readable and UI-friendly, with case, phase/track, model, run type, date/date-range, scenario, and variant tokens
- side-by-side comparison boards, captions, and talking points are explicitly recorded so later UI or packaging steps can consume them
- Mindoro and DWH panel boards remain honest about upstream inherited-provisional status where that status still exists

### `output/figure_package_publication/`

Purpose:
Canonical publication-grade and defense-grade figure package derived from stored rasters, tracks, manifests, and Phase 4 tables only.

Expected files:

- `publication_figure_manifest.json`
- `publication_figure_registry.csv`
- `publication_figure_captions.md`
- `publication_figure_talking_points.md`
- `CASE_MINDORO_RETRO_2023__...__paper__*.png`
- `CASE_MINDORO_RETRO_2023__...__slide__*.png`
- `CASE_DWH_RETRO_2010_72H__...__paper__*.png`
- `CASE_DWH_RETRO_2010_72H__...__slide__*.png`

Interpretation:

- this directory is now the canonical presentation layer for defense slides and paper-ready single figures
- it keeps the raw and panel galleries as technical archives rather than overwriting them
- filenames remain machine-readable, with case, phase/track, model, run type, date/date-range, scenario, view type, and variant tokens
- recommended-for-defense and recommended-for-paper flags are recorded in the publication registry and manifest
- Phase 3 OpenDrift-versus-PyGNOME comparison figures are included where those comparator products exist now
- Mindoro Phase 4 is shown as OpenDrift-only; the package includes a deferred-comparison note figure instead of a fake Phase 4 OpenDrift-versus-PyGNOME board
- Mindoro publication figures remain inherited-provisional from the unfinished Phase 1/2 freeze story, while DWH figures remain transfer-validation/support visuals

## Frozen Thesis Validation Package

### `output/final_validation_package/`

Purpose:
Frozen thesis validation bundle built from completed scientific outputs without recomputing them.

Representative files:

- `final_validation_manifest.json`
- `final_validation_case_registry.csv`
- `final_validation_main_table.csv`
- `final_validation_benchmark_table.csv`
- `final_validation_observation_table.csv`
- `final_validation_limitations.csv`
- `final_validation_claims_guardrails.md`
- `final_validation_chapter_sync_memo.md`
- `final_validation_interpretation_memo.md`
- `final_validation_summary.md`

Interpretation:

- this remains the thesis-facing summary bundle
- Phase 5 reuses it rather than replacing it

## Final Reproducibility Package

### `output/final_reproducibility_package/`

Purpose:
Phase 5 launcher/docs/reproducibility/package synchronization layer built from the current local repo state.

Expected files:

- `software_versions.csv`
- `final_case_registry.csv`
- `final_config_snapshot_index.csv`
- `final_manifest_index.csv`
- `final_output_catalog.csv`
- `final_log_index.csv`
- `final_phase_status_registry.csv`
- `final_reproducibility_summary.md`
- `final_reproducibility_manifest.json`
- `phase5_packaging_sync_memo.md`
- `phase5_final_verdict.md`
- `launcher_user_guide.md`

Interpretation:

- use this directory to audit current reproducibility/package state
- this layer is intentionally non-scientific and does not recompute scientific scores
- the phase-status registry is the machine-readable summary of what is reportable, frozen, or inherited-provisional

## Read-Only Dashboard Layer

### `ui/`

Purpose:
Read-only local dashboard code that consumes the synced reproducibility package, final validation package, raw/panel/publication figure registries, Phase 4 outputs, and the Phase 4 cross-model audit.

Representative files:

- `ui/app.py`
- `ui/data_access.py`
- `ui/plots.py`
- `ui/pages/`
- `ui/assets/`

Interpretation:

- this is a Phase 5 exploration layer rather than a new scientific phase
- it is intentionally read-only in the first version
- it defaults to the publication-grade figure package for panel-friendly viewing
- it surfaces the Phase 4 cross-model deferred status explicitly instead of fabricating comparison products

## Case Output Trees

### `output/CASE_MINDORO_RETRO_2023/`

Contains:

- official deterministic forecast products
- ensemble products
- strict Phase 3B scoring outputs
- broader public-support outputs
- benchmark and sensitivity branches

### `output/CASE_DWH_RETRO_2010_72H/`

Contains:

- external-case setup products
- scientific forcing readiness products
- deterministic transfer-validation products
- ensemble comparison products
- PyGNOME comparator products

## Legacy Prototype Outputs

### `output/CASE_2016-09-01/`
### `output/CASE_2016-09-06/`
### `output/CASE_2016-09-17/`

Purpose:
Legacy prototype debugging/regression outputs.

Guardrail:

- these are not the final Phase 1 regional corpus
- these do not replace the accepted/rejected segment registry required for the final frozen baseline story

## Trackable vs Excluded Artifacts

Intentionally trackable where appropriate:

- small CSV/JSON/MD/PNG audit and package artifacts
- final validation summaries
- final reproducibility summaries
- Mindoro Phase 4 summary artifacts
- publication-grade figure package summaries and presentation PNGs

Intentionally excluded:

- bulky raw data
- bulk case output trees
- large scientific rasters and NetCDF files
- local-only transient logs beyond summary indexes
