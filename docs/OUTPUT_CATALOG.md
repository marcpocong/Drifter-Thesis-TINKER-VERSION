# Output Catalog

This catalog follows the final manuscript evidence order: focused Mindoro Phase 1 provenance, Phase 2 standardized forecast products, Mindoro `B1`, Mindoro `Track A` comparator-only support, DWH external transfer validation, Mindoro oil-type/shoreline support, `prototype_2016` legacy/archive support, and the read-only governance/package surfaces.

## Data-Source Provenance Registry

The panel-facing data-source registry is [config/data_sources.yaml](../config/data_sources.yaml), with the readable guide at [DATA_SOURCES.md](DATA_SOURCES.md) and a matching `Data Sources & Provenance` dashboard page.

Interpretation:

- this registry is read-only governance documentation
- it points to configs, manifests, inventories, and stored outputs instead of rerunning science
- rows with missing exact source endpoints are marked for verification rather than guessed
- it keeps drifter provenance, public-observation masks, forcing datasets, shoreline/oil support, and model/tool provenance in separate evidence roles

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

- use this directory to answer whether a matched Phase 4 OpenDrift-versus-PyGNOME comparison is scientifically available now, pilot-only, or still not packaged
- do not infer a valid Phase 4 cross-model comparison just from the existence of Phase 3 spatial PyGNOME comparator outputs
- `output/phase4_crossmodel_comparison/` should only appear later if the audit finds one or more quantities honestly comparable

## Phase 1 Production Rerun Outputs

### `output/phase1_production_rerun/`

Purpose:
Dedicated 2016-2022 historical/regional Phase 1 scientific rerun outputs built from the strict drogued-only non-overlapping 72 h segment policy.

Expected files:

- `phase1_drifter_registry.csv`
- `phase1_accepted_segment_registry.csv`
- `phase1_rejected_segment_registry.csv`
- `phase1_local_input_inventory.csv`
- `phase1_loading_audit.csv`
- `phase1_segment_metrics.csv`
- `phase1_recipe_summary.csv`
- `phase1_recipe_ranking.csv`
- `phase1_production_manifest.json`
- `phase1_baseline_selection_candidate.yaml`

Interpretation:

- `phase1_drifter_registry.csv` is the consolidated candidate-segment registry and includes accepted/rejected status, reject reason, drifter ID, time window, drogue status fields, non-overlap status, and regional-box status
- `phase1_local_input_inventory.csv` records the workflow-scoped persisted monthly drifter and forcing store under `data/historical_validation_inputs/<workflow_mode>/...`, including any month files staged forward from legacy `output/.../_scratch` locations
- the official recipe family for this lane is only `cmems_era5`, `cmems_gfs`, `hycom_era5`, and `hycom_gfs`
- this directory is the scientific evidence bundle for the completed regional rerun and is separate from Mindoro or DWH spill-case outputs
- `phase1_baseline_selection_candidate.yaml` is staged only; it does not overwrite `config/phase1_baseline_selection.yaml`
- downstream trial runs may point `BASELINE_SELECTION_PATH` at the staged candidate artifact explicitly

## Prototype PyGNOME Similarity Outputs

### `output/prototype_2021_pygnome_similarity/`

Purpose:
Read-only consolidated preferred accepted-segment debug transport benchmark summary for the fixed 2021 deterministic OpenDrift-vs-deterministic PyGNOME cases.

Expected files:

- `prototype_pygnome_case_registry.csv`
- `prototype_pygnome_similarity_by_case.csv`
- `prototype_pygnome_fss_by_case_window.csv`
- `prototype_pygnome_kl_by_case_hour.csv`
- `prototype_pygnome_figure_registry.csv`
- `prototype_pygnome_figure_captions.md`
- `prototype_pygnome_similarity_manifest.json`
- `prototype_pygnome_similarity_summary.md`
- `qa_prototype_pygnome_fss_by_case_window.png`
- `qa_prototype_pygnome_kl_by_case_hour.png`
- `qa_prototype_pygnome_scorecard.png`
- `figures/*.png`

Interpretation:

- this directory is comparator-only and accepted-segment debug support only
- it consolidates existing deterministic benchmark artifacts from the fixed `prototype_2021` cases rather than inventing a new cross-model method
- it now also materializes actual `24/48/72 h` deterministic OpenDrift and deterministic PyGNOME forecast figures plus one side-by-side board per case, all built from the stored benchmark rasters only and shown with neutral/case-local context rather than reusing the old Mindoro-specific prototype framing
- when the stored prototype case assets expose a defensible provenance point, these figures also include a provenance source-point star
- PyGNOME is a comparator, not the scoring reference
- the ranking is relative within the prototype set only and uses higher mean `FSS @ 5 km` followed by lower mean `KL`
- do not treat this package as final Chapter 3 evidence or as a substitute for the dedicated 2016-2022 Phase 1 regional production rerun

### `output/prototype_2016_pygnome_similarity/`

Purpose:
Read-only legacy/debug transport benchmark summary preserved from the original 2016 prototype cases.

Interpretation:

- keep this package for historical reproducibility and regression checking
- it is no longer the preferred support family for launcher/docs/publication work

### `output/2016 Legacy Runs FINAL Figures/`

Purpose:
Authoritative curated `prototype_2016` legacy/archive support export. This is the easiest place to browse the preserved 2016 support package. Some internal package names still contain legacy Phase 4/Phase 5 labels, but those are not primary defended evidence.

Expected structure:

- `README.md`
- `CASE_2016-09-01/`, `CASE_2016-09-06/`, `CASE_2016-09-17/`
- `publication/phase3a/`
- `publication/phase4/CASE_2016-09-01/`
- `publication/phase4/CASE_2016-09-06/`
- `publication/phase4/CASE_2016-09-17/`
- `scientific_source_pngs/phase3a/`
- `scientific_source_pngs/phase4/`
- `summary/phase3a/`
- `summary/phase4/`
- `manifests/legacy_final_output_manifest.json`
- `manifests/prototype_2016_final_output_registry.csv`
- `manifests/prototype_2016_final_output_registry.json`
- `manifests/prototype_2016_provenance_metadata.json`
- `phase5/prototype_2016_packaging_summary.md`
- compatibility files `final_figure_manifest.json` and `missing_figures.csv`

Interpretation:

- treat this as the authoritative curated root for `prototype_2016`
- the flat per-case `CASE_2016-.../` figure directories remain for backward compatibility
- `publication/phase3a/` reuses the legacy OpenDrift-vs-deterministic-PyGNOME comparator figures
- `publication/phase4/` reuses stored weathering/fate budget PNGs and adds honest shoreline summary figures rendered from stored shoreline CSVs only
- `publication/phase4_comparator/` packages the budget-only deterministic PyGNOME Phase 4 pilot when stored case-local comparator outputs exist
- `scientific_source_pngs/` keeps the exact stored source PNGs used by the curated publication family
- `summary/` keeps the copied similarity tables, per-case pairing artifacts, weathering CSVs, the Phase 4 comparator decision note, and packaging notes
- `manifests/prototype_2016_provenance_metadata.json` records the shared first-code search box `[108.6465, 121.3655, 6.1865, 20.3515]`, the three original source boxes, and the historical-origin note explaining why the first three 2016 drifter cases became the team’s initial proof-of-pipeline focus on the west coast of the Philippines
- `prototype_2016` is legacy/archive support only; some internal package names still contain Phase 4/Phase 5 labels, but those are not primary defended evidence
- this lane has no primary Mindoro `B1` or DWH validation claim
- the generic `output/figure_package_publication/` package remains mostly a 2016 Phase 3A figure source; the dedicated legacy archive packaging lives here
- the Phase 4 comparator pilot stays budget-only and descriptive; shoreline comparison is not packaged unless matched PyGNOME shoreline products exist
- the shared first-code search box is historical-origin metadata only and does not replace the stored case-local prototype scientific/display extents

## Phase 4 Support Outputs

### `output/phase4/CASE_MINDORO_RETRO_2023/`

Purpose:
Dedicated Mindoro oil-type and shoreline-impact support bundle. Outside `prototype_2016`, treat it as technical/context support rather than a main thesis phase.

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
Read-only static technical gallery of trajectories, overlays, comparison maps, and Mindoro Phase 4 support/context shoreline/oil-type figures for panel inspection before any UI exists.

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
Canonical publication-grade and defense-grade figure package derived from stored rasters, tracks, manifests, and Phase 4 support tables only.

Expected files:

- `publication_figure_manifest.json`
- `publication_figure_registry.csv`
- `publication_figure_captions.md`
- `publication_figure_talking_points.md`
- `publication_figure_inventory.md`
- `CASE_MINDORO_RETRO_2023__...__paper__*.png`
- `CASE_MINDORO_RETRO_2023__...__slide__*.png`
- `CASE_DWH_RETRO_2010_72H__...__paper__*.png`
- `CASE_DWH_RETRO_2010_72H__...__slide__*.png`
- `THESIS_STUDY_CONTEXT__...__paper__*.png`
- `CASE_2016_09_01__...*.png`
- `CASE_2016_09_06__...*.png`
- `CASE_2016_09_17__...*.png`

Interpretation:

- this directory is now the canonical thesis-facing presentation layer for defense slides and paper-ready single figures
- it keeps the raw and panel galleries as technical archives rather than overwriting them
- filenames remain machine-readable, with case, phase/track, model, run type, date/date-range, scenario, view type, and variant tokens
- `publication_figure_registry.csv` is the full figure inventory and now records `thesis_surface`, `archive_only`, `legacy_support`, `comparator_support`, `display_order`, `page_target`, `study_box_id`, `study_box_numbers`, `study_box_label`, and `recommended_scope`
- `thesis_surface=true` rows are the curated thesis-facing figure surface; non-main rows remain indexed here but are classified into archive/support instead of being silently deleted
- `publication_figure_inventory.md` is the human-readable index for the same registry-driven curation
- Phase 3 OpenDrift-versus-PyGNOME comparison figures are included where those comparator products exist now
- the package also includes a shared thesis study-box reference figure that now foregrounds Study Box `2` (`mindoro_case_domain`) plus Study Box `4` (the curated prototype_2016 historical-origin box), an archived full-context overview, and separate per-box geography panels built from stored Phase 1 baseline/config metadata, the stored Mindoro scoring-grid display bounds, and curated prototype provenance metadata only; Study Boxes `1` and `3` remain archive/advanced/support only
- Mindoro Phase 4 is shown as OpenDrift/OpenOil-only support/context material; the package includes a no-matched-PyGNOME note figure instead of a fake Phase 4 OpenDrift-versus-PyGNOME board
- R0 material, stale study-box references, duplicate boards, and raw technical panels not needed for the main presentation are retained through archive/support indexing rather than the thesis-facing surface
- publication family `K` republishes the preferred accepted-segment 2021 deterministic OpenDrift-vs-PyGNOME forecast figures as support-only material rather than main-defense evidence, while the 2016 prototype package remains preserved as legacy output
- Mindoro publication figures now report the promoted March 13 -> March 14 R1 primary validation row while explicitly inheriting recipe provenance from the separate focused 2016-2023 Mindoro Phase 1 rerun; DWH figures remain transfer-validation/support visuals

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
- `mindoro_validation_archive_decision.md`
- `final_validation_summary.md`

Interpretation:

- this remains the thesis-facing summary/table bundle
- use this package for the frozen validation narrative, summary tables, and case registry rather than for broad figure browsing
- use `output/figure_package_publication/` for the curated thesis-facing figure surface and `output/Phase 3B March13-14 Final Output Archive R0 Legacy/` plus the `Mindoro Validation Archive` page for archive-only visuals and provenance figures
- for Mindoro, thesis-facing reporting now uses only the March 13 -> March 14 R1 primary validation row plus Track A comparator support
- the March 13 -> March 14 R0 archived baseline and the preserved March-family legacy rows remain repo-preserved archive-only materials rather than thesis-facing package rows
- repo support layers reuse it rather than replacing it

## Curated B1 Final Output Export

### `output/Phase 3B March13-14 Final Output/`

Purpose:
Read-only curated export of the promoted B1 family for thesis-facing delivery under `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents`.

Expected files:

- `README.md`
- `final_output_manifest.json`
- `publication/observations/*.png`
- `publication/opendrift_primary/*.png`
- `publication/comparator_pygnome/*.png`
- `scientific_source_pngs/opendrift_primary/*.png`
- `scientific_source_pngs/comparator_pygnome/*.png`
- `summary/opendrift_primary/*`
- `summary/comparator_pygnome/*`
- `manifests/final_output_manifest.json`
- `manifests/phase3b_final_output_registry.csv`
- `manifests/phase3b_final_output_registry.json`

Interpretation:

- this directory is an alias/export layer, not the canonical scientific directory
- it packages the B1 publication figures, the stored canonical March 13 -> March 14 QA/source PNGs, and the summary artifacts in one thesis-facing location with primary OpenDrift and comparator-only PyGNOME groups kept separate
- the stored B1 raw-generation outputs remain preserved in their canonical scientific directories, while the active thesis-facing recipe provenance now points to the separate focused `phase1_mindoro_focus_pre_spill_2016_2023` rerun
- the broader `phase1_regional_2016_2022` rerun remains preserved as a regional reference/governance lane rather than the active provenance for B1
- the focused rerun searched through early 2023 but its accepted registry does not include near-2023 accepted segments
- the focused rerun now evaluates the full four-recipe family after a GFS completeness preflight; its raw historical winner is `cmems_gfs`, and official B1 now also uses `cmems_gfs`
- the observation-independence note remains explicit; March 13 and March 14 are independent NOAA-published day-specific public-observation products

## Curated DWH Phase 3C Final Output Export

### `output/Phase 3C DWH Final Output/`

Purpose:
Read-only curated export of the frozen DWH Phase 3C family for thesis-facing delivery as a separate external transfer-validation lane.

Expected files:

- `README.md`
- `publication/observations/*.png`
- `publication/opendrift_deterministic/*.png`
- `publication/opendrift_ensemble/*.png`
- `publication/comparator_pygnome/*.png`
- `publication/context_optional/*.png` only when a clearly labeled context-only trajectory figure exists
- `scientific_source_pngs/deterministic/*.png`
- `scientific_source_pngs/ensemble/*.png`
- `scientific_source_pngs/comparator_pygnome/*.png`
- `summary/deterministic/*`
- `summary/ensemble/*`
- `summary/comparator_pygnome/*`
- `summary/comparison/*`
- `manifests/phase3c_final_output_manifest.json`
- `manifests/phase3c_final_output_registry.csv`
- `manifests/phase3c_final_output_registry.json`

Interpretation:

- this directory is an alias/export layer, not the canonical scientific directory
- it packages the DWH observation truth-context figures, deterministic footprint overlays, daily `mask_p50` / `mask_p90` / exact dual-threshold overview boards, daily OpenDrift-vs-PyGNOME overview boards including the three-row `mask_p50` / `mask_p90` / PyGNOME board, PyGNOME comparator figures, canonical scientific source PNGs, and stored summary artifacts in one thesis-facing location
- the `C1/C2/C3` families exported here are frozen reportable tracks, not deferred placeholders
- DWH remains a separate Phase 3C external transfer-validation lane; do not read this folder as a second local Phase 1 or drifter-calibration study
- the authoritative forcing stack remains `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- the frozen time labels are `24 h = 2010-05-21`, `48 h = 2010-05-22`, `72 h = 2010-05-23`, and `event corridor = 2010-05-21_to_2010-05-23`
- deterministic remains the clean baseline transfer-validation result, `mask_p50` is the preferred probabilistic extension, `mask_p90` is support/comparison only, and PyGNOME is comparator-only
- the thesis-facing order is truth context first, then `C1`, then `C2 mask_p50`, then the daily `mask_p50` / `mask_p90` / exact dual-threshold overview boards, then deterministic-vs-`mask_p50`-vs-`mask_p90`, then `C3`, then the daily OpenDrift-vs-PyGNOME overview boards including the three-row `mask_p50` / `mask_p90` / PyGNOME board, then the OpenDrift-vs-PyGNOME support boards
- the public DWH masks remain truth, remain the scoring reference for all displayed DWH FSS values, and the folder keeps date-composite honesty rather than implying exact sub-daily observation acquisition times

## Final Reproducibility Package

### `output/final_reproducibility_package/`

Purpose:
Read-only launcher/docs/reproducibility/package support layer built from the current local repo state.

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
- because this docs-only pass does not regenerate artifacts, some machine-readable files here may still use legacy Phase 4/Phase 5 package labels until `phase5_sync` is rerun

## Read-Only Dashboard Layer

### `ui/`

Purpose:
Read-only local dashboard code that consumes the synced reproducibility package, final validation package, raw/panel/publication figure registries, Phase 4 support outputs, and the Phase 4 cross-model audit.

Representative files:

- `ui/app.py`
- `ui/data_access.py`
- `ui/plots.py`
- `ui/pages/`
- `ui/assets/`

Interpretation:

- this is a read-only support/explorer layer rather than a new scientific phase
- it is intentionally read-only in the first version
- it now prioritizes the curated final package roots (`Phase 3B March13-14 Final Output`, `Phase 3C DWH Final Output`, and `2016 Legacy Runs FINAL Figures`) before falling back to the generic publication package or raw case trees
- it now routes the March 13 -> March 14 R0 archived baseline plus the preserved March-family legacy rows to the dedicated `Mindoro Validation Archive` page instead of the main thesis-facing Mindoro page
- it surfaces the Phase 4 cross-model deferred status explicitly instead of fabricating comparison products

## Case Output Trees

### `output/CASE_MINDORO_RETRO_2023/`

Contains:

- official deterministic forecast products
- ensemble products
- validation-focused Phase 3B scoring outputs
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
- Mindoro Phase 4 support summary artifacts
- publication-grade figure package summaries and presentation PNGs

Intentionally excluded:

- bulky raw data
- bulk case output trees
- large scientific rasters and NetCDF files
- local-only transient logs beyond summary indexes
