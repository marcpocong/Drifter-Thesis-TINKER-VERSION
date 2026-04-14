# Drifter-Validated Oil Spill Forecasting System

Thesis workflow for transport validation, machine-readable forecast generation, public-observation validation, external-case transfer validation, and read-only support packaging using OpenDrift and PyGNOME. Legacy `prototype_2016` alone extends that story into thesis-facing `Phase 4` and `Phase 5` legacy runs.

## Plain-Language Status

- Phase 1: Mindoro-specific recipe provenance is now finalized through the separate focused `2016-2023` drifter rerun, which evaluated the full four-recipe family, found `cmems_gfs` as the historical winner, and now promotes `cmems_gfs` directly into official B1; the broader `2016-2022` regional rerun remains preserved as a reference/governance lane and currently selects `cmems_gfs`.
- Phase 2: scientifically usable as implemented, but not scientifically frozen.
- Mindoro Phase 3: validation-focused and reportable, with `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents` carried by the March 13 -> March 14 B1 row, March 6 preserved as a legacy sparse-reference honesty case, the promotion recorded in `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml` rather than by rewriting the frozen March 3 -> March 6 base case YAML, and B1 now inheriting its recipe provenance from the separate focused `2016-2023` Mindoro drifter rerun without claiming direct drifter ingestion inside Phase 3B itself.
- DWH Phase 3C: frozen validation-only external rich-data transfer case under the current case definition, kept separate from Phase 1 drifter calibration.
- DWH forcing rule: readiness-gated historical stack, not the Phase 1 drifter-selected baseline; current stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.
- DWH thesis-facing C-track semantics: `C1` deterministic external transfer validation, `C2` ensemble extension and deterministic-vs-ensemble comparison, `C3` PyGNOME comparator-only.
- DWH thesis-facing recommendation: deterministic remains the clean baseline transfer-validation result, `mask_p50` is the preferred probabilistic extension, `mask_p90` is support/comparison only, and PyGNOME remains comparator-only.
- DWH output matrix: the thesis-facing DWH package is now organized as a daily plus event-corridor matrix with observation truth context first, then deterministic footprint overlays, daily `mask_p50` / `mask_p90` / exact dual-threshold overview boards, deterministic-vs-`mask_p50`-vs-`mask_p90`, daily OpenDrift-vs-PyGNOME overview boards including a three-row `mask_p50` / `mask_p90` / PyGNOME daily board, and finally PyGNOME support boards built from stored outputs only.
- DWH score reference: the official public observation-derived DWH date-composite masks remain the scoring reference for every displayed DWH FSS value, including the new daily overview boards.
- Forcing-outage policy: strict/reportable lanes fail hard by default, while appendix/legacy/experimental lanes may continue in explicit degraded mode for forcing-only outages; drifter and ArcGIS/observation truth inputs remain hard requirements.
- Mindoro `phase4_oiltype_and_shoreline`: implemented as a support/context oil-type and shoreline bundle outside the main thesis phase count; thesis-facing `Phase 4` labeling is reserved for `prototype_2016` legacy runs.
- Phase 4 cross-model comparison: no matched Mindoro Phase 4 PyGNOME package is stored yet; current PyGNOME branches remain transport comparators rather than matched Phase 4 fate-and-shoreline outputs.
- Repo sync, galleries, publication packaging, and dashboard layers: read-only support/explorer surfaces outside the main thesis phase count; thesis-facing `Phase 5` labeling is reserved for `prototype_2016` legacy runs.
- Trajectory gallery: read-only static technical figure layer for panel inspection, built from existing outputs only.
- Trajectory gallery panel pack: read-only polished board layer for non-technical panel review, built from the existing gallery and stored outputs only.
- Publication figure package: canonical publication-grade and defense-grade presentation layer built from existing outputs only, with Phase 3 OpenDrift-vs-PyGNOME comparison boards, Phase 4 OpenDrift-only support/context figures, an explicit deferred-comparison note figure for Phase 4, and a support-only `K` family for the preferred 2021 accepted-segment prototype comparator figures.
- Thesis study-box reference figures: the publication package now also includes one shared panel-ready WGS84 box reference plus separate per-box geography figures built from stored metadata only so the focused Mindoro Phase 1 box, `mindoro_case_domain`, the scoring-grid display bounds, and the prototype_2016 first-code search box stay explicit and separate.
- Read-only local dashboard: support/explorer layer over the publication package, panel/raw galleries, final reproducibility package, and the Phase 4 cross-model audit.

## Workflow Lanes

- `prototype_2021`: preferred accepted-segment debug/demo workflow. It is frozen from the two accepted 2021 strict-gate drifter segments, uses only the official Phase 1 recipe family, stops at the transport-core bundle (`prep -> 1_2 -> benchmark -> prototype_pygnome_similarity_summary`), and is still support-only rather than final Chapter 3 evidence.
- `prototype_2016`: legacy debug/regression workflow. This lane is preserved because it records the earliest prototype stage of the study. The very first prototype code used the shared first-code search box `[108.6465, 121.3655, 6.1865, 20.3515]` on the west coast of the Philippines (Palawan-side western Philippine context). Because the ingestion-and-validation pipeline was still in its early stage, that first code surfaced the first three 2016 drifter cases, and the team then intentionally kept those three as the first study focus to build the workflow and prove the pipeline was working. It is not the final Chapter 3 Phase 1 study. This historical-origin note does not replace the stored per-case local prototype extents, which remain the operative scientific/display extents. Its visible thesis-facing support flow now remains `Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`. Methodologically, it shows that the early pipeline could carry drifter-driven transport validation through Phase 1 and Phase 2, then through a Phase 3A OpenDrift-versus-deterministic-PyGNOME comparator check using fraction skill score, before continuing into legacy Phase 4 weathering/fate work and the legacy Phase 5 figure/package story. The comparator result remains support-only: a non-zero FSS means the ensemble footprint was not completely disjoint from the deterministic PyGNOME forecast, not that PyGNOME is truth or that this lane is final proof. Prototype prep still attempts `gfs_wind.nc` on a best-effort basis, the legacy Phase 3A package now surfaces deterministic plus `p50`/`p90` OpenDrift support tracks against deterministic PyGNOME, the legacy Phase 4 weathering path now seeds from the selected drifter-of-record start in `data/drifters/CASE_2016-*/drifters_noaa.csv` rather than from the old prototype polygon, and the Phase 4 support package can now include a budget-only deterministic PyGNOME comparator pilot with shoreline comparison kept explicitly unavailable. There is no thesis-facing `Phase 3B` or `Phase 3C` in this 2016 lane.
- `mindoro_retro_2023`: main Philippine thesis lane for spill-case validation. Thesis-facing, it is presented as a separate focused drifter-based Phase 1 provenance path, then Phase 2, then Phase 3B primary validation on the March 13 -> March 14 B1 row; March 6 remains the visible B2 legacy reference row, and the March 13 -> March 14 PyGNOME lane stays comparator-only on the same case.
- `dwh_retro_2010`: frozen external rich-data transfer-validation lane for deterministic, ensemble, and PyGNOME comparator work. It keeps DWH observed daily masks as truth, keeps PyGNOME comparator-only, uses no thesis-facing drifter baseline, freezes a readiness-gated HYCOM GOFS 3.1 + ERA5 + CMEMS wave/Stokes stack rather than inheriting Phase 1 drifter-selected baseline logic, and treats `C1/C2/C3` as reportable stored tracks rather than deferred placeholders.
- `phase1_regional_2016_2022`: dedicated historical/regional Phase 1 scientific rerun lane preserved as the broader reference/governance lane for the strict drogued-only non-overlapping 72 h validation corpus; it is not the active provenance for Mindoro B1.
- `phase1_mindoro_focus_pre_spill_2016_2023`: separate Mindoro-focused Phase 1 provenance lane for the B1 recipe story. It is the active Mindoro-specific provenance path, remains separate from the broader regional reference lane, now evaluates the full four-recipe family with a full accepted-month GFS preflight, and promotes the focused historical winner directly into official B1.

## Current Launcher

The current launcher entrypoint is [start.ps1](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/start.ps1). It is driven by [config/launcher_matrix.json](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/config/launcher_matrix.json) and separates:

- scientific / reportable reruns
- sensitivity / appendix branches
- read-only packaging and audit utilities
- legacy prototype tracks

Safe first commands:

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Entry phase5_sync -NoPause
.\start.ps1 -Entry trajectory_gallery -NoPause
.\start.ps1 -Entry trajectory_gallery_panel -NoPause
.\start.ps1 -Entry figure_package_publication -NoPause
.\start.ps1 -Entry prototype_legacy_final_figures -NoPause
```

Intentional scientific reruns remain available, but they are no longer hidden behind a single stale "Mindoro full" option.

## Mindoro Phase 3 Promotion Rule

- The frozen Mindoro base case definition remains `config/case_mindoro_retro_2023.yaml` and still represents the original March 3 -> March 6 case.
- The promoted Phase 3B public-validation row is recorded separately in `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.
- The canonical launcher entry for that promoted row is `mindoro_phase3b_primary_public_validation`.
- The older `mindoro_march13_14_noaa_reinit_stress_test` launcher entry is retained as a backward-compatible alias only.
- The thesis-facing title for B1 is `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents`.
- The separate `phase1_mindoro_focus_pre_spill_2016_2023` drifter rerun now serves as the active Mindoro-specific recipe-provenance lane; its completed four-recipe historical winner is `cmems_gfs`, and the official B1 artifact now also uses `cmems_gfs`. This does not replace the raw-generation history of the original March 13 -> March 14 bundle.
- That focused rerun searched through early 2023, but its accepted registry does not include near-2023 accepted segments.
- Thesis-facing Mindoro sequencing is: separate focused drifter-based Phase 1 provenance -> Phase 2 -> Phase 3B primary validation.
- Mindoro track semantics are locked as: `B1` = the only main-text primary validation row, `A` = the same-case March 13 -> March 14 comparator-support track attached to B1, `B2` = the March 6 legacy reference row, and `B3` = the March 3 -> March 6 broader-support legacy row.
- March 6 remains visible as a legacy reference row and must not be called primary.
- The March 13 -> March 14 PyGNOME lane remains same-case supporting comparator evidence only under Track `A`, and the shared-imagery caveat means March 13 -> March 14 must not be described as independent day-to-day validation.
- Keep the three Mindoro spatial extents distinct: focused Phase 1 validation box `[118.751, 124.305, 10.620, 16.026]`, broad `mindoro_case_domain` fallback transport/overview extent `[115.0, 122.0, 6.0, 14.5]`, and the current scoring-grid display bounds `[120.90964677179262, 122.0621541786303, 12.249384840763462, 13.783655303175253]`.
- Keep the prototype_2016 first-code search box `[108.6465, 121.3655, 6.1865, 20.3515]` explicit as historical-origin support metadata rather than collapsing it into the active Mindoro box vocabulary.

## Read-Only Dashboard

The local UI is intentionally read-only in this first version. It reads the existing figure packages, manifests, audit bundles, and synced reproducibility indexes without rerunning science.

Launch command:

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Then open `http://localhost:8501`.

What it shows first:

- curated final package roots for Mindoro B1, Mindoro comparator support, DWH Phase 3C, and the legacy 2016 support package
- publication-grade recommended defense figures and a plain-language study structure overview
- a dedicated Phase 1 page that explains the focused Mindoro provenance rerun and how B1 inherits the selected recipe
- that same Phase 1 page now also surfaces a shared panel-ready thesis study-box reference figure plus separate per-box geography panels built from stored config, manifest, and legacy provenance metadata only
- a dedicated Mindoro B1 page that keeps B1 primary, A comparator-only, B2 legacy reference, and B3 broader support explicit
- a dedicated DWH Phase 3C page that keeps observation truth context first, then C1 deterministic, C2 `mask_p50`, C2 daily overview boards, C2 deterministic-vs-`mask_p50`-vs-`mask_p90`, C3 comparator-only, daily OpenDrift-vs-PyGNOME overview boards including the three-row `mask_p50` / `mask_p90` / PyGNOME board, and no-drifter-baseline wording explicit
- a dedicated legacy 2016 support page for the curated `output/2016 Legacy Runs FINAL Figures` package
- a plain-language Mindoro Phase 4 context page that states no matched PyGNOME Phase 4 package is packaged yet
- a secondary reference page for registries, manifests, and logs
- advanced read-only access to panel figures, raw galleries, and lower-level artifact inspection

Page map:

- `Home / Overview`
- `Phase 1 Recipe Selection`
- `Mindoro B1 Primary Validation`
- `Mindoro Cross-Model Comparator`
- `DWH Phase 3C Transfer Validation`
- `Phase 4 Oil-Type and Shoreline Context`
- `Legacy 2016 Support Package`
- `Artifacts / Logs / Registries` as a secondary reference page
- `Trajectory Explorer` in advanced mode

Branding:

- drop a real logo into `ui/assets/logo.svg` or `ui/assets/logo.png`
- optional sidebar/browser icon: `ui/assets/logo_icon.png` or `ui/assets/logo_icon.svg`
- if no logo is present, the UI falls back to text-only branding without breaking
- see [docs/UI_BRANDING.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/UI_BRANDING.md) or [ui/assets/README.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/ui/assets/README.md)

## Safe Phase Commands

Read-only utilities:

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase4_crossmodel_comparability_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2021 -e PIPELINE_PHASE=prototype_pygnome_similarity_summary pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
```

Mindoro support oil-type and shoreline workflow:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
```

Intentional scientific reruns:

```bash
docker-compose exec -T -e WORKFLOW_MODE=phase1_regional_2016_2022 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_run pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_ensemble_comparison pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_dwh_pygnome_comparator gnome python -m src
```

Experimental/support reruns:

```bash
.\start.ps1 -Entry phase1_mindoro_focus_pre_spill_experiment
docker-compose exec -e WORKFLOW_MODE=phase1_mindoro_focus_pre_spill_2016_2023 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=phase1_mindoro_focus_pre_spill_2016_2023 -e PIPELINE_PHASE=phase1_production_rerun -e INPUT_CACHE_POLICY=reuse_if_valid -e FORCING_SOURCE_BUDGET_SECONDS=120 -e FORCING_OUTAGE_POLICY=continue_degraded pipeline python -m src
```

The launcher command and the direct `docker-compose exec` form without `-T` are the interactive startup-prompt paths. The `-T` form stays non-interactive by design and now prints the resolved startup policy so you can see whether it used explicit env values or silent defaults.

## Scientific Boundaries To Keep

- Do not treat the old three-case prototype logic as the final Phase 1 study.
- Treat `2016-2022` as a pre-2023 historical calibration window by design; do not defend it by claiming 2023 drifters were absent unless that is separately verified.
- Prototype prep now pads the legacy 72 h forcing window by `+/- 3 h` so the preserved ensemble jitter does not run off the edge of the prepared currents/winds.
- Interactive `.\start.ps1 -Entry ...` runs now ask once for the forcing wait budget and, when eligible input caches already exist, whether to reuse validated local inputs or force a refresh. Direct interactive `python -m src` runs inside the container do the same once per run when you omit `-T`; non-interactive runs stay prompt-free and print the resolved startup policy at process start for promptable phases.
- The shared input-cache env is now `INPUT_CACHE_POLICY=default|reuse_if_valid|force_refresh`. Silent non-interactive defaults resolve to `reuse_if_valid`, while `PREP_FORCE_REFRESH=1` remains a backward-compatible alias when the new env is unset.
- Prep is now cache-first across workflows: if the canonical same-case drifter/forcing/ArcGIS input already exists locally and still validates for the requested window, the repo reuses it instead of re-downloading unless `INPUT_CACHE_POLICY=force_refresh` is selected.
- The dedicated historical Phase 1 reruns now also persist their monthly drifter and forcing ingests under `data/historical_validation_inputs/<workflow_mode>/...` so similar reruns can reuse prepared local inputs; the old `output/.../_scratch` monthly stores remain legacy backfill only, and each rerun now records the active local store in `phase1_local_input_inventory.csv`.
- Forcing-only outage handling is now explicit: `FORCING_OUTAGE_POLICY=default|continue_degraded|fail_hard`. By default, scientific/reportable lanes fail hard, while appendix/legacy/experimental lanes may skip the affected forcing-dependent branch or recipe subset with manifest honesty fields and `rerun_required=true`. This does not apply to drifter truth or ArcGIS/observation truth acquisition.
- Forcing providers now also fail fast under `FORCING_SOURCE_BUDGET_SECONDS` with a default per-provider wall-clock budget of `300` seconds. Set `FORCING_SOURCE_BUDGET_SECONDS=0` only when you intentionally want the old wait-forever debugging behavior.
- Keep historical/regional transport validation separate from spill-case validation.
- Treat `Phase 3B` and `Phase 3C` as validation-only lanes.
- Keep the drifter-debug lanes separate from the support-only `phase4_oiltype_and_shoreline` workflow outside `prototype_2016`.
- Keep `prototype_2016` framed as legacy `Phase 1 / 2 / 3A / 4 / 5` support only, with no thesis-facing 3B/3C lane.
- Keep the Mindoro-focused Phase 1 rerun as the active Mindoro-specific recipe-provenance path; do not treat it as direct drifter ingestion inside Phase 3B or as a rewrite of stored B1 raw-generation history.
- For `prototype_2016`, treat the selected drifter-of-record start lat/lon/time as the authoritative release origin. Some legacy audit fields may still point at `data/arcgis/CASE_2016-*/source_point_metadata.geojson` for compatibility, but that file is not the release geometry used by the run.
- Do not auto-promote `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` over `config/phase1_baseline_selection.yaml`.
- Do not describe DWH Phase 3C as using the Phase 1 drifter-selected baseline for forcing selection; DWH uses a readiness-gated HYCOM GOFS 3.1 + ERA5 + CMEMS wave/Stokes stack.
- Do not ingest new drifter datasets into the thesis-facing DWH lane or recast DWH as a second local Phase 1 calibration study.
- Do not treat DWH observed masks as anything other than truth, and do not treat PyGNOME as anything other than comparator-only.
- Do not invent exact sub-daily DWH observation acquisition times; keep DWH claims date-composite honest.
- Do not present DWH as the main Philippine thesis case; keep Mindoro primary and DWH separate.
- Do not relabel thresholded ensemble products: `mask_p50` and `mask_p90` semantics are unchanged.
- Do not mix Phase 4 oil-type sensitivity into Phase 2 or Phase 3 baseline products.
- Do not pretend Phase 1 or Phase 2 are fully frozen when they are not.
- Do not silently rewrite the original March 3 -> March 6 Mindoro case definition when discussing the promoted March 13 -> March 14 row.
- Do not claim independent day-to-day validation for March 13 -> March 14 while both NOAA/NESDIS public products still cite the same March 12 WorldView-3 imagery.

## Main Output Areas

- [output/phase1_finalization_audit](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase1_finalization_audit): read-only Phase 1 architecture audit.
- [output/phase2_finalization_audit](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase2_finalization_audit): read-only Phase 2 semantics/manifests audit.
- [output/phase1_production_rerun](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase1_production_rerun): dedicated 2016-2022 historical/regional Phase 1 rerun outputs, including the consolidated candidate-segment registry, loading audit, recipe ranking, manifest, and staged candidate baseline artifact.
- [output/prototype_2021_pygnome_similarity](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/prototype_2021_pygnome_similarity): preferred accepted-segment debug support package for the two fixed 2021 deterministic OpenDrift-vs-PyGNOME benchmark cases, including summary tables, per-forecast singles, side-by-side boards, and support-only captions/registry files built from stored benchmark rasters only.
- [output/prototype_2016_pygnome_similarity](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/prototype_2016_pygnome_similarity): consolidated legacy/debug Phase 3A comparator package for the three 2016 prototype benchmark cases, including deterministic plus `p50`/`p90` OpenDrift support tracks, deterministic PyGNOME comparator views, per-forecast singles, multi-track boards, case-local drifter-centered context, and support-only captions/registry files.
- [output/2016 Legacy Runs FINAL Figures](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/2016%20Legacy%20Runs%20FINAL%20Figures): authoritative curated `prototype_2016` legacy Phase 5 package. The root keeps the older flat per-case figure exports for compatibility, but the thesis-facing browse path is now the structured package under `publication/phase3a/`, `publication/phase4/`, `publication/phase4_comparator/`, `scientific_source_pngs/`, `summary/`, `manifests/`, and `phase5/`. It is support-only, keeps Phase 3A comparator-only, treats Phase 4 as weathering/fate from the drifter-of-record start, and may package a budget-only deterministic PyGNOME Phase 4 comparator pilot while keeping shoreline comparison explicitly unavailable. Its manifests now also record the shared first-code search box and the three original source boxes as historical-origin metadata for the first three 2016 drifter cases, without changing the stored per-case local extents. It has no thesis-facing Phase 3B or Phase 3C.
- [output/CASE_MINDORO_RETRO_2023](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/CASE_MINDORO_RETRO_2023): official Mindoro deterministic, ensemble, and scoring outputs.
- [output/CASE_DWH_RETRO_2010_72H](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/CASE_DWH_RETRO_2010_72H): DWH transfer-validation outputs.
- [output/phase4/CASE_MINDORO_RETRO_2023](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase4/CASE_MINDORO_RETRO_2023): Mindoro support-layer oil budgets, shoreline arrival timing, shoreline segments, oil-type comparison, and verdict bundle.
- [output/phase4_crossmodel_comparability_audit](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/phase4_crossmodel_comparability_audit): read-only verdict on whether current Phase 4 OpenDrift outputs can be compared honestly to the repo's existing PyGNOME artifacts.
- [output/final_validation_package](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/final_validation_package): frozen thesis validation package reused by later packaging work.
- [output/Phase 3B March13-14 Final Output](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/Phase%203B%20March13-14%20Final%20Output): read-only curated export of the promoted B1 family, including publication figures, canonical scientific source PNGs, summary artifacts, and a local export manifest.
- [output/Phase 3C DWH Final Output](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/Phase%203C%20DWH%20Final%20Output): read-only curated export of the frozen DWH Phase 3C family, including daily and event-corridor observation truth-context figures, deterministic footprint overlays, daily `mask_p50` / `mask_p90` / exact dual-threshold overview boards, daily OpenDrift-vs-PyGNOME overview boards including the three-row `mask_p50` / `mask_p90` / PyGNOME board, PyGNOME comparator figures, comparison scorecards/notes, canonical scientific source PNGs, and local registries/manifests.
- [output/final_reproducibility_package](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/final_reproducibility_package): read-only reproducibility, manifest, output, log, status, and launcher/package support indexes.
- [output/trajectory_gallery](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/trajectory_gallery): static technical gallery of trajectories, overlays, comparison maps, and Mindoro Phase 4 support figures.
- [output/trajectory_gallery_panel](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/trajectory_gallery_panel): polished panel-ready figure boards with captions, locator insets, and talking points.
- [output/figure_package_publication](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/output/figure_package_publication): canonical generic publication-grade figure package with paper-ready singles, side-by-side comparison boards, a shared thesis study-box reference figure set with per-box geography panels, Phase 4 OpenDrift-only support figures, a Phase 4 deferred-comparison note figure, the support-only prototype family `K`, captions, and defense talking points. For `prototype_2016`, this generic package remains mainly a Phase 3A figure source; the authoritative 2016 Phase 4/Phase 5 packaging now lives in `output/2016 Legacy Runs FINAL Figures/`.
- [ui](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/ui): read-only local dashboard code that consumes the packaged outputs and figure registries.

## Git Hygiene

The repo now keeps bulky case output trees ignored by default, while allowing lightweight audit/package artifacts to remain trackable where appropriate:

- Phase 1 audit outputs
- Phase 2 audit outputs
- Mindoro Phase 4 support summary artifacts
- final validation package summaries
- final reproducibility package summaries

Large raw data, scientific raster stacks, NetCDF outputs, and bulk case rerun artifacts remain excluded.

## Documentation Map

- [docs/PHASE_STATUS.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/PHASE_STATUS.md)
- [docs/ARCHITECTURE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/ARCHITECTURE.md)
- [docs/DWH_METHODS_NOTE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/DWH_METHODS_NOTE.md)
- [docs/DWH_CONSISTENCY_CHECKLIST.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/DWH_CONSISTENCY_CHECKLIST.md)
- [docs/DWH_PHASE3C_FINAL.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/DWH_PHASE3C_FINAL.md)
- [docs/DWH_PHASE3C_FREEZE_SYNC_NOTE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/DWH_PHASE3C_FREEZE_SYNC_NOTE.md)
- [docs/OUTPUT_CATALOG.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/OUTPUT_CATALOG.md)
- [docs/FIGURE_GALLERY.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/FIGURE_GALLERY.md)
- [docs/QUICKSTART.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/QUICKSTART.md)
- [docs/UI_GUIDE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/UI_GUIDE.md)
- [docs/UI_BRANDING.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/UI_BRANDING.md)
- [docs/COMMAND_MATRIX.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/COMMAND_MATRIX.md)
- [docs/LAUNCHER_USER_GUIDE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/LAUNCHER_USER_GUIDE.md)
- [docs/MINDORO_PRIMARY_VALIDATION_MIGRATION.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/MINDORO_PRIMARY_VALIDATION_MIGRATION.md)
- [docs/METHODOLOGY_AMENDMENT_2016_MINDORO.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/METHODOLOGY_AMENDMENT_2016_MINDORO.md)
- [docs/MINDORO_PHASE1_PROVENANCE_FINAL.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/MINDORO_PHASE1_PROVENANCE_FINAL.md)
- [docs/PHASE4_COMPARATOR_DECISION.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/PHASE4_COMPARATOR_DECISION.md)
- [docs/MINDORO_TRACK_SEMANTICS_FINAL.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/MINDORO_TRACK_SEMANTICS_FINAL.md)
- [docs/PHASE1_REGIONAL_2016_2022_REFERENCE_NOTE.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/PHASE1_REGIONAL_2016_2022_REFERENCE_NOTE.md)

## Contact

For questions or issues, contact `marcpocong@gmail.com`.

## Status Stamp

- Last updated: 2026-04-14
- Current sync state: March 13 -> March 14 Mindoro B1 rerun, final-output export, and read-only package surfaces now reflect the official `cmems_gfs` recipe provenance
- Biggest remaining scientific follow-up: the broader `2016-2022` regional/reference Phase 1 lane remains separate from the finalized Mindoro-specific B1 provenance lane and is still not scientifically frozen as a completed multi-year study
