# Launcher User Guide

## Purpose

`start.ps1` is now the honest current entrypoint for this repo. It reads `config/launcher_matrix.json` and separates:

- scientific/reportable rerun tracks
- sensitivity/appendix tracks
- read-only packaging/help utilities
- legacy prototype tracks

It no longer hides everything behind one stale "Mindoro full workflow" story.

## Safe First Steps

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Entry phase5_sync -NoPause
.\start.ps1 -Entry trajectory_gallery -NoPause
.\start.ps1 -Entry trajectory_gallery_panel -NoPause
.\start.ps1 -Entry figure_package_publication -NoPause
.\start.ps1 -Entry prototype_legacy_final_figures -NoPause
```

These commands are the safest starting point because they do not trigger full scientific reruns by default.

## Read-Only Dashboard

The local dashboard is implemented, but it is intentionally kept outside the launcher entry catalog in this first version. Launch it directly:

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

This keeps the launcher honest: it remains a matrix-driven workflow launcher, while the UI remains a separate read-only exploration surface over the packaged outputs.

## Main Parameters

- `-List`: print the current launcher catalog grouped by category
- `-Help`: print usage guidance and current project guardrails
- `-Entry <entry_id>`: run one launcher entry from the matrix
- `-NoPause`: skip the final pause so the command can be used in scripted or CI-style runs

Important runtime env override:

- `FORCING_OUTAGE_POLICY=default|continue_degraded|fail_hard`
- `FORCING_SOURCE_BUDGET_SECONDS=<seconds>` with default `300`; set `0` only to disable the fail-fast forcing timeout for debugging
- `INPUT_CACHE_POLICY=default|reuse_if_valid|force_refresh`

Interactive launcher runs now ask once per entry for:

- the forcing wait budget, when the entry can hit forcing providers
- whether to reuse validated local input caches or force refresh, but only when eligible input caches already exist

Non-interactive launcher runs such as `-NoPause` default silently to:

- `FORCING_SOURCE_BUDGET_SECONDS=300`
- `INPUT_CACHE_POLICY=reuse_if_valid`

Direct interactive `python -m src` runs in the container follow the same one-time prompt flow when you omit `-T` from `docker-compose exec`. Prompt-free direct runs keep the same silent defaults and now print the resolved startup policy at process start for promptable phases.

## Important Entry IDs

Read-only utilities:

- `phase1_audit`
- `phase2_audit`
- `final_validation_package`
- `phase5_sync`
- `trajectory_gallery`
- `trajectory_gallery_panel`
- `figure_package_publication`
- `prototype_legacy_final_figures`

Intentional scientific reruns:

- `phase1_production_rerun`
- `mindoro_phase3b_primary_public_validation`
- `mindoro_reportable_core`
- `mindoro_phase4_only`
- `dwh_reportable_bundle`

`phase1_production_rerun` is intentionally expensive and stages `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` only. It does not auto-overwrite `config/phase1_baseline_selection.yaml`.
`mindoro_phase3b_primary_public_validation` is the canonical March 13 -> March 14 Phase 3B public-validation entry. It preserves the original March 3 -> March 6 case YAML and relies on the separate amendment file `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.

Appendix and sensitivity:

- `mindoro_appendix_sensitivity_bundle`
- `phase1_mindoro_focus_pre_spill_experiment`
- `mindoro_march13_14_noaa_reinit_stress_test` remains available only as a backward-compatible alias for the promoted B1 bundle plus the comparator lane.

`phase1_mindoro_focus_pre_spill_experiment` is the preferred interactive path for the experimental Mindoro-focused pre-spill rerun. Use `.\start.ps1 -Entry phase1_mindoro_focus_pre_spill_experiment` if you want the launcher to ask once about cache reuse and forcing wait budget, or run `docker-compose exec -e WORKFLOW_MODE=phase1_mindoro_focus_pre_spill_2016_2023 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src` if you want the same prompt flow directly in the container.

Legacy/debug:

- `prototype_2021_bundle`
- `prototype_legacy_bundle`

`prototype_2021_bundle` is now the preferred debug/demo lane. It is frozen from the two accepted 2021 strict-gate drifter segments, uses only the official four-recipe Phase 1 family, and stops at the transport-core bundle: `prep -> 1_2 -> benchmark -> prototype_pygnome_similarity_summary`.

`prototype_legacy_bundle` remains available for backward-compatible regression work. It preserves the earliest prototype stage of the study, when the ingestion-and-validation pipeline was first exercised on 2016 drifter records in the Palawan-side western Philippine context before the study widened toward the broader west-coast Palawan/Mindoro context. Its visible thesis-facing support flow is now `prep -> 1_2 -> benchmark -> prototype_pygnome_similarity_summary -> prototype_legacy_phase4_weathering -> prototype_legacy_final_figures`, which corresponds to legacy `Phase 1 -> Phase 2 -> Phase 3A -> Phase 4` plus the read-only final paper-figure export and separate `phase5_sync` packaging continuity. In methodological terms, that lane carries the early pipeline from drifter-driven Phase 1 and Phase 2 validation into a Phase 3A OpenDrift-versus-deterministic-PyGNOME comparator check using fraction skill score, then into legacy Phase 4 packaging. A non-zero FSS there means only that the ensemble footprint was not completely disjoint from the deterministic PyGNOME forecast; it does not make PyGNOME truth or elevate this lane into final proof. It still attempts the modern GFS-backed prototype recipes as a best-effort legacy extension, but there is no thesis-facing `Phase 3B` or `Phase 3C` in this 2016 lane.
Within that legacy lane, the spill origin comes from the selected drifter-of-record start in `data/drifters/CASE_2016-*/drifters_noaa.csv`. If a saved audit or manifest still mentions `data/arcgis/CASE_2016-*/source_point_metadata.geojson`, treat that as compatibility/provenance residue rather than as the actual release point used by the run.

The preferred similarity package now writes to `output/prototype_2021_pygnome_similarity/`. The older `output/prototype_2016_pygnome_similarity/` package is preserved as a legacy artifact. The curated prototype_2016 paper set now writes separately to `output/2016 Legacy Runs FINAL Figures/`, while `output/figure_package_publication/` remains the canonical generic publication package.

If you only need to rebuild that consolidated summary from existing benchmark outputs, run it directly:

```bash
docker-compose exec -T -e WORKFLOW_MODE=prototype_2021 -e PIPELINE_PHASE=prototype_pygnome_similarity_summary pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
```

## Current Guardrails

- Phase 1 now has a dedicated `phase1_production_rerun` entry that stages the 2016-2022 regional rerun outputs and a candidate baseline artifact without auto-overwriting `config/phase1_baseline_selection.yaml`.
- Forcing-only outages now follow one shared policy surface. Reportable/scientific lanes fail hard by default; appendix, legacy prototype, and explicitly experimental lanes can continue in degraded mode with explicit honesty fields and rerun-required flags.
- The launcher now surfaces fail-fast forcing budget details when a degraded skip happens, including the budget, elapsed time, whether the budget was exhausted, and the provider failure stage.
- Startup prompting now resolves the run-level wait budget and input-cache policy once per entry, then passes those values to every child phase so the pipeline does not re-prompt mid-run.
- Direct `docker-compose exec -T ...` runs stay non-interactive by design. They now print the resolved startup policy and point to the matching launcher entry or non-`-T` direct form when you would rather be asked.
- Direct truth inputs stay strict. Remote drifter inputs and ArcGIS/observation truth inputs do not use degraded continuation.
- The launcher now treats the standardized degraded forcing-skip exit as non-fatal for allowed appendix/legacy/experimental entries and continues to the next step after printing the skip reason.
- Phase 2 is scientifically usable, but not scientifically frozen.
- Phase 4 is scientifically reportable now for Mindoro, but inherited-provisional from the upstream Phase 1/2 state.
- The frozen Mindoro base case remains `config/case_mindoro_retro_2023.yaml`; promoting March 13 -> March 14 does not silently rewrite March 3 -> March 6 provenance.
- March 6 remains a legacy honesty-only row and should never be called the primary Mindoro validation row.
- March 13 -> March 14 must keep the shared-imagery caveat explicit, and PyGNOME remains comparator-only in that promoted lane.
- `prototype_2021` is the preferred accepted-segment debug lane, but it is still not the final Phase 1 study.
- `prototype_2016` remains backward-compatible and keeps the preserved `+/- 3 h` ensemble jitter by padding its prep window.
- `prototype_2016` is thesis-facing only as legacy `Phase 1 / 2 / 3A / 4`, with `phase5_sync` separate and no thesis-facing `3B` or `3C`.
- `prototype_2016` release origin is the selected drifter-of-record start, not the stale `source_point_metadata.geojson` point that can still appear in some compatibility fields.
- `Phase 3A` is the transport comparator lane, `Phase 3B` is observation-based scoring, and `Phase 4` is Oil-Type Fate and Shoreline Impact Analysis. Do not describe the prototype/debug lanes as if they prove official Phase 3B or official Phase 4 validation.
- The legacy prototype similarity summary is comparator-only: deterministic plus support-only `p50`/`p90` OpenDrift tracks versus deterministic PyGNOME transport footprints and densities. It is not a truth lane and not final Chapter 3 evidence.

## Not Implemented Yet

- interactive UI run controls
- deeper artifact filtering inside the UI
- DWH Phase 4 appendix pilot

The raw technical trajectory gallery, the polished panel-ready gallery, the publication-grade figure package, and the read-only local dashboard are all implemented. The remaining items above are still recorded as future work.
