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

## Important Entry IDs

Read-only utilities:

- `phase1_audit`
- `phase2_audit`
- `final_validation_package`
- `phase5_sync`
- `trajectory_gallery`
- `trajectory_gallery_panel`
- `figure_package_publication`

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
- `mindoro_march13_14_noaa_reinit_stress_test` remains available only as a backward-compatible alias for the promoted B1 bundle plus the comparator lane.

Legacy/debug:

- `prototype_2021_bundle`
- `prototype_legacy_bundle`

`prototype_2021_bundle` is now the preferred debug/demo lane. It is frozen from the two accepted 2021 strict-gate drifter segments, uses only the official four-recipe Phase 1 family, and stops at the transport-core bundle: `prep -> 1_2 -> benchmark -> prototype_pygnome_similarity_summary`.

`prototype_legacy_bundle` remains available for backward-compatible regression work. It still attempts the modern GFS-backed prototype recipes as a best-effort legacy extension, and `3` / `3b` remain there only as legacy appendix/smoke follow-ons.

The preferred similarity package now writes to `output/prototype_2021_pygnome_similarity/`. The older `output/prototype_2016_pygnome_similarity/` package is preserved as a legacy artifact.

If you only need to rebuild that consolidated summary from existing benchmark outputs, run it directly:

```bash
docker-compose exec -T -e WORKFLOW_MODE=prototype_2021 -e PIPELINE_PHASE=prototype_pygnome_similarity_summary pipeline python -m src
```

## Current Guardrails

- Phase 1 now has a dedicated `phase1_production_rerun` entry that stages the 2016-2022 regional rerun outputs and a candidate baseline artifact without auto-overwriting `config/phase1_baseline_selection.yaml`.
- Phase 2 is scientifically usable, but not scientifically frozen.
- Phase 4 is scientifically reportable now for Mindoro, but inherited-provisional from the upstream Phase 1/2 state.
- The frozen Mindoro base case remains `config/case_mindoro_retro_2023.yaml`; promoting March 13 -> March 14 does not silently rewrite March 3 -> March 6 provenance.
- March 6 remains a legacy honesty-only row and should never be called the primary Mindoro validation row.
- March 13 -> March 14 must keep the shared-imagery caveat explicit, and PyGNOME remains comparator-only in that promoted lane.
- `prototype_2021` is the preferred accepted-segment debug lane, but it is still not the final Phase 1 study.
- `prototype_2016` remains backward-compatible and keeps the preserved `+/- 3 h` ensemble jitter by padding its prep window.
- `Phase 3A` is the deterministic OpenDrift-vs-PyGNOME transport benchmark, `Phase 3B` is observation-based scoring, and `Phase 4` is Oil-Type Fate and Shoreline Impact Analysis. Do not describe the prototype/debug lanes as if they prove Phase 4.
- The prototype similarity summary is comparator-only: deterministic OpenDrift control versus deterministic PyGNOME transport footprints and densities, plus support-only forecast figures built from those stored benchmark rasters. It is not a truth lane and not final Chapter 3 evidence.

## Not Implemented Yet

- interactive UI run controls
- deeper artifact filtering inside the UI
- DWH Phase 4 appendix pilot

The raw technical trajectory gallery, the polished panel-ready gallery, the publication-grade figure package, and the read-only local dashboard are all implemented. The remaining items above are still recorded as future work.
