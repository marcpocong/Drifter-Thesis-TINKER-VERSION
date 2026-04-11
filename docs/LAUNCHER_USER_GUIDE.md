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
docker-compose exec pipeline streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
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

- `mindoro_reportable_core`
- `mindoro_phase4_only`
- `dwh_reportable_bundle`

Appendix and sensitivity:

- `mindoro_appendix_sensitivity_bundle`

Legacy/debug:

- `prototype_legacy_bundle`

## Current Guardrails

- Phase 1 is architecture-audited, but the full 2016-2022 production rerun is still needed.
- Phase 2 is scientifically usable, but not scientifically frozen.
- Phase 4 is scientifically reportable now for Mindoro, but inherited-provisional from the upstream Phase 1/2 state.
- Prototype mode remains backward-compatible, but it is not the final Phase 1 study.

## Not Implemented Yet

- interactive UI run controls
- deeper artifact filtering inside the UI
- DWH Phase 4 appendix pilot

The raw technical trajectory gallery, the polished panel-ready gallery, the publication-grade figure package, and the read-only local dashboard are all implemented. The remaining items above are still recorded as future work.
