# Launcher User Guide

## Purpose

`start.ps1` is the current user-facing workflow entrypoint for this repo.

- Panel mode is the defense-safe default path.
- The full launcher is the researcher / audit path.
- The launcher groups entries by thesis role instead of one flat technical list.

## Current Startup Paths

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Entry <entry_id>
```

## Preferred Entry IDs

Main evidence:

- `phase1_mindoro_focus_provenance`
- `mindoro_phase3b_primary_public_validation`
- `dwh_reportable_bundle`
- `mindoro_reportable_core`

Support/context:

- `mindoro_phase4_only`
- `mindoro_appendix_sensitivity_bundle`

Archive/provenance:

- `phase1_regional_reference_rerun`
- `mindoro_march13_14_phase1_focus_trial`
- `mindoro_march6_recovery_sensitivity`
- `mindoro_march23_extended_public_stress_test`

Legacy/archive support:

- `prototype_legacy_final_figures`
- `prototype_2021_bundle`
- `prototype_legacy_bundle`

Read-only governance:

- `b1_drifter_context_panel`
- `phase1_audit`
- `phase2_audit`
- `final_validation_package`
- `phase5_sync`
- `trajectory_gallery`
- `trajectory_gallery_panel`
- `figure_package_publication`

## Compatibility Aliases

- `phase1_mindoro_focus_pre_spill_experiment` -> prefer `phase1_mindoro_focus_provenance`
- `phase1_production_rerun` -> prefer `phase1_regional_reference_rerun`
- `mindoro_march13_14_noaa_reinit_stress_test` -> compatibility alias only; prefer `mindoro_phase3b_primary_public_validation`

## Runtime Controls

- `FORCING_OUTAGE_POLICY=default|continue_degraded|fail_hard`
- `FORCING_SOURCE_BUDGET_SECONDS=<seconds>` with default `300`
- `INPUT_CACHE_POLICY=default|reuse_if_valid|force_refresh`

Interactive launcher runs ask once per entry for forcing wait budget and eligible input-cache reuse choices. Prompt-free container runs with `-T` do not ask those questions.

## How The UI Fits

The Streamlit UI remains outside the launcher matrix. Launch it directly:

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

If you want the freshest read-only packaging before opening it, refresh one or more of these first:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
```

## Final Guardrails

- Use launcher entry IDs and role groups as the primary workflow vocabulary.
- `B1` is the only main-text primary Mindoro validation row.
- March 13-14 keeps the shared-imagery caveat explicit.
- `Track A` and every PyGNOME branch remain comparator-only support.
- DWH stays a separate external transfer-validation story with observed masks as truth.
- Mindoro oil-type and shoreline outputs remain support/context only.
- `phase1_production_rerun` stages `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` only; it does not auto-overwrite `config/phase1_baseline_selection.yaml`.
- `prototype_2016` is legacy/archive support only; some internal package names may still contain Phase 4/Phase 5 labels, but those are not primary defended evidence.

## Where To Look Next

- [docs/COMMAND_MATRIX.md](docs/COMMAND_MATRIX.md)
- [docs/QUICKSTART.md](docs/QUICKSTART.md)
- [docs/UI_GUIDE.md](docs/UI_GUIDE.md)
