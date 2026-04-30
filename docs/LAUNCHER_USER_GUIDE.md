# Launcher User Guide

## Purpose

`start.ps1` is the current user-facing workflow entrypoint for this repo.

- Panel mode is the defense-safe default path.
- The full launcher is the researcher / audit path.
- The launcher groups entries by thesis role instead of one flat technical list.
- Panel mode and read-only entries do not rerun science.

## Current Startup Paths

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -ExportPlan -NoPause
.\start.ps1 -Entry <entry_id>
.\start.ps1 -Entry <entry_id> -DryRun -NoPause
```

macOS with Homebrew:

```bash
brew install powershell
pwsh ./panel.ps1
pwsh ./start.ps1 -Panel
pwsh ./start.ps1
pwsh ./start.ps1 -List -NoPause
pwsh ./start.ps1 -Entry <entry_id>
pwsh ./start.ps1 -Entry <entry_id> -DryRun -NoPause
```

Use the stable Homebrew formula `powershell`; do not use the deprecated `powershell@preview` cask.

Linux uses the same `pwsh ./start.ps1 ...` command shape after PowerShell 7 is installed through that distribution's package manager.

## Shared Controls

- `B`, `BACK`, `0` go back when a previous menu exists.
- `C`, `CANCEL` cancel the current selection cleanly.
- `Q`, `QUIT`, `EXIT` leave the launcher cleanly.
- `H`, `HELP` open launcher help.
- `L`, `LIST` show the launcher catalog.
- `S`, `SEARCH` searches entry IDs, thesis roles, run kinds, categories, and notes.
- `P`, `PANEL` jump to the defense-safe panel path.
- `U`, `UI` open the read-only dashboard where that shortcut is available.
- `R`, `RESTART` restart the read-only dashboard where that shortcut is available.
- In a section menu, `X` opens inline inspect mode for visible menu numbers or entry IDs without running anything.
- Inline inspect mode stays inside the current section, shows a compact preview first, and accepts `M`, `MORE` to expand the most recent inspected entry to the full thesis-facing preview.
- After inspect/search preview, `E`, `EXPORT` writes `output/launcher_plans/<entry_id>.md` and `.json` without running science.
- Typing a hidden alias entry ID resolves to the canonical entry preview before any execution path.
- Pressing `Enter` at an execution confirmation prompt cancels cleanly with `Cancelled. No workflow was executed.`

## Preferred Entry IDs

Main evidence scientific reruns:

- `phase1_mindoro_focus_provenance`
- `mindoro_phase3b_primary_public_validation`
- `dwh_reportable_bundle`
- `mindoro_reportable_core`

Support/context reruns:

- `mindoro_phase4_only`
- `mindoro_appendix_sensitivity_bundle`

Archive/provenance reruns:

- `phase1_regional_reference_rerun`
- `mindoro_march13_14_phase1_focus_trial`
- `mindoro_march6_recovery_sensitivity`
- `mindoro_march23_extended_public_stress_test`

Legacy/archive support:

- `prototype_legacy_final_figures`
- `prototype_2021_bundle`
- `prototype_legacy_bundle`

Read-only governance:

- Read-only dashboard launch is a shortcut, not a launcher entry ID. Use panel option `1` or `U` / `UI`.
- Data sources and provenance registry is a read-only panel option `8`, backed by `docs/DATA_SOURCES.md`.
- `b1_drifter_context_panel`
- `phase1_audit`
- `phase2_audit`
- `final_validation_package`
- `phase5_sync`
- `trajectory_gallery`
- `trajectory_gallery_panel`
- `figure_package_publication`

## Compatibility Aliases / Hidden Legacy IDs

- `phase1_mindoro_focus_pre_spill_experiment` -> prefer `phase1_mindoro_focus_provenance`
- `phase1_production_rerun` -> prefer `phase1_regional_reference_rerun`
- `mindoro_march13_14_noaa_reinit_stress_test` -> hidden legacy ID that resolves to `mindoro_phase3b_primary_public_validation`; it does not run the Track A/PyGNOME comparator lane

## Runtime Controls

- `FORCING_OUTAGE_POLICY=default|continue_degraded|fail_hard`
- `FORCING_SOURCE_BUDGET_SECONDS=<seconds>` with default `300`
- `INPUT_CACHE_POLICY=default|reuse_if_valid|force_refresh`
- `LAUNCHER_DRY_RUN=1` or `-DryRun` for a no-Docker, no-output-modification command preview
- `-Explain <entry_id> -ExportPlan` for a no-Docker run-plan export under `output/launcher_plans/`

Interactive launcher runs ask once per entry for forcing wait budget and eligible input-cache reuse choices. Prompt-free container runs with `-T` do not ask those questions.

## Launcher Regression Tests

Run the launcher safety checks locally with:

```powershell
python -m pytest tests/test_start_ps1_interactive_navigation.py tests/test_launcher_menu_docs_consistency.py
```

The PowerShell subprocess tests skip clearly when `pwsh` is unavailable. They use dry-run or clean-cancel paths plus fake Docker commands on `PATH`, so they do not require Docker, network access, or scientific workflow execution.

## Dashboard Shortcut

The read-only dashboard is a launcher shortcut rather than a catalog entry ID. Open it from panel option `1`, `U` / `UI`, or launch it directly:

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Use `b1_drifter_context_panel` when you want the dashboard to land on the `B1 Drifter Provenance` page from stored outputs only.

If you want the freshest read-only packaging before opening the dashboard, refresh one or more of these first:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
```

## Final Guardrails

- Use launcher entry IDs and role groups as the primary workflow vocabulary.
- `B1` is the only main Philippine public-observation validation claim.
- `B1` supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.
- March 13-14 keeps the observation-independence note explicit.
- `Track A` and every PyGNOME branch remain comparator-only support.
- PyGNOME is never observational truth.
- DWH is external transfer validation, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs remain support/context only.
- Read-only dashboard, packaging, audit, and docs entries do not recompute science.
- `phase1_production_rerun` stages `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` only; it does not auto-overwrite `config/phase1_baseline_selection.yaml`.
- `prototype_2016` is legacy/archive support only; some internal package names may still contain Phase 4/Phase 5 labels, but those are not primary defended evidence.

## Where To Look Next

- [COMMAND_MATRIX.md](COMMAND_MATRIX.md)
- [DATA_SOURCES.md](DATA_SOURCES.md)
- [QUICKSTART.md](QUICKSTART.md)
- [UI_GUIDE.md](UI_GUIDE.md)
