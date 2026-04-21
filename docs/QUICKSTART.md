# Quickstart

## 1. Start The Containers

Install Docker Desktop first, then run the setup from the repository root.

macOS / Linux:

```bash
cd ~/Documents/GitHub/Drifter-Thesis-TINKER-VERSION
[ -f .env ] || cp .env.example .env
docker compose up -d --build
docker compose ps
```

Windows PowerShell:

```powershell
cd C:\path\to\Drifter-Thesis-TINKER-VERSION
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d --build
docker compose ps
```

Use `docker compose` with current Docker Desktop. If you are on an older Compose v1 install, replace `docker compose` with `docker-compose`.

The guarded `.env` command does not overwrite an existing local environment file. On zsh/macOS, do not paste inline comments after the `cp` command; use the guarded command above.

## 2. List The Current Launcher Entries

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

These commands are the safe way to inspect the current launcher catalog before running anything.

On macOS or Linux with PowerShell 7 installed, use `pwsh ./start.ps1 -List -NoPause` and `pwsh ./start.ps1 -Help -NoPause`.

## 3. Use The Canonical Interactive Launcher Path

Run workflows through the launcher with:

```powershell
.\start.ps1 -Entry <entry_id>
```

Use `-NoPause` only when you intentionally want the launcher to finish without the final pause.

Read-only entries to use first:

```powershell
.\start.ps1 -Entry phase1_audit
.\start.ps1 -Entry phase2_audit
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
.\start.ps1 -Entry prototype_legacy_final_figures
```

## 4. Run Reportable Workflows Intentionally

These are the current reportable launcher entries:

```powershell
.\start.ps1 -Entry phase1_production_rerun
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry mindoro_reportable_core
.\start.ps1 -Entry dwh_reportable_bundle
```

Use them only when you want a deliberate rerun of the underlying science or reportable package.

## 5. Run Support And Archive Workflows Only When Needed

```powershell
.\start.ps1 -Entry mindoro_phase4_only
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle
.\start.ps1 -Entry phase1_mindoro_focus_pre_spill_experiment
.\start.ps1 -Entry mindoro_march13_14_phase1_focus_trial
.\start.ps1 -Entry mindoro_march6_recovery_sensitivity
.\start.ps1 -Entry mindoro_march23_extended_public_stress_test
.\start.ps1 -Entry prototype_2021_bundle
.\start.ps1 -Entry prototype_legacy_bundle
```

Compatibility note:

- `mindoro_march13_14_noaa_reinit_stress_test` still works, but it is a legacy alias only and is no longer the primary command wording.

## 6. Use The Canonical Prompt-Free Container Path For Scripts Or CI

```bash
docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
```

Common read-only examples:

```bash
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
```

Use [docs/COMMAND_MATRIX.md](/c:/Users/marcp/Downloads/drifter-validated-oilspill-forecasting-rc-v1.0/drifter-validated-oilspill-forecasting-rc-v1.0/docs/COMMAND_MATRIX.md) for the exact prompt-free phase sequences behind each multi-step launcher entry.

## 7. Launch The Read-Only Local Dashboard

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Then open `http://localhost:8501`.

If you want the UI to reflect the latest packaged read-only outputs first, refresh one or more of these entries before opening it:

```powershell
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
```

The dashboard stays read-only and does not expose scientific rerun controls.

## 8. Current Caution Notes

- Phase 1 dedicated `2016-2022` rerun outputs now exist and stage a candidate baseline, but the default spill-case baseline remains a manual promotion or trial decision.
- Phase 2 is scientifically usable, but not scientifically frozen.
- The frozen Mindoro March 3 -> March 6 base case remains in `config/case_mindoro_retro_2023.yaml`; the promoted March 13 -> March 14 R1 primary validation row is tracked in `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.
- The March 13 -> March 14 R0 archived baseline, March 6 B2, and March 3 -> March 6 B3 remain repo-preserved as archive-only provenance material rather than thesis-facing Mindoro validation rows.
- March 13 -> March 14 must keep the shared-imagery caveat explicit, so do not describe it as independent day-to-day validation.
- `Phase 3B` and `Phase 3C` are validation-only lanes.
- Outside `prototype_2016`, `phase4_oiltype_and_shoreline`, `phase5_sync`, the galleries, and the dashboard are support layers rather than main thesis phases.
- Phase 4 cross-model comparison is still deferred; the UI surfaces that status directly and does not fake Phase 4 PyGNOME comparison pages.
- Prototype mode remains for debugging and regression only.
