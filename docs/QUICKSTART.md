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

## 2. Start With Panel Mode

Use panel mode first:

```powershell
.\panel.ps1
.\start.ps1 -Panel
```

Panel mode is the default review path. It stays review-only / stored-output-only unless you intentionally switch to researcher or audit reruns.

## 3. Inspect The Current Launcher Catalog

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -Help -NoPause
```

On macOS or Linux with PowerShell 7 installed, use `pwsh ./start.ps1 -List -NoPause` and `pwsh ./start.ps1 -Help -NoPause`.

## 4. Current Evidence Defaults

- Preferred focused B1 provenance entry: `phase1_mindoro_focus_provenance`
- Focused Mindoro Phase 1 selected recipe: `cmems_gfs`
- Official `B1` adopts `cmems_gfs`
- Broader regional/reference compatibility lane: `phase1_regional_reference_rerun`
- Compatibility alias only: `phase1_mindoro_focus_pre_spill_experiment`
- `B1` is the only main Philippine / Mindoro validation claim
- `Track A` and PyGNOME branches are comparator-only support
- DWH remains external transfer validation only
- Mindoro oil-type and shoreline outputs remain support/context only
- `prototype_2016` remains legacy/archive support only

## 5. Use The Canonical Interactive Launcher Path

Run workflows through:

```powershell
.\start.ps1 -Entry <entry_id>
```

Read-only entries to use first:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
.\start.ps1 -Entry phase1_audit
.\start.ps1 -Entry phase2_audit
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
.\start.ps1 -Entry prototype_legacy_final_figures
```

## 6. Run Scientific Reruns Intentionally

Main evidence reruns:

```powershell
.\start.ps1 -Entry phase1_mindoro_focus_provenance
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry dwh_reportable_bundle
.\start.ps1 -Entry mindoro_reportable_core
```

Support and archive reruns:

```powershell
.\start.ps1 -Entry mindoro_phase4_only
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle
.\start.ps1 -Entry phase1_regional_reference_rerun
.\start.ps1 -Entry mindoro_march13_14_phase1_focus_trial
.\start.ps1 -Entry mindoro_march6_recovery_sensitivity
.\start.ps1 -Entry mindoro_march23_extended_public_stress_test
.\start.ps1 -Entry prototype_2021_bundle
.\start.ps1 -Entry prototype_legacy_bundle
```

Compatibility aliases:

- `phase1_mindoro_focus_pre_spill_experiment` still works, but it is a compatibility alias for `phase1_mindoro_focus_provenance`.
- `phase1_production_rerun` still works as a compatibility/internal label for the broader regional/reference lane represented by `phase1_regional_reference_rerun`.
- `mindoro_march13_14_noaa_reinit_stress_test` still works, but it is a compatibility alias for `mindoro_phase3b_primary_public_validation`.

## 7. Use The Canonical Prompt-Free Container Path

```bash
docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
```

Common read-only examples:

```bash
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=panel_b1_drifter_context pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
```

## 8. Launch The Read-Only Local Dashboard

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Then open `http://localhost:8501`.

If you want the UI to reflect the latest packaged read-only outputs first, refresh one or more of these entries before opening it:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
.\start.ps1 -Entry figure_package_publication
```

## 9. Current Guardrails

- `B1` is the only main-text primary Mindoro validation row.
- March 13-14 keeps the shared-imagery caveat explicit and must not be described as independent day-to-day validation.
- `Track A` and every PyGNOME branch remain comparator-only support.
- DWH remains a separate external transfer-validation story.
- Mindoro oil-type and shoreline outputs remain support/context only.
- `prototype_2016` is legacy/archive support only; some internal package names may still contain Phase 4/Phase 5 labels, but those are not primary defended evidence.
- The dashboard stays read-only and does not expose scientific rerun controls.
