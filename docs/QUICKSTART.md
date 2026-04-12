# Quickstart

## 1. Start The Containers

```bash
docker-compose up -d
```

## 2. Inspect The Launcher Safely First

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

These commands do not rerun expensive science. They show the current launcher catalog and the safe read-only entry IDs.

## 3. Refresh The Current Reproducibility Package

```powershell
.\start.ps1 -Entry phase5_sync -NoPause
```

Equivalent direct command:

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
```

## 4. Run The Read-Only Audit Utilities

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
```

## 5. Build The Figure Galleries

```powershell
.\start.ps1 -Entry trajectory_gallery -NoPause
.\start.ps1 -Entry trajectory_gallery_panel -NoPause
.\start.ps1 -Entry figure_package_publication -NoPause
```

These write to `output/trajectory_gallery/`, `output/trajectory_gallery_panel/`, and `output/figure_package_publication/` from existing outputs only and are safe for technical refreshes, non-technical board refreshes, and canonical defense/paper figure refreshes.

## 6. Launch The Read-Only Local Dashboard

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Then open `http://localhost:8501`.

Use panel-friendly mode first if you want:

- publication-grade recommended defense figures
- simple validation summaries
- Mindoro Phase 4 figures
- the explicit Phase 4 cross-model deferred-status page

Switch to advanced mode only when you need:

- panel and raw archive figures
- manifests and logs
- lower-level artifact inspection

The dashboard is read-only in this first version and does not expose scientific rerun controls.

## 7. Run Mindoro Phase 4 Only When Needed

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
```

This writes to `output/phase4/CASE_MINDORO_RETRO_2023/` and does not overwrite stored Phase 3 validation outputs.

## 8. Use Scientific Reruns Intentionally

Examples:

```powershell
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Entry mindoro_reportable_core -NoPause
.\start.ps1 -Entry dwh_reportable_bundle -NoPause
```

These are intentional rerun paths. Do not use them casually for status inspection.

## 9. Current Caution Notes

- Phase 1 is architecture-audited, but the final multi-year production rerun is still needed.
- Phase 2 is scientifically usable, but not scientifically frozen.
- The frozen Mindoro March 3 -> March 6 base case remains in `config/case_mindoro_retro_2023.yaml`; the promoted March 13 -> March 14 B1 row is tracked in `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.
- March 6 remains visible as a legacy honesty-only row, not as the primary Mindoro validation result.
- March 13 -> March 14 must keep the shared-imagery caveat explicit, so do not describe it as independent day-to-day validation.
- Phase 4 is reportable now for Mindoro, but inherited-provisional from the upstream Phase 1/2 status.
- Phase 4 cross-model comparison is still deferred; the UI surfaces that status directly and does not fake Phase 4 PyGNOME comparison pages.
- Prototype mode remains for debugging/regression only.
