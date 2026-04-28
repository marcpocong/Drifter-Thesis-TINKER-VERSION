# Panel Review Guide

This panel mode verifies the stored thesis-facing outputs against the manuscript. It does not rerun expensive scientific simulations by default. Full scientific reruns remain available through the advanced launcher for audit purposes.

## 1. What the software does

This repository packages a read-only defense and review surface over stored thesis outputs for:

- Mindoro `B1` public-observation validation
- Mindoro `Track A` OpenDrift-vs-PyGNOME comparator support
- DWH external transfer validation
- Mindoro oil-type and shoreline support/context outputs
- Final validation, reproducibility, and publication figure packaging

The panel path is meant for inspection and verification, not for fresh scientific reruns.

## 2. What the panel can safely run

Recommended startup:

```powershell
.\panel.ps1
```

Equivalent startup:

```powershell
.\start.ps1 -Panel
```

Safe panel actions:

- Open read-only dashboard
- Verify paper numbers against stored scorecards
- Rebuild publication figures from stored outputs only
- Refresh final validation package from stored outputs only
- Refresh final reproducibility package / command documentation from stored outputs only
- Show paper-to-output registry

## 3. What results should match the paper

The panel verification script checks stored outputs for:

- Mindoro `B1` FSS, mean FSS, forecast/observed cells, distance diagnostics, IoU, and Dice
- Mindoro `Track A` OpenDrift and PyGNOME comparator summary values
- DWH event-corridor FSS and IoU values for `C1`, `C2 p50`, `C2 p90`, and `C3`
- Mindoro oil-type support percentages and first shoreline-arrival times, when stored in machine-readable outputs

Verification outputs are written only to:

- `output/panel_review_check/panel_results_match_check.csv`
- `output/panel_review_check/panel_results_match_check.json`
- `output/panel_review_check/panel_results_match_check.md`
- `output/panel_review_check/panel_review_manifest.json`

## 4. Which commands are read-only

Read-only panel entrypoints:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

Read-only dashboard:

```powershell
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Read-only package and sync commands available from the launcher:

```powershell
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry figure_package_publication
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
```

## 5. Which commands are expensive scientific reruns

These remain available through the advanced launcher and are not part of the default panel path:

```powershell
.\start.ps1 -Entry phase1_production_rerun
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry mindoro_reportable_core
.\start.ps1 -Entry dwh_reportable_bundle
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle
```

These commands can rerun stored science or expensive validation phases and are therefore researcher/audit paths, not default defense paths.

## 6. Why PyGNOME is comparator-only

PyGNOME is shown as a comparator because:

- it is useful for same-case cross-model discussion;
- it is not the observational truth source;
- it does not reproduce the exact OpenDrift forcing and wave/Stokes stack used in the main thesis-facing OpenDrift lanes.

This matters in both Mindoro `Track A` and DWH `C3`.

## 7. Why B1 is the only main Mindoro validation row

`B1` is the promoted March 13 -> March 14 `R1_previous` public-observation validation row.

It is the only thesis-facing main Mindoro validation row because:

- it is the manuscript-facing primary validation claim;
- the March 13 -> March 14 `R0` branch is preserved for archive/provenance only;
- the older March-family rows remain honesty/provenance support, not replacements for `B1`.

## 8. Why the March 13-14 B1 pair has a shared-imagery caveat

The March 13 and March 14 NOAA/NESDIS products cite the same March 12 WorldView-3 imagery. That means:

- the pair is still useful as a reinitialization-based validation row;
- it should not be overclaimed as a fully independent day-to-day validation pair.

The panel mode keeps that caveat visible on purpose.

## 9. Why the 5,000-element personal experiment is excluded

The 5,000-element personal experiment is excluded from the default panel-facing mode because:

- it is not part of the thesis-facing frozen result set;
- it is experimental/sensitivity work rather than the canonical defense path;
- including it in the default menu would blur the boundary between main results and personal experimentation.

It remains outside the default panel registry and menu.

## 10. How to open the full advanced launcher

From panel mode, choose:

- `A. Open full research launcher`

Or start directly with:

```powershell
.\start.ps1
```

## Defense-friendly summary

- Start with `.\panel.ps1`.
- Use option `1` for the dashboard.
- Use option `2` to verify paper numbers.
- Use options `3`, `4`, and `5` only for packaging/sync refreshes from stored outputs.
- Use `A` only when the panel intentionally wants the full research/developer launcher.
