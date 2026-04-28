# Panel Review Guide

This guide is for panel members who want to inspect the stored thesis outputs, verify that the software matches the manuscript, and stay inside the final defended evidence boundaries.

The practical rule is simple: panel mode is for review, not for launching fresh science.

## 1. Start Here

Use either command:

```powershell
.\panel.ps1
.\start.ps1 -Panel
```

That opens the panel-safe launcher path instead of the full research launcher.

## 2. What The Panel Menu Safely Does

1. Open the read-only dashboard
2. Verify stored manuscript numbers against stored scorecards
3. Rebuild publication figures from stored outputs only
4. Inspect the focused Phase 1 drifter provenance behind `B1`
5. Refresh the final validation package from stored outputs only
6. Refresh the reproducibility / docs package from stored outputs only
7. Open the paper-to-output registry

None of those steps are meant to rerun expensive science.

## 3. Current Manuscript Evidence Order

1. Focused Mindoro Phase 1 provenance
2. Phase 2 standardized forecast products
3. Mindoro `B1` primary public-observation validation
4. Mindoro `Track A` same-case OpenDrift versus PyGNOME comparator-only support
5. DWH external transfer validation
6. Mindoro oil-type and shoreline support/context
7. `prototype_2016` legacy/archive support
8. Reproducibility / governance / read-only package layer

Panel mode is built to respect that order instead of flattening the repo into one undifferentiated list.

## 4. Key Claim Boundaries

- `B1` is the only main-text primary Mindoro validation row.
- March 13-14 keeps the shared-imagery caveat explicit.
- `Track A` is comparator-only support.
- PyGNOME is comparator-only and never the observational scoring reference.
- DWH is external transfer validation, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs are support/context only.
- `prototype_2016` is legacy/archive support only.
- The dashboard, publication package, and figure package are read-only presentation/governance surfaces.

## 5. Result Values Checklist

### Focused Phase 1 Provenance

- Workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Historical window: `2016-01-01` to `2023-03-02`
- Focused validation box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- Ranked February-April subset: `19`
- Recipe winner: `cmems_gfs`
- `cmems_gfs` mean / median NCS: `4.5886 / 4.6305`
- `cmems_era5` mean / median NCS: `4.6237 / 4.5916`
- `hycom_gfs` mean / median NCS: `4.7027 / 4.9263`
- `hycom_era5` mean / median NCS: `4.7561 / 5.0106`

### Mindoro `B1`

- FSS `1 / 3 / 5 / 10 km`: `0.0000 / 0.0441 / 0.1371 / 0.2490`
- Mean FSS: `0.1075`
- `R0`: did not reach target date; forecast cells `0`; observed cells `22`
- `R1_previous`: forecast cells `5`; observed cells `22`; nearest distance `1414.21 m`; centroid distance `7358.16 m`
- `IoU = 0.0`; `Dice = 0.0`
- Interpretation: `R1_previous` is promoted because it survives and is scoreable, not because it is an exact-grid match

### Mindoro `Track A`

- OpenDrift `R1_previous`: forecast cells `5`; nearest distance `1414.21 m`; mean FSS `0.1075`
- OpenDrift `R0`: forecast cells `0`; mean FSS `0.0000`
- PyGNOME deterministic comparator-only support: forecast cells `6`; nearest distance `6082.76 m`; FSS `0.0000 / 0.0000 / 0.0000 / 0.0244`; mean FSS `0.0061`

### DWH

- Case ID: `CASE_DWH_RETRO_2010_72H`
- Forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- `C1` event-corridor mean FSS: `0.5568`
- `C2 p50` event-corridor mean FSS: `0.5389`
- `C2 p90` event-corridor mean FSS: `0.4966`
- `C3` PyGNOME comparator event-corridor mean FSS: `0.3612`
- Interpretation: DWH remains an external transfer validation story and does not recalibrate Mindoro

### Mindoro Oil-Type / Shoreline Support

- Light oil: final beached fraction `0.02%`; first arrival `4 h`; impacted segments `11`; QC pass
- Fixed-base medium-heavy proxy: final beached fraction `0.61%`; first arrival `4 h`; impacted segments `10`; QC flagged
- Heavier oil: final beached fraction `0.63%`; first arrival `4 h`; impacted segments `11`; QC pass

## 6. What The Verification Step Writes

The panel verification step reads stored outputs and writes only to:

- `output/panel_review_check/panel_results_match_check.csv`
- `output/panel_review_check/panel_results_match_check.json`
- `output/panel_review_check/panel_results_match_check.md`
- `output/panel_review_check/panel_review_manifest.json`

## 7. Which Commands Are Panel-Safe

Main panel-safe entry points:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

Read-only / packaging-only launcher entries:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry figure_package_publication
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
```

Dashboard:

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

## 8. Inspecting Drifter Provenance Behind `B1`

Panel members can use panel option `7` or:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
```

That view stays stored-output-only.

- It explains the focused historical Phase 1 provenance behind the selected recipe.
- It does not turn drifters into the March 13-14 truth mask.
- It explicitly says when no direct March 13-14 2023 accepted drifter segment is stored for `B1`.

## 9. Why `B1` Is The Main Mindoro Row

`B1` is the promoted March 13-14 `R1_previous` row carried into the main validation argument.

- It is the only main-text primary Mindoro validation row.
- The March 13-14 `R0` branch is preserved for archive/provenance.
- The other March-family rows remain useful context, but they are not replacements for `B1`.

## 10. Why The Shared-Imagery Caveat Matters

The March 13 and March 14 public products both cite March 12 WorldView-3 imagery.

That means the row is still useful, but it must be described honestly as a reinitialization-based public-observation validation pair rather than as independent day-to-day validation.

## 11. Which Commands Are Researcher / Audit Reruns

These belong to the full launcher and are not the default defense path:

```powershell
.\start.ps1 -Entry phase1_mindoro_focus_provenance
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry dwh_reportable_bundle
.\start.ps1 -Entry mindoro_reportable_core
.\start.ps1 -Entry phase1_regional_reference_rerun
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle
```

Use them only when you intentionally want researcher or audit reruns.
