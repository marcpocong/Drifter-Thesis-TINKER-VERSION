# Drifter-Validated 24–72 h Oil-Spill Forecasting for Philippine Coasts: Probability Footprints and Oil-Type Fate

This repository is the panel-ready reproducibility and review package for the thesis workflow. It preserves the stored Mindoro, DWH, oil-type, publication, and UI artifacts used for manuscript review, while keeping panel mode and the presentation surfaces read-only unless a researcher intentionally launches a rerun.

## Panel / Defense Quick Start

Start with:

```powershell
.\panel.ps1
```

Or:

```powershell
.\start.ps1 -Panel
```

On macOS, install PowerShell 7 with Homebrew's formula and invoke the launcher through `pwsh`:

```bash
brew install powershell
cd ~/Documents/GitHub/Drifter-Thesis-TINKER-VERSION
pwsh ./panel.ps1
pwsh ./start.ps1 -Panel
```

If the containers are already up and you only need the dashboard:

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Panel mode and read-only entries do not rerun science. Use the full launcher only for intentional research or audit reruns.

See [PANEL_QUICK_START.md](PANEL_QUICK_START.md) and [docs/PANEL_REVIEW_GUIDE.md](docs/PANEL_REVIEW_GUIDE.md).

## Data Sources and Provenance

Panel reviewers can inspect the external observation, drifter, forcing, shoreline, oil-property, and model/tool sources in [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md). The machine-readable registry is [config/data_sources.yaml](config/data_sources.yaml), and the read-only Streamlit UI includes a `Data Sources & Provenance` reference page.

## Current Manuscript Alignment

1. Focused Mindoro Phase 1 provenance lane = historical drifter-based transport validation and recipe selection.
2. Phase 2 = standardized deterministic and 50-member machine-readable forecast product generation.
3. Mindoro `B1` = March 13-14 `R1_previous` primary public-observation validation row and the only main-text primary Philippine / Mindoro validation claim; it supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.
4. Mindoro `Track A` = same-case OpenDrift versus PyGNOME comparator-only support against the March 14 public mask; never the observational scoring reference.
5. `DWH` = separate external rich-data transfer validation lane; not Mindoro recalibration and not a second local Phase 1.
6. Mindoro oil-type and shoreline bundle = downstream support/context only; not a second primary validation phase and not a matched Mindoro OpenDrift-versus-PyGNOME fate / shoreline comparison.
7. `prototype_2016` = legacy/archive support only; useful for workflow history and preserved comparator/budget-only archive, but not direct public-spill validation and not part of the defended local validation claim.
8. Publication package, figure package, and UI = read-only presentation/governance surfaces; they organize stored outputs but do not create new scientific results.

## Key Stored Results for Panel Review

### Focused Mindoro Phase 1 Provenance

- Workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Historical window: `2016-01-01` to `2023-03-02`
- Focused validation box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- Ranked February-April subset: `19`
- Selected recipe: `cmems_gfs`

| Recipe | Mean NCS | Median NCS | Status |
| --- | ---: | ---: | --- |
| `cmems_gfs` | `4.5886` | `4.6305` | winner |
| `cmems_era5` | `4.6237` | `4.5916` | not selected |
| `hycom_gfs` | `4.7027` | `4.9263` | not selected |
| `hycom_era5` | `4.7561` | `5.0106` | not selected |

### Mindoro `B1` Primary Public-Observation Validation

- `B1` is the March 13-14 `R1_previous` row and the only main-text primary Mindoro validation claim; it supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.
- FSS: `1 km = 0.0000`, `3 km = 0.0441`, `5 km = 0.1371`, `10 km = 0.2490`, `mean = 0.1075`
- Branch diagnostics:
- `R0` did not reach the target date; forecast cells `0`; observed cells `22`
- `R1_previous` reached the target date; forecast cells `5`; observed cells `22`; nearest distance `1414.21 m`; centroid distance `7358.16 m`
- `R1_previous` is promoted because it is scoreable and survives to the target date, not because it is an exact-grid match
- `IoU = 0.0` and `Dice = 0.0`, so the row must not be described as exact overlap or exact 1 km success
- Caveat: March 13-14 is a reinitialization-based public-observation validation check. Both public products cite the same March 12 WorldView-3 imagery provenance, so it is not an independent day-to-day validation pair.

### Mindoro `Track A` Comparator Support Only

- Interpret `Track A` as same-case comparator-only support against the March 14 public mask, not as independent validation.
- OpenDrift `R1_previous`: forecast cells `5`; nearest distance `1414.21 m`; FSS `0.0000 / 0.0441 / 0.1371 / 0.2490`; mean FSS `0.1075`
- OpenDrift `R0`: forecast cells `0`; mean FSS `0.0000`
- PyGNOME deterministic comparator-only support: forecast cells `6`; nearest distance `6082.76 m`; FSS `0.0000 / 0.0000 / 0.0000 / 0.0244`; mean FSS `0.0061`

### DWH External Transfer Validation

- Case ID: `CASE_DWH_RETRO_2010_72H`
- Scientific forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- `C1` = deterministic external transfer validation
- `C2` = 50-member ensemble extension; `mask_p50` preferred, `mask_p90` support/comparison only
- `C3` = PyGNOME comparator-only
- Event-corridor mean FSS:
- `C1 deterministic = 0.5568`
- `C2 p50 = 0.5389`
- `C2 p90 = 0.4966`
- `C3 PyGNOME comparator = 0.3612`
- DWH remains an external transfer validation story and is not used to recalibrate the Mindoro baseline.

### Mindoro Oil-Type And Shoreline Support / Context

| Scenario | Final beached fraction | First arrival | Impacted segments | QC |
| --- | ---: | ---: | ---: | --- |
| light oil | `0.02%` | `4 h` | `11` | pass |
| fixed-base medium-heavy proxy | `0.61%` | `4 h` | `10` | flagged |
| heavier oil | `0.63%` | `4 h` | `11` | pass |

These values are support/context only, not primary validation evidence.

### Probability Semantics

- `prob_presence` = cellwise ensemble probability of presence
- `mask_p50` = probability of presence `>= 0.50`; preferred probabilistic footprint
- `mask_p90` = probability of presence `>= 0.90`; conservative support/comparison product only
- Never relabel `mask_p90` as a broader envelope.

## Current Launcher

The launcher entrypoint is [start.ps1](start.ps1), and the current entry catalog lives in [config/launcher_matrix.json](config/launcher_matrix.json).

- Panel mode is the defense-safe default.
- The full launcher is the researcher/audit path.
- Use launcher entry IDs and role groups as the user-facing vocabulary.
- Read-only dashboard launch is a shortcut, not a separate matrix entry ID. Use panel option `1` or `U` / `UI`.
- Panel option `8` opens the read-only data sources and provenance registry at `docs/DATA_SOURCES.md`.
- The README entry IDs below were checked against `config/launcher_matrix.json`.

### Preferred Entry IDs

- Main thesis evidence / reportable: `phase1_mindoro_focus_provenance`, `mindoro_phase3b_primary_public_validation`, `dwh_reportable_bundle`, `mindoro_reportable_core`
- Support/context and appendix: `mindoro_phase4_only`, `mindoro_appendix_sensitivity_bundle`
- Archive/provenance: `phase1_regional_reference_rerun`, `mindoro_march13_14_phase1_focus_trial`, `mindoro_march6_recovery_sensitivity`, `mindoro_march23_extended_public_stress_test`
- Read-only dashboard / packaging / audits / docs: `b1_drifter_context_panel`, `phase1_audit`, `phase2_audit`, `final_validation_package`, `phase5_sync`, `trajectory_gallery`, `trajectory_gallery_panel`, `figure_package_publication`
- Legacy/archive support: `prototype_legacy_final_figures`, `prototype_2021_bundle`, `prototype_legacy_bundle`

### Compatibility Aliases / Hidden Legacy IDs

- `phase1_mindoro_focus_pre_spill_experiment` -> prefer `phase1_mindoro_focus_provenance`
- `phase1_production_rerun` -> prefer `phase1_regional_reference_rerun`
- `mindoro_march13_14_noaa_reinit_stress_test` -> hidden legacy ID that resolves to `mindoro_phase3b_primary_public_validation`; it does not run the Track A/PyGNOME comparator lane

### Useful Commands

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -ExportPlan -NoPause
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause
```

macOS / Linux equivalents use `pwsh`:

```bash
pwsh ./panel.ps1
pwsh ./start.ps1 -Panel
pwsh ./start.ps1 -List -NoPause
pwsh ./start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause
```

Launcher controls:

- `B`, `BACK`, `0` go back when a previous launcher menu exists.
- `C`, `CANCEL` cancel the current selection cleanly.
- `Q`, `QUIT`, `EXIT` leave the launcher cleanly.
- `X`, `INSPECT` preview entries inline inside a launcher section without running them.
- `S`, `SEARCH` searches entry IDs, thesis roles, run kinds, categories, and notes.
- `E`, `EXPORT` after an inspect/search preview writes `output/launcher_plans/<entry_id>.md` and `.json` without running science.
- Hidden aliases still work, but they resolve to the canonical entry preview before execution.

## Scientific Guardrails

- `B1` is the only main Philippine public-observation validation claim.
- `B1` supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.
- Keep the March 13-14 shared-imagery caveat explicit.
- `Track A` and PyGNOME branches are comparator-only support, never the observational scoring reference.
- DWH is external transfer validation, not Mindoro recalibration, the main Philippine case, or a second local Phase 1.
- Do not treat Mindoro oil-type / shoreline outputs as primary validation.
- `prototype_2016` is legacy/archive support only; some internal package names may still contain Phase 4/Phase 5 labels, but those are not primary defended evidence.
- UI, publication packages, and figure packages are read-only presentation/governance surfaces built from stored outputs only.
- Do not claim universal operational accuracy.

## Read-Only Output Surfaces

- [output/phase1_mindoro_focus_pre_spill_2016_2023](output/phase1_mindoro_focus_pre_spill_2016_2023): focused Mindoro Phase 1 provenance artifacts
- [output/CASE_MINDORO_RETRO_2023](output/CASE_MINDORO_RETRO_2023): canonical Mindoro deterministic, ensemble, scoring, and support outputs
- [output/Phase 3B March13-14 Final Output](output/Phase%203B%20March13-14%20Final%20Output): curated `B1` plus Track A export layer
- [output/CASE_DWH_RETRO_2010_72H](output/CASE_DWH_RETRO_2010_72H): canonical DWH scientific outputs
- [output/Phase 3C DWH Final Output](output/Phase%203C%20DWH%20Final%20Output): curated DWH transfer-validation export layer
- [output/phase4/CASE_MINDORO_RETRO_2023](output/phase4/CASE_MINDORO_RETRO_2023): Mindoro oil-type and shoreline support/context outputs
- [output/2016 Legacy Runs FINAL Figures](output/2016%20Legacy%20Runs%20FINAL%20Figures): curated `prototype_2016` legacy/archive support package
- [output/final_validation_package](output/final_validation_package): frozen review package for stored results and claim boundaries
- [output/figure_package_publication](output/figure_package_publication): read-only publication and defense figure package
- [ui](ui): read-only dashboard code

## Documentation Map

- [PANEL_QUICK_START.md](PANEL_QUICK_START.md)
- [docs/PANEL_REVIEW_GUIDE.md](docs/PANEL_REVIEW_GUIDE.md)
- [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md)
- [docs/PHASE_STATUS.md](docs/PHASE_STATUS.md)
- [docs/COMMAND_MATRIX.md](docs/COMMAND_MATRIX.md)
- [docs/THESIS_SURFACE_GOVERNANCE.md](docs/THESIS_SURFACE_GOVERNANCE.md)
- [docs/MINDORO_PHASE1_PROVENANCE_FINAL.md](docs/MINDORO_PHASE1_PROVENANCE_FINAL.md)
- [docs/MINDORO_TRACK_SEMANTICS_FINAL.md](docs/MINDORO_TRACK_SEMANTICS_FINAL.md)
- [docs/OUTPUT_CATALOG.md](docs/OUTPUT_CATALOG.md)
- [docs/QUICKSTART.md](docs/QUICKSTART.md)
- [docs/UI_GUIDE.md](docs/UI_GUIDE.md)
- [docs/LAUNCHER_USER_GUIDE.md](docs/LAUNCHER_USER_GUIDE.md)
- [docs/README_FINALIZATION_CHECKLIST.md](docs/README_FINALIZATION_CHECKLIST.md)

## Contact

For questions or issues, contact `marcpocong@gmail.com`.

## Status Stamp

- Last updated: `2026-04-29`
- Current sync state: aligned to current manuscript evidence structure and stored-output review package
- Biggest remaining scientific follow-up: broader `2016-2022` regional/reference Phase 1 lane remains separate from finalized Mindoro-specific B1 provenance and is not the main Mindoro validation claim
