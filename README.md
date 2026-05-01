# Drifter-Validated 24–72 h Oil-Spill Forecasting for Philippine Coasts: Probability Footprints and Oil-Type Fate

This repository is the panel-ready reproducibility package for the final manuscript. It preserves the stored Mindoro, DWH, oil-type, legacy-support, figure, and UI artifacts needed for review while keeping panel paths read-only and packaging-safe.

## Panel Review Start

Panel reviewers should start with either:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -Dashboard -NoPause
```

Panel mode opens the defense-safe launcher path. It reads stored outputs, opens review surfaces, checks package consistency, and rebuilds presentation packages from existing artifacts only.
The Streamlit dashboard is also read-only by default: its pages organize stored outputs only and do not create new scientific results. The dashboard shortcut prints `http://localhost:8501` after starting or reusing the local Streamlit process.

The full launcher stays available for intentional researcher or audit work, but it is grouped by role instead of presenting archive, support, legacy, and main evidence as one flat list:

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -ListRole archive_provenance -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
```

`-Explain` prints the label, manuscript section, thesis role, claim boundary, run kind, rerun cost, `safe_default`, role flags, expected outputs, and requested/canonical entry IDs for aliases before anything can run.

Expensive scientific reruns require an explicit full-launcher entry selection, such as:

```powershell
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause
```

Use `.\start.ps1 -List -NoPause` or `.\start.ps1 -Help -NoPause` to inspect the entry catalog before selecting any rerun.

## Current Final Manuscript Alignment

The concise source-of-truth bridge between the final paper and this repository is [docs/FINAL_PAPER_ALIGNMENT.md](docs/FINAL_PAPER_ALIGNMENT.md).

Evidence order:

1. Focused Mindoro Phase 1 transport provenance.
2. Phase 2 standardized deterministic and 50-member forecast products.
3. Primary Mindoro March 13-14 validation case (B1 internal alias).
4. Mindoro same-case OpenDrift-PyGNOME comparator (Track A internal alias).
5. DWH external transfer validation.
6. Mindoro oil-type and shoreline support/context.
7. Secondary 2016 drifter-track and legacy FSS support.
8. Reproducibility, governance, and read-only package layer.

No uploaded manuscript file is required to inspect, validate, or run the repo. The stored outputs, launcher matrix, registries, and alignment docs are the review surface.

## Scientific Guardrails

- The Primary Mindoro March 13-14 validation case is the only main Philippine public-observation validation claim; `B1` is an internal alias.
- The primary case supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.
- PyGNOME is comparator-only and never observation truth.
- DWH is external transfer validation only, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs are support/context only.
- Secondary 2016 drifter-track and legacy FSS outputs are support only, not public-spill validation.
- `mask_p50` is the preferred probabilistic footprint; `mask_p90` is a conservative support/comparison product only.

## What Is Archived And Why

Archived, experimental, legacy, and comparator-only outputs are preserved for provenance, auditability, and reproducibility. They are not promoted into the main paper claim, and they remain routed through archive/provenance or support launcher roles.

See [docs/ARCHIVE_GOVERNANCE.md](docs/ARCHIVE_GOVERNANCE.md) and [config/archive_registry.yaml](config/archive_registry.yaml).

## Data Sources And Provenance

External observation, drifter, forcing, shoreline, oil-property, and model/tool sources are documented in [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md). The machine-readable source registry is [config/data_sources.yaml](config/data_sources.yaml).
In the dashboard, the default navigation begins with `Overview / Final Manuscript Alignment`, then `Data Sources & Provenance`, and then the focused Mindoro provenance, Primary Mindoro March 13-14 validation case, same-case comparator, DWH external transfer validation, oil-type/shoreline support, secondary 2016 support, archive/provenance, and governance pages.

## Panel Documentation

- [PANEL_QUICK_START.md](PANEL_QUICK_START.md)
- [docs/PANEL_REVIEW_GUIDE.md](docs/PANEL_REVIEW_GUIDE.md)
- [docs/FINAL_PAPER_ALIGNMENT.md](docs/FINAL_PAPER_ALIGNMENT.md)
- [docs/PAPER_TO_REPO_CROSSWALK.md](docs/PAPER_TO_REPO_CROSSWALK.md)
- [docs/UI_GUIDE.md](docs/UI_GUIDE.md)
- [docs/COMMAND_MATRIX.md](docs/COMMAND_MATRIX.md)
- [docs/LAUNCHER_USER_GUIDE.md](docs/LAUNCHER_USER_GUIDE.md)

## Read-Only Output Surfaces

- [output/phase1_mindoro_focus_pre_spill_2016_2023](output/phase1_mindoro_focus_pre_spill_2016_2023): focused Mindoro Phase 1 provenance artifacts.
- [output/Phase 3B March13-14 Final Output](output/Phase%203B%20March13-14%20Final%20Output): curated Primary Mindoro March 13-14 validation case (B1 alias) and Mindoro same-case OpenDrift-PyGNOME comparator (Track A alias) export layer for panel review. Raw `output/CASE_MINDORO_RETRO_2023` paths are rerun/staging provenance and are not the reviewer first-open folder in the submitted package.
- [output/Phase 3C DWH Final Output](output/Phase%203C%20DWH%20Final%20Output): curated DWH transfer-validation export layer.
- Raw `output/CASE_DWH_RETRO_2010_72H` paths are rerun/staging provenance and are not the reviewer first-open folder in the submitted package.
- [output/phase4/CASE_MINDORO_RETRO_2023](output/phase4/CASE_MINDORO_RETRO_2023): Mindoro oil-type and shoreline support/context outputs.
- [output/2016 Legacy Runs FINAL Figures](output/2016%20Legacy%20Runs%20FINAL%20Figures): curated secondary 2016 legacy-support package.
- [output/final_validation_package](output/final_validation_package): frozen review package for stored results and claim boundaries.
- [output/figure_package_publication](output/figure_package_publication): read-only publication and defense figure package.
- [ui](ui): read-only dashboard code.

## Contact

For questions or issues, contact `marcpocong@gmail.com`.
