# UI Guide

## Purpose

The local Streamlit app is a read-only thesis presentation layer over the artifacts that already exist in this repo. It does not rerun science, does not mutate outputs, and does not expose write-back controls.

## Launch Command

Start the pipeline container if needed:

macOS / Linux:

```bash
[ -f .env ] || cp .env.example .env
docker compose up -d pipeline
```

Windows PowerShell:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d pipeline
```

Launch the UI:

```bash
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Open:

```text
http://localhost:8501
```

Use `docker compose` with current Docker Desktop. If you are on an older Compose v1 install, replace `docker compose` with `docker-compose`.

## How It Fits With The Launcher

- The UI is intentionally not a launcher entry.
- List current workflow entries with `.\start.ps1 -List -NoPause` or `.\start.ps1 -Help -NoPause`.
- Refresh read-only UI-facing surfaces with `phase5_sync`, `trajectory_gallery`, `trajectory_gallery_panel`, `figure_package_publication`, or `prototype_legacy_final_figures` before launching the UI when you need updated packages.

## Read-Only Guarantee

- The app reads current packaged outputs only.
- No scientific rerun controls are exposed in the UI.
- No edit, delete, or write-back controls are exposed in the UI.
- Missing optional artifacts should fail softly and keep the rest of the dashboard available.

## Panel Mode vs Advanced Mode

Panel mode is the default presentation surface:

- final paper / defense story first
- publication figures first
- plain-language page framing
- main result and support lanes kept clearly separate
- fewer technical controls
- archive and legacy lanes kept secondary
- artifact promotion/archive routing follows the canonical thesis-surface registry in `src/core/artifact_status.py`

Advanced mode stays read-only, but opens lower-level inspection:

- panel and raw figure layers
- longer registries and source tables
- artifact preview selectors
- reproducibility notes and manifest browsing

## Final Page Map

Primary thesis-story pages in panel mode:

- `Home / Overview`
- `Phase 1 Recipe Selection`
- `Mindoro B1 Primary Validation`
- `Mindoro Cross-Model Comparator`
- `DWH Phase 3C Transfer Validation`
- `Phase 4 Oil-Type and Shoreline Context`

Secondary lanes in panel mode:

- `Mindoro Validation Archive`
- `Legacy 2016 Support Package`

Reference page in panel mode:

- `Artifacts / Logs / Registries`

Advanced-only page:

- `Trajectory Explorer`

## Output Roots Behind The Main Pages

- `Home / Overview`: curated package roots plus `output/figure_package_publication/`, with featured figures now ordered from publication-governance `page_target` and `display_order` fields so workflow/provenance context leads before Mindoro B1, comparator support, DWH, and Phase 4
- `Phase 1 Recipe Selection`: `output/phase1_mindoro_focus_pre_spill_2016_2023/`, `output/phase1_production_rerun/`, and the shared thesis study-box reference figure set from `output/figure_package_publication/`, where the default overview now foregrounds Study Boxes `2` and `4` while Study Boxes `1` and `3` remain archived secondary references
- `Mindoro B1 Primary Validation`: `output/Phase 3B March13-14 Final Output/`, filtered so the main thesis-facing page shows only the March 13 -> March 14 R1 primary validation row plus Track A comparator support
- `Mindoro Validation Archive`: `output/final_validation_package/`, `output/Phase 3B March13-14 Final Output/`, and repo-preserved archived March-family materials routed through archive-only curation
- `Mindoro Cross-Model Comparator`: `output/Phase 3B March13-14 Final Output/publication/comparator_pygnome/`
- `DWH Phase 3C Transfer Validation`: `output/Phase 3C DWH Final Output/`
- `Phase 4 Oil-Type and Shoreline Context`: `output/phase4/CASE_MINDORO_RETRO_2023/`
- `Legacy 2016 Support Package`: `output/2016 Legacy Runs FINAL Figures/`
- `Artifacts / Logs / Registries`: `output/final_reproducibility_package/` and `output/final_validation_package/`

## Study Box Numbering

- Study Box `1`: focused Mindoro Phase 1 validation box. Archive/advanced/support only.
- Study Box `2`: `mindoro_case_domain` overview extent. Thesis-facing.
- Study Box `3`: scoring-grid display bounds. Archive/advanced/support only.
- Study Box `4`: `prototype_2016` first-code search box. Thesis-facing as historical-origin support.

## Surface Badges

- `Thesis-facing`: current main evidence or current thesis-story context
- `Comparator support`: current support material kept separate from the primary claim
- `Archive only`: preserved for provenance, audit, or reproducibility only
- `Legacy support`: preserved historical or prototype support material

## Export / PDF Mode

Add `?export=1` to any UI URL:

```text
http://localhost:8501/?export=1
http://localhost:8501/home?export=1
```

Export mode is still read-only and intentionally simplified:

- sidebar and navigation chrome are hidden
- publication-first layout is kept
- tabs flatten into sequential sections
- interactive-only controls are hidden
- featured figures are reduced to a smaller static subset where needed

## Branding

The app supports optional real logo assets and falls back cleanly when they are absent.

- preferred main logo: `ui/assets/logo.svg` or `ui/assets/logo.png`
- optional icon: `ui/assets/logo_icon.svg` or `ui/assets/logo_icon.png`
- missing logo files do not break the app
- text-only branding is shown when no logo is present

See `docs/UI_BRANDING.md` or `ui/assets/README.md` for the exact filenames, size guidance, and replacement steps.
