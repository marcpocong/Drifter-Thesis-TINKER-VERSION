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

Open `http://localhost:8501`.

## How It Fits With The Launcher

- The UI is intentionally not a launcher entry.
- List current workflow entries with `.\start.ps1 -List -NoPause` or `.\start.ps1 -Help -NoPause`.
- Refresh UI-facing read-only surfaces with `phase5_sync`, `trajectory_gallery`, `trajectory_gallery_panel`, `figure_package_publication`, or `prototype_legacy_final_figures` before launching the UI when you need updated packages.

## Read-Only Guarantee

- The app reads current packaged outputs only.
- No scientific rerun controls are exposed in the UI.
- Missing optional artifacts fail softly so the rest of the dashboard remains available.

## Responsive Review Checks

- The defense dashboard uses responsive metric cards for panel-summary values so long labels, values, and status notes wrap instead of being truncated.
- Before panel review, check the UI at browser widths near `1280`, `1440`, `1600`, and `1920` px and at zoom levels `90%`, `100%`, `125%`, and `150%`.
- These checks are presentation-only; they must not rerun science, rewrite packaged outputs, or change thesis evidence semantics.

## Panel Mode Surface

Panel mode keeps the final evidence order visible:

1. Focused Mindoro Phase 1 provenance
2. Phase 2 standardized forecast products
3. Mindoro `B1` primary public-observation validation
4. Mindoro `Track A` comparator-only support
5. DWH external transfer validation
6. Mindoro oil-type and shoreline support/context
7. `prototype_2016` legacy/archive support
8. Reproducibility / governance / read-only package layer

Primary pages:

- `Defense / Panel Review`
- `Phase 1 Recipe Selection`
- `B1 Drifter Provenance`
- `Mindoro B1 Primary Validation`
- `Mindoro Cross-Model Comparator`
- `DWH Phase 3C Transfer Validation`
- `Phase 4 Oil-Type and Shoreline Context`

Secondary pages:

- `Mindoro Validation Archive`
- `Legacy 2016 Support Package`

Reference page:

- `Artifacts / Logs / Registries`

Advanced-only page:

- `Trajectory Explorer`

## Output Roots Behind The Main Pages

- `Defense / Panel Review`: curated package roots plus `output/figure_package_publication/`
- `Phase 1 Recipe Selection`: `output/phase1_mindoro_focus_pre_spill_2016_2023/` and shared study-context figures
- `B1 Drifter Provenance`: `output/phase1_mindoro_focus_pre_spill_2016_2023/` and `output/panel_drifter_context/`
- `Mindoro B1 Primary Validation`: `output/Phase 3B March13-14 Final Output/`
- `Mindoro Validation Archive`: `output/final_validation_package/` and archive-routed March-family materials
- `Mindoro Cross-Model Comparator`: `output/Phase 3B March13-14 Final Output/publication/comparator_pygnome/`
- `DWH Phase 3C Transfer Validation`: `output/Phase 3C DWH Final Output/`
- `Phase 4 Oil-Type and Shoreline Context`: `output/phase4/CASE_MINDORO_RETRO_2023/`
- `Legacy 2016 Support Package`: `output/2016 Legacy Runs FINAL Figures/`
- `Artifacts / Logs / Registries`: `output/final_reproducibility_package/` and `output/final_validation_package/`

## Surface Guardrails

- `B1` is the only main-text primary Mindoro validation row.
- March 13-14 keeps the shared-imagery caveat explicit.
- `Track A` and PyGNOME views remain comparator-only support.
- Mindoro oil-type and shoreline views remain support/context only.
- UI, figure packages, and publication packages organize stored outputs only; they do not create new scientific results.

## Study Box Numbering

- Study Box `1`: focused Mindoro Phase 1 validation box. Archive/advanced/support only.
- Study Box `2`: `mindoro_case_domain` overview extent. Thesis-facing context.
- Study Box `3`: scoring-grid display bounds. Archive/advanced/support only.
- Study Box `4`: `prototype_2016` first-code search box. Historical-origin support only.

## Branding

The app supports optional logo assets and falls back cleanly when they are absent.

- Preferred main logo: `ui/assets/logo.svg` or `ui/assets/logo.png`
- Optional icon: `ui/assets/logo_icon.svg` or `ui/assets/logo_icon.png`
- Missing logo files do not break the app

See `docs/UI_BRANDING.md` or `ui/assets/README.md` for the exact filenames and replacement guidance.
