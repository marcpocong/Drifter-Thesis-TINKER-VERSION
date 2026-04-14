# UI Guide

## Purpose

The local Streamlit app is a read-only thesis presentation layer over the artifacts that already exist in this repo. It does not rerun science, does not mutate outputs, and does not expose write-back controls.

## Launch Command

Start the pipeline container if needed:

```bash
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

## Read-Only Guarantee

- The app reads current packaged outputs only.
- No scientific rerun controls are exposed in the UI.
- No edit, delete, or write-back controls are exposed in the UI.
- Missing optional artifacts should fail softly and keep the rest of the dashboard available.

## Panel Mode vs Advanced Mode

Panel mode is the default presentation surface:

- publication figures first
- plain-language page framing
- main result and support lanes kept clearly separate
- fewer technical controls
- reference pages kept secondary

Advanced mode stays read-only, but opens lower-level inspection:

- panel and raw figure layers
- longer registries and source tables
- artifact preview selectors
- reproducibility notes and manifest browsing

## Final Page Map

Main pages in panel mode:

- `Home / Overview`
- `Phase 1 Recipe Selection`
- `Mindoro B1 Primary Validation`
- `Mindoro Cross-Model Comparator`
- `DWH Phase 3C Transfer Validation`
- `Phase 4 Oil-Type and Shoreline Context`
- `Legacy 2016 Support Package`

Reference page in panel mode:

- `Artifacts / Logs / Registries`

Advanced-only page:

- `Trajectory Explorer`

## Output Roots Behind The Main Pages

- `Home / Overview`: curated package roots plus `output/figure_package_publication/`
- `Phase 1 Recipe Selection`: `output/phase1_mindoro_focus_pre_spill_2016_2023/`, `output/phase1_production_rerun/`, and the shared thesis study-box reference figure set with per-box geography panels from `output/figure_package_publication/`
- `Mindoro B1 Primary Validation`: `output/Phase 3B March13-14 Final Output/`
- `Mindoro Cross-Model Comparator`: `output/Phase 3B March13-14 Final Output/publication/comparator_pygnome/`
- `DWH Phase 3C Transfer Validation`: `output/Phase 3C DWH Final Output/`
- `Phase 4 Oil-Type and Shoreline Context`: `output/phase4/CASE_MINDORO_RETRO_2023/`
- `Legacy 2016 Support Package`: `output/2016 Legacy Runs FINAL Figures/`
- `Artifacts / Logs / Registries`: `output/final_reproducibility_package/` and `output/final_validation_package/`

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
