# UI Guide

## Purpose

The local dashboard is a read-only exploration/support layer over the outputs that already exist in this repo. It does not rerun model branches, it does not modify scientific artifacts, and it does not pretend that missing comparisons already exist.

## Launch Command

Start the pipeline container first if needed:

```bash
docker-compose up -d pipeline
```

Then launch the UI:

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Open:

```text
http://localhost:8501
```

## What The UI Reads

The UI reads existing artifacts only:

- `output/final_validation_package/`
- `output/final_reproducibility_package/`
- `output/trajectory_gallery/`
- `output/trajectory_gallery_panel/`
- `output/figure_package_publication/`
- `output/phase4/CASE_MINDORO_RETRO_2023/`
- `output/phase4_crossmodel_comparability_audit/`
- relevant Mindoro and DWH manifests, score tables, rasters, and NetCDF-backed figure sources

Missing optional files are tolerated. The UI shows a gentle notice instead of failing where practical.

## Panel-Friendly Mode

This is the default mode. It prioritizes:

- publication-grade figures
- plain-language interpretations from the publication registry
- recommended defense figures
- simplified summary tables
- the current phase-status registry

The phase-status display is driven by generated reproducibility artifacts. In this docs-only sync pass, that generated registry may temporarily lag the source docs until the read-only audit/sync bundles are refreshed.

Recommended first stops:

- `Home / Overview`
- `Mindoro Validation`
- `Phase 4 Oil-Type & Shoreline`
- `Phase 4 Cross-Model Status`
- `DWH Transfer Validation`

## Advanced Mode

Advanced mode opens lower-level inspection without changing the scientific state:

- panel-gallery and raw-gallery figure layers
- manifest previews
- log previews
- output-catalog browsing
- trajectory source artifact inspection

This mode is still read-only.

## Pages

- `Home / Overview`
- `Mindoro Validation`
- `DWH Transfer Validation`
- `Cross-Model Comparison`
- `Phase 4 Oil-Type & Shoreline`
- `Phase 4 Cross-Model Status`
- `Trajectory Explorer`
- `Artifacts / Logs`

## Honesty Rules Surfaced In The UI

- Phase 1 dedicated `2016-2022` rerun outputs now exist and stage a candidate baseline, but the default spill-case baseline has not been manually replaced yet.
- Phase 2 is scientifically usable, but not scientifically frozen.
- `Phase 3B` and `Phase 3C` are validation-only pages in the thesis-facing story.
- Outside `prototype_2016`, the Mindoro Phase 4 pages are support/context views rather than additional thesis phases, and they inherit upstream Phase 1/2 provisional status.
- Phase 4 OpenDrift-versus-PyGNOME comparison is currently deferred.
- The current blocker is that the stored Mindoro PyGNOME benchmark is transport-only with `weathering_enabled=false`, so it does not expose matched Phase 4 fate or shoreline semantics.

## No Run Buttons Yet

The first dashboard version is intentionally read-only. It does not expose scientific rerun controls, write actions, or packaging rebuild buttons.
