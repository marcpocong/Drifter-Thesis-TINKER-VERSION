# Data Sources and Provenance

This page records the repository-facing provenance for the final paper:
**Drifter-Validated 24-72 h Oil-Spill Forecasting for Philippine Coasts: Probability Footprints and Oil-Type Fate.**

Machine-readable registry: [config/data_sources.yaml](../config/data_sources.yaml)

The registry separates public-observation validation masks, drifter transport validation, forcing inputs, shoreline/oil support, model/tool provenance, and the read-only review UI. Large external datasets are not fully mirrored here; the repo preserves configs, inventories, manifests, derived masks, package registries, and stored outputs needed to audit what was used.

## Observation Validation Masks

| Source | Provider | Product / layer | Role | Evidence boundary | Repo verification |
| --- | --- | --- | --- | --- | --- |
| Mindoro March 13-14 public NOAA/NESDIS ArcGIS products | NOAA/NESDIS public ArcGIS products | March 13 seed extent and March 14 target extent | Public-observation validation reference after common-grid processing | The March 13 and March 14 products are independent day-specific public-observation products. This is the only main Philippine public-observation validation lane and supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy. | `config/case_mindoro_retro_2023*.yaml`; `output/Phase 3B March13-14 Final Output/`; raw `output/CASE_MINDORO_RETRO_2023/...` paths are optional rerun/staging provenance in the registry |
| DWH public daily observation masks | NOAA OR&R / ERMA public ArcGIS service | `Deepwaterhorizon_Oilspill_WM` FeatureServer daily layers 5-8 | External transfer validation references | DWH validates transfer behavior for `CASE_DWH_RETRO_2010_72H` only. It is not Mindoro recalibration. | `config/case_dwh_retro_2010_72h.yaml`; `output/Phase 3C DWH Final Output/`; raw `output/CASE_DWH_RETRO_2010_72H/...` paths are optional rerun/staging provenance in the registry |

Access notes: the Mindoro and DWH services are public external ArcGIS/ERMA products. This repo stores inventories, selected-layer registries, source taxonomy, common-grid/derived masks, scorecards, and final package manifests rather than a full mirror of the services.

## Transport Validation

| Source | Provider | Product / layer | Role | Evidence boundary | Repo verification |
| --- | --- | --- | --- | --- | --- |
| NOAA OSMC / Global Drifter Program `drifter_6hour_qc` | NOAA AOML / Global Drifter Program via NOAA OSMC ERDDAP | `drifter_6hour_qc` | Historical transport validation and recipe provenance | Drifters support focused Mindoro Phase 1 transport provenance, accepted-segment filtering, and recipe selection. They are not oil-footprint public observations and do not replace Mindoro B1 or DWH validation. | `config/phase1_mindoro_focus_pre_spill_2016_2023.yaml`; `config/recipes.yaml`; `output/phase1_mindoro_focus_pre_spill_2016_2023/` |

Focused Phase 1 used the 2016-01-01 to 2023-03-02 historical window, focused box `[118.751, 124.305, 10.620, 16.026]`, 65 strict accepted segments, and 19 February-April ranked subset segments. The selected recipe is `cmems_gfs`.

## Forcing Inputs

| Source | Provider | Product / layer | Role | Evidence boundary | Repo verification |
| --- | --- | --- | --- | --- | --- |
| CMEMS currents | Copernicus Marine Service | `GLOBAL_ANALYSISFORECAST_PHY_001_024` and `GLOBAL_MULTIYEAR_PHY_001_030` families where confirmed | Ocean-current forcing input | Phase 1 recipe family and Mindoro `cmems_gfs` current forcing where manifests confirm. Forcing is not a validation target. | `config/recipes.yaml`; focused Phase 1 manifest; final reproducibility manifest index; curated DWH package manifest |
| GFS winds | NOAA / NCEP, with UCAR GDEX fallback where configured | NCEI GFS analysis files and UCAR GDEX `d084001` secondary source | Wind forcing input | Phase 1 and selected Mindoro `cmems_gfs` wind forcing. DWH uses ERA5 winds in the confirmed stack. | `config/recipes.yaml`; focused Phase 1 manifest; final reproducibility manifest index |
| HYCOM GOFS currents | HYCOM Consortium / GOFS | GOFS 3.1 reanalysis `GLBv0.08 expt_53.X` and candidate families | Ocean-current forcing input | Phase 1 candidate family and confirmed DWH current stack. HYCOM is not promoted into the Mindoro B1 selected recipe. | `config/case_dwh_retro_2010_72h.yaml`; curated Phase 3C package manifest and run manifests |
| ERA5 winds | ECMWF / Copernicus Climate Data Store | `reanalysis-era5-single-levels` | Wind forcing input | Phase 1 candidate family, DWH wind stack, and support/context runs where manifests confirm. Mindoro B1 selected `cmems_gfs` uses GFS wind. | `config/recipes.yaml`; curated Phase 3C package manifest; Phase 4 run manifest |
| CMEMS wave/Stokes | Copernicus Marine Service | `GLOBAL_ANALYSISFORECAST_WAV_001_027` and `GLOBAL_MULTIYEAR_WAV_001_032` families where confirmed | Wave/Stokes forcing input | Required/fixed wave and Stokes forcing where manifests confirm. These fields are not validation targets and do not define observation references. | `config/recipes.yaml`; `config/ensemble.yaml`; final reproducibility manifest index; curated Phase 3C package manifest |

Access notes: Copernicus Marine and CDS access can require user credentials. No credentials, tokens, or account-specific secrets are stored in this repository. The registry does not assert exact per-run API request endpoints unless the repo already stores them in configs or manifests.

## Support and Context Sources

| Source | Provider | Product / layer | Role | Evidence boundary | Repo verification |
| --- | --- | --- | --- | --- | --- |
| GSHHG shoreline | GSHHG / SOEST-hosted shoreline archive | GSHHG version `2.3.7` | Shoreline support | Supports land/sea masks, shoreline segmentation, and scoreable-ocean masking. It is support/context only. | `data_processed/grids/shoreline_mask_manifest.json`; `output/phase4/CASE_MINDORO_RETRO_2023/phase4_run_manifest.json` |
| ADIOS/OilLibrary | NOAA ADIOS / OilLibrary and OpenOil-compatible generic oil records | Generic diesel, intermediate fuel oil, heavy fuel oil, and preserved ADIOS constants | Oil-property support | Supports Mindoro oil-type/weathering context only. It is not primary validation and does not recalibrate transport. | `config/oil.yaml`; `config/phase4_oil_scenarios.csv`; `output/phase4/CASE_MINDORO_RETRO_2023/` |

Oil-type and shoreline outputs remain support/context: light oil, fixed-base medium-heavy proxy, and heavier-oil summaries are not promoted into the main validation claim.

## Model, Comparator, and Review Tools

| Source | Provider | Product / layer | Role | Evidence boundary | Repo verification |
| --- | --- | --- | --- | --- | --- |
| OpenDrift/OpenOil | OpenDrift project and OpenOil model components | `opendrift` dependency in Docker pyproject files and OpenOil workflow code | Primary model/tool provenance | Primary model/tool provenance for stored OpenDrift/OpenOil outputs. Model outputs are forecasts or support products, not observation references. | `docker/pipeline/pyproject.toml`; `docker/gnome/pyproject.toml`; `src/`; final output manifests |
| PyGNOME/GNOME | NOAA OR&R ERD PyGNOME/GNOME | PyGNOME `v1.1.18` install source in `docker/gnome/Dockerfile` | Comparator-only model/tool provenance | Comparator-only for Mindoro same-case support, DWH C3, and legacy/archive support. It is never a validation reference and never a co-primary model claim. | `docker/gnome/Dockerfile`; Mindoro/DWH comparator summaries; 2016 legacy comparator manifests |
| Streamlit | Streamlit package used by the local dashboard | `streamlit` dependency in `docker/pipeline/pyproject.toml` | Read-only review UI | Organizes stored outputs, registries, and provenance. It does not create new scientific results. | `ui/`; `docs/UI_GUIDE.md`; final reproducibility package manifests |

## What Is Not Claimed

- Public spill products are used for spatial scoring only after common-grid processing.
- Drifter records support transport validation and recipe provenance, not direct oil-footprint validation.
- Forcing products are model inputs, not validation targets.
- PyGNOME is comparator-only and is never promoted into the main paper claim.
- DWH is external transfer validation only and is not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs are support/context only, not primary validation.
- Secondary 2016 drifter-track and legacy FSS outputs are support only; they are not public-spill validation and do not replace Mindoro B1 or DWH.
- `mask_p50` means `prob_presence >= 0.50` and is the preferred probabilistic footprint. `mask_p90` means `prob_presence >= 0.90` and is conservative support/comparison only; it is not a broad envelope.
- The repo does not claim exact-grid success for Mindoro B1: the final-paper interpretation is coastal-neighborhood usefulness.
- The repo does not assert exact API endpoints for sources where only package/config/manifests, not endpoint records, are stored.

## How To Verify From Repo

1. Read the machine-readable registry at [config/data_sources.yaml](../config/data_sources.yaml).
2. Run `python scripts/validate_data_sources_registry.py` to check required fields, claim boundaries, secret-like values, and tracked manifest path status.
3. Check Mindoro March 13-14 public-observation provenance in `output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_run_manifest.json` and `output/Phase 3B March13-14 Final Output/manifests/phase3b_final_output_registry.csv`; raw `output/CASE_MINDORO_RETRO_2023/...` inventory paths are optional rerun/staging provenance in the registry.
4. Check DWH public observation-mask provenance in `output/Phase 3C DWH Final Output/manifests/phase3c_final_output_manifest.json` and `output/Phase 3C DWH Final Output/summary/comparison/phase3c_main_scorecard.csv`; raw `output/CASE_DWH_RETRO_2010_72H/...` setup paths are optional rerun/staging provenance in the registry.
5. Check focused Phase 1 drifter and recipe provenance in `config/phase1_mindoro_focus_pre_spill_2016_2023.yaml`, `config/recipes.yaml`, and `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_production_manifest.json`.
6. Check stored forcing windows in the Mindoro and DWH forcing manifests; do not infer exact API endpoints that are not written there.
7. Check shoreline support in `data_processed/grids/shoreline_mask_manifest.json`.
8. Check oil-type support in `config/oil.yaml`, `config/phase4_oil_scenarios.csv`, and `output/phase4/CASE_MINDORO_RETRO_2023/`.
9. Check model/tool provenance in `docker/pipeline/pyproject.toml`, `docker/gnome/pyproject.toml`, `docker/gnome/Dockerfile`, and the final stored output manifests.
