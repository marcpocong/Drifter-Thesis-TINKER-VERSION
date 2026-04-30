# Experiment: Mindoro March 9-12 Multi-Source

This lane is experimental/archive-only. It does not replace the current B1
thesis-facing validation claim, and it does not relabel Track A/PyGNOME, DWH,
final validation, publication, or read-only UI outputs as evidence.

## Launcher

- Entry ID: `mindoro_mar09_12_multisource_experiment`
- Category: `sensitivity_appendix_tracks`
- Thesis role: `archive_provenance`
- Safe default: `false`
- Experimental only: `true`
- Reportable: `false`
- Thesis facing: `false`
- Output root: `output/CASE_MINDORO_RETRO_2023/experiments/mar09_11_12_multisource/`

The entry is hidden/non-default and asks for confirmation before running. It has
safe non-model phases for forcing resolution and mask ingestion, then a gated
pipeline stage and a gnome-service PyGNOME comparator finalization stage. The
model stage does not run unless
`RUN_MINDORO_MAR09_12_MULTISOURCE_EXPERIMENT=1` is explicitly set.

## Observation Inputs

The ingestion policy prefers ArcGIS FeatureServer or GeoJSON polygons over
report-image mask extraction. Report PNGs are retained as provenance and visual
QA only. Positive oil cells include `Possible Oil` and `Possible Thicker Oil`;
suspected source points are excluded.

Primary masks:

- `OBS_MAR09_TERRAMODIS`: Terra MODIS, `MindoroOilSpill_NOAA_230309`, seed for
  the March 9 -> March 11 and March 9 -> March 12 forecasts.
- `OBS_MAR11_ICEYE`: ICEYE, `MindoroOilSpill_NOAA_230311`, validation target
  for the 48 h forecast and seed for the 24 h forecast.
- `OBS_MAR12_COMBINED`: union/dissolve after reprojection of the three preserved
  March 12 source masks.

March 12 source masks preserved before union:

- `OBS_MAR12_WORLDVIEW3_NOAA_230314`: WorldView-3, resolved through the public
  `Possible_Oil_Spills_March_14` FeatureServer because the direct
  `MindoroOilSpill_NOAA_230314` root is not a usable layer endpoint.
- `OBS_MAR12_WORLDVIEW3_NOAA_230313`: WorldView-3,
  `MindoroOilSpill_NOAA_230313`.
- `OBS_MAR12_ICEYE`: ICEYE, `MindoroOilSpill_NOAA_230312`.

## Forecast Pairs

| Pair | Seed | Target | Nominal Lead |
|---|---|---|---:|
| `E1_MAR09_TO_MAR11_48H` | `OBS_MAR09_TERRAMODIS` | `OBS_MAR11_ICEYE` | 48 h |
| `E2_MAR09_TO_MAR12_72H` | `OBS_MAR09_TERRAMODIS` | `OBS_MAR12_COMBINED` | 72 h |
| `E3_MAR11_TO_MAR12_24H` | `OBS_MAR11_ICEYE` | `OBS_MAR12_COMBINED` | 24 h |

OpenDrift uses the current official Mindoro transport configuration and the
resolved March 13-14 B1 forcing-recipe winner. It does not rerun Phase 1 recipe
selection, does not ingest drifters, does not generate accepted segments, and
does not run historical GFS preflight or broad GFS/monthly ingestion. If March
9-12 forcing is not already available in validated local/case stores, the
default behavior is to write `missing_forcing_manifest.json` and stop. A
bounded case-forcing fetch is allowed only with
`ALLOW_MINIMAL_CASE_FORCING_FETCH=1`, and it is limited to the March 8 evening
through March 13 case window and the configured Mindoro fallback bbox.

OpenDrift deterministic and ensemble-member runs are capped at 5,000 elements.
PyGNOME is run as a deterministic comparator only and is capped at 5,000
particles where supported; any mismatch in wave/Stokes handling is recorded in
the comparator manifest and README.

## Expected Outputs

The final scorecard contains 12 FSS rows: 3 forecast pairs x 4 model surfaces.
Rows include 1, 3, 5, and 10 km windows plus mean FSS. Geometry diagnostics store
positive cells, areas, area ratio, IoU, Dice, nearest distance, centroid
distance, timing fields, and row notes.

Expected output files include:

- `scorecard_fss_by_pair_surface.csv`
- `scorecard_geometry_diagnostics.csv`
- `observation_mask_inventory.csv`
- `manifest.json`
- `run_config_resolved.yaml`
- `source_ingestion_manifest.json`
- `scoring_manifest.json`
- `pygnome_comparator_manifest.json`
- `README.md`

Expected figures are written under `figures/`, including observation masks, one
forecast-panel figure per pair, a mean-FSS summary, and a March 12 union QA
figure.
