# Secondary 2016 Drifter-Track Benchmark

Across the 2016 drifter-track benchmark, the OpenDrift promoted configuration was 1.47 km nearer than the PyGNOME deterministic comparator under case-mean time-averaged separation. This is a secondary drifter-track benchmark and does not replace the primary Mindoro public-observation validation.

## Scope

This package compares stored 2016 observed drifter tracks with matched stored OpenDrift and PyGNOME outputs. The observed drifter track is the reference. PyGNOME is a deterministic comparator, and the OpenDrift ensemble diagnostics are support diagnostics.

This package is secondary support only. It does not modify or replace the primary Mindoro March 13-14 public-observation validation outputs, and it does not modify DWH outputs.

## Methods

- Existing internal case IDs are preserved.
- Observed tracks use the repository's drifter-of-record selection rule from `data/drifters/<case_id>/drifters_noaa.csv`.
- Deterministic model tracks use the median active-particle position at each model output time.
- Model tracks are linearly interpolated to observed drifter timestamps in the 0-72 h window.
- Time-averaged separation is the mean great-circle distance over aligned observed timestamps.
- Normalized cumulative separation is the sum of aligned separation distances divided by the observed cumulative 0-72 h track length.
- Ensemble nearest-member distance uses each ensemble member's median active-particle track.
- Ensemble footprint diagnostics sample stored member-occupancy probability rasters at 24 h, 48 h, and 72 h.

## Cases

- `CASE_2016-09-01`: time-averaged OD 10.21 km, PyGNOME 13.12 km, nearest ensemble member 9.72 km. Board: `output/2016_drifter_benchmark/case_boards/CASE_2016-09-01_drifter_track_benchmark.png`.
- `CASE_2016-09-06`: time-averaged OD 7.49 km, PyGNOME 9.01 km, nearest ensemble member 7.43 km. Board: `output/2016_drifter_benchmark/case_boards/CASE_2016-09-06_drifter_track_benchmark.png`.
- `CASE_2016-09-17`: time-averaged OD 18.51 km, PyGNOME 18.48 km, nearest ensemble member 17.72 km. Board: `output/2016_drifter_benchmark/case_boards/CASE_2016-09-17_drifter_track_benchmark.png`.

## Outputs

- `scorecard.csv`
- `scorecard.json`
- `case_boards/*.png`
- `manifest.json`
- `README.md`
