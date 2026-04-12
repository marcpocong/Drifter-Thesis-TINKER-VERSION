# Prototype 2016 PyGNOME Similarity Summary

This package consolidates the three legacy 2016 deterministic, p50, and p90 OpenDrift transport benchmarks against deterministic PyGNOME.

Guardrails:

- legacy/debug support only
- status label: Prototype 2016 legacy debug support
- provenance: Legacy/debug regression lane preserved for reproducibility only.
- transport benchmark only
- PyGNOME is a comparator, not truth
- not final Chapter 3 evidence

Relative similarity ranking:

- `OpenDrift deterministic`:
  Rank 1: `CASE_2016-09-06` | mean FSS @ 5 km = 0.435, mean KL = 22.833, pairs = 3
  Rank 2: `CASE_2016-09-17` | mean FSS @ 5 km = 0.290, mean KL = 22.418, pairs = 3
  Rank 3: `CASE_2016-09-01` | mean FSS @ 5 km = 0.116, mean KL = 22.418, pairs = 3
- `OpenDrift p50 threshold`:
  Rank 1: `CASE_2016-09-17` | mean FSS @ 5 km = 0.209, mean KL = 7.530, pairs = 3
  Rank 2: `CASE_2016-09-01` | mean FSS @ 5 km = 0.116, mean KL = 7.530, pairs = 3
  Rank 3: `CASE_2016-09-06` | mean FSS @ 5 km = 0.000, mean KL = 0.501, pairs = 3
- `OpenDrift p90 threshold`:
  Rank 1: `CASE_2016-09-01` | mean FSS @ 5 km = 0.000, mean KL = 0.086, pairs = 3
  Rank 2: `CASE_2016-09-17` | mean FSS @ 5 km = 0.000, mean KL = 0.086, pairs = 3
  Rank 3: `CASE_2016-09-06` | mean FSS @ 5 km = 0.000, mean KL = 0.501, pairs = 3

Per-case snapshot highlights:

- `CASE_2016-09-06` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.609 / 0.348 / 0.348; KL (24/48/72 h) = 23.021 / 22.881 / 22.598
- `CASE_2016-09-17` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.626 / 0.243 / 0.000; KL (24/48/72 h) = 22.347 / 22.391 / 22.517
- `CASE_2016-09-01` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.348 / 0.000 / 0.000; KL (24/48/72 h) = 22.347 / 22.391 / 22.517
- `CASE_2016-09-17` / `OpenDrift p50 threshold`: FSS @ 5 km (24/48/72 h) = 0.626 / 0.000 / 0.000; KL (24/48/72 h) = 22.347 / 0.058 / 0.184
- `CASE_2016-09-01` / `OpenDrift p50 threshold`: FSS @ 5 km (24/48/72 h) = 0.348 / 0.000 / 0.000; KL (24/48/72 h) = 22.347 / 0.058 / 0.184
- `CASE_2016-09-06` / `OpenDrift p50 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.688 / 0.548 / 0.266
- `CASE_2016-09-01` / `OpenDrift p90 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.015 / 0.058 / 0.184
- `CASE_2016-09-17` / `OpenDrift p90 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.015 / 0.058 / 0.184
- `CASE_2016-09-06` / `OpenDrift p90 threshold`: FSS @ 5 km (24/48/72 h) = 0.000 / 0.000 / 0.000; KL (24/48/72 h) = 0.688 / 0.548 / 0.266

Interpretation:

- Higher FSS means stronger footprint overlap between the named OpenDrift track and deterministic PyGNOME.
- Lower KL means the normalized density fields are more similar over the ocean cells.
- The ranking is relative within each comparison track inside the prototype_2016 support set only.
- The per-forecast figures under `figures/` are support visuals built from the stored benchmark rasters only, now shown with exact stored raster cells and exact footprint outlines over case-local drifter-centered geographic context, with a provenance source-point star when available.
