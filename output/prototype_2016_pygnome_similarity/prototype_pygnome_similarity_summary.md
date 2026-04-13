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


PyGNOME transport forcing status:

- PyGNOME remains comparator-only but uses matched prepared grid wind plus grid current forcing for these support benchmarks.
- `OpenDrift deterministic`:
  Rank 1: `CASE_2016-09-17` | mean FSS @ 5 km = 0.729, mean KL = 5.334, pairs = 3
  Rank 2: `CASE_2016-09-01` | mean FSS @ 5 km = 0.546, mean KL = 14.955, pairs = 3
  Rank 3: `CASE_2016-09-06` | mean FSS @ 5 km = 0.501, mean KL = 19.004, pairs = 3
- `OpenDrift p50 occupancy footprint`:
  Rank 1: `CASE_2016-09-06` | mean FSS @ 5 km = 0.542, mean KL = 1.997, pairs = 3
  Rank 2: `CASE_2016-09-01` | mean FSS @ 5 km = 0.433, mean KL = 2.211, pairs = 3
  Rank 3: `CASE_2016-09-17` | mean FSS @ 5 km = 0.360, mean KL = 2.349, pairs = 3
- `OpenDrift p90 occupancy footprint`:
  Rank 1: `CASE_2016-09-06` | mean FSS @ 5 km = 0.589, mean KL = 1.871, pairs = 3
  Rank 2: `CASE_2016-09-01` | mean FSS @ 5 km = 0.482, mean KL = 2.080, pairs = 3
  Rank 3: `CASE_2016-09-17` | mean FSS @ 5 km = 0.396, mean KL = 2.233, pairs = 3

Per-case snapshot highlights:

- `CASE_2016-09-17` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 1.000 / 0.783 / 0.404; KL (24/48/72 h) = 0.000 / 15.040 / 0.961
- `CASE_2016-09-01` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.783 / 0.575 / 0.281; KL (24/48/72 h) = 19.691 / 2.541 / 22.634
- `CASE_2016-09-06` / `OpenDrift deterministic`: FSS @ 5 km (24/48/72 h) = 0.464 / 0.464 / 0.575; KL (24/48/72 h) = 22.763 / 17.933 / 16.317
- `CASE_2016-09-06` / `OpenDrift p50 occupancy footprint`: FSS @ 5 km (24/48/72 h) = 0.764 / 0.524 / 0.338; KL (24/48/72 h) = 2.104 / 1.475 / 2.412
- `CASE_2016-09-01` / `OpenDrift p50 occupancy footprint`: FSS @ 5 km (24/48/72 h) = 0.497 / 0.380 / 0.423; KL (24/48/72 h) = 1.696 / 2.389 / 2.546
- `CASE_2016-09-17` / `OpenDrift p50 occupancy footprint`: FSS @ 5 km (24/48/72 h) = 0.242 / 0.301 / 0.537; KL (24/48/72 h) = 2.197 / 2.081 / 2.768
- `CASE_2016-09-06` / `OpenDrift p90 occupancy footprint`: FSS @ 5 km (24/48/72 h) = 0.806 / 0.607 / 0.354; KL (24/48/72 h) = 1.987 / 1.267 / 2.360
- `CASE_2016-09-01` / `OpenDrift p90 occupancy footprint`: FSS @ 5 km (24/48/72 h) = 0.559 / 0.421 / 0.467; KL (24/48/72 h) = 1.563 / 2.256 / 2.422
- `CASE_2016-09-17` / `OpenDrift p90 occupancy footprint`: FSS @ 5 km (24/48/72 h) = 0.271 / 0.319 / 0.598; KL (24/48/72 h) = 2.079 / 2.012 / 2.606

Interpretation:

- Higher FSS means stronger footprint overlap between the named OpenDrift track and deterministic PyGNOME.
- Lower KL means the normalized density fields are more similar over the ocean cells.
- In prototype_2016, p50/p90 are exact valid-time member-occupancy footprints; they are not pooled-particle-density thresholds.
- The ranking is relative within each comparison track inside the prototype_2016 support set only.
- The per-forecast figures under `figures/` are support visuals built from the stored benchmark rasters only, now shown with exact stored raster cells and exact footprint outlines over case-local drifter-centered geographic context, with a provenance source-point star when available.
