# Prototype 2016 PyGNOME Similarity Summary

This package consolidates the three legacy 2016 deterministic OpenDrift control vs deterministic PyGNOME transport benchmarks.

Guardrails:

- legacy/debug/regression only
- transport benchmark only
- PyGNOME is a comparator, not truth
- not final Chapter 3 evidence

Relative similarity ranking:

- Rank 1: `CASE_2016-09-06` | mean FSS @ 5 km = 0.783, mean KL = 17.254, pairs = 3
- Rank 2: `CASE_2016-09-01` | mean FSS @ 5 km = 0.385, mean KL = 21.363, pairs = 3
- Rank 3: `CASE_2016-09-17` | mean FSS @ 5 km = 0.339, mean KL = 21.397, pairs = 3

Per-case snapshot highlights:

- `CASE_2016-09-06`: FSS @ 5 km (24/48/72 h) = 0.812 / 0.747 / 0.789; KL (24/48/72 h) = 19.676 / 21.453 / 10.632
- `CASE_2016-09-01`: FSS @ 5 km (24/48/72 h) = 0.693 / 0.357 / 0.105; KL (24/48/72 h) = 21.365 / 21.362 / 21.361
- `CASE_2016-09-17`: FSS @ 5 km (24/48/72 h) = 0.542 / 0.318 / 0.158; KL (24/48/72 h) = 21.414 / 21.398 / 21.381

Interpretation:

- Higher FSS means stronger footprint overlap between deterministic OpenDrift and deterministic PyGNOME.
- Lower KL means the normalized density fields are more similar over the ocean cells.
- The ranking is relative within the legacy 2016 prototype set only.
- The per-forecast figures under `figures/` are support visuals built from the stored benchmark rasters only, now shown as higher-density core and broader support envelopes over canonical Mindoro land/shoreline context, with a provenance source-point star when available.
