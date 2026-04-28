# Thesis Surface Governance

## Purpose

This repo keeps one canonical artifact-surface vocabulary in `src/core/artifact_status.py` so the UI, publication registry, launcher, and validation/export packaging all agree on what is main evidence, comparator support, archive-only, legacy/archive support, or advanced-only.

## Current Manuscript Evidence Order

1. Focused Mindoro Phase 1 provenance
2. Phase 2 standardized forecast products
3. Mindoro `B1` primary public-observation validation
4. Mindoro `Track A` comparator-only support
5. DWH external transfer validation
6. Mindoro oil-type and shoreline support/context
7. `prototype_2016` legacy/archive support
8. Reproducibility / governance / read-only package layer

## Surface Keys

- `thesis_main`: artifacts eligible for the curated thesis-facing route
- `comparator_support`: support, comparator, or context material kept separate from the primary claim
- `archive_only`: preserved for provenance, reproducibility, and audit only
- `legacy_support`: historical/prototype support material that remains visible as support only
- `advanced_only`: lower-level inspection material that stays out of default panel surfaces

## Main Surface Rules

- Mindoro March 13-14 `R1_previous` is the only thesis-facing `B1` row.
- `Track A` is comparator-only support attached to `B1`, not a co-primary result.
- DWH `C1` and `C2 p50` remain thesis-facing transfer-validation material, while `C2 p90` and `C3` stay support/comparison only.
- Mindoro oil-type and shoreline outputs remain support/context only.
- UI pages, figure packages, and publication packages organize stored outputs only; they do not create new scientific results.

## Archive And Legacy Rules

- Mindoro March 13-14 `R0`, `B2`, and `B3` stay `archive_only`.
- `prototype_2016` stays `legacy_support`.
- Some internal package names may still contain Phase 4/Phase 5 labels inside the legacy archive surfaces, but those labels do not create new defended evidence.

## Why Preserve Without Promoting

Archive material is preserved because it still matters for:

- provenance and methods traceability
- reproducibility and audit
- explaining why the promoted row was chosen

Preservation is not promotion. The defended thesis-facing surface is intentionally narrower than the full preserved repo history.
