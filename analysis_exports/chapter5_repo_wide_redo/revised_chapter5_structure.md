# Revised Chapter 5 Structure

This repo-wide redo supports a fuller Chapter 5 that shows the progression from prototype development to focused recipe selection, standardized forecast products, public validation, external transfer validation, and artifact governance. The chapter should not read like a publication-package inventory only.

## 5.1 Overview of Reportable Result Lanes and Evidence Boundaries

Purpose: define what each evidence lane can and cannot support before interpreting any figures.

Use:
- Table 5.1 for the reportable lanes, allowed thesis use, and interpretation boundaries.
- Figure 5.1 if generated from governance assets.

Key boundary statements:
- Prototype, exploratory, and legacy figures are admissible as provenance, workflow-development, and transport-realism support.
- Prototype or drifter figures are not public oil-footprint truth.
- Mindoro B1 remains the only main-text primary Mindoro public-validation row.
- Track A remains comparator-only support.
- DWH Phase 3C is a separate external transfer-validation lane.
- Phase 4 Mindoro outputs are support/context only.

## 5.2 Prototype Development and Drifter-Track Transport Results

Purpose: show that the workflow was tested on real transport behavior before the final public-mask validation case.

Use:
- Figure 5.2 as the main prototype-development figure.
- Figure 5.3 as provenance/support geography.
- Table 5.2 for the preserved 2016 lane summary if space allows.

Interpretation:
- Emphasize prototype-development, exploratory, and transport-realism language.
- State that drifters support transport realism and recipe selection only.
- Connect this section forward to the focused Phase 1 Mindoro selection lane.

## 5.3 Focused Mindoro Phase 1 Recipe-Selection Findings

Purpose: document the active Mindoro provenance lane that selected the retained forcing recipe.

Use:
- Figure 5.4 if generated from the accepted-segment and ranking-subset registries.
- Figure 5.5 if generated from the ranking CSVs.
- Table 5.3 for the stored recipe ranking.
- Table 5.4 for the loading-audit summary.

Interpretation:
- Distinguish the full strict accepted set of 65 segments from the ranked February-April subset of 19 segments.
- Keep the focused lane separate from the older regional rerun.
- Report `cmems_gfs` as the winning recipe for the focused lane.

## 5.4 Standardized Forecast Product Outputs

Purpose: show what the operationalized forecast products are and why they are scoreable.

Use:
- Figure 5.6 as a strong transport-to-product bridge figure.
- Figure 5.7 if generated as a compact product-family board.
- Figure 5.8 as the scoring-grid and geography support figure.
- Table 5.5 for the product inventory and mask semantics.

Interpretation:
- Define `prob_presence` as the fraction of ensemble members with oil presence.
- Define `mask_p50` as probability greater than or equal to 0.50.
- Define `mask_p90` as probability greater than or equal to 0.90.
- Explain that scored comparisons require a shared projected grid, aligned mask, and valid-ocean scoring domain.

## 5.5 Mindoro Public-Validation Findings

Purpose: present the thesis primary Mindoro validation row without letting comparator or archive material blur the evidence hierarchy.

Use:
- Figure 5.9 as the section anchor.
- Figure 5.10 as the focused B1 overlay.
- Figure 5.11 when the seed-target relationship needs clarification.
- Figure 5.12 for the clearly separated Track A comparator lane.
- Table 5.6 for B1 FSS by window and promoted branch outcome.
- Table 5.7 for Track A comparator metrics.

Interpretation:
- Keep B1 as the only main-text primary Mindoro public-validation row.
- Retain the March 13 to March 14 observation-independence note explicitly.
- Keep Track A and PyGNOME in comparator-only support language.

## 5.6 Deepwater Horizon External Transfer-Validation Findings

Purpose: show that the workflow transfers to a separate event with public observed daily masks as truth.

Use:
- Figure 5.13 for C1 deterministic transfer validation.
- Figure 5.14 for the C2 ensemble extension.
- Figure 5.15 for the C3 comparator-only branch.
- Table 5.8 for the daily and event-corridor FSS summary.

Interpretation:
- Keep DWH Phase 3C separate from Mindoro calibration.
- State the DWH forcing stack exactly as stored: HYCOM GOFS 3.1 currents, ERA5 winds, and CMEMS wave/Stokes.
- Treat C1 as deterministic transfer validation, C2 as ensemble extension, and C3 as comparator-only support.

## 5.7 Mindoro Oil-Type and Shoreline Support Findings

Purpose: include the stored Phase 4 support outputs without turning them into a second primary validation lane.

Use:
- Figure 5.16 for the oil-budget board.
- Figure 5.17 for the shoreline-arrival and segment-impact board.
- Figure 5.18 if the deferred cross-model note is useful in the main text or appendix.
- Table 5.9 for oil-budget metrics.
- Table 5.10 for shoreline-arrival and impacted-segment metrics.

Interpretation:
- Keep Phase 4 as support/context only.
- Mention mass-balance QC status where relevant.
- Do not claim a matched Mindoro Phase 4 OpenDrift-versus-PyGNOME comparison unless a stored matched package is later found.

## 5.8 Reproducibility, Artifact Governance, and Result Routing

Purpose: explain why the chapter uses a broader repo evidence base than the publication package alone.

Use:
- Table 5.11 for readiness, freeze status, and manifest routing.
- Figure 5.1 if generated, or keep the lane-boundary logic here in tabular form only.

Interpretation:
- Distinguish publication-package assets from broader repo evidence assets.
- Mark archive-only, comparator-only, prototype-only, and governance-only outputs explicitly.
- Clarify which assets are thesis-facing and which remain support or provenance.

## 5.9 Summary of Main Findings

Close the chapter with a short synthesis that follows this sequence:
- the workflow was prototyped and tested on drifter transport behavior,
- the focused Mindoro Phase 1 lane selected `cmems_gfs`,
- standardized deterministic and ensemble products were produced on a scoreable grid,
- Mindoro B1 provided the main public-validation row with the imagery caveat retained,
- DWH Phase 3C demonstrated external transfer validation,
- Phase 4 added oil-type and shoreline context,
- governance tables explain why not every stored asset has the same evidentiary role.
