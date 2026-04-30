# Methodology Amendment: 2016 And Mindoro

## Purpose

This note aligns the thesis-facing wording in the repo with the current code, launcher behavior, and stored artifacts. It is a wording sync only. It does not rerun science, rewrite stored outputs, or promote any staged baseline artifact automatically.

## Prototype 2016

`prototype_2016` remains a legacy support-only lane. Thesis-facing, it is preserved as:

`Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`

This lane records the earliest prototype stage of the study, keeps the selected drifter-of-record start as the authoritative release origin, and keeps PyGNOME comparator-only. It is not rewritten into a Mindoro-style spill-validation lane, and it does not gain thesis-facing `Phase 3B` or `Phase 3C`.

## Mindoro

Mindoro keeps two separate stories:

- A separate `phase1_mindoro_focus_pre_spill_2016_2023` rerun now used as the active Mindoro-specific recipe-provenance lane for the B1 story.
- The canonical spill-case validation story built from the frozen March 3 -> March 6 base case, official Phase 2 outputs, and the promoted March 13 -> March 14 R1 primary validation row.

Thesis-facing, the Mindoro sequence is:

separate focused drifter-based Phase 1 provenance -> Phase 2 -> Phase 3B primary validation

The focused Phase 1 rerun now supplies the active Mindoro-specific recipe provenance. It does not rewrite the stored B1 raw-generation history, does not claim that Phase 3B itself directly used drifters, and does not erase the broader 2016-2022 regional rerun, which remains preserved as reference/governance context.

## Main Row And Archive Rows

March 13 -> March 14 R1 is the canonical B1 primary validation row through `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.

The March 13 -> March 14 R0 archived baseline, March 6 B2, and March 3 -> March 6 B3 remain repo-preserved for archival provenance and reproducibility only. They remain inspectable in the repo and in the `Mindoro Validation Archive` page, but they will not be used in thesis-facing methodology, tables, figures, or headline validation claims.

The observation-independence note remains explicit: March 13 and March 14 are independent NOAA-published day-specific public-observation products, so March 13 -> March 14 R1 is a reinitialization-based public-validation pair from the March 13 public seed to the March 14 public target.

## PyGNOME

PyGNOME stays comparator-only in every lane discussed here.

- In `prototype_2016`, it remains legacy support/comparator evidence.
- In Mindoro, the March 13 -> March 14 PyGNOME lane is same-case supporting comparator evidence only.
- In DWH, PyGNOME remains comparator-only and DWH stays separate as Phase 3C.
