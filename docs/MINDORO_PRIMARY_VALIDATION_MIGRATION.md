# Mindoro Primary Validation Migration

## Decision

March 13 -> March 14 R1 is now the canonical thesis-facing Mindoro Phase 3B public-validation row, but the original March 3 -> March 6 case definition remains frozen in `config/case_mindoro_retro_2023.yaml`.
The promotion is recorded separately in `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml` so repo history preserves the original case provenance instead of silently rewriting it.

## Old Paths And New Paths

- Frozen base case path retained: `config/case_mindoro_retro_2023.yaml`
- New amendment path: `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`
- New canonical launcher entry: `mindoro_phase3b_primary_public_validation`
- Backward-compatible alias retained: `mindoro_march13_14_noaa_reinit_stress_test`
- Migration note path: `docs/MINDORO_PRIMARY_VALIDATION_MIGRATION.md`

## Output Paths Kept Stable

- Promoted B1 outputs remain under `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/`
- Legacy B2 outputs remain under `output/CASE_MINDORO_RETRO_2023/phase3b/`
- Legacy B3 outputs remain under `output/CASE_MINDORO_RETRO_2023/public_obs_appendix/`
- Comparator-only A outputs remain under `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/`

No existing March-family output directory was deleted, renamed, or re-labeled as primary.

## Backward Compatibility Behavior

- Existing code and docs may still reference `mindoro_march13_14_noaa_reinit_stress_test`; that entry is preserved as an alias so older scripts and notes do not break.
- The alias no longer defines the authoritative scientific label. The authoritative launcher entry is now `mindoro_phase3b_primary_public_validation`.
- `config/case_mindoro_retro_2023.yaml` remains loadable as the base case file. The promotion metadata is additive and does not mutate the original March 3 -> March 6 definition.
- The March 13 -> March 14 R0 archived baseline plus B2/B3 remain repo-preserved archive-only rows and are no longer thesis-facing Mindoro validation rows.

## Final Table-Field Changes

The final validation package now carries these promotion/provenance fields for Mindoro rows:

- `case_definition_path`
- `case_freeze_amendment_path`
- `base_case_definition_preserved`
- `row_role`
- `observation_independence_note`

The synced Phase 5 case registry now also carries:

- `case_freeze_amendment_path`
- `primary_launcher_entry_id`
- `launcher_alias_entry_id`

## Final Row Roles

- `A`: comparator-only cross-model lane; PyGNOME remains comparator-only.
- `B1`: `primary_public_validation`
- `archive_r0`: `archive_only_baseline`
- `B2`: `archive_only_reference`
- `B3`: `archive_only_reference`

## Claim Guardrail

March 13 and March 14 are independent NOAA-published day-specific public-observation products.
That means the promoted B1 row should be described as a reinitialization-based public-validation pair from the March 13 public seed observation to the March 14 public target observation.
