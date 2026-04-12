# Mindoro Primary Validation Migration

## Decision

March 13 -> March 14 is now the canonical Mindoro Phase 3B public-validation row, but the original March 3 -> March 6 case definition remains frozen in `config/case_mindoro_retro_2023.yaml`.
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

No existing March 6 output directory was deleted, renamed, or re-labeled as primary.

## Backward Compatibility Behavior

- Existing code and docs may still reference `mindoro_march13_14_noaa_reinit_stress_test`; that entry is preserved as an alias so older scripts and notes do not break.
- The alias no longer defines the authoritative scientific label. The authoritative launcher entry is now `mindoro_phase3b_primary_public_validation`.
- `config/case_mindoro_retro_2023.yaml` remains loadable as the base case file. The promotion metadata is additive and does not mutate the original March 3 -> March 6 definition.
- March 6 remains visible as B2, but it is now explicitly framed as `legacy_honesty_only`.

## Final Table-Field Changes

The final validation package now carries these promotion/provenance fields for Mindoro rows:

- `case_definition_path`
- `case_freeze_amendment_path`
- `base_case_definition_preserved`
- `row_role`
- `shared_imagery_caveat`

The synced Phase 5 case registry now also carries:

- `case_freeze_amendment_path`
- `primary_launcher_entry_id`
- `launcher_alias_entry_id`

## Final Row Roles

- `A`: comparator-only cross-model lane; PyGNOME remains comparator-only.
- `B1`: `primary_public_validation`
- `B2`: `legacy_honesty_only`
- `B3`: `legacy_support_only`

## Claim Guardrail

Both NOAA/NESDIS public products for the promoted March 13 -> March 14 pair cite WorldView-3 imagery acquired on `2023-03-12`.
That means the promoted B1 row is a reinitialization-based public-validation pair with shared-imagery provenance, not a fully independent day-to-day validation.
