# Registry Path Reconciliation Report

- Git HEAD: `263ca5fb1089743f3987f79aa77c530227a6583c`
- Timestamp UTC: `2026-05-01T15:03:00.193546+00:00`
- Scope: reconcile registries with tracked final package paths for the cheap repo-consistency guardrails.
- No expensive science was rerun.
- No scientific output was fabricated.

## Root Cause

Paper-to-output and data-source registries pointed some primary/external evidence and manifest entries at raw CASE_* paths that were available locally but not tracked in the submitted repository. The validators checked filesystem presence instead of git-tracked package presence, so local runs could mask clean-checkout CI failures.

## Failing Commands Before Fix

- `python -m pytest tests/test_final_paper_consistency_guardrails.py -q` -> `FAILED_IN_GITHUB_ACTIONS`: CI reported failures in test_paper_to_output_registry and test_data_sources_registry because registries referenced missing tracked paths as existing outputs.
- `python scripts/validate_paper_to_output_registry.py` -> `LOCAL_FALSE_PASS_BEFORE_FIX`: Before the fix, local validation used filesystem existence and could pass when ignored/raw output folders were present outside git tracking.
- `python scripts/validate_data_sources_registry.py` -> `LOCAL_FALSE_PASS_BEFORE_FIX`: Before the fix, local validation used filesystem existence and could pass when ignored/raw output folders were present outside git tracking.
- `python scripts/validate_final_paper_guardrails.py` -> `LOCAL_FALSE_PASS_BEFORE_FIX`: Before the fix, this inherited the same filesystem-based registry checks.

## Registry Changes

- Updated paper registry entries to tracked curated paths: `table_3_11`, `table_3_12`, `table_4_5`, `table_4_6`, `table_4_7`, `output_mindoro_case_root`, `table_4_8`, `output_track_a_comparator_root`, `table_3_13`, `table_4_9`, `table_4_10`, `output_dwh_case_root` and related data-source entries.
- Marked old raw/canonical paths as explicit optional/missing provenance: `output/CASE_MINDORO_RETRO_2023/forecast/...`, `output/CASE_MINDORO_RETRO_2023/ensemble/...`, `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public...`, `output/CASE_MINDORO_RETRO_2023/..._pygnome_comparison...`, `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_setup/...`, `output/CASE_DWH_RETRO_2010_72H/dwh_phase3c_scientific_forcing_ready/...`, `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/...`, `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/...`, `output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/...`.
- Inventory used: `output/ci_guardrail_hotfix/tracked_paths_inventory.txt`.

## Docs Corrected

- `README.md`
- `docs/DATA_SOURCES.md`
- `docs/PAPER_OUTPUT_REGISTRY.md`
- `docs/PAPER_TO_REPO_CROSSWALK.md`
- `output/Phase 3B March13-14 Final Output/README.md`
- `output/Phase 3C DWH Final Output/README.md`

## Validators Changed

- scripts/validate_paper_to_output_registry.py now checks git-tracked paths, supports explicit optional/missing markers with reasons, rejects URL/escaping paths, and fails output_exists=true with no tracked match.
- scripts/validate_data_sources_registry.py now checks git-tracked manifest paths and accepts optional/missing markers only when explicit.

## Validation After Fix

- `python -m pytest tests/test_final_paper_consistency_guardrails.py -q` -> `PASS`: 9 passed in 0.79s after registry/path reconciliation.
- `python scripts/validate_paper_to_output_registry.py` -> `PASS`: 51 entries checked; 0 errors; 0 warnings. Validator now resolves local paths against git tracked files.
- `python scripts/validate_data_sources_registry.py` -> `PASS`: 13 source groups checked; required fields, claim boundaries, secret scan, and tracked manifest paths passed.
- `python scripts/validate_final_paper_guardrails.py` -> `PASS`: Checked tracked labels, launcher routing, claim boundaries, alignment facts, registries, and p90 semantics.
- `python scripts/validate_archive_registry.py` -> `PASS`: Archive registry validation passed against launcher matrix.
- `pwsh ./start.ps1 -ValidateMatrix -NoPause` -> `LOCAL_LIMITATION`: Local shell does not provide pwsh; did not fail the hotfix for missing local PowerShell.
- `python -m src.utils.validate_launcher_matrix --no-write` -> `PASS`: Fallback launcher matrix validation passed: 27 entries, 27 pass, 0 fail.
- `python -m pytest tests -q` -> `LOCAL_ENV_FAIL_EXPECTED`: Collection failed because local Python lacks heavy/science dependencies including numpy, pandas, geopandas, xarray, and yaml. Cheap guardrail tests passed separately.
- `runtime-constructed forbidden-label scan over git ls-files` -> `PASS`: No tracked text file contains the forbidden manuscript label.

## Claim Boundaries Preserved

- PyGNOME remains comparator-only and never observation truth.
- DWH remains external transfer validation only and not Mindoro recalibration.
- Mindoro B1 March 13-14 remains the only main Philippine public-observation validation claim.
- p50 is P >= 0.50 and preferred; p90 is P >= 0.90 and conservative support/comparison only.
