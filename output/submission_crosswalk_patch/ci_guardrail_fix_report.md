# CI Guardrail Fix Report

## Cause Of Failure

GitHub Actions was failing `tests/test_final_paper_consistency_guardrails.py::test_final_alignment_facts` because `docs/FINAL_PAPER_ALIGNMENT.md` no longer contained exact internal fact-group snippets expected by `scripts/validate_final_paper_guardrails.py`.

After restoring those snippets, the requested pytest bundle also flagged that `docs/PAPER_OUTPUT_REGISTRY.md` needed to stay explicitly framed as a panel-review/defense-inspection surface. That doc-framing line was restored as a text-only CI compatibility fix.

## Exact Snippets Restored

In `docs/FINAL_PAPER_ALIGNMENT.md`:

```text
Mindoro B1 is the March 13-14 primary public-observation validation row.
```

```text
It is interpreted as coastal-neighborhood usefulness, not exact 1 km overlap.
```

```text
The 2016 material provides direct drifter-track and legacy OpenDrift-PyGNOME FSS support only.
It is not public-spill validation and is not a replacement for Mindoro B1 or DWH.
```

## Files Changed

- `docs/FINAL_PAPER_ALIGNMENT.md`
- `docs/PAPER_OUTPUT_REGISTRY.md`
- `output/submission_crosswalk_patch/ci_guardrail_fix_report.md`

## Tests And Validators Run

```text
python -m pytest -q tests/test_final_paper_consistency_guardrails.py tests/test_no_draft_version_labels.py tests/test_validate_launcher_matrix.py tests/test_launcher_matrix_metadata.py tests/test_defense_claim_boundaries.py
python scripts/validate_final_paper_guardrails.py
python scripts/validate_archive_registry.py
python scripts/validate_paper_to_output_registry.py
python scripts/validate_data_sources_registry.py
python -m src.utils.validate_launcher_matrix --no-write
```

## Pass / Fail Results

- Pytest guardrail bundle: PASS, `34 passed`.
- `validate_final_paper_guardrails.py`: PASS.
- `validate_archive_registry.py`: PASS.
- `validate_paper_to_output_registry.py`: PASS, 57 entries, 0 errors, 0 warnings.
- `validate_data_sources_registry.py`: PASS.
- `src.utils.validate_launcher_matrix --no-write`: PASS, 27 entries, 0 failures.

## Scientific Rerun / Download Statement

No scientific reruns, launcher entries, model simulations, remote downloads, or data downloads were performed.

## Git Diff Summary Before Commit

```text
docs/FINAL_PAPER_ALIGNMENT.md | 14 ++++++++++++--
docs/PAPER_OUTPUT_REGISTRY.md |  1 +
2 files changed, 13 insertions(+), 2 deletions(-)
```

## Final Push Note

Final push result is reported in Codex final response; this report is not edited after final push to avoid leaving a dirty working tree.
