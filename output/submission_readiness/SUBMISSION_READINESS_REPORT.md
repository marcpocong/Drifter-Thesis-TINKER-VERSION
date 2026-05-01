# Submission Readiness Report

## 1. Overall Status

READY WITH WARNINGS

All required guardrails, core validators, table-label checks, Figure 4 registry/path checks, final-value checks, and claim-boundary checks passed. The remaining warnings are local optional-tool/dependency limits: `numpy` is unavailable for `tests/test_figure_package_publication.py`, and `pwsh` is unavailable for the exact launcher commands.

## 2. Current Branch and Latest Commit

- Branch: `main`
- Latest commit at report generation: `288a095 docs: harmonize panel docs and UI labels`

## 3. Checks Run and Results

| Check | Result |
| --- | --- |
| `git branch --show-current` | PASS: `main` |
| `git status --short` before readiness checks | PASS: clean |
| `git diff --name-status` | PASS: no unstaged diff |
| `git diff --cached --name-status` | PASS: no staged diff |
| `git pull --rebase origin main` | PASS: already up to date |
| Required file existence check | PASS: 10 required files present |
| Reviewer-facing docs existence check | PASS: 10 docs present |
| UI page existence check | PASS: 5 required UI pages present; oil-type/support and archive/governance pages present |
| Custom stdlib readiness verification | PASS: files, docs, UI pages, evidence order, table labels, figure labels/paths, final values, and claim-boundary tokens passed |
| `python -m pytest -q tests/test_figure_package_publication.py` | NOT RUN TO COMPLETION: numpy unavailable locally; no install/download attempted. |
| `pwsh ./start.ps1 -List -NoPause` | NOT RUN: `pwsh` unavailable locally |
| `pwsh ./start.ps1 -Help -NoPause` | NOT RUN: `pwsh` unavailable locally |
| `pwsh ./start.ps1 -ValidateMatrix -NoPause` | NOT RUN: `pwsh` unavailable locally |

## 4. Validators Passed/Failed

| Validator | Result |
| --- | --- |
| `python scripts/validate_final_paper_guardrails.py` | PASS |
| `python scripts/validate_archive_registry.py` | PASS |
| `python scripts/validate_paper_to_output_registry.py` | PASS: 57 entries, 0 errors, 0 warnings |
| `python scripts/validate_data_sources_registry.py` | PASS: 13 sources registered |
| `python -m src.utils.validate_launcher_matrix --no-write` | PASS: 27 entries, 27 pass, 0 fail |
| Guardrail pytest bundle | PASS: 34 passed |

## 5. Publication Figure Package Status

- Required Figure 4.1 through Figure 4.13 PNG paths exist.
- `publication_figure_registry.csv` contains the final Figure 4.1 through Figure 4.13 labels and paths.
- `publication_figure_manifest.json` contains the final Figure 4.1 through Figure 4.13 labels and paths.
- No `optional_missing` marker remains on final Figure 4.1 through Figure 4.13 registry rows.
- Optional pytest status: NOT RUN TO COMPLETION: numpy unavailable locally; no install/download attempted.

## 6. Remaining Warnings

- Local optional dependency warning: `numpy` is not installed, so `tests/test_figure_package_publication.py` failed during collection before executing assertions.
- Local tool warning: `pwsh` is not installed, so the exact launcher commands requested for PowerShell Core were not run.

## 7. Files Still Needing Manual Review

None identified by the readiness checks.

## 8. Claim-Boundary Verification Summary

Claim-boundary validation passed through `scripts/validate_final_paper_guardrails.py` and the guardrail pytest bundle. The stdlib sweep also confirmed the required boundary language is present:

- PyGNOME remains comparator-only and never observational truth.
- DWH remains external transfer validation only and does not recalibrate Mindoro.
- `mask_p90` remains a conservative high-confidence core/support product, not a broad envelope.
- The Mindoro March 13-14 case does not claim exact 1 km success.
- Secondary 2016 remains drifter-track and legacy FSS support, not public-spill validation.
- Mindoro oil-type and shoreline outputs remain support/context only.
- Raw `CASE_*` paths remain optional, staging, provenance, or archive paths unless curated for review.

## 9. Table-Label Verification Summary

PASS. The required final table labels were found across the reviewer-facing alignment/config/package text checked by the stdlib sweep:

- Chapter 3 labels checked: Tables 3.7 through 3.17.
- Chapter 4 labels checked: Tables 4.1 through 4.13, including 4.11A and 4.11B.
- No blocking stale table-label mismatch was detected by the final-paper guardrail validator.

## 10. Figure-Label Verification Summary

PASS. The required final Figure 4 labels and PNG paths were verified in the publication registry and manifest:

- Figure 4.1 through Figure 4.13 labels are present.
- Figure 4.4A, Figure 4.4B, and Figure 4.4C are present.
- Every required Figure 4 PNG path exists under `output/figure_package_publication`.
- No final Figure 4 row is marked `optional_missing`.

## 11. Final-Value Verification Summary

PASS. The stdlib sweep confirmed the required final value tokens for:

- Focused Mindoro Phase 1 workflow mode, historical window, focused box, accepted segments, ranked subset, selected recipe, and four-recipe ranking values.
- Primary Mindoro March 13-14 FSS, cell-count, distance, IoU, and Dice values.
- Mindoro same-case OpenDrift-PyGNOME comparator cell-count, distance, and mean-FSS values.
- DWH daily FSS, corridor mean FSS, and event-corridor geometry values.
- Mindoro oil-type support/context beached percentages, arrival time, impacted segment counts, and QC status.
- Secondary 2016 drifter-track separation values and legacy FSS values.

## 12. Safe Commands for a Panel Reviewer

```powershell
./panel.ps1
./start.ps1 -Panel
./start.ps1 -List -NoPause
python scripts/validate_paper_to_output_registry.py
```

## 13. Git Diff Summary Before Commit

Before report creation, the tree was clean after `git pull --rebase origin main`.

Expected final report commit diff:

```text
A output/submission_readiness/SUBMISSION_READINESS_REPORT.md
```

## 14. Proposed Final Commit Message for Any Future Cleanup

`chore: address post-readiness optional local tool warnings`

## 15. Safety Statement

No scientific reruns, model simulations, remote downloads, manuscript-PDF extraction, or archive/provenance/legacy output deletions were performed during the final submission-readiness check.

## 16. Final Push Note

Final push result is reported in Codex final response; this report is not edited after final push.
