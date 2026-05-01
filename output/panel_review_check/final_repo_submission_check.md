# Final Repo Submission Check

Status: `ready_with_local_environment_limitations`

- Timestamp: `2026-05-01T15:27:10Z`
- Branch: `main`
- Commit checked: `081e78f325d61ccb42b6d8acb70a5a92de1d00f7`
- Checklist: `docs/FINAL_REPO_SUBMISSION_CHECKLIST.md`
- Machine report: `output/panel_review_check/final_repo_submission_check.json`
- Full local pytest limitation: missing numpy, pandas, xarray, geopandas, and yaml/PyYAML.
- `pwsh` limitation: executable unavailable; Windows PowerShell and Python fallback launcher checks passed.
- No expensive science rerun: true
- No scientific outputs fabricated: true
- GitHub Actions must be checked after push.

## Validation Summary

1. `python -m pytest tests/test_final_paper_consistency_guardrails.py -q` - pass. 9 passed in 0.81 s.
2. `python -m pytest tests -q` - local_environment_limitation. Collection stopped with 56 import errors and 2 skipped because the local Python 3.14 environment lacks numpy, pandas, xarray, geopandas, and yaml/PyYAML.
3. `python scripts/validate_archive_registry.py` - pass. Archive registry validation passed against launcher matrix.
4. `python scripts/validate_paper_to_output_registry.py` - pass. Paper-to-output registry validation passed: 51 entries, 0 errors, 0 warnings.
5. `python scripts/validate_data_sources_registry.py` - pass. Data-source provenance registry validation passed: 13 sources; roles and secret policy valid.
6. `python scripts/validate_final_paper_guardrails.py` - pass. Final-paper guardrail validation passed for labels, launcher routing, claim boundaries, values, registries, and p90 semantics.
7. `pwsh ./start.ps1 -ValidateMatrix -NoPause` - local_environment_limitation. pwsh executable is unavailable locally; Windows PowerShell fallback and Python launcher schema fallback passed.
8. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -ValidateMatrix -NoPause` - pass. Launcher matrix validation passed: 27 entries, 27 pass, 0 fail.
9. `python launcher-matrix fallback schema check` - pass. Fallback validation passed for 27 entries.
10. `pwsh ./start.ps1 -List -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback list completed.
11. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -List -NoPause` - pass. Catalog grouped by thesis role; archive/legacy/support/read-only routes are separated from main evidence.
12. `pwsh ./start.ps1 -ListRole primary_evidence -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback list completed.
13. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -ListRole primary_evidence -NoPause` - pass. Primary evidence entries listed: phase1_mindoro_focus_provenance, mindoro_phase3b_primary_public_validation, dwh_reportable_bundle, mindoro_reportable_core.
14. `pwsh ./start.ps1 -ListRole archive_provenance -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback list completed.
15. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -ListRole archive_provenance -NoPause` - pass. Archive/provenance aliases, archive entries, and hidden experimental entries listed separately from primary evidence.
16. `pwsh ./start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback explain completed.
17. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause` - pass. B1 preview confirms primary public-observation validation boundary, expensive rerun status, and curated Phase 3B output path.
18. `pwsh ./start.ps1 -Explain dwh_reportable_bundle -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback explain completed.
19. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -Explain dwh_reportable_bundle -NoPause` - pass. DWH preview confirms external transfer validation boundary, comparator-only PyGNOME, and curated Phase 3C output path.
20. `pwsh ./start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback dry run completed.
21. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause` - pass. Dry run printed the two Docker commands that would run and confirmed no workflow executed and no outputs modified.
22. `runtime-constructed forbidden-label scan` - pass. Forbidden manuscript label absent from tracked text files.
23. `uploaded manuscript filename / stale wording scan` - pass. No uploaded manuscript-like filenames or stale current-surface wording found. Safe negated overclaim hits: 66. Historical generated-output hits: 59, documented as archived/generated and not current reviewer-first docs/config.
