# Final Submission Readiness

QA date: 2026-05-02

## Result

PASS. The launcher and read-only dashboard are ready for panel review in the tested environment. No expensive science was run, no support/archive/legacy material was promoted to primary evidence, and no tracked changes appeared under `output/`, `data_processed/`, or `thesis_outputs/` during this QA pass.

## Launcher Commands Tested

`pwsh` was not installed on this workstation, so the documented Windows PowerShell 5.1 fallback was used for the same launcher calls:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ValidateMatrix -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Help -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -List -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ListRole primary_evidence -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ListRole read_only_governance -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\panel.ps1 -NoPause
```

All passed. `panel.ps1 -NoPause` returned without hanging.

## UI Commands Tested

```powershell
python -m py_compile ui/app.py ui/data_access.py ui/pages/common.py ui/pages/__init__.py
```

Passed.

## Test Suite

Plain `pytest` was not on PATH in this shell, so the repository virtualenv was used:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_final_paper_consistency_guardrails.py tests/test_defense_claim_boundaries.py tests/test_defense_launcher_matrix.py tests/test_launcher_matrix_metadata.py tests/test_launcher_menu_docs_consistency.py tests/test_start_ps1_validate_matrix.py tests/test_start_ps1_interactive_navigation.py tests/test_defense_panel_smoke.py tests/test_defense_dashboard_imports.py tests/test_ui_app_smoke.py tests/test_ui_data_access.py tests/test_ui_display_vocabulary.py tests/test_ui_readonly_support.py -q
```

Result: `141 passed, 28 skipped, 142 subtests passed`.

## Docker Checks

Docker Compose was available.

```powershell
docker compose config
docker compose up -d pipeline
docker compose exec -T pipeline python -m py_compile ui/app.py ui/data_access.py ui/pages/common.py ui/pages/__init__.py
```

All passed.

Dashboard import/state check:

```powershell
@'
import ui.app
from ui.data_access import build_dashboard_state
state = build_dashboard_state()
assert isinstance(state, dict)
print("dashboard state ok", len(state))
'@ | docker compose exec -T pipeline python -
```

Result: `dashboard state ok 82`.

## No-Science-Change Confirmation

Before and after this QA pass:

```powershell
git status --short -- output data_processed thesis_outputs
```

returned no changes. No stored scorecards, stored rasters, `data_processed/` files, `thesis_outputs/` files, or scientific result values were modified. No allowed read-only packaging tests created tracked output files during this QA pass.

## Known Limitations

- PowerShell 7 (`pwsh`) was not installed on this workstation; Windows PowerShell 5.1 fallback was verified.
- Plain `pytest` was not on PATH; the repository virtualenv pytest entrypoint was verified.
- Some UI smoke tests skip locally when optional dashboard runtime dependencies are absent; Docker-backed import and state-build checks passed.
- The Docker check starts/reuses the `pipeline` service. No Streamlit dashboard process was left running after QA.

## Final Reviewer Instructions

Start here:

```powershell
.\panel.ps1
```

Equivalent launcher path:

```powershell
.\start.ps1 -Panel
```

Direct dashboard shortcut:

```powershell
.\start.ps1 -Dashboard -NoPause
```

Dashboard URL:

```text
http://localhost:8501
```

The panel path and dashboard are read-only review surfaces over stored outputs. Scientific reruns require explicit full-launcher entry selection and confirmation.
