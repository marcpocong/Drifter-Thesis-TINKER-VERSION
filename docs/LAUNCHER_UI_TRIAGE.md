# Launcher And UI Triage

Date run: 2026-05-02

Scope: diagnosis only. No scientific result values, stored scorecards, stored rasters, stored manifests, or curated result registries should be edited to repair these issues.

## Commands Run

| Command | Result | Notes |
| --- | --- | --- |
| `pwsh ./start.ps1 -Help -NoPause` | FAIL | Host environment issue: `pwsh` is not installed or not on PATH. Repo code did not run. |
| `pwsh ./start.ps1 -ValidateMatrix -NoPause` | FAIL | Same `pwsh` host issue. Repo code did not run. |
| `pwsh ./start.ps1 -List -NoPause` | FAIL | Same `pwsh` host issue. Repo code did not run. |
| `pwsh ./start.ps1 -ListRole primary_evidence -NoPause` | FAIL | Same `pwsh` host issue. Repo code did not run. |
| `pwsh ./start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause` | FAIL | Same `pwsh` host issue. Repo code did not run. |
| `pwsh ./panel.ps1 -NoPause` | FAIL | Same `pwsh` host issue. Repo code did not run. |
| `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Help -NoPause` | PASS | Windows PowerShell fallback works. |
| `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ValidateMatrix -NoPause` | PASS WITH SIDE EFFECT | Validation passed, but wrote `output/launcher_matrix_validation/launcher_matrix_audit.*`. This is not science, but it violates the intended no-output-write triage posture. |
| `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -List -NoPause` | PASS | Catalog lists successfully. |
| `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ListRole primary_evidence -NoPause` | PASS | Role filter lists the four primary-evidence entries. |
| `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause` | PASS | Preview only; no workflow executed. |
| `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\panel.ps1 -NoPause` | PASS | Non-interactive panel preview returns immediately; no hang. |
| `docker compose config` | PASS | Compose file is valid. Caution: the command expands `.env` values into stdout, so do not paste its full output into public docs. |
| `docker compose up -d pipeline` | PASS | `phase1` container was already/running. Service command is `tail -f /dev/null`, so this did not run science. |
| `docker compose exec -T pipeline python -m py_compile ui/app.py` | PASS | No syntax error. |
| `docker compose exec -T pipeline python -m py_compile ui/data_access.py ui/pages/common.py ui/pages/__init__.py` | PASS | No syntax error. |
| `docker compose exec -T pipeline python - <<'PY' ... PY` | FAIL | Windows PowerShell syntax issue. Bash heredoc form is not valid in this shell. |
| PowerShell-equivalent pipe of the same Python payload into `docker compose exec -T pipeline python -` | FAIL | Actual UI state-build error found: unknown publication `status_key`. |

## Exact Errors

### `pwsh` Not Available

All six requested `pwsh ...` commands failed with the same command-resolution error before repo code ran. Example from `pwsh ./start.ps1 -Help -NoPause`:

```text
pwsh : The term 'pwsh' is not recognized as the name of a cmdlet, function, script file, or operable program. Check 
the spelling of the name, or if a path was included, verify that the path is correct and try again.
At line:2 char:1
+ pwsh ./start.ps1 -Help -NoPause
+ ~~~~
    + CategoryInfo          : ObjectNotFound: (pwsh:String) [], CommandNotFoundException
    + FullyQualifiedErrorId : CommandNotFoundException
```

The same message appeared for:

```text
pwsh ./start.ps1 -ValidateMatrix -NoPause
pwsh ./start.ps1 -List -NoPause
pwsh ./start.ps1 -ListRole primary_evidence -NoPause
pwsh ./start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
pwsh ./panel.ps1 -NoPause
```

### Bash Heredoc In Windows PowerShell

The exact heredoc command failed in the host shell before Python ran:

```text
At line:2 char:43
+ docker compose exec -T pipeline python - <<'PY'
+                                           ~
Missing file specification after redirection operator.
At line:2 char:42
+ docker compose exec -T pipeline python - <<'PY'
+                                          ~
The '<' operator is reserved for future use.
At line:2 char:43
+ docker compose exec -T pipeline python - <<'PY'
+                                           ~
The '<' operator is reserved for future use.
At line:4 char:1
+ from ui.data_access import build_dashboard_state, dashboard_state_sig ...
+ ~~~~
The 'from' keyword is not supported in this version of the language.
At line:5 char:20
+ print("signature:", dashboard_state_signature())
+                    ~
Missing expression after ','.
At line:5 char:21
+ print("signature:", dashboard_state_signature())
+                     ~~~~~~~~~~~~~~~~~~~~~~~~~
Unexpected token 'dashboard_state_signature' in expression or statement.
At line:5 char:20
+ print("signature:", dashboard_state_signature())
+                    ~
Missing closing ')' in expression.
At line:5 char:47
+ print("signature:", dashboard_state_signature())
+                                               ~
An expression was expected after '('.
At line:5 char:48
+ print("signature:", dashboard_state_signature())
+                                                ~
Unexpected token ')' in expression or statement.
At line:6 char:31
+ state = build_dashboard_state()
+                               ~
An expression was expected after '('.
Not all parse errors were reported.  Correct the reported errors and try again.
    + CategoryInfo          : ParserError: (:) [], ParentContainsErrorRecordException
    + FullyQualifiedErrorId : MissingFileSpecification
```

Use a PowerShell here-string pipe for this shell:

```powershell
@'
import ui.app
from ui.data_access import build_dashboard_state, dashboard_state_signature
print("signature:", dashboard_state_signature())
state = build_dashboard_state()
print("state keys:", len(state))
print("publication figures:", len(state.get("publication_registry", [])))
'@ | docker compose exec -T pipeline python -
```

### Actual UI State-Build Error

The PowerShell-compatible equivalent reached the container and failed during `build_dashboard_state()`:

```text
signature: 363fd6c94417f2d70dbb606117316ee588f90e17fbf96fb03f06df03c6dc4b40
2026-05-01 20:07:42.783 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
2026-05-01 20:07:42.792 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
2026-05-01 20:07:43.530 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
Traceback (most recent call last):
  File "<stdin>", line 4, in <module>
  File "/app/ui/data_access.py", line 1755, in build_dashboard_state
    "publication_registry": publication_registry(root),
  File "/app/ui/data_access.py", line 526, in publication_registry
    payload = _attach_status_fields(
  File "/app/ui/data_access.py", line 402, in _attach_status_fields
    status_rows.append(artifact_status_columns_for_key(status_key, row))
  File "/app/src/core/artifact_status.py", line 794, in artifact_status_columns_for_key
    status = STATUS_REGISTRY[status_key]
KeyError: 'focused_phase1_transport_provenance'
```

Additional publication registry status keys currently missing from `src/core/artifact_status.py`:

```text
focused_phase1_recipe_provenance
focused_phase1_transport_provenance
legacy_2016_fss_support
mindoro_product_family_support
secondary_2016_drifter_track_support
```

## Failure Categories

| Category | Finding |
| --- | --- |
| PowerShell syntax or execution-policy issue | `pwsh` is unavailable on this Windows host. Windows PowerShell works with `-ExecutionPolicy Bypass`. No execution-policy failure was observed. |
| Docker Compose detection issue | Docker Compose v2 works. `docker compose config` and `docker compose up -d pipeline` passed. |
| Missing `.env` or container startup issue | `.env` exists and compose starts. `docker compose config` expands sensitive `.env` values into stdout; docs should avoid asking users to paste full config output. |
| Path quoting issue, especially output folders with spaces | No direct path quoting failure reproduced. Compose mounts the repo-level `output` directory; UI uses `Path(...)` objects for folders such as `output/Phase 3B March13-14 Final Output`. |
| `launcher_matrix` alias/role/list/explain issue | Matrix validation passed and list/list-role/explain work under Windows PowerShell. Hidden aliases resolve in the validator. |
| Streamlit import/version issue | No `py_compile` or `import ui.app` failure. Streamlit only warns that no runtime is present during direct Python import. |
| UI optional artifact load issue | Not the primary failure. The crash occurs while building global dashboard state, before page-level optional artifact guards can catch it. |
| Missing read-only package or missing registry issue | The publication registry exists. The problem is a contract mismatch: stored registry status keys are newer than `STATUS_REGISTRY`. |
| Non-interactive prompt / `-NoPause` hang issue | Not observed. Windows PowerShell `-NoPause` help/list/list-role/explain/panel preview all returned. |

## Suspected Root Causes

1. The documented/requested `pwsh` command shape assumes PowerShell 7. This machine only has Windows PowerShell on PATH.
2. The heredoc diagnostic command is Bash syntax but was run from Windows PowerShell.
3. `ui.data_access.publication_registry()` trusts stored `status_key` values and calls `artifact_status_columns_for_key()` for each nonblank key. The stored publication registry now contains five final Figure 4 / support status keys that are not defined in `src/core/artifact_status.py`.
4. `start.ps1 -ValidateMatrix -NoPause` advertises validation without Docker or science execution, but it does not pass the existing `--no-write` flag to `src.utils.validate_launcher_matrix`, so it writes audit files under `output/launcher_matrix_validation/`.

## Minimal Safe Repair Plan

1. Environment or docs: either install PowerShell 7 so `pwsh` works, or document the Windows fallback:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\panel.ps1 -NoPause
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Help -NoPause
```

2. Replace Bash heredoc examples in Windows-facing docs with a PowerShell here-string pipe, or provide both Bash and PowerShell forms.
3. Patch the UI status contract in code only, without editing stored outputs:
   - `src/core/artifact_status.py`: add the five missing status keys to `STATUS_REGISTRY` and map them in `STATUS_SURFACE_KEY_MAP`.
   - `src/core/publication_figure_governance.py`: route these statuses to the correct dashboard surfaces and display order so the final-paper hierarchy is preserved.
   - `ui/evidence_contract.py`: include focused Phase 1 and product-family statuses in thesis-facing allowed sets, and secondary 2016 / legacy FSS statuses in secondary/legacy support sets.
4. Patch validation launch behavior:
   - `start.ps1`: call `python -m src.utils.validate_launcher_matrix --no-write` for the default `-ValidateMatrix -NoPause` path, or add an explicit opt-in write flag.
   - `src/utils/validate_launcher_matrix.py` already has `--no-write`; no scientific logic change is needed.
5. Re-run only cheap checks:
   - launcher help/list/list-role/explain/panel preview,
   - `docker compose config`,
   - `docker compose up -d pipeline`,
   - UI `py_compile`,
   - the PowerShell-compatible dashboard state import probe.

## Files To Patch

- `src/core/artifact_status.py`
- `src/core/publication_figure_governance.py`
- `ui/evidence_contract.py`
- `start.ps1`
- `docs/UI_GUIDE.md`
- `docs/LAUNCHER_USER_GUIDE.md`
- `PANEL_QUICK_START.md`

## Files And Values That Must Not Change

- `output/` stored scientific results, stored scorecards, stored rasters, stored manifests, and stored publication registries.
- `data_processed/`.
- Any stored result values or evidence hierarchy.
- PyGNOME must remain comparator-only.
- Streamlit must remain read-only and must not recompute science.

## Triage Side Effects Observed

- `powershell.exe ... .\start.ps1 -ValidateMatrix -NoPause` wrote ignored launcher audit files under `output/launcher_matrix_validation/`. This should be treated as a launcher validation side-effect bug, not as scientific output generation.
- Docker/Python `py_compile` created ignored `__pycache__` folders under `ui/`; those generated bytecode folders were removed after the check.
