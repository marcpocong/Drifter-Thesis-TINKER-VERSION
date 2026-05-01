# Submission Crosswalk Patch Report

## Recovery Summary

Prompt 2 previously stopped before patching because the required initial pull failed:

```text
error: cannot pull with rebase: You have unstaged changes.
error: Please commit or stash them.
```

Prompt 2R stopped in Phase A because `scripts/figures/make_descriptive_label_manuscript_figures.py` was an untracked source-code file outside the safe pre-existing prompt/report path list.

For this recovery run, `scripts/figures/make_descriptive_label_manuscript_figures.py` was inspected before action:

- Size: `27317` bytes
- SHA256: `4e00eb45489d6278ea3b8b5efd30d956f96a55cc6c3ae7d0f5c62386a9b066dc`
- Classification: SAFE
- Basis: descriptive manuscript figure-label helper only; no network/download, subprocess, delete, model simulation, launcher, OpenDrift/PyGNOME rerun, or scientific-output mutation calls were found. The script reads stored rasters/tables/config-derived manifests and, when run, writes separate descriptive-label publication helper PNG/manifest outputs.
- Action: committed as safe preflight helper.
- Preflight script commit: `b814a030001eeedaf4aa6b059821cf28274b95c7`

Prompt 0 contract artifacts were also committed and pushed before patching:

- Preflight contract/report commit: `e1f1cad6ee9ab3f6c185503da2275b99c7d55435`
- Preflight push: `origin/main`

## Files Changed By Crosswalk / Registry Patch

- `docs/PAPER_TO_REPO_CROSSWALK.md`
- `config/paper_to_output_registry.yaml`
- `docs/PAPER_OUTPUT_REGISTRY.md`

No scientific output files were changed.

## Stale Labels Corrected

- Table 3.11 changed from stale Mindoro deterministic product setup to final Mindoro March 13-14 primary validation case definition.
- Table 3.12 changed from stale Mindoro ensemble/probability products to final Mindoro manuscript labels.
- Phase 2 deterministic and ensemble/probability product concepts were moved under Tables 3.9 and 3.10.
- Table 4.8 now carries the Mindoro same-case OpenDrift-PyGNOME comparator detail.
- DWH daily/event-corridor FSS is mapped to Table 4.9.
- DWH event-corridor geometry diagnostics are mapped to Table 4.10.
- Secondary 2016 direct drifter-track support starts at Table 4.11, with 4.11A and 4.11B assigned to scorecard summary and endpoint/ensemble-footprint diagnostics.
- Legacy 2016 OpenDrift-versus-PyGNOME mean FSS is mapped to Table 4.12.
- Table 4.13 is mapped to synthesis of principal findings and thesis use.
- Figure 4.1 changed from generic study-box context to the focused Phase 1 accepted February-April segment map.
- Figure 4.2 changed from generic geography/domain reference to the focused Phase 1 recipe ranking chart.
- Figure 4.3 was added as the Mindoro product-family board trace.
- Figures 4.7-4.9 were aligned to the final DWH board sequence.
- Figures 4.10-4.12 were aligned to the three named 2016 secondary drifter-track cases.
- Figure 4.13 was changed from legacy fate/shoreline support to the legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart.

## Validators Run

1. `python scripts/validate_paper_to_output_registry.py`
2. `python scripts/panel_verify_paper_results.py`

## Validator Results

`python scripts/validate_paper_to_output_registry.py` passed:

```text
Paper-to-output registry validation: PASS
Entries checked: 57
Errors: 0
Warnings: 0
```

`python scripts/panel_verify_paper_results.py` did not start because the local environment lacks PyYAML:

```text
ModuleNotFoundError: No module named 'yaml'
```

No dependency install or download was attempted.

## Git Diff Summary Before Final Commit

```text
config/paper_to_output_registry.yaml | 1067 +++++++++++++++++++---------------
docs/PAPER_OUTPUT_REGISTRY.md        |   86 +--
docs/PAPER_TO_REPO_CROSSWALK.md      |  157 +++--
3 files changed, 733 insertions(+), 577 deletions(-)
```

## Remaining Warnings

- The optional final-layout placeholders for Figures 4.1, 4.2, 4.3, and 4.6 are explicitly marked `optional_missing` in the registry with reasons; their stored source CSVs/manifests remain the reviewer-facing evidence.
- `python scripts/panel_verify_paper_results.py` requires the unavailable local `yaml` module.
- Git reported CRLF conversion warnings for edited Markdown files on this Windows checkout.

## Scientific Rerun / Download Statement

No scientific reruns, launcher entries, model simulations, remote downloads, or data downloads were performed.

## Final Push Note

Final push result is reported in Codex final response; this report is not edited after final push to avoid leaving a dirty working tree.
