# UI Guide

## Purpose

The Streamlit dashboard is a read-only final-paper review surface over already packaged artifacts. It presents stored outputs, figures, registries, and governance notes; it must not rerun science, mutate outputs, rewrite manifests, edit configs, or reinterpret stored result values.

Use [FINAL_PAPER_ALIGNMENT.md](FINAL_PAPER_ALIGNMENT.md) as the source-of-truth bridge for current manuscript alignment. Use [DATA_SOURCES.md](DATA_SOURCES.md) and [ARCHIVE_GOVERNANCE.md](ARCHIVE_GOVERNANCE.md) for provenance and archive routing.

## Layout System

The redesigned UI uses a shared card-and-section system in `ui/pages/common.py` so pages read like a modern thesis-defense microsite rather than a stack of default Streamlit alerts. The main defense pages follow this pattern:

1. Modern hero with the page title, evidence-role pill, and one-sentence purpose.
2. Key takeaway card for panel-facing interpretation.
3. Main result area with feature figures, metric cards, or comparison cards.
4. Caveat or boundary ribbon that keeps evidence limits visible.
5. Details section for tables, registries, notes, and extra figures.
6. Archive/support material separated visually from the main claim.

Archive, legacy, reference, and advanced pages use the same helpers, but with muted archive/advanced styling so they remain visibly secondary.

## Reusable Helpers

Primary shared helpers live in `ui/pages/common.py`:

- `render_modern_hero(...)`
- `render_key_takeaway(...)`
- `render_caveat_ribbon(...)`
- `render_support_notice(...)`
- `render_archive_notice(...)`
- `render_evidence_path(...)`
- `render_feature_card(...)`
- `render_feature_grid(...)`
- `render_metric_story_grid(...)`
- `render_section_header(...)`
- `render_figure_feature(...)`
- `render_page_footer_note(...)`
- Existing read-only table, gallery, package, markdown, badge, and export helpers.

These helpers use escaped HTML where appropriate and should remain dependency-free beyond the existing Streamlit/Pandas/Pillow stack.

## Running The Dashboard

Preferred launcher shortcut:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -Dashboard -NoPause
```

The dashboard shortcut creates `.env` from `.env.example` when `.env` is missing, starts only the `pipeline` compose service, reuses an existing Streamlit process on port `8501` when one is already serving, and prints `http://localhost:8501`.

Dry-run the dashboard shortcut without Docker:

```powershell
.\start.ps1 -Dashboard -DryRun -NoPause
```

Start the pipeline container if needed:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d pipeline
```

Launch Streamlit:

```powershell
docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Open:

```text
http://localhost:8501
```

If Docker is unavailable, the launcher prints: `Docker is not available. Install/start Docker Desktop, then rerun .\panel.ps1 or run the manual Streamlit command in docs/UI_GUIDE.md.`

For print/export review, append `?export=1` to a page URL. Export mode hides sidebar/navigation chrome and keeps cards/figures print-friendly.

## Viewing Modes

Panel-friendly mode is the default. It should show the final-paper evidence order first:

1. Overview / Final Manuscript Alignment
2. Data Sources & Provenance
3. Focused Mindoro Phase 1 Provenance
4. Primary Mindoro March 13-14 Validation Case (B1)
5. Mindoro Same-Case OpenDrift-PyGNOME Comparator (Track A)
6. DWH External Transfer Validation
7. Mindoro Oil-Type and Shoreline Support/Context
8. Secondary 2016 Support
9. Archive/Provenance and Legacy Support
10. Reproducibility / Governance / Audit

Advanced mode exposes technical inspection layers:

- B1 Recipe Provenance - Not Truth Mask
- Oil-Type/Shoreline Comparator Availability
- Trajectory Explorer
- Raw/panel galleries and registry previews where available

Advanced pages are audit surfaces only. They should not look like primary validation pages.

## Evidence Guardrails

Keep these claims stable across page text, labels, captions, and export views:

- Focused Mindoro Phase 1 is transport provenance / recipe selection, not oil-footprint truth.
- The Primary Mindoro March 13-14 validation case is the only main Philippine public-observation validation claim; `B1` is an internal alias.
- The primary case supports coastal-neighborhood usefulness, not exact 1 km overlap.
- The March 13-14 primary case keeps the observation-independence / reinitialization note visible.
- The Mindoro same-case OpenDrift-PyGNOME comparator and PyGNOME are comparator-only support; `Track A` is an internal alias.
- DWH is external transfer validation only; it does not recalibrate Mindoro.
- Mindoro oil-type and shoreline outputs are support/context only.
- Secondary 2016 outputs are drifter-track and legacy FSS support only; they are not public-spill validation.
- Archive/provenance and legacy/debug outputs are preserved for provenance, audit, and reproducibility only.
- UI/publication packages are presentation and governance surfaces over stored outputs only.
Dashboard pages organize stored outputs only and do not create new scientific results.

Legacy/internal folder names may contain Phase 3B, Phase 3C, or Phase 4, but panel-facing labels should use Primary Mindoro March 13-14 validation case, Mindoro same-case OpenDrift-PyGNOME comparator, DWH external transfer validation, and Mindoro oil-type/shoreline support/context.

## Read-Only Rules

UI work may modify presentation files such as:

- `ui/app.py`
- `ui/assets/style.css`
- `ui/pages/common.py`
- `ui/pages/*.py`
- `docs/UI_GUIDE.md`

UI work must not modify:

- `output/`
- `data_processed/`
- science scripts or rerun pipelines
- configs/manifests that define scientific runs
- stored scorecards or scientific result values

The dashboard should fail softly if optional artifacts are absent, but it must never fill gaps by generating new science.

## Visual QA Checklist

Before review, check the app at laptop and projector widths:

- Main pages should use cards, ribbons, metrics, and figure boards, not piles of default `st.info` / `st.warning` blocks.
- Long metric values should wrap cleanly without horizontal overflow.
- Figure cards should keep captions readable and avoid large empty gaps.
- Sidebar navigation should be readable, structured, and less visually dominant than the page content.
- Archive/provenance and legacy/debug pages should use muted hero/card styling and remain visually secondary.
- Export mode should hide chrome and avoid breaking inside hero/cards where possible.

## Branding

Optional branding assets are supported and should fail gracefully when missing:

- `ui/assets/logo.svg` or `ui/assets/logo.png`
- `ui/assets/logo_icon.svg` or `ui/assets/logo_icon.png`

See `docs/UI_BRANDING.md` and `ui/assets/README.md` for asset naming guidance.
