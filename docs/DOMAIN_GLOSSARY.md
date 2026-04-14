# Domain Glossary

This repo now keeps three thesis-facing geographic concepts separate:

| Key | Meaning | Current bounds |
| --- | --- | --- |
| `mindoro_phase1_focused_validation_box` | Active thesis-facing focused Mindoro Phase 1 validation box used by the separate `phase1_mindoro_focus_pre_spill_2016_2023` provenance lane that supports the B1 recipe story. | `[118.751, 124.305, 10.620, 16.026]` |
| `phase1_validation_box` | Chapter 3 historical/regional transport-validation box used by the broader `phase1_regional_2016_2022` reference lane, Phase 1 reruns, and settings-level fallback metadata. | `[119.5, 124.5, 11.5, 16.5]` |
| `mindoro_case_domain` | Broad official Mindoro spill-case fallback transport/forcing domain and overview extent for the March 2023 case workflow. It is not the focused Phase 1 validation box and not the canonical scoring-grid display bounds. | `[115.0, 122.0, 6.0, 14.5]` |
| `scoring_grid.display_bounds_wgs84` | Canonical scoreable Mindoro scoring-grid display bounds used when the stored scoring-grid artifact is present. This is the narrow operational scoring extent, not the broad fallback `mindoro_case_domain`. | `[120.90964677179262, 122.0621541786303, 12.249384840763462, 13.783655303175253]` |
| `prototype_2016_first_code_search_box` | Historical-origin search box used by the very first prototype code that surfaced the first three 2016 drifter cases on the west coast of the Philippines. It is support/provenance metadata only and does not replace the stored per-case local prototype extents. | `[108.6465, 121.3655, 6.1865, 20.3515]` |
| `legacy_prototype_display_domain` | Prototype/debug plotting extent only. This can differ by prototype lane and must not be reused as the official study-area label. | repo default `[115.0, 122.0, 6.0, 14.5]`; `prototype_2021` override `[119.5, 124.5, 11.5, 16.5]` |

Compatibility notes:

- `region` remains a backward-compatible alias only.
- `CaseContext.region` still resolves to the active workflow domain or fallback extent so older runtime code keeps working.
- Thesis-facing box summaries and the shared publication figure should prefer the explicit Mindoro-focused, Mindoro spill-case, scoring-grid, and prototype first-code keys above rather than collapsing them into one box.
- For official Mindoro runs, stored scoring-grid display bounds should be treated as the scoreable extent when available; `mindoro_case_domain` remains the broader fallback case-domain label.
- Thesis-facing configs, audits, and summaries should prefer the explicit keys above.
