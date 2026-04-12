# Domain Glossary

This repo now keeps three thesis-facing geographic concepts separate:

| Key | Meaning | Current bounds |
| --- | --- | --- |
| `phase1_validation_box` | Chapter 3 historical/regional transport-validation box used for drifter-window acceptance, Phase 1 reruns, and Phase 1 audit metadata. | `[119.5, 124.5, 11.5, 16.5]` |
| `mindoro_case_domain` | Official Mindoro spill-case transport/scoring domain for the March 2023 case workflow. | `[115.0, 122.0, 6.0, 14.5]` |
| `legacy_prototype_display_domain` | Prototype/debug plotting extent only. This can differ by prototype lane and must not be reused as the official study-area label. | repo default `[115.0, 122.0, 6.0, 14.5]`; `prototype_2021` override `[119.5, 124.5, 11.5, 16.5]` |

Compatibility notes:

- `region` remains a backward-compatible alias only.
- `CaseContext.region` still resolves to the active workflow domain so older runtime code keeps working.
- Thesis-facing configs, audits, and summaries should prefer the explicit keys above.
