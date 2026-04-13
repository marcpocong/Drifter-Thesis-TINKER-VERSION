# Phase 4 Comparator Decision

Final decision: no thesis-facing Mindoro Phase 4 PyGNOME comparator is packaged yet.

Why:

- the current Mindoro Phase 4 bundle is a real OpenDrift/OpenOil scenario package with stored budget summaries, shoreline-arrival rows, and shoreline-segment tables
- the available Mindoro PyGNOME branch is a transport comparator with `weathering_enabled=false`
- the stored PyGNOME NetCDF diagnostics collapse to `100% surface / 0% evaporated / 0% dispersed / 0% beached`
- no matched PyGNOME shoreline-arrival table or shoreline-segment registry exists in the current repo outputs

What this means:

- current Mindoro Phase 4 results remain OpenDrift/OpenOil scenario outputs only
- PyGNOME is not promoted into a Phase 4 comparison claim
- the read-only audit verdict is still reportable as an honesty/provenance note
- any future comparator must be a separate matched Mindoro PyGNOME Phase 4 branch with the same scenario registry, budget semantics, and shoreline tables before it can be shown as a cross-model Phase 4 result

Guardrails:

- this does not change the existing Mindoro B1 Phase 3B validation claim
- this does not change DWH Phase 3C
- this does not rerun OpenDrift, OpenOil, or PyGNOME science
