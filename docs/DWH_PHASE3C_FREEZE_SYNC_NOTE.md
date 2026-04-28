# DWH Phase 3C Freeze Sync Note

This note records the final semantics-sync decision for the stored DWH `Phase 3C` lane.

- `C1` = deterministic external transfer validation
- `C2` = ensemble extension and deterministic-vs-ensemble comparison
- `C3` = PyGNOME comparator-only

These tracks are treated as frozen reportable stored outputs in the current repo state. This note does not authorize or imply any new DWH scientific rerun.

Guardrails that stay explicit:

- no drifter baseline
- truth = public observation-derived daily masks and the event-corridor union
- forcing = `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- date-composite honesty remains required
- PyGNOME is comparator-only and never the scoring reference

This sync is metadata/docs/config cleanup only. It does not reopen forcing selection, ingest new drifter data, rerun OpenDrift, or rerun PyGNOME.
