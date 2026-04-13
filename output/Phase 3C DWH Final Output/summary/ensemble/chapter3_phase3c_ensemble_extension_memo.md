# Chapter 3 Phase 3C Ensemble Extension Memo

Phase 3C1 is deterministic DWH transfer validation. Phase 3C2 extends that branch to ensemble DWH transfer validation on the same public observation-derived masks and the same frozen DWH scoring grid.

Mindoro remains the main Philippine thesis case. DWH remains a separate external transfer-validation/support case.

DWH observed masks remain truth. The cumulative DWH layer remains context-only. PyGNOME remains comparator-only in the later cross-model branch.

The ensemble branch keeps the same readiness-gated forcing rule and date-composite logic, rather than any Phase 1 drifter-selected baseline logic, while documenting DWH-specific differences such as clipping requested negative start offsets when the frozen forcing stack begins at the nominal case start time.

In the current repo state, that frozen DWH stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.
