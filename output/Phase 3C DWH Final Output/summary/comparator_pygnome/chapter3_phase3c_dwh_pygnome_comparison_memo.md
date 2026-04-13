# Chapter 3 Phase 3C DWH PyGNOME Comparison Memo

Phase 3C is the external rich-data transfer-validation branch. Mindoro remains the main Philippine thesis case, and DWH remains a separate external transfer-validation/support case.

The DWH observed masks remain truth throughout this comparison. The cumulative DWH layer remains context-only.

The DWH forcing stack remains the readiness-gated historical stack, not a Phase 1 drifter-selected baseline choice. In the current repo state that stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.

OpenDrift deterministic and the OpenDrift ensemble products remain the main science tracks for the DWH external case. PyGNOME is comparator only, not truth.

This branch completes the DWH cross-model validation story for Phase 3C by placing the new PyGNOME comparator on the same EPSG:32616 1 km scoring grid and against the same May 21-23 public observation-derived masks and event-corridor logic.

Where PyGNOME cannot reproduce the OpenDrift/OpenOil forcing stack identically, the mismatch is stated explicitly rather than hidden. In the current implementation PyGNOME reuses the scientific DWH current and wind family but does not attach a matching Stokes-wave mover.
