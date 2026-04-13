# Chapter 3 Phase 3C External Case Run Memo

Phase 3C is the external rich-data transfer-validation branch placed after Phase 3B and before Phase 4.

Mindoro remains the main Philippine thesis case. Deepwater Horizon is a separate external transfer-validation/support case used to test whether the workflow carries into an observation-rich spill without replacing Mindoro as the main case.

The DWH truth source is the public observation-derived daily mask set for May 21, May 22, and May 23, 2010. The cumulative DWH layer remains context-only and is not used as truth.

The DWH run uses date-composite logic because the public daily layers do not support defensible exact sub-daily acquisition times.

The DWH forcing family is chosen by scientific-readiness gating rather than by Phase 1 drifter-selected baseline logic. This branch freezes the first complete real historical current+wind+wave stack that is not smoke-only, spans the required DWH window, exposes the required variables with usable metadata, opens in the OpenDrift reader, and passes the reader-check forecast.

In the current stored run, that frozen stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.

PyGNOME comparator run status: False. Any PyGNOME output remains comparator-only, not truth.
