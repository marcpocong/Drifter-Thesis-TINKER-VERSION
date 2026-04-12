# DWH Phase 3C Methods Note

Deepwater Horizon (DWH) Phase 3C is treated as a separate external transfer-validation case, not as the main Philippine thesis case. Its role is to test whether the transport-validation workflow argued primarily on the Mindoro case transfers to a richer public-observation spill without collapsing the two case stories into one claim.

The May 20, 2010 `T20100520_Composite` polygon is used as the observation-initialized release geometry, and the May 21, 22, and 23 daily composites are converted into separate validation masks on the fixed EPSG:32616 1 km scoring grid. The DWH wellhead is retained as provenance only, and the cumulative composite is context-only rather than quantitative truth. Because the public layer names encode dates but do not provide defensible sub-daily acquisition times, Phase 3C uses date-composite logic rather than invented timestamps.

DWH does not inherit the Phase 1 drifter-selected baseline recipe logic. Instead, it freezes the first complete real historical current+wind+wave stack that passes a scientific-readiness gate for the May 20-23, 2010 window: non-smoke inputs only, full temporal coverage, required variables and usable metadata present, clean OpenDrift reader exposure, and a successful small reader-check forecast. In the current repo state, that frozen DWH stack is HYCOM GOFS 3.1 currents, ERA5 winds, and CMEMS wave/Stokes.

OpenDrift deterministic output is the main DWH Phase 3C transport result, and the 50-member ensemble is a support/comparative extension on the same scoring grid and observation masks. Validation is performed against the observed daily masks and the May 21-23 event corridor derived from them. PyGNOME is retained as a comparator-only cross-model branch; it is scored against the same observed masks but is not treated as truth, and any wave/Stokes mismatch with the OpenDrift stack is stated explicitly.

In thesis framing, DWH Phase 3C supports claims of external transferability and robustness, while Mindoro remains the main Philippine case and Phase 4 remains Mindoro-only in the current framework.
