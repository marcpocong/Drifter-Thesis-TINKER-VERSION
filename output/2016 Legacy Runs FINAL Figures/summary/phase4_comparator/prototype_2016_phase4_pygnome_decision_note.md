# Prototype 2016 Phase 4 PyGNOME Comparator Decision

A limited deterministic Phase 4 PyGNOME comparator is packaged for the legacy `prototype_2016` lane.

- Scope: budget-only descriptive comparison
- Cases: 2016-09-01, 2016-09-06, 2016-09-17
- Scenarios: `light` and `heavy` only; no frozen `base` scenario exists in the stored prototype_2016 Phase 4 weathering package
- Forcing: matched case-specific grid wind and grid current when available in the stored 2016 forcing directories
- Metrics: absolute percentage-point difference at 24/48/72 h plus MAE/RMSE across the normalized budget-fraction time series
- Guardrail: these are comparator-only descriptive cross-model differences, not observational skill metrics
- Shoreline comparison: not packaged, because the PyGNOME pilot does not write matched shoreline-arrival or shoreline-segment products
