# README Finalization Checklist

## What Was Changed

- Replaced the generic repository title in `README.md` with the exact manuscript title.
- Added a panel-ready top summary and kept panel mode commands near the top.
- Rewrote the main evidence order so `README.md` and the panel docs match the final manuscript structure.
- Added compact stored-result checklists for focused Phase 1 provenance, Mindoro `B1`, Mindoro `Track A`, DWH, and Mindoro oil-type / shoreline support.
- Reframed `prototype_2016` as legacy/archive support only.
- Added a lightweight docs consistency checker at [scripts/check_docs_against_manuscript_claims.py](../scripts/check_docs_against_manuscript_claims.py).

## Exact Source-Of-Truth Manuscript Facts

### Title And Evidence Order

- Title: `Drifter-Validated 24–72 h Oil-Spill Forecasting for Philippine Coasts: Probability Footprints and Oil-Type Fate`
- Focused Mindoro Phase 1 provenance
- Phase 2 standardized forecast products
- Mindoro `B1` primary public-observation validation
- Mindoro `Track A` comparator-only support
- DWH external transfer validation
- Mindoro oil-type and shoreline support/context
- `prototype_2016` legacy/archive support
- Reproducibility / governance / read-only package layer

### Focused Phase 1 Provenance

- Workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Historical window: `2016-01-01` to `2023-03-02`
- Focused validation box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- Ranked February-April subset: `19`
- Selected recipe: `cmems_gfs`

| Recipe | Mean NCS | Median NCS | Status |
| --- | ---: | ---: | --- |
| `cmems_gfs` | `4.5886` | `4.6305` | winner |
| `cmems_era5` | `4.6237` | `4.5916` | not selected |
| `hycom_gfs` | `4.7027` | `4.9263` | not selected |
| `hycom_era5` | `4.7561` | `5.0106` | not selected |

### Mindoro `B1`

- FSS `1 / 3 / 5 / 10 km`: `0.0000 / 0.0441 / 0.1371 / 0.2490`
- Mean FSS: `0.1075`
- `R0` did not reach target date; forecast cells `0`; observed cells `22`
- `R1_previous` forecast cells `5`; observed cells `22`; nearest distance `1414.21 m`; centroid distance `7358.16 m`
- `R1_previous` is promoted because it survives and is scoreable, not because it is an exact-grid match
- `IoU = 0.0`
- `Dice = 0.0`
- Caveat: March 13-14 is a reinitialization-based public-observation validation check; both public products cite the same March 12 WorldView-3 imagery provenance; do not call it independent day-to-day validation

### Mindoro `Track A`

- OpenDrift `R1_previous`: forecast cells `5`; nearest distance `1414.21 m`; mean FSS `0.1075`
- OpenDrift `R0`: forecast cells `0`; mean FSS `0.0000`
- PyGNOME deterministic comparator-only support: forecast cells `6`; nearest distance `6082.76 m`; mean FSS `0.0061`

### DWH

- Case ID: `CASE_DWH_RETRO_2010_72H`
- Scientific forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- `C1 = 0.5568`
- `C2 p50 = 0.5389`
- `C2 p90 = 0.4966`
- `C3 PyGNOME comparator = 0.3612`

### Mindoro Oil-Type / Shoreline Support

- Light oil: `0.02%`, `4 h`, `11`, QC pass
- Fixed-base medium-heavy proxy: `0.61%`, `4 h`, `10`, QC flagged
- Heavier oil: `0.63%`, `4 h`, `11`, QC pass

### Probability Semantics

- `prob_presence` = cellwise ensemble probability of presence
- `mask_p50` = probability of presence `>= 0.50`
- `mask_p90` = probability of presence `>= 0.90`

## How To Run The Docs Consistency Check

Windows PowerShell:

```powershell
python -m py_compile scripts/check_docs_against_manuscript_claims.py
python scripts/check_docs_against_manuscript_claims.py
python -m json.tool config/launcher_matrix.json > $null
```

Unix-like shells:

```bash
python -m py_compile scripts/check_docs_against_manuscript_claims.py
python scripts/check_docs_against_manuscript_claims.py
python -m json.tool config/launcher_matrix.json > /dev/null
```

## Final Guardrails For Panel Review

- Keep `B1` as the only main-text primary Philippine / Mindoro validation claim.
- Keep March 13-14 framed as reinitialization-based public-observation validation with the shared-imagery caveat visible.
- Keep `Track A` and every PyGNOME branch comparator-only.
- Keep DWH external only; do not present it as Mindoro recalibration.
- Keep Mindoro oil-type and shoreline outputs as support/context only.
- Keep `prototype_2016` legacy/archive only.
- Keep the publication package, figure package, and UI read-only.
- Do not recompute scientific results as part of docs/governance cleanup.
