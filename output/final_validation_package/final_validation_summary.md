# Final Validation Summary

This package is read-only with respect to completed scientific outputs. No Mindoro or DWH scientific result files were overwritten.

## Headline Results

- Mindoro March 13 -> March 14 primary validation (B1): FSS(1/3/5/10 km) = 0.0000, 0.0441, 0.1371, 0.2490; IoU=0.0000; Dice=0.0000.
- Mindoro promoted cross-model top track (A): OpenDrift R1 previous reinit p50 with FSS(1/3/5/10 km) = 0.0000, 0.0441, 0.1371, 0.2490.
- Mindoro legacy March 6 honesty-only sparse reference (B2): FSS(1/3/5/10 km) = 0.0000, 0.0000, 0.0000, 0.0000; IoU=0.0000; Dice=0.0000.
- Mindoro legacy March 3-6 broader-support reference (B3): FSS(1/3/5/10 km) = 0.1722, 0.2004, 0.2166, 0.2438; IoU=0.0942; Dice=0.1722.
- DWH deterministic event corridor (C1): FSS(1/3/5/10 km) = 0.5033, 0.5523, 0.5700, 0.6018; IoU=0.3362; Dice=0.5033.
- DWH ensemble p50 event corridor (C2): FSS(1/3/5/10 km) = 0.4997, 0.5299, 0.5467, 0.5790; overall mean leader = OpenDrift ensemble p50 (0.5013).
- DWH PyGNOME comparator (C3) event corridor: FSS(1/3/5/10 km) = 0.3197, 0.3495, 0.3689, 0.4068; IoU=0.1903; Dice=0.3197.

## Recommended Final Structure

- Base-case provenance: keep `config/case_mindoro_retro_2023.yaml` frozen for March 3 -> March 6 and carry the B1 promotion through `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`.
- Main text: Mindoro B1 as the March 13 -> March 14 primary validation with the shared-imagery caveat plus DWH Phase 3C as the rich-data transfer-validation success.
- Comparative discussion: Mindoro A cross-model comparator and DWH deterministic-vs-ensemble-vs-PyGNOME comparison.
- Legacy/reference and sensitivities: Mindoro B2/B3 legacy rows, recipe/init/source-history sensitivities, and optional future DWH extensions.

## Final Recommendation

- Main text should emphasize Mindoro B1 as the March 13 -> March 14 NOAA reinit validation with an explicit caveat that both NOAA products cite March 12 WorldView-3 imagery, while DWH Phase 3C remains the rich-data transfer-validation success; comparative discussion should emphasize the March 13 -> March 14 Mindoro cross-model comparator and the DWH deterministic-vs-ensemble-vs-PyGNOME comparison; legacy/reference and appendix sections should retain the Mindoro March 6 sparse reference, the March 3-6 broader-support reference, recipe/init/source-history sensitivities, and any future DWH threshold or harmonization extensions.
