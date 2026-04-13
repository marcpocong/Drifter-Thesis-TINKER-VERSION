# March 13 -> March 14 NOAA Reinit Primary Validation Decision Note

- Canonical Phase 3B primary validation source: true
- Appendix-only track: false
- Seed source key: 8f8e3944748c4772910efc9829497e20
- Seed source name: MindoroOilSpill_NOAA_230313
- Target source key: 10b37c42a9754363a5f7b14199b077e6
- Target source name: MindoroOilSpill_NOAA_230314
- Seed observation date: 2023-03-13
- Scored target date: 2023-03-14
- Release start UTC: 2023-03-12T16:00:00Z
- Seed release geometry: accepted_march13_noaa_processed_polygon
- Requested element count: 100000
- Best branch by mean FSS: R1_previous
- Best branch mean FSS: 0.107546
- Best branch FSS 1/3/5/10 km: 0.000000 / 0.044101 / 0.137133 / 0.248951
- Limitation note: Both NOAA/NESDIS public products cite WorldView-3 imagery acquired on 2023-03-12, so the promoted March 13 -> March 14 row is a reinitialization-based public-validation pair with shared-imagery provenance rather than a fully independent day-to-day validation.
- Decision: At least one branch produced a scoreable March 14 p50 mask, so this bundle is usable as the canonical Phase 3B public-validation source row when the shared-imagery caveat is kept explicit.

This bundle is the canonical Phase 3B public-validation source for packaging and figure builders.
It does not rewrite the frozen March 3 -> March 6 official case definition, and it does not delete the March 6 legacy honesty outputs.
The comparison is intentionally limited to R0 and R1_previous, with March 13 polygon reinitialization and March 14 scoring.
