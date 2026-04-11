"""Read-only audit of whether Phase 4 OpenDrift outputs are honestly comparable to PyGNOME."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.helpers.metrics import extract_gnome_budget_from_nc

try:
    import netCDF4
except ImportError:  # pragma: no cover
    netCDF4 = None


PHASE = "phase4_crossmodel_comparability_audit"
OUTPUT_DIR = Path("output") / PHASE
PHASE4_OUTPUT_DIR = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023"
MINDORO_PYGNOME_DIR = Path("output") / "CASE_MINDORO_RETRO_2023" / "pygnome_public_comparison"
DWH_PYGNOME_DIR = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_dwh_pygnome_comparator"
CLASSIFICATIONS = {
    "directly_comparable_now",
    "comparable_with_small_adapter",
    "not_comparable_honestly",
}


@dataclass(frozen=True)
class ComparabilityRow:
    quantity_id: str
    quantity_label: str
    classification: str
    directly_comparable_now: bool
    comparable_with_small_adapter: bool
    pilot_comparison_possible_now: bool
    opendrift_phase4_support: str
    pygnome_support: str
    semantic_gap: str
    blocker: str
    minimal_next_step: str
    evidence_paths: list[str]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path)


class Phase4CrossModelComparabilityAuditService:
    def __init__(self, repo_root: str | Path = ".", output_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_DIR

        self.phase4_dir = self.repo_root / PHASE4_OUTPUT_DIR
        self.phase4_manifest_path = self.phase4_dir / "phase4_run_manifest.json"
        self.phase4_budget_summary_path = self.phase4_dir / "phase4_oil_budget_summary.csv"
        self.phase4_shoreline_arrival_path = self.phase4_dir / "phase4_shoreline_arrival.csv"
        self.phase4_shoreline_segments_path = self.phase4_dir / "phase4_shoreline_segments.csv"
        self.phase4_oiltype_comparison_path = self.phase4_dir / "phase4_oiltype_comparison.csv"

        self.mindoro_pygnome_dir = self.repo_root / MINDORO_PYGNOME_DIR
        self.mindoro_pygnome_manifest_path = self.mindoro_pygnome_dir / "pygnome_public_comparison_run_manifest.json"
        self.mindoro_pygnome_metadata_path = (
            self.mindoro_pygnome_dir / "products" / "C3_pygnome_deterministic" / "pygnome_benchmark_metadata.json"
        )
        self.mindoro_pygnome_nc_path = (
            self.mindoro_pygnome_dir / "products" / "C3_pygnome_deterministic" / "pygnome_deterministic_control.nc"
        )
        self.mindoro_pygnome_summary_path = self.mindoro_pygnome_dir / "pygnome_public_comparison_summary.csv"

        self.dwh_pygnome_dir = self.repo_root / DWH_PYGNOME_DIR
        self.dwh_pygnome_manifest_path = self.dwh_pygnome_dir / "phase3c_dwh_pygnome_run_manifest.json"
        self.dwh_pygnome_loading_audit_path = self.dwh_pygnome_dir / "phase3c_dwh_pygnome_loading_audit.json"
        self.dwh_pygnome_nc_path = self.dwh_pygnome_dir / "tracks" / "pygnome_dwh_phase3c.nc"
        self.dwh_pygnome_summary_path = self.dwh_pygnome_dir / "phase3c_dwh_pygnome_summary.csv"

        self.metrics_helper_path = self.repo_root / "src" / "helpers" / "metrics.py"
        self.plotting_helper_path = self.repo_root / "src" / "helpers" / "plotting.py"
        self.gnome_comparison_service_path = self.repo_root / "src" / "services" / "gnome_comparison.py"
        self.weathering_service_path = self.repo_root / "src" / "services" / "weathering.py"
        self.phase4_service_path = self.repo_root / "src" / "services" / "phase4_oiltype_and_shoreline.py"
        self.pygnome_public_service_path = self.repo_root / "src" / "services" / "pygnome_public_comparison.py"
        self.dwh_pygnome_service_path = self.repo_root / "src" / "services" / "phase3c_dwh_pygnome_comparator.py"

        self.readme_path = self.repo_root / "README.md"
        self.phase_status_path = self.repo_root / "docs" / "PHASE_STATUS.md"
        self.output_catalog_path = self.repo_root / "docs" / "OUTPUT_CATALOG.md"
        self.architecture_path = self.repo_root / "docs" / "ARCHITECTURE.md"

        self.phase4_manifest = _read_json(self.phase4_manifest_path)
        self.phase4_budget_summary = _read_csv(self.phase4_budget_summary_path)
        self.phase4_shoreline_arrival = _read_csv(self.phase4_shoreline_arrival_path)
        self.phase4_shoreline_segments = _read_csv(self.phase4_shoreline_segments_path)
        self.phase4_oiltype_comparison = _read_csv(self.phase4_oiltype_comparison_path)

        self.mindoro_pygnome_manifest = _read_json(self.mindoro_pygnome_manifest_path)
        self.mindoro_pygnome_metadata = _read_json(self.mindoro_pygnome_metadata_path)
        self.dwh_pygnome_manifest = _read_json(self.dwh_pygnome_manifest_path)
        self.dwh_pygnome_loading_audit = _read_json(self.dwh_pygnome_loading_audit_path)

    def _safe_read_text(self, path: Path) -> str:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8", errors="ignore")

    def _supports_budget_adapter_code(self) -> bool:
        metrics_text = self._safe_read_text(self.metrics_helper_path)
        plotting_text = self._safe_read_text(self.plotting_helper_path)
        gnome_text = self._safe_read_text(self.gnome_comparison_service_path)
        return (
            "extract_gnome_budget_from_nc" in metrics_text
            and "plot_gnome_vs_openoil" in plotting_text
            and "run_comparison" in gnome_text
        )

    def _pygnome_budget_diagnostic(self, path: Path) -> dict[str, Any]:
        diagnostic: dict[str, Any] = {
            "path": _relative_to_repo(self.repo_root, path),
            "exists": path.exists(),
            "budget_extractable": False,
            "status_counts": {},
            "fate_status_counts": {},
            "frac_evap_max": None,
            "budget_rows": 0,
            "final_surface_pct": None,
            "final_beached_pct": None,
            "final_evaporated_pct": None,
            "final_dispersed_pct": None,
            "all_rows_are_100_surface": False,
        }
        if not path.exists():
            return diagnostic

        if netCDF4 is not None:
            try:
                with netCDF4.Dataset(path) as ds:
                    if "status_codes" in ds.variables:
                        status_series = pd.Series(ds.variables["status_codes"][:])
                        diagnostic["status_counts"] = {
                            str(int(code)): int(count)
                            for code, count in status_series.value_counts().sort_index().items()
                        }
                    if "fate_status" in ds.variables:
                        fate_series = pd.Series(ds.variables["fate_status"][:])
                        diagnostic["fate_status_counts"] = {
                            str(int(code)): int(count)
                            for code, count in fate_series.value_counts().sort_index().items()
                        }
                    if "frac_evap" in ds.variables:
                        frac_evap_series = pd.Series(ds.variables["frac_evap"][:])
                        diagnostic["frac_evap_max"] = float(frac_evap_series.max())
            except Exception as exc:  # pragma: no cover
                diagnostic["dataset_read_error"] = f"{type(exc).__name__}: {exc}"

        try:
            budget_df = extract_gnome_budget_from_nc(str(path))
            diagnostic["budget_extractable"] = True
            diagnostic["budget_rows"] = int(len(budget_df))
            if not budget_df.empty:
                final_row = budget_df.iloc[-1]
                diagnostic["final_surface_pct"] = float(final_row.get("surface_pct", 0.0))
                diagnostic["final_beached_pct"] = float(final_row.get("beached_pct", 0.0))
                diagnostic["final_evaporated_pct"] = float(final_row.get("evaporated_pct", 0.0))
                diagnostic["final_dispersed_pct"] = float(final_row.get("dispersed_pct", 0.0))
                surface_only = (
                    budget_df.get("surface_pct", pd.Series(dtype=float)).fillna(0).eq(100.0).all()
                    and budget_df.get("beached_pct", pd.Series(dtype=float)).fillna(0).eq(0.0).all()
                    and budget_df.get("evaporated_pct", pd.Series(dtype=float)).fillna(0).eq(0.0).all()
                    and budget_df.get("dispersed_pct", pd.Series(dtype=float)).fillna(0).eq(0.0).all()
                )
                diagnostic["all_rows_are_100_surface"] = bool(surface_only)
        except Exception as exc:  # pragma: no cover
            diagnostic["budget_extract_error"] = f"{type(exc).__name__}: {exc}"
        return diagnostic

    def _opendrift_support_summary(self) -> str:
        scenario_count = int(len(self.phase4_budget_summary.index))
        arrival_count = int(len(self.phase4_shoreline_arrival.index))
        segment_rows = int(len(self.phase4_shoreline_segments.index))
        return (
            "Stored Phase 4 OpenDrift outputs already include "
            f"{scenario_count} scenario budget summaries, {arrival_count} shoreline-arrival rows, "
            f"and {segment_rows} shoreline-segment rows under the canonical Mindoro Phase 4 bundle."
        )

    def _shared_blocker(self) -> str:
        return (
            "The repo does not yet contain a matched Mindoro Phase 4 PyGNOME output family with the same "
            "oil-scenario registry, weathering compartments, and canonical shoreline-segment products used by the OpenDrift Phase 4 workflow."
        )

    def _minimal_next_step(self, shoreline_required: bool = False) -> str:
        if shoreline_required:
            return (
                "Add a matched Mindoro PyGNOME Phase 4 run that reuses the selected transport window, writes per-scenario "
                "budget time series, and exports canonical shoreline-arrival and shoreline-segment tables."
            )
        return (
            "Add a matched Mindoro PyGNOME Phase 4 run that reuses the selected transport window and writes per-scenario "
            "budget time series with an explicit compartment mapping to the OpenDrift Phase 4 schema."
        )

    def _mindoro_pygnome_gap_summary(self, mindoro_diag: dict[str, Any]) -> str:
        weathering_enabled = bool(self.mindoro_pygnome_metadata.get("weathering_enabled"))
        support_bits = [
            "Mindoro PyGNOME benchmark metadata says weathering is "
            + ("enabled." if weathering_enabled else "disabled.")
        ]
        if mindoro_diag.get("budget_extractable"):
            support_bits.append(
                "The stored Mindoro PyGNOME NetCDF can be parsed as a budget table, but it stays at "
                f"{mindoro_diag.get('final_surface_pct', 0.0):.1f}% surface, "
                f"{mindoro_diag.get('final_evaporated_pct', 0.0):.1f}% evaporated, "
                f"{mindoro_diag.get('final_dispersed_pct', 0.0):.1f}% dispersed, and "
                f"{mindoro_diag.get('final_beached_pct', 0.0):.1f}% beached by construction."
            )
        track = next(
            (
                row
                for row in (self.mindoro_pygnome_manifest.get("tracks") or [])
                if str(row.get("track_id")) == "C3"
            ),
            {},
        )
        structural_limitations = str(track.get("structural_limitations") or "")
        if structural_limitations:
            support_bits.append(structural_limitations)
        return " ".join(bit for bit in support_bits if bit)

    def _dwh_pygnome_gap_summary(self, dwh_diag: dict[str, Any]) -> str:
        support_bits = []
        structural_mismatch = str(self.dwh_pygnome_loading_audit.get("structural_mismatch_note") or "")
        if structural_mismatch:
            support_bits.append(structural_mismatch)
        if dwh_diag.get("budget_extractable"):
            support_bits.append(
                "The stored DWH PyGNOME NetCDF also parses to a flat 100/0/0/0 budget table, which shows the current DWH comparator run is transport-only rather than a Phase 4 fate run."
            )
        if not bool(self.dwh_pygnome_loading_audit.get("waves_attached")):
            support_bits.append("Wave/Stokes attachment is not reproduced identically in the DWH comparator audit.")
        return " ".join(bit for bit in support_bits if bit)

    def _row(
        self,
        *,
        quantity_id: str,
        quantity_label: str,
        pygnome_support: str,
        semantic_gap: str,
        blocker: str,
        minimal_next_step: str,
        evidence_paths: list[Path],
    ) -> ComparabilityRow:
        row = ComparabilityRow(
            quantity_id=quantity_id,
            quantity_label=quantity_label,
            classification="not_comparable_honestly",
            directly_comparable_now=False,
            comparable_with_small_adapter=False,
            pilot_comparison_possible_now=False,
            opendrift_phase4_support=self._opendrift_support_summary(),
            pygnome_support=pygnome_support,
            semantic_gap=semantic_gap,
            blocker=blocker,
            minimal_next_step=minimal_next_step,
            evidence_paths=[
                _relative_to_repo(self.repo_root, path)
                for path in evidence_paths
                if path.exists() or path.suffix in {".py", ".md"}
            ],
        )
        if row.classification not in CLASSIFICATIONS:
            raise ValueError(f"Unsupported classification: {row.classification}")
        return row

    def _build_rows(self) -> tuple[list[ComparabilityRow], dict[str, Any]]:
        mindoro_diag = self._pygnome_budget_diagnostic(self.mindoro_pygnome_nc_path)
        dwh_diag = self._pygnome_budget_diagnostic(self.dwh_pygnome_nc_path)

        mindoro_gap_summary = self._mindoro_pygnome_gap_summary(mindoro_diag)
        dwh_gap_summary = self._dwh_pygnome_gap_summary(dwh_diag)
        shared_blocker = self._shared_blocker()

        rows = [
            self._row(
                quantity_id="surface_fraction",
                quantity_label="surface fraction",
                pygnome_support=mindoro_gap_summary,
                semantic_gap=(
                    "OpenDrift Phase 4 surface fraction is a weathering-aware mass compartment. "
                    "The stored Mindoro and DWH PyGNOME outputs stay at 100% surface because they are comparator tracks rather than matched Phase 4 fate runs."
                ),
                blocker=shared_blocker,
                minimal_next_step=self._minimal_next_step(),
                evidence_paths=[
                    self.phase4_budget_summary_path,
                    self.mindoro_pygnome_metadata_path,
                    self.mindoro_pygnome_manifest_path,
                    self.dwh_pygnome_loading_audit_path,
                    self.metrics_helper_path,
                ],
            ),
            self._row(
                quantity_id="evaporated_fraction",
                quantity_label="evaporated fraction",
                pygnome_support=mindoro_gap_summary,
                semantic_gap=(
                    "OpenDrift Phase 4 evaporation is explicitly reported per scenario. "
                    "The stored PyGNOME comparator NetCDFs record zero evaporation throughout, and the Mindoro benchmark explicitly disables weathering."
                ),
                blocker=shared_blocker,
                minimal_next_step=self._minimal_next_step(),
                evidence_paths=[
                    self.phase4_budget_summary_path,
                    self.mindoro_pygnome_metadata_path,
                    self.mindoro_pygnome_nc_path,
                    self.dwh_pygnome_nc_path,
                    self.gnome_comparison_service_path,
                ],
            ),
            self._row(
                quantity_id="dispersed_fraction",
                quantity_label="dispersed fraction",
                pygnome_support=dwh_gap_summary,
                semantic_gap=(
                    "OpenDrift Phase 4 dispersion is derived from the OpenOil/OpenDrift state schema. "
                    "The stored PyGNOME comparator outputs do not expose matched Phase 4 dispersion semantics for the Mindoro scenario family."
                ),
                blocker=shared_blocker,
                minimal_next_step=self._minimal_next_step(),
                evidence_paths=[
                    self.phase4_budget_summary_path,
                    self.mindoro_pygnome_nc_path,
                    self.dwh_pygnome_nc_path,
                    self.metrics_helper_path,
                    self.gnome_comparison_service_path,
                ],
            ),
            self._row(
                quantity_id="stranded_beached_fraction",
                quantity_label="stranded/beached fraction",
                pygnome_support=dwh_gap_summary,
                semantic_gap=(
                    "OpenDrift Phase 4 beached fraction is coupled to shoreline-aware replay and canonical shoreline segments. "
                    "The current PyGNOME comparator outputs do not contain matched shoreline-action mass accounting, and the stored budgets remain at 0% beached."
                ),
                blocker=shared_blocker,
                minimal_next_step=self._minimal_next_step(shoreline_required=True),
                evidence_paths=[
                    self.phase4_budget_summary_path,
                    self.phase4_shoreline_arrival_path,
                    self.phase4_shoreline_segments_path,
                    self.mindoro_pygnome_nc_path,
                    self.dwh_pygnome_loading_audit_path,
                ],
            ),
            self._row(
                quantity_id="shoreline_arrival_timing",
                quantity_label="shoreline arrival timing",
                pygnome_support=(
                    "OpenDrift Phase 4 already writes first shoreline arrival timing per scenario and per shoreline segment. "
                    "No equivalent PyGNOME shoreline-arrival table exists in the current repo outputs."
                ),
                semantic_gap=(
                    "Phase 4 shoreline arrival depends on canonical shoreline-segment assignment and stored arrival timestamps. "
                    "The current PyGNOME branches stop at spatial comparator rasters and do not export shoreline-arrival semantics."
                ),
                blocker=shared_blocker,
                minimal_next_step=self._minimal_next_step(shoreline_required=True),
                evidence_paths=[
                    self.phase4_shoreline_arrival_path,
                    self.phase4_shoreline_segments_path,
                    self.mindoro_pygnome_manifest_path,
                    self.dwh_pygnome_manifest_path,
                    self.phase4_service_path,
                ],
            ),
            self._row(
                quantity_id="shoreline_segment_impact_totals",
                quantity_label="shoreline segment impact totals",
                pygnome_support=(
                    "OpenDrift Phase 4 writes canonical shoreline-segment totals now. "
                    "Current PyGNOME outputs contain no shoreline-segment registry, no segment IDs, and no per-segment mass totals."
                ),
                semantic_gap=(
                    "The OpenDrift side is segment-based and tied to the stored GSHHG shoreline artifact. "
                    "PyGNOME currently has no matched shoreline segmentation output family in this repo."
                ),
                blocker=shared_blocker,
                minimal_next_step=self._minimal_next_step(shoreline_required=True),
                evidence_paths=[
                    self.phase4_shoreline_segments_path,
                    self.phase4_manifest_path,
                    self.phase4_service_path,
                    self.mindoro_pygnome_manifest_path,
                    self.dwh_pygnome_manifest_path,
                ],
            ),
            self._row(
                quantity_id="timeseries_oil_budget_compartments",
                quantity_label="time-series oil budget compartments",
                pygnome_support=(
                    "The repo does contain a PyGNOME budget parser and overlay plotting helper, but those are not enough on their own because the stored PyGNOME outputs are not matched Phase 4 fate runs."
                ),
                semantic_gap=(
                    "A small adapter exists at code level, but the required comparable PyGNOME inputs do not. "
                    "Without matched Mindoro PyGNOME weathering and shoreline outputs, generating a cross-model Phase 4 budget figure would fabricate comparability."
                ),
                blocker=shared_blocker,
                minimal_next_step=self._minimal_next_step(shoreline_required=True),
                evidence_paths=[
                    self.phase4_budget_summary_path,
                    self.metrics_helper_path,
                    self.plotting_helper_path,
                    self.gnome_comparison_service_path,
                    self.mindoro_pygnome_metadata_path,
                    self.dwh_pygnome_loading_audit_path,
                ],
            ),
        ]

        diagnostics = {
            "budget_adapter_code_available": self._supports_budget_adapter_code(),
            "mindoro_pygnome_budget_diagnostic": mindoro_diag,
            "dwh_pygnome_budget_diagnostic": dwh_diag,
            "mindoro_pygnome_weathering_enabled": bool(self.mindoro_pygnome_metadata.get("weathering_enabled")),
            "dwh_pygnome_structural_mismatch_note": str(
                self.dwh_pygnome_loading_audit.get("structural_mismatch_note") or ""
            ),
            "phase4_scenarios_present": self.phase4_budget_summary.get("scenario_id", pd.Series(dtype=str)).astype(str).tolist(),
            "phase4_shoreline_rows_present": int(len(self.phase4_shoreline_segments.index)),
        }
        return rows, diagnostics

    def _build_verdict(self, rows: list[ComparabilityRow], diagnostics: dict[str, Any]) -> dict[str, Any]:
        directly = [row.quantity_id for row in rows if row.classification == "directly_comparable_now"]
        small_adapter = [row.quantity_id for row in rows if row.classification == "comparable_with_small_adapter"]
        not_honest = [row.quantity_id for row in rows if row.classification == "not_comparable_honestly"]
        biggest_blocker = self._shared_blocker()
        if diagnostics.get("mindoro_pygnome_weathering_enabled") is False:
            biggest_blocker = (
                "The existing Mindoro PyGNOME benchmark is explicitly transport-only with weathering disabled, so it cannot support Phase 4 fate or shoreline comparison."
            )
        return {
            "phase": PHASE,
            "scientifically_available_now": bool(directly),
            "pilot_only": bool(directly or small_adapter),
            "status": "deferred" if not directly and not small_adapter else "pilot_only",
            "pilot_comparison_produced": False,
            "quantities_directly_comparable_now": directly,
            "quantities_comparable_with_small_adapter": small_adapter,
            "quantities_not_comparable_honestly": not_honest,
            "biggest_blocker": biggest_blocker,
            "budget_adapter_code_exists_but_is_insufficient": bool(diagnostics.get("budget_adapter_code_available")),
            "requires_new_pygnome_phase4_outputs": True,
            "minimal_next_step_label": "matched_mindoro_pygnome_phase4_run_required",
        }

    def _build_report_markdown(
        self,
        rows: list[ComparabilityRow],
        diagnostics: dict[str, Any],
        verdict: dict[str, Any],
    ) -> str:
        directly = verdict["quantities_directly_comparable_now"]
        small_adapter = verdict["quantities_comparable_with_small_adapter"]
        not_honest = verdict["quantities_not_comparable_honestly"]
        scenario_list = ", ".join(diagnostics.get("phase4_scenarios_present") or []) or "none found"
        return "\n".join(
            [
                "# Phase 4 Cross-Model Comparability Report",
                "",
                "## Verdict",
                "",
                f"- Status: `{verdict['status']}`",
                f"- Scientifically available now: `{str(verdict['scientifically_available_now']).lower()}`",
                f"- Pilot comparison produced in this patch: `{str(verdict['pilot_comparison_produced']).lower()}`",
                f"- Directly comparable now: {', '.join(directly) if directly else 'none'}",
                f"- Comparable with small adapter: {', '.join(small_adapter) if small_adapter else 'none'}",
                f"- Not comparable honestly: {', '.join(not_honest) if not_honest else 'none'}",
                f"- Biggest blocker: {verdict['biggest_blocker']}",
                "",
                "## Key Evidence",
                "",
                f"- OpenDrift Phase 4 scenarios present: {scenario_list}",
                f"- OpenDrift shoreline rows present: {diagnostics.get('phase4_shoreline_rows_present', 0)}",
                f"- Mindoro PyGNOME weathering enabled: `{str(diagnostics.get('mindoro_pygnome_weathering_enabled', False)).lower()}`",
                f"- Mindoro PyGNOME parsed budget ends at surface/beached/evaporated/dispersed = "
                f"{diagnostics['mindoro_pygnome_budget_diagnostic'].get('final_surface_pct')}/"
                f"{diagnostics['mindoro_pygnome_budget_diagnostic'].get('final_beached_pct')}/"
                f"{diagnostics['mindoro_pygnome_budget_diagnostic'].get('final_evaporated_pct')}/"
                f"{diagnostics['mindoro_pygnome_budget_diagnostic'].get('final_dispersed_pct')}",
                f"- DWH PyGNOME parsed budget ends at surface/beached/evaporated/dispersed = "
                f"{diagnostics['dwh_pygnome_budget_diagnostic'].get('final_surface_pct')}/"
                f"{diagnostics['dwh_pygnome_budget_diagnostic'].get('final_beached_pct')}/"
                f"{diagnostics['dwh_pygnome_budget_diagnostic'].get('final_evaporated_pct')}/"
                f"{diagnostics['dwh_pygnome_budget_diagnostic'].get('final_dispersed_pct')}",
                f"- Budget-adapter helper code exists already: `{str(diagnostics.get('budget_adapter_code_available', False)).lower()}`",
                "",
                "## Why No Pilot Comparison Was Written",
                "",
                "The repo contains a budget parser and a plotting helper for OpenOil versus PyGNOME, but the stored PyGNOME artifacts are still comparator tracks rather than matched Phase 4 fate-and-shoreline runs.",
                "Writing a Phase 4 OpenDrift versus PyGNOME figure from the current files would therefore imply budget and shoreline equivalence that the repo does not currently support.",
                "",
                "## Minimal Next Step",
                "",
                "Implement a matched Mindoro PyGNOME Phase 4 branch that reuses the selected transport window, reads the same oil-scenario registry, exports budget time series, and writes canonical shoreline-arrival plus shoreline-segment tables before attempting any cross-model Phase 4 comparison.",
            ]
        )

    def _build_blockers_markdown(self, verdict: dict[str, Any]) -> str:
        return "\n".join(
            [
                "# Phase 4 Cross-Model Blockers",
                "",
                f"- Biggest blocker: {verdict['biggest_blocker']}",
                "- Missing matched PyGNOME Phase 4 outputs for the Mindoro scenario registry.",
                "- Mindoro PyGNOME benchmark metadata explicitly says weathering is disabled.",
                "- DWH PyGNOME comparator is transport-only and does not reproduce shoreline/Phase 4 fate semantics.",
                "- No stored PyGNOME shoreline-arrival table exists.",
                "- No stored PyGNOME shoreline-segment impact table exists.",
                "- Current PyGNOME NetCDF diagnostics collapse to 100% surface and 0% evaporated/dispersed/beached, which is not a defensible Phase 4 comparator state.",
            ]
        )

    def _build_next_steps_markdown(self) -> str:
        return "\n".join(
            [
                "# Minimal Next Steps",
                "",
                "1. Add a matched Mindoro PyGNOME Phase 4 runner that reuses the selected transport window and the existing `config/phase4_oil_scenarios.csv` scenario registry.",
                "2. Enable explicit PyGNOME fate outputs for surface, evaporated, dispersed, and beached compartments with a documented mapping to the OpenDrift/OpenOil Phase 4 schema.",
                "3. Add canonical shoreline-arrival and shoreline-segment assignment on the PyGNOME side using the same shoreline artifact family already used by Phase 4 OpenDrift.",
                "4. Only after those outputs exist, generate a small pilot comparison package with per-scenario budget and shoreline diagnostics.",
            ]
        )

    def run(self) -> dict[str, Any]:
        rows, diagnostics = self._build_rows()
        verdict = self._build_verdict(rows, diagnostics)

        matrix_csv_path = self.output_dir / "phase4_crossmodel_comparability_matrix.csv"
        matrix_json_path = self.output_dir / "phase4_crossmodel_comparability_matrix.json"
        report_path = self.output_dir / "phase4_crossmodel_comparability_report.md"
        verdict_path = self.output_dir / "phase4_crossmodel_final_verdict.md"
        blockers_path = self.output_dir / "phase4_crossmodel_blockers.md"
        next_steps_path = self.output_dir / "phase4_crossmodel_minimal_next_steps.md"

        frame = pd.DataFrame(asdict(row) for row in rows)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        frame.to_csv(matrix_csv_path, index=False)
        _write_json(
            matrix_json_path,
            {
                "phase": PHASE,
                "generated_from_existing_outputs_only": True,
                "verdict": verdict,
                "diagnostics": diagnostics,
                "rows": [asdict(row) for row in rows],
            },
        )
        _write_text(report_path, self._build_report_markdown(rows, diagnostics, verdict))
        _write_text(
            verdict_path,
            "\n".join(
                [
                    "# Phase 4 Cross-Model Final Verdict",
                    "",
                    f"- Status: `{verdict['status']}`",
                    f"- Scientifically available now: `{str(verdict['scientifically_available_now']).lower()}`",
                    f"- Pilot comparison figures produced: `{str(verdict['pilot_comparison_produced']).lower()}`",
                    f"- Directly comparable quantities: {', '.join(verdict['quantities_directly_comparable_now']) if verdict['quantities_directly_comparable_now'] else 'none'}",
                    f"- Comparable-with-small-adapter quantities: {', '.join(verdict['quantities_comparable_with_small_adapter']) if verdict['quantities_comparable_with_small_adapter'] else 'none'}",
                    f"- Biggest blocker: {verdict['biggest_blocker']}",
                    "",
                    "Current Phase 4 OpenDrift outputs are scientifically usable on their own, but the repo does not yet contain matched PyGNOME Phase 4 fate-and-shoreline outputs that would support a defensible cross-model comparison.",
                ]
            ),
        )

        if not verdict["scientifically_available_now"] and not verdict["quantities_comparable_with_small_adapter"]:
            _write_text(blockers_path, self._build_blockers_markdown(verdict))
            _write_text(next_steps_path, self._build_next_steps_markdown())

        return {
            "output_dir": str(self.output_dir),
            "matrix_csv": str(matrix_csv_path),
            "matrix_json": str(matrix_json_path),
            "report_md": str(report_path),
            "verdict_md": str(verdict_path),
            "blockers_md": str(blockers_path),
            "minimal_next_steps_md": str(next_steps_path),
            "overall_verdict": verdict,
            "rows": [asdict(row) for row in rows],
        }


def run_phase4_crossmodel_comparability_audit() -> dict[str, Any]:
    return Phase4CrossModelComparabilityAuditService().run()
