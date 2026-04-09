"""
Before/after comparison utilities for the official shoreline-mask rerun.

Usage:
  python -m src.services.shoreline_rerun snapshot_before
  python -m src.services.shoreline_rerun compare_after
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.scoring import precheck_same_grid
from src.services.ingestion import derive_bbox_from_display_bounds
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.io import (
    get_case_output_dir,
    get_download_manifest_path,
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
    get_official_mask_p50_datecomposite_path,
    resolve_validation_mask_path,
)

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - runtime optional
    plt = None


class ShorelineRerunComparisonService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("shoreline_rerun comparison is only supported for official workflows.")

        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.phase3b_dir = self.case_output_dir / "phase3b"
        self.appendix_dir = self.case_output_dir / "public_obs_appendix"
        self.output_dir = self.case_output_dir / "shoreline_rerun"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.before_snapshot_path = self.output_dir / "before_snapshot.json"
        self.before_arrays_path = self.output_dir / "before_arrays.npz"
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.sea_mask_bool = (self.helper.sea_mask > 0.5) if self.helper.sea_mask is not None else None

    def _compute_binary_pair(
        self,
        *,
        pair_id: str,
        forecast_path: Path,
        observation_path: Path,
    ) -> tuple[dict, dict[str, np.ndarray]]:
        precheck = precheck_same_grid(
            forecast=forecast_path,
            target=observation_path,
            report_base_path=self.output_dir / f"precheck_{pair_id}",
        )
        if not precheck.passed:
            raise RuntimeError(f"Shoreline rerun pair {pair_id} failed same-grid precheck: {precheck.json_report_path}")

        forecast_mask = self.helper._load_binary_score_mask(forecast_path)
        observation_mask = self.helper._load_binary_score_mask(observation_path)
        diagnostics = self.helper._compute_mask_diagnostics(forecast_mask, observation_mask)
        row = {
            "pair_id": pair_id,
            "forecast_path": str(forecast_path),
            "observation_path": str(observation_path),
            "precheck_csv": str(precheck.csv_report_path),
            "precheck_json": str(precheck.json_report_path),
            **diagnostics,
        }
        for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
            row[f"fss_{window_km}km"] = float(
                np.clip(
                    calculate_fss(
                        forecast_mask,
                        observation_mask,
                        window=self.helper._window_km_to_cells(window_km),
                        valid_mask=self.sea_mask_bool,
                    ),
                    0.0,
                    1.0,
                )
            )
        return row, {
            "forecast": forecast_mask.astype(np.float32),
            "observation": observation_mask.astype(np.float32),
        }

    def _appendix_perdate_records(self) -> list[dict]:
        summary_path = self.appendix_dir / "appendix_perdate_summary.csv"
        if not summary_path.exists():
            return []
        df = pd.read_csv(summary_path)
        return df.to_dict(orient="records")

    def _eventcorridor_pair(self) -> tuple[Path, Path]:
        return (
            self.appendix_dir / "appendix_eventcorridor_model_union_2023-03-03_to_2023-03-06.tif",
            self.appendix_dir / "appendix_eventcorridor_obs_union_2023-03-03_to_2023-03-06.tif",
        )

    def snapshot_before(self) -> dict:
        official_row, official_arrays = self._compute_binary_pair(
            pair_id="official_main_before",
            forecast_path=get_official_mask_p50_datecomposite_path(self.case.run_name),
            observation_path=resolve_validation_mask_path(self.case.run_name),
        )
        event_model_path, event_obs_path = self._eventcorridor_pair()
        event_row, event_arrays = self._compute_binary_pair(
            pair_id="appendix_eventcorridor_before",
            forecast_path=event_model_path,
            observation_path=event_obs_path,
        )

        land_mask = self.helper._read_mask(Path("data_processed/grids/land_mask.tif"))
        sea_mask = self.helper._read_mask(Path("data_processed/grids/sea_mask.tif"))
        snapshot = {
            "run_name": self.case.run_name,
            "official_main": official_row,
            "appendix_eventcorridor": event_row,
            "appendix_perdate": self._appendix_perdate_records(),
            "land_mask_path": "data_processed/grids/land_mask.tif",
            "sea_mask_path": "data_processed/grids/sea_mask.tif",
        }
        with open(self.before_snapshot_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
            f.write("\n")
        np.savez_compressed(
            self.before_arrays_path,
            official_forecast=official_arrays["forecast"],
            official_observation=official_arrays["observation"],
            appendix_eventcorridor_forecast=event_arrays["forecast"],
            appendix_eventcorridor_observation=event_arrays["observation"],
            land_mask=land_mask.astype(np.float32),
            sea_mask=sea_mask.astype(np.float32),
        )
        return snapshot

    def _load_before_snapshot(self) -> tuple[dict, dict[str, np.ndarray]]:
        if not self.before_snapshot_path.exists() or not self.before_arrays_path.exists():
            raise FileNotFoundError(
                "Before snapshot artifacts are missing. Run "
                "`python -m src.services.shoreline_rerun snapshot_before` before rerunning prep/forecast/scoring."
            )
        with open(self.before_snapshot_path, "r", encoding="utf-8") as f:
            snapshot = json.load(f) or {}
        arrays = dict(np.load(self.before_arrays_path))
        return snapshot, arrays

    def _forcing_domain_manifest(self) -> dict:
        scoring_grid_path = Path("data_processed/grids/scoring_grid.yaml")
        with open(scoring_grid_path, "r", encoding="utf-8") as f:
            scoring_grid = yaml.safe_load(f) or {}
        halo_degrees = float(scoring_grid.get("forcing_bbox_halo_degrees", 0.5) or 0.5)
        display_bounds = scoring_grid.get("display_bounds_wgs84") or list(self.case.region)
        derived_bbox = derive_bbox_from_display_bounds(display_bounds, halo_degrees=halo_degrees)

        download_manifest_path = get_download_manifest_path()
        download_payload = {}
        if download_manifest_path.exists():
            with open(download_manifest_path, "r", encoding="utf-8") as f:
                download_payload = json.load(f) or {}
        run_entry = download_payload.get(self.case.run_name) or {}
        config = run_entry.get("config") or {}

        forecast_manifest = {}
        ensemble_manifest = {}
        if get_forecast_manifest_path(self.case.run_name).exists():
            with open(get_forecast_manifest_path(self.case.run_name), "r", encoding="utf-8") as f:
                forecast_manifest = json.load(f) or {}
        if get_ensemble_manifest_path(self.case.run_name).exists():
            with open(get_ensemble_manifest_path(self.case.run_name), "r", encoding="utf-8") as f:
                ensemble_manifest = json.load(f) or {}

        phase2_audit_path = self.case_output_dir / "forecast" / "phase2_loading_audit.json"
        phase2_audit = {}
        if phase2_audit_path.exists():
            with open(phase2_audit_path, "r", encoding="utf-8") as f:
                phase2_audit = json.load(f) or {}

        return {
            "run_name": self.case.run_name,
            "case_region_wgs84": list(self.case.region),
            "scoring_domain_display_bounds_wgs84": display_bounds,
            "halo_degrees": halo_degrees,
            "derived_forcing_bbox_wgs84": derived_bbox,
            "download_manifest_bbox": config.get("bbox", ""),
            "download_manifest_bbox_source": config.get("bbox_source", ""),
            "legacy_broad_region_usage_detected": "legacy" in str(config.get("bbox_source", "")).lower()
            or "fallback" in str(config.get("bbox_source", "")).lower(),
            "forecast_manifest_path": str(get_forecast_manifest_path(self.case.run_name)),
            "ensemble_manifest_path": str(get_ensemble_manifest_path(self.case.run_name)),
            "forecast_grid_shoreline_signature": ((forecast_manifest.get("grid") or {}).get("shoreline_mask_signature") or ""),
            "ensemble_grid_shoreline_signature": ((ensemble_manifest.get("grid") or {}).get("shoreline_mask_signature") or ""),
            "phase2_loading_audit_path": str(phase2_audit_path) if phase2_audit_path.exists() else "",
            "phase2_run_count": len(phase2_audit.get("runs") or []),
        }

    @staticmethod
    def _comparison_rows(before_row: dict, after_row: dict, comparison_group: str, source_name: str = "") -> list[dict]:
        metrics = [
            "fss_1km",
            "fss_3km",
            "fss_5km",
            "fss_10km",
            "forecast_nonzero_cells",
            "obs_nonzero_cells",
            "centroid_distance_m",
            "area_ratio_forecast_to_obs",
            "iou",
            "dice",
            "nearest_distance_to_obs_m",
            "ocean_cell_count",
        ]
        rows = []
        for metric in metrics:
            before_value = before_row.get(metric)
            after_value = after_row.get(metric)
            delta = (
                float(after_value) - float(before_value)
                if pd.notna(before_value) and pd.notna(after_value)
                else np.nan
            )
            rows.append(
                {
                    "comparison_group": comparison_group,
                    "source_name": source_name,
                    "metric": metric,
                    "before_value": before_value,
                    "after_value": after_value,
                    "delta": delta,
                }
            )
        return rows

    def _plot_overlay_pair(self, ax, forecast_mask: np.ndarray, observation_mask: np.ndarray, title: str) -> None:
        overlap = np.logical_and(forecast_mask > 0, observation_mask > 0)
        canvas = np.ones((forecast_mask.shape[0], forecast_mask.shape[1], 3), dtype=np.float32)
        canvas[observation_mask > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast_mask > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
        ax.imshow(canvas, origin="upper")
        ax.set_title(title)
        ax.set_axis_off()

    def _write_overlay_artifacts(
        self,
        *,
        before_arrays: dict[str, np.ndarray],
        after_official_arrays: dict[str, np.ndarray],
        after_event_arrays: dict[str, np.ndarray],
    ) -> dict[str, Path]:
        written: dict[str, Path] = {}
        if plt is None:
            return written

        shoreline_path = self.output_dir / "qa_shoreline_before_after_overlay.png"
        fig, axes = plt.subplots(1, 2, figsize=(12, 6))
        axes[0].imshow(before_arrays["land_mask"], origin="upper", cmap="Greys")
        axes[0].set_title("Before shoreline land mask")
        axes[0].set_axis_off()
        axes[1].imshow(self.helper._read_mask(Path("data_processed/grids/land_mask.tif")), origin="upper", cmap="Greys")
        axes[1].set_title("After shoreline land mask")
        axes[1].set_axis_off()
        fig.tight_layout()
        fig.savefig(shoreline_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        written["shoreline_overlay"] = shoreline_path

        official_path = self.output_dir / "qa_official_main_before_after.png"
        fig, axes = plt.subplots(1, 2, figsize=(12, 6))
        self._plot_overlay_pair(
            axes[0],
            before_arrays["official_forecast"],
            before_arrays["official_observation"],
            "Before official main pair",
        )
        self._plot_overlay_pair(
            axes[1],
            after_official_arrays["forecast"],
            after_official_arrays["observation"],
            "After official main pair",
        )
        fig.tight_layout()
        fig.savefig(official_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        written["official_overlay"] = official_path

        appendix_path = self.output_dir / "qa_appendix_eventcorridor_before_after.png"
        fig, axes = plt.subplots(1, 2, figsize=(12, 6))
        self._plot_overlay_pair(
            axes[0],
            before_arrays["appendix_eventcorridor_forecast"],
            before_arrays["appendix_eventcorridor_observation"],
            "Before appendix event corridor",
        )
        self._plot_overlay_pair(
            axes[1],
            after_event_arrays["forecast"],
            after_event_arrays["observation"],
            "After appendix event corridor",
        )
        fig.tight_layout()
        fig.savefig(appendix_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        written["appendix_overlay"] = appendix_path
        return written

    def compare_after(self) -> dict:
        before_snapshot, before_arrays = self._load_before_snapshot()
        after_official_row, after_official_arrays = self._compute_binary_pair(
            pair_id="official_main_after",
            forecast_path=get_official_mask_p50_datecomposite_path(self.case.run_name),
            observation_path=resolve_validation_mask_path(self.case.run_name),
        )
        event_model_path, event_obs_path = self._eventcorridor_pair()
        after_event_row, after_event_arrays = self._compute_binary_pair(
            pair_id="appendix_eventcorridor_after",
            forecast_path=event_model_path,
            observation_path=event_obs_path,
        )
        after_perdate_records = self._appendix_perdate_records()

        comparison_rows = []
        comparison_rows.extend(
            self._comparison_rows(
                before_snapshot["official_main"],
                after_official_row,
                comparison_group="official_main",
            )
        )
        comparison_rows.extend(
            self._comparison_rows(
                before_snapshot["appendix_eventcorridor"],
                after_event_row,
                comparison_group="appendix_eventcorridor",
            )
        )

        before_perdate = {record["pair_id"]: record for record in before_snapshot.get("appendix_perdate") or []}
        for record in after_perdate_records:
            pair_id = str(record.get("pair_id") or "")
            if pair_id not in before_perdate:
                continue
            comparison_rows.extend(
                self._comparison_rows(
                    before_perdate[pair_id],
                    record,
                    comparison_group="appendix_perdate",
                    source_name=str(record.get("source_name") or ""),
                )
            )

        before_after_path = self.output_dir / "shoreline_rerun_before_after.csv"
        pd.DataFrame(comparison_rows).to_csv(before_after_path, index=False)

        summary_rows = [
            {
                "comparison_group": "official_main",
                **{f"before_{key}": value for key, value in before_snapshot["official_main"].items() if key.startswith("fss_") or key in {
                    "forecast_nonzero_cells",
                    "obs_nonzero_cells",
                    "centroid_distance_m",
                    "area_ratio_forecast_to_obs",
                    "iou",
                    "dice",
                }},
                **{f"after_{key}": value for key, value in after_official_row.items() if key.startswith("fss_") or key in {
                    "forecast_nonzero_cells",
                    "obs_nonzero_cells",
                    "centroid_distance_m",
                    "area_ratio_forecast_to_obs",
                    "iou",
                    "dice",
                }},
            },
            {
                "comparison_group": "appendix_eventcorridor",
                **{f"before_{key}": value for key, value in before_snapshot["appendix_eventcorridor"].items() if key.startswith("fss_") or key in {
                    "forecast_nonzero_cells",
                    "obs_nonzero_cells",
                    "centroid_distance_m",
                    "area_ratio_forecast_to_obs",
                    "iou",
                    "dice",
                }},
                **{f"after_{key}": value for key, value in after_event_row.items() if key.startswith("fss_") or key in {
                    "forecast_nonzero_cells",
                    "obs_nonzero_cells",
                    "centroid_distance_m",
                    "area_ratio_forecast_to_obs",
                    "iou",
                    "dice",
                }},
            },
        ]
        summary_path = self.output_dir / "shoreline_rerun_summary.csv"
        pd.DataFrame(summary_rows).to_csv(summary_path, index=False)

        diagnostics_rows = []
        diagnostics_rows.append({"comparison_group": "official_main", **before_snapshot["official_main"], **{f"after_{k}": v for k, v in after_official_row.items()}})
        diagnostics_rows.append({"comparison_group": "appendix_eventcorridor", **before_snapshot["appendix_eventcorridor"], **{f"after_{k}": v for k, v in after_event_row.items()}})
        for record in after_perdate_records:
            pair_id = str(record.get("pair_id") or "")
            before_record = before_perdate.get(pair_id)
            if before_record is None:
                continue
            diagnostics_rows.append(
                {
                    "comparison_group": "appendix_perdate",
                    **before_record,
                    **{f"after_{k}": v for k, v in record.items()},
                }
            )
        diagnostics_path = self.output_dir / "shoreline_rerun_diagnostics.csv"
        pd.DataFrame(diagnostics_rows).to_csv(diagnostics_path, index=False)

        forcing_manifest = self._forcing_domain_manifest()
        forcing_json = self.output_dir / "forcing_domain_manifest.json"
        forcing_csv = self.output_dir / "forcing_domain_manifest.csv"
        with open(forcing_json, "w", encoding="utf-8") as f:
            json.dump(forcing_manifest, f, indent=2)
            f.write("\n")
        pd.DataFrame([forcing_manifest]).to_csv(forcing_csv, index=False)

        overlay_paths = self._write_overlay_artifacts(
            before_arrays=before_arrays,
            after_official_arrays=after_official_arrays,
            after_event_arrays=after_event_arrays,
        )

        report_path = self.output_dir / "shoreline_rerun_report.md"
        report_lines = [
            "# Shoreline Rerun Report",
            "",
            f"- Official main FSS before: {before_snapshot['official_main']['fss_1km']:.6f}, {before_snapshot['official_main']['fss_3km']:.6f}, {before_snapshot['official_main']['fss_5km']:.6f}, {before_snapshot['official_main']['fss_10km']:.6f}",
            f"- Official main FSS after: {after_official_row['fss_1km']:.6f}, {after_official_row['fss_3km']:.6f}, {after_official_row['fss_5km']:.6f}, {after_official_row['fss_10km']:.6f}",
            f"- Appendix event-corridor FSS before: {before_snapshot['appendix_eventcorridor']['fss_1km']:.6f}, {before_snapshot['appendix_eventcorridor']['fss_3km']:.6f}, {before_snapshot['appendix_eventcorridor']['fss_5km']:.6f}, {before_snapshot['appendix_eventcorridor']['fss_10km']:.6f}",
            f"- Appendix event-corridor FSS after: {after_event_row['fss_1km']:.6f}, {after_event_row['fss_3km']:.6f}, {after_event_row['fss_5km']:.6f}, {after_event_row['fss_10km']:.6f}",
            f"- Forcing bbox source after rerun: {forcing_manifest['download_manifest_bbox_source']}",
            f"- Legacy broad-region usage detected after rerun: {forcing_manifest['legacy_broad_region_usage_detected']}",
        ]
        if overlay_paths:
            report_lines.extend(
                [
                    f"- Shoreline overlay: `{overlay_paths.get('shoreline_overlay', '')}`",
                    f"- Official main overlay: `{overlay_paths.get('official_overlay', '')}`",
                    f"- Appendix event-corridor overlay: `{overlay_paths.get('appendix_overlay', '')}`",
                ]
            )
        report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

        return {
            "before_after_csv": str(before_after_path),
            "summary_csv": str(summary_path),
            "diagnostics_csv": str(diagnostics_path),
            "report_md": str(report_path),
            "forcing_domain_json": str(forcing_json),
            "forcing_domain_csv": str(forcing_csv),
        }


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Capture and compare shoreline rerun metrics.")
    parser.add_argument("action", choices=["snapshot_before", "compare_after"])
    args = parser.parse_args(argv)

    service = ShorelineRerunComparisonService()
    if args.action == "snapshot_before":
        result = service.snapshot_before()
    else:
        result = service.compare_after()
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":  # pragma: no cover - manual execution entrypoint
    main()
