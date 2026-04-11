"""Read-only static trajectory gallery build from existing artifacts."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from matplotlib import pyplot as plt
from matplotlib.colors import ListedColormap
from pyproj import Transformer
from rasterio.plot import show
from shapely.geometry import MultiPoint

matplotlib.use("Agg")

PHASE = "trajectory_gallery_build"
OUTPUT_DIR = Path("output") / "trajectory_gallery"
FINAL_REPRO_DIR = Path("output") / "final_reproducibility_package"
FINAL_REPRO_MANIFEST_JSON = FINAL_REPRO_DIR / "final_reproducibility_manifest.json"
FINAL_PHASE_STATUS_CSV = FINAL_REPRO_DIR / "final_phase_status_registry.csv"
FINAL_OUTPUT_CATALOG_CSV = FINAL_REPRO_DIR / "final_output_catalog.csv"
FINAL_VALIDATION_MANIFEST_JSON = Path("output") / "final_validation_package" / "final_validation_manifest.json"
PHASE4_MANIFEST_JSON = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_run_manifest.json"

FIGURE_GROUPS: dict[str, dict[str, str]] = {
    "A": {"group_id": "mindoro_deterministic_track_paths", "label": "Mindoro deterministic track/path visuals"},
    "B": {"group_id": "mindoro_ensemble_sampled_member_trajectories", "label": "Mindoro ensemble sampled-member trajectories"},
    "C": {"group_id": "mindoro_centroid_corridor_hull_views", "label": "Mindoro centroid/corridor/hull views"},
    "D": {"group_id": "mindoro_forecast_vs_observation_overlays", "label": "Mindoro forecast-vs-observation overlays"},
    "E": {"group_id": "mindoro_opendrift_vs_pygnome_comparison_maps", "label": "Mindoro OpenDrift vs PyGNOME comparison maps"},
    "F": {"group_id": "dwh_deterministic_track_paths", "label": "DWH deterministic track/path visuals"},
    "G": {"group_id": "dwh_ensemble_p50_p90_overlays", "label": "DWH ensemble p50/p90 overlays"},
    "H": {"group_id": "dwh_opendrift_vs_pygnome_comparison_maps", "label": "DWH OpenDrift vs PyGNOME comparison maps"},
    "I": {"group_id": "mindoro_phase4_oil_budget_figures", "label": "Mindoro Phase 4 oil-budget figures"},
    "J": {"group_id": "mindoro_phase4_shoreline_impact_figures", "label": "Mindoro Phase 4 shoreline-arrival / shoreline-segment impact figures"},
}


@dataclass
class FigureRecord:
    figure_id: str
    figure_group_code: str
    figure_group_id: str
    figure_group_label: str
    case_id: str
    phase_or_track: str
    model_name: str
    run_type: str
    date_token: str
    scenario_id: str
    figure_slug: str
    relative_path: str
    filename: str
    generation_mode: str
    ready_for_panel_presentation: bool
    provisional_context: str
    source_paths: str
    notes: str

    def as_row(self) -> dict[str, Any]:
        return {
            "figure_id": self.figure_id,
            "figure_group_code": self.figure_group_code,
            "figure_group_id": self.figure_group_id,
            "figure_group_label": self.figure_group_label,
            "case_id": self.case_id,
            "phase_or_track": self.phase_or_track,
            "model_name": self.model_name,
            "run_type": self.run_type,
            "date_token": self.date_token,
            "scenario_id": self.scenario_id,
            "figure_slug": self.figure_slug,
            "relative_path": self.relative_path,
            "filename": self.filename,
            "generation_mode": self.generation_mode,
            "ready_for_panel_presentation": self.ready_for_panel_presentation,
            "provisional_context": self.provisional_context,
            "source_paths": self.source_paths,
            "notes": self.notes,
        }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    df = df[columns]
    df.to_csv(path, index=False)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


def _safe_token(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    chars = [char if char.isalnum() else "_" for char in text]
    normalized = "".join(chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def build_figure_filename(
    *,
    case_id: str,
    phase_or_track: str,
    model_name: str,
    run_type: str,
    date_token: str,
    figure_slug: str,
    scenario_id: str = "",
    extension: str = "png",
) -> str:
    tokens = [
        _safe_token(case_id),
        _safe_token(phase_or_track),
        _safe_token(model_name),
        _safe_token(run_type),
        _safe_token(date_token),
    ]
    scenario_token = _safe_token(scenario_id)
    if scenario_token:
        tokens.append(scenario_token)
    tokens.append(_safe_token(figure_slug))
    return "__".join(token for token in tokens if token) + f".{extension.lstrip('.')}"


class TrajectoryGalleryBuildService:
    def __init__(self, repo_root: str | Path = ".", output_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_DIR
        self.final_repro_manifest = _read_json(self.repo_root / FINAL_REPRO_MANIFEST_JSON)
        self.final_phase_status = _read_csv(self.repo_root / FINAL_PHASE_STATUS_CSV)
        self.final_output_catalog = _read_csv(self.repo_root / FINAL_OUTPUT_CATALOG_CSV)
        self.final_validation_manifest = _read_json(self.repo_root / FINAL_VALIDATION_MANIFEST_JSON)
        self.phase4_manifest = _read_json(self.repo_root / PHASE4_MANIFEST_JSON)
        self.figure_records: list[FigureRecord] = []
        self.missing_optional_artifacts: list[dict[str, str]] = []
        self.generated_group_codes: set[str] = set()

    def _resolve_case_output_path(self, relative_path: str, case_id: str) -> Path:
        candidate = self.repo_root / relative_path
        if candidate.exists():
            return candidate
        case_candidate = self.repo_root / "output" / case_id / relative_path
        if case_candidate.exists():
            return case_candidate
        return candidate

    def _phase_row(self, phase_id: str, track_id: str) -> dict[str, Any]:
        if self.final_phase_status.empty:
            return {}
        mask = (self.final_phase_status["phase_id"] == phase_id) & (self.final_phase_status["track_id"] == track_id)
        if not mask.any():
            return {}
        row = self.final_phase_status.loc[mask].iloc[0]
        return {str(key): row[key] for key in row.index}

    def _provisional_context(self, case_id: str, phase_or_track: str) -> str:
        if case_id == "CASE_MINDORO_RETRO_2023" and phase_or_track.startswith("phase4"):
            if self.phase4_manifest.get("provisional_inherited_from_transport", True):
                return "Mindoro Phase 4 is scientifically reportable, but inherited-provisional from the unfinished Phase 1/2 freeze story."
        if case_id == "CASE_MINDORO_RETRO_2023" and self._phase_row("phase2", "phase2_machine_readable_forecast"):
            return "Mindoro transport visuals reflect a scientifically usable but not yet frozen Phase 2 transport framework."
        if case_id == "CASE_DWH_RETRO_2010_72H" and self._phase_row("phase3c", "C1"):
            return "DWH transfer-validation visuals are reportable, but the upstream transport baseline story remains inherited-provisional."
        return ""

    def _record_missing(self, source_path: Path, notes: str) -> None:
        self.missing_optional_artifacts.append(
            {"relative_path": _relative_to_repo(self.repo_root, source_path), "notes": notes}
        )

    def _register_figure(
        self,
        *,
        figure_group_code: str,
        case_id: str,
        phase_or_track: str,
        model_name: str,
        run_type: str,
        date_token: str,
        figure_slug: str,
        relative_path: str,
        generation_mode: str,
        source_paths: list[str],
        notes: str,
        scenario_id: str = "",
    ) -> FigureRecord:
        group = FIGURE_GROUPS[figure_group_code]
        record = FigureRecord(
            figure_id=Path(relative_path).stem,
            figure_group_code=figure_group_code,
            figure_group_id=group["group_id"],
            figure_group_label=group["label"],
            case_id=case_id,
            phase_or_track=phase_or_track,
            model_name=model_name,
            run_type=run_type,
            date_token=date_token,
            scenario_id=scenario_id,
            figure_slug=figure_slug,
            relative_path=relative_path,
            filename=Path(relative_path).name,
            generation_mode=generation_mode,
            ready_for_panel_presentation=True,
            provisional_context=self._provisional_context(case_id, phase_or_track),
            source_paths=";".join(source_paths),
            notes=notes,
        )
        self.figure_records.append(record)
        self.generated_group_codes.add(figure_group_code)
        return record

    def _copy_existing_figure(self, **spec: str) -> FigureRecord | None:
        source_relative_path = str(spec["source_relative_path"])
        source_path = self.repo_root / source_relative_path
        if not source_path.exists():
            self._record_missing(source_path, str(spec["notes"]))
            return None
        filename = build_figure_filename(
            case_id=str(spec["case_id"]),
            phase_or_track=str(spec["phase_or_track"]),
            model_name=str(spec["model_name"]),
            run_type=str(spec["run_type"]),
            date_token=str(spec["date_token"]),
            scenario_id=str(spec.get("scenario_id") or ""),
            figure_slug=str(spec["figure_slug"]),
        )
        destination = self.output_dir / filename
        shutil.copy2(source_path, destination)
        return self._register_figure(
            figure_group_code=str(spec["figure_group_code"]),
            case_id=str(spec["case_id"]),
            phase_or_track=str(spec["phase_or_track"]),
            model_name=str(spec["model_name"]),
            run_type=str(spec["run_type"]),
            date_token=str(spec["date_token"]),
            figure_slug=str(spec["figure_slug"]),
            scenario_id=str(spec.get("scenario_id") or ""),
            relative_path=_relative_to_repo(self.repo_root, destination),
            generation_mode="copied_existing_qa",
            source_paths=[source_relative_path],
            notes=str(spec["notes"]),
        )

    def _load_background(self, relative_path: str | None, target_crs: str) -> gpd.GeoDataFrame | None:
        if not relative_path:
            return None
        path = self.repo_root / relative_path
        if not path.exists():
            self._record_missing(path, "Optional vector background missing for a gallery map.")
            return None
        gdf = gpd.read_file(path)
        if gdf.empty:
            return None
        return gdf.to_crs(target_crs) if target_crs else gdf

    def _extract_opendrift_arrays(
        self,
        nc_path: Path,
        target_crs: str,
        sample_count: int,
    ) -> tuple[list[np.ndarray], list[np.ndarray], np.ndarray, np.ndarray, np.ndarray]:
        with xr.open_dataset(nc_path) as ds:
            lon = np.asarray(ds["lon"].values, dtype=float)
            lat = np.asarray(ds["lat"].values, dtype=float)
        if lon.ndim != 2 or lat.ndim != 2:
            raise ValueError(f"Expected 2-D lon/lat arrays in {nc_path}")

        transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
        sample_indices = np.linspace(0, lon.shape[0] - 1, num=max(1, min(sample_count, lon.shape[0])), dtype=int)
        sampled_x: list[np.ndarray] = []
        sampled_y: list[np.ndarray] = []
        for idx in sample_indices:
            mask = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
            if not mask.any():
                continue
            x_values, y_values = transformer.transform(lon[idx][mask], lat[idx][mask])
            sampled_x.append(np.asarray(x_values, dtype=float))
            sampled_y.append(np.asarray(y_values, dtype=float))

        centroid_lon = np.nanmean(lon, axis=0)
        centroid_lat = np.nanmean(lat, axis=0)
        centroid_mask = np.isfinite(centroid_lon) & np.isfinite(centroid_lat)
        centroid_x, centroid_y = transformer.transform(centroid_lon[centroid_mask], centroid_lat[centroid_mask])

        final_points: list[tuple[float, float]] = []
        final_indices = np.linspace(0, lon.shape[0] - 1, num=min(40, lon.shape[0]), dtype=int)
        for idx in final_indices:
            mask = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
            if not mask.any():
                continue
            final_lon = float(lon[idx][mask][-1])
            final_lat = float(lat[idx][mask][-1])
            x_value, y_value = transformer.transform(final_lon, final_lat)
            final_points.append((float(x_value), float(y_value)))
        return sampled_x, sampled_y, np.asarray(centroid_x), np.asarray(centroid_y), np.asarray(final_points, dtype=float)

    def _apply_common_map_style(self, ax: plt.Axes, target_crs: str) -> None:
        ax.grid(True, linestyle="--", linewidth=0.4, alpha=0.35)
        if target_crs == "EPSG:4326":
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
        else:
            ax.set_xlabel(f"{target_crs} x")
            ax.set_ylabel(f"{target_crs} y")

    def _set_axis_bounds(self, ax: plt.Axes, x_values: list[np.ndarray], y_values: list[np.ndarray], background: gpd.GeoDataFrame | None = None) -> None:
        if background is not None and not background.empty:
            min_x, min_y, max_x, max_y = background.total_bounds
        else:
            merged_x = np.concatenate([values for values in x_values if values.size]) if x_values else np.array([])
            merged_y = np.concatenate([values for values in y_values if values.size]) if y_values else np.array([])
            if merged_x.size == 0 or merged_y.size == 0:
                return
            min_x = float(np.nanmin(merged_x))
            max_x = float(np.nanmax(merged_x))
            min_y = float(np.nanmin(merged_y))
            max_y = float(np.nanmax(merged_y))
        pad_x = max(1000.0, (max_x - min_x) * 0.08)
        pad_y = max(1000.0, (max_y - min_y) * 0.08)
        ax.set_xlim(min_x - pad_x, max_x + pad_x)
        ax.set_ylim(min_y - pad_y, max_y + pad_y)

    def _plot_mask_overlay(self, ax: plt.Axes, relative_path: str | None, color: str, alpha: float) -> bool:
        if not relative_path:
            return False
        path = self.repo_root / relative_path
        if not path.exists():
            self._record_missing(path, "Optional raster overlay missing for a gallery figure.")
            return False
        with rasterio.open(path) as dataset:
            array = dataset.read(1)
            mask = np.ma.masked_where(~np.isfinite(array) | (array <= 0), array)
            if mask.mask.all():
                return False
            show(mask, transform=dataset.transform, ax=ax, cmap=ListedColormap([color]), alpha=alpha)
        return True

    def _generate_track_map(self, **spec: Any) -> FigureRecord | None:
        nc_relative_path = str(spec["nc_relative_path"])
        nc_path = self.repo_root / nc_relative_path
        if not nc_path.exists():
            self._record_missing(nc_path, str(spec["notes"]))
            return None

        target_crs = str(spec["target_crs"])
        background_gdf = self._load_background(str(spec.get("background_vector_relative_path") or ""), target_crs)
        sampled_x, sampled_y, centroid_x, centroid_y, final_points = self._extract_opendrift_arrays(
            nc_path,
            target_crs,
            int(spec["sample_count"]),
        )
        if not sampled_x or centroid_x.size == 0:
            self._record_missing(nc_path, f"{spec['notes']} The stored track file did not expose valid particle paths.")
            return None

        filename = build_figure_filename(
            case_id=str(spec["case_id"]),
            phase_or_track=str(spec["phase_or_track"]),
            model_name=str(spec["model_name"]),
            run_type=str(spec["run_type"]),
            date_token=str(spec["date_token"]),
            figure_slug=str(spec["figure_slug"]),
        )
        destination = self.output_dir / filename
        fig, ax = plt.subplots(figsize=(11, 8))
        if background_gdf is not None and not background_gdf.empty:
            background_gdf.plot(ax=ax, color="#B9B4AB", linewidth=0.6, alpha=0.9, zorder=1)
        self._plot_mask_overlay(ax, str(spec.get("background_raster_relative_path") or ""), "#D6D3D1", 0.40)
        for x_values, y_values in zip(sampled_x, sampled_y):
            ax.plot(x_values, y_values, color="#2563EB", linewidth=0.45, alpha=0.14, zorder=2)
        ax.plot(centroid_x, centroid_y, color="#111827", linewidth=2.2, label="Centroid path", zorder=3)
        ax.scatter(centroid_x[0], centroid_y[0], color="#16A34A", s=45, marker="o", zorder=4, label="Start")
        ax.scatter(centroid_x[-1], centroid_y[-1], color="#DC2626", s=45, marker="X", zorder=4, label="End")
        if final_points.size >= 6:
            hull = MultiPoint(final_points.tolist()).convex_hull
            if hasattr(hull, "exterior"):
                hull_x, hull_y = hull.exterior.xy
                ax.plot(hull_x, hull_y, color="#F97316", linewidth=1.4, alpha=0.85, zorder=3, label="Final hull")
        self._set_axis_bounds(ax, sampled_x + [centroid_x], sampled_y + [centroid_y], background_gdf)
        self._apply_common_map_style(ax, target_crs)
        ax.set_title(str(spec["title"]))
        ax.legend(loc="upper right", fontsize=8)
        fig.tight_layout()
        fig.savefig(destination, dpi=180, bbox_inches="tight")
        plt.close(fig)

        return self._register_figure(
            figure_group_code=str(spec["figure_group_code"]),
            case_id=str(spec["case_id"]),
            phase_or_track=str(spec["phase_or_track"]),
            model_name=str(spec["model_name"]),
            run_type=str(spec["run_type"]),
            date_token=str(spec["date_token"]),
            figure_slug=str(spec["figure_slug"]),
            relative_path=_relative_to_repo(self.repo_root, destination),
            generation_mode="generated_from_existing_tracks",
            source_paths=[nc_relative_path] + ([str(spec["background_raster_relative_path"])] if spec.get("background_raster_relative_path") else []),
            notes=str(spec["notes"]),
        )

    def _generate_mindoro_member_centroid_map(self) -> FigureRecord | None:
        manifest_path = self.repo_root / "output" / "CASE_MINDORO_RETRO_2023" / "ensemble" / "ensemble_manifest.json"
        manifest = _read_json(manifest_path)
        member_runs = manifest.get("member_runs") or []
        if not member_runs:
            self._record_missing(manifest_path, "Mindoro ensemble member manifest missing for gallery member-centroid view.")
            return None

        target_crs = str((manifest.get("grid") or {}).get("crs") or "EPSG:32651")
        shoreline_path = str((manifest.get("grid") or {}).get("shoreline_segments_path") or "")
        shoreline_gdf = self._load_background(shoreline_path, target_crs)

        filename = build_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase2_official",
            model_name="opendrift",
            run_type="ensemble_sampled_member_centroids",
            date_token="2023-03-03_to_2023-03-06",
            figure_slug="member_centroid_paths",
        )
        destination = self.output_dir / filename
        fig, ax = plt.subplots(figsize=(11, 8))
        if shoreline_gdf is not None and not shoreline_gdf.empty:
            shoreline_gdf.plot(ax=ax, color="#D4D0C8", linewidth=0.55, alpha=0.9, zorder=1)

        colors = plt.cm.tab10(np.linspace(0, 1, min(8, len(member_runs))))
        selected_indices = np.linspace(0, len(member_runs) - 1, num=min(8, len(member_runs)), dtype=int)
        source_paths: list[str] = []
        all_x: list[np.ndarray] = []
        all_y: list[np.ndarray] = []
        for color, idx in zip(colors, selected_indices):
            member = member_runs[int(idx)]
            member_rel_path = str(member.get("relative_path") or "")
            member_path = self._resolve_case_output_path(member_rel_path, "CASE_MINDORO_RETRO_2023")
            if not member_path.exists():
                self._record_missing(member_path, "Mindoro sampled-member trajectory missing for gallery view.")
                continue
            _, _, centroid_x, centroid_y, _ = self._extract_opendrift_arrays(member_path, target_crs, sample_count=8)
            if centroid_x.size == 0:
                continue
            ax.plot(centroid_x, centroid_y, color=color, linewidth=1.7, alpha=0.95, zorder=3)
            ax.scatter(centroid_x[-1], centroid_y[-1], color=color, s=24, zorder=4)
            ax.text(centroid_x[-1], centroid_y[-1], f"M{int(member.get('member_id', 0)):02d}", fontsize=7, color="#111827", ha="left", va="bottom", zorder=5)
            source_paths.append(_relative_to_repo(self.repo_root, member_path))
            all_x.append(centroid_x)
            all_y.append(centroid_y)

        if not all_x:
            plt.close(fig)
            self._record_missing(manifest_path, "Mindoro sampled-member centroid figure could not find valid member trajectories.")
            return None

        self._set_axis_bounds(ax, all_x, all_y, shoreline_gdf)
        self._apply_common_map_style(ax, target_crs)
        ax.set_title("Mindoro sampled ensemble member centroid trajectories")
        fig.tight_layout()
        fig.savefig(destination, dpi=180, bbox_inches="tight")
        plt.close(fig)

        return self._register_figure(
            figure_group_code="B",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase2_official",
            model_name="opendrift",
            run_type="ensemble_sampled_member_centroids",
            date_token="2023-03-03_to_2023-03-06",
            figure_slug="member_centroid_paths",
            relative_path=_relative_to_repo(self.repo_root, destination),
            generation_mode="generated_from_existing_tracks",
            source_paths=source_paths,
            notes="Generated from stored Mindoro ensemble member NetCDFs using sampled member centroid paths rather than every particle track.",
        )

    def _generate_mindoro_corridor_hull_view(self) -> FigureRecord | None:
        ensemble_manifest_path = self.repo_root / "output" / "CASE_MINDORO_RETRO_2023" / "ensemble" / "ensemble_manifest.json"
        ensemble_manifest = _read_json(ensemble_manifest_path)
        grid = ensemble_manifest.get("grid") or {}
        target_crs = str(grid.get("crs") or "EPSG:32651")
        shoreline_gdf = self._load_background(str(grid.get("shoreline_segments_path") or ""), target_crs)
        members = ensemble_manifest.get("member_runs") or []
        if not members:
            self._record_missing(ensemble_manifest_path, "Mindoro ensemble manifest missing for corridor/hull gallery figure.")
            return None

        p50_rel_path = "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p50_2023-03-06_datecomposite.tif"
        p90_rel_path = "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p90_2023-03-06T09-59-00Z.tif"
        centroid_lines_x: list[np.ndarray] = []
        centroid_lines_y: list[np.ndarray] = []
        final_points: list[tuple[float, float]] = []
        source_paths = [p50_rel_path, p90_rel_path]
        selected_indices = np.linspace(0, len(members) - 1, num=min(10, len(members)), dtype=int)
        for idx in selected_indices:
            member_rel_path = str(members[int(idx)].get("relative_path") or "")
            member_path = self._resolve_case_output_path(member_rel_path, "CASE_MINDORO_RETRO_2023")
            if not member_path.exists():
                self._record_missing(member_path, "Mindoro ensemble member missing for corridor/hull gallery figure.")
                continue
            _, _, centroid_x, centroid_y, member_final_points = self._extract_opendrift_arrays(member_path, target_crs, sample_count=6)
            if centroid_x.size:
                centroid_lines_x.append(centroid_x)
                centroid_lines_y.append(centroid_y)
            if member_final_points.size:
                final_points.extend(member_final_points.tolist())
            source_paths.append(_relative_to_repo(self.repo_root, member_path))

        if not centroid_lines_x:
            self._record_missing(ensemble_manifest_path, "Mindoro corridor/hull gallery figure could not find valid member trajectories.")
            return None

        filename = build_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase2_phase3b",
            model_name="opendrift",
            run_type="corridor_hull_view",
            date_token="2023-03-06",
            figure_slug="p50_p90_hull_overlay",
        )
        destination = self.output_dir / filename
        fig, ax = plt.subplots(figsize=(11, 8))
        if shoreline_gdf is not None and not shoreline_gdf.empty:
            shoreline_gdf.plot(ax=ax, color="#D4D0C8", linewidth=0.55, alpha=0.85, zorder=1)
        self._plot_mask_overlay(ax, p50_rel_path, "#2563EB", 0.28)
        self._plot_mask_overlay(ax, p90_rel_path, "#1D4ED8", 0.45)
        for centroid_x, centroid_y in zip(centroid_lines_x, centroid_lines_y):
            ax.plot(centroid_x, centroid_y, color="#0F172A", linewidth=0.7, alpha=0.45, zorder=3)
        if len(final_points) >= 3:
            hull = MultiPoint(final_points).convex_hull
            if hasattr(hull, "exterior"):
                hull_x, hull_y = hull.exterior.xy
                ax.plot(hull_x, hull_y, color="#F97316", linewidth=1.6, alpha=0.9, zorder=4, label="Sampled final-position hull")
        self._set_axis_bounds(ax, centroid_lines_x, centroid_lines_y, shoreline_gdf)
        self._apply_common_map_style(ax, target_crs)
        ax.set_title("Mindoro centroid/corridor/hull view from stored ensemble outputs")
        ax.legend(loc="upper right", fontsize=8)
        fig.tight_layout()
        fig.savefig(destination, dpi=180, bbox_inches="tight")
        plt.close(fig)

        return self._register_figure(
            figure_group_code="C",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase2_phase3b",
            model_name="opendrift",
            run_type="corridor_hull_view",
            date_token="2023-03-06",
            figure_slug="p50_p90_hull_overlay",
            relative_path=_relative_to_repo(self.repo_root, destination),
            generation_mode="generated_from_existing_rasters_and_tracks",
            source_paths=source_paths,
            notes="Generated from the stored Mindoro p50 date-composite mask, p90 72 h mask, and sampled member centroid/final-position geometry.",
        )

    def _generate_phase4_shoreline_arrival_summary(self) -> FigureRecord | None:
        arrival_path = self.repo_root / "output" / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_shoreline_arrival.csv"
        if not arrival_path.exists():
            self._record_missing(arrival_path, "Mindoro Phase 4 shoreline arrival CSV missing for gallery summary figure.")
            return None
        arrival_df = pd.read_csv(arrival_path)
        if arrival_df.empty:
            self._record_missing(arrival_path, "Mindoro Phase 4 shoreline arrival CSV is empty.")
            return None

        filename = build_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            model_name="openoil",
            run_type="shoreline_arrival_summary",
            date_token="2023-03-03_to_2023-03-06",
            figure_slug="scenario_arrival_bars",
            scenario_id="all_scenarios",
        )
        destination = self.output_dir / filename
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        labels = arrival_df["scenario_id"].astype(str).tolist()
        colors = ["#FF8C00", "#8C564B", "#4B0082"][: len(labels)]
        axes[0].bar(labels, arrival_df["first_shoreline_arrival_h"], color=colors)
        axes[0].set_title("First shoreline arrival (hours)")
        axes[0].set_ylabel("Hours from release")
        axes[0].tick_params(axis="x", rotation=18)
        axes[1].bar(labels, arrival_df["total_beached_kg"], color=colors)
        axes[1].set_title("Total beached mass")
        axes[1].set_ylabel("kg")
        axes[1].tick_params(axis="x", rotation=18)
        fig.suptitle("Mindoro Phase 4 shoreline arrival summary")
        fig.tight_layout()
        fig.savefig(destination, dpi=180, bbox_inches="tight")
        plt.close(fig)

        return self._register_figure(
            figure_group_code="J",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            model_name="openoil",
            run_type="shoreline_arrival_summary",
            date_token="2023-03-03_to_2023-03-06",
            figure_slug="scenario_arrival_bars",
            scenario_id="all_scenarios",
            relative_path=_relative_to_repo(self.repo_root, destination),
            generation_mode="generated_from_existing_phase4_csv",
            source_paths=[_relative_to_repo(self.repo_root, arrival_path)],
            notes="Generated from the stored Mindoro Phase 4 shoreline arrival summary without rerunning the weathering workflow.",
        )

    def _generate_phase4_segment_impact_map(self) -> FigureRecord | None:
        segments_path = self.repo_root / "output" / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_shoreline_segments.csv"
        if not segments_path.exists():
            self._record_missing(segments_path, "Mindoro Phase 4 shoreline segment CSV missing for gallery impact map.")
            return None
        segments_df = pd.read_csv(segments_path)
        if segments_df.empty:
            self._record_missing(segments_path, "Mindoro Phase 4 shoreline segment CSV is empty.")
            return None

        filename = build_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            model_name="openoil",
            run_type="shoreline_segment_impact_map",
            date_token="2023-03-03_to_2023-03-06",
            figure_slug="segment_midpoint_impacts",
            scenario_id="all_scenarios",
        )
        destination = self.output_dir / filename
        shoreline_gdf = self._load_background(str(self.phase4_manifest.get("shoreline_segments_path") or ""), "EPSG:4326")
        scenario_ids = segments_df["scenario_id"].astype(str).unique().tolist()
        fig, axes = plt.subplots(1, len(scenario_ids), figsize=(5 * len(scenario_ids), 5), sharex=True, sharey=True)
        if len(scenario_ids) == 1:
            axes = [axes]
        for axis, scenario_id in zip(axes, scenario_ids):
            scenario_df = segments_df.loc[segments_df["scenario_id"] == scenario_id].copy()
            if shoreline_gdf is not None and not shoreline_gdf.empty:
                shoreline_gdf.plot(ax=axis, color="#D4D0C8", linewidth=0.6, alpha=0.9, zorder=1)
            scatter = axis.scatter(
                scenario_df["segment_midpoint_lon"],
                scenario_df["segment_midpoint_lat"],
                s=np.clip(scenario_df["total_beached_kg"] / 120.0, 18, 140),
                c=scenario_df["total_beached_kg"],
                cmap="inferno",
                alpha=0.85,
                edgecolors="black",
                linewidths=0.2,
                zorder=3,
            )
            axis.set_title(str(scenario_id))
            axis.grid(True, linestyle="--", linewidth=0.35, alpha=0.30)
            axis.set_xlabel("Longitude")
            axis.set_ylabel("Latitude")
        fig.colorbar(scatter, ax=axes, shrink=0.78, label="Beached kg")
        fig.suptitle("Mindoro Phase 4 shoreline segment impacts by scenario")
        fig.subplots_adjust(top=0.82, wspace=0.18)
        fig.savefig(destination, dpi=180, bbox_inches="tight")
        plt.close(fig)

        return self._register_figure(
            figure_group_code="J",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            model_name="openoil",
            run_type="shoreline_segment_impact_map",
            date_token="2023-03-03_to_2023-03-06",
            figure_slug="segment_midpoint_impacts",
            scenario_id="all_scenarios",
            relative_path=_relative_to_repo(self.repo_root, destination),
            generation_mode="generated_from_existing_phase4_csv",
            source_paths=[_relative_to_repo(self.repo_root, segments_path), str(self.phase4_manifest.get("shoreline_segments_path") or "")],
            notes="Generated from the stored Phase 4 shoreline segment registry and canonical shoreline segment geometry.",
        )

    def _build_figures(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        forecast_manifest = _read_json(self.repo_root / "output" / "CASE_MINDORO_RETRO_2023" / "forecast" / "forecast_manifest.json")
        mindoro_grid = forecast_manifest.get("grid") or {}
        mindoro_target_crs = str(mindoro_grid.get("crs") or "EPSG:32651")
        mindoro_shoreline = str(mindoro_grid.get("shoreline_segments_path") or "")

        self._generate_track_map(
            nc_relative_path="output/CASE_MINDORO_RETRO_2023/forecast/deterministic_control_cmems_era5.nc",
            target_crs=mindoro_target_crs,
            background_vector_relative_path=mindoro_shoreline,
            background_raster_relative_path=None,
            title="Mindoro deterministic control path map",
            figure_group_code="A",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase2_official",
            model_name="opendrift",
            run_type="deterministic_track_map",
            date_token="2023-03-03_to_2023-03-06",
            figure_slug="sampled_particle_paths",
            notes="Generated from the stored Mindoro deterministic OpenDrift control NetCDF using sampled particle tracks and a centroid path.",
            sample_count=140,
        )
        self._generate_mindoro_member_centroid_map()
        self._generate_mindoro_corridor_hull_view()

        dwh_event_summary_path = self.repo_root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run" / "phase3c_eventcorridor_summary.csv"
        dwh_event_df = pd.read_csv(dwh_event_summary_path) if dwh_event_summary_path.exists() else pd.DataFrame()
        dwh_obs_raster = str(dwh_event_df.iloc[0]["observation_path"]) if (not dwh_event_df.empty and "observation_path" in dwh_event_df.columns) else ""
        self._generate_track_map(
            nc_relative_path="output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/tracks/opendrift_control_dwh_phase3c.nc",
            target_crs="EPSG:32616",
            background_vector_relative_path=None,
            background_raster_relative_path=dwh_obs_raster,
            title="DWH deterministic transfer-validation path map",
            figure_group_code="F",
            case_id="CASE_DWH_RETRO_2010_72H",
            phase_or_track="phase3c_external_case_run",
            model_name="opendrift",
            run_type="deterministic_track_map",
            date_token="2010-05-20_to_2010-05-23",
            figure_slug="sampled_particle_paths",
            notes="Generated from the stored DWH deterministic OpenDrift control NetCDF with the public observation event-corridor mask shown as context.",
            sample_count=120,
        )

        copy_specs = [
            {
                "source_relative_path": "output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_obsmask_vs_p50.png",
                "figure_group_code": "D",
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase3b_strict",
                "model_name": "opendrift",
                "run_type": "forecast_vs_observation_overlay",
                "date_token": "2023-03-06",
                "figure_slug": "obsmask_vs_p50",
                "notes": "Existing strict March 6 overlay reused as a panel-ready gallery figure.",
            },
            {
                "source_relative_path": "output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_source_init_validation_overlay.png",
                "figure_group_code": "D",
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase3b_strict",
                "model_name": "opendrift",
                "run_type": "forecast_vs_observation_overlay",
                "date_token": "2023-03-03_to_2023-03-06",
                "figure_slug": "source_init_validation_overlay",
                "notes": "Existing Mindoro validation overlay reused from the stored Phase 3B QA bundle.",
            },
            {
                "source_relative_path": "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/qa_pygnome_public_comparison_overlays.png",
                "figure_group_code": "E",
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase3a_benchmark",
                "model_name": "opendrift_vs_pygnome",
                "run_type": "comparison_overlay",
                "date_token": "2023-03-04_to_2023-03-06",
                "figure_slug": "public_obs_overlays",
                "notes": "Existing Mindoro OpenDrift vs PyGNOME overlay reused from the benchmark comparison outputs.",
            },
            {
                "source_relative_path": "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/qa_pygnome_public_comparison_eventcorridor_overlay.png",
                "figure_group_code": "E",
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase3a_benchmark",
                "model_name": "opendrift_vs_pygnome",
                "run_type": "comparison_overlay",
                "date_token": "2023-03-04_to_2023-03-06",
                "figure_slug": "eventcorridor_overlay",
                "notes": "Existing Mindoro event-corridor comparison map reused from the stored benchmark bundle.",
            },
            {
                "source_relative_path": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/qa_phase3c_ensemble_overlays.png",
                "figure_group_code": "G",
                "case_id": "CASE_DWH_RETRO_2010_72H",
                "phase_or_track": "phase3c_ensemble",
                "model_name": "opendrift",
                "run_type": "ensemble_overlay",
                "date_token": "2010-05-21_to_2010-05-23",
                "figure_slug": "p50_p90_overlays",
                "notes": "Existing DWH ensemble overlay reused from the stored Phase 3C ensemble comparison bundle.",
            },
            {
                "source_relative_path": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/qa_phase3c_ensemble_eventcorridor_overlay.png",
                "figure_group_code": "G",
                "case_id": "CASE_DWH_RETRO_2010_72H",
                "phase_or_track": "phase3c_ensemble",
                "model_name": "opendrift",
                "run_type": "ensemble_overlay",
                "date_token": "2010-05-21_to_2010-05-23",
                "figure_slug": "eventcorridor_overlay",
                "notes": "Existing DWH event-corridor p50/p90 overlay reused from the stored ensemble comparison QA bundle.",
            },
            {
                "source_relative_path": "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/qa_phase3c_dwh_pygnome_overlays.png",
                "figure_group_code": "H",
                "case_id": "CASE_DWH_RETRO_2010_72H",
                "phase_or_track": "phase3c_pygnome_comparator",
                "model_name": "opendrift_vs_pygnome",
                "run_type": "comparison_overlay",
                "date_token": "2010-05-21_to_2010-05-23",
                "figure_slug": "per_date_overlays",
                "notes": "Existing DWH OpenDrift vs PyGNOME per-date overlay reused from the comparator bundle.",
            },
            {
                "source_relative_path": "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/qa_phase3c_dwh_pygnome_eventcorridor_overlay.png",
                "figure_group_code": "H",
                "case_id": "CASE_DWH_RETRO_2010_72H",
                "phase_or_track": "phase3c_pygnome_comparator",
                "model_name": "opendrift_vs_pygnome",
                "run_type": "comparison_overlay",
                "date_token": "2010-05-21_to_2010-05-23",
                "figure_slug": "eventcorridor_overlay",
                "notes": "Existing DWH OpenDrift vs PyGNOME event-corridor comparison reused from the comparator bundle.",
            },
            {
                "source_relative_path": "output/phase4/CASE_MINDORO_RETRO_2023/mass_budget_comparison.png",
                "figure_group_code": "I",
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase4",
                "model_name": "openoil",
                "run_type": "oil_budget_summary",
                "date_token": "2023-03-03_to_2023-03-06",
                "scenario_id": "all_scenarios",
                "figure_slug": "mass_budget_comparison",
                "notes": "Existing Mindoro Phase 4 mass-budget comparison figure reused from the stored Phase 4 bundle.",
            },
            {
                "source_relative_path": "output/phase4/CASE_MINDORO_RETRO_2023/qa_phase4_oiltype_comparison.png",
                "figure_group_code": "I",
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase4",
                "model_name": "openoil",
                "run_type": "oil_type_comparison",
                "date_token": "2023-03-03_to_2023-03-06",
                "scenario_id": "all_scenarios",
                "figure_slug": "oiltype_comparison",
                "notes": "Existing Mindoro Phase 4 oil-type comparison QA figure reused from the stored Phase 4 bundle.",
            },
            {
                "source_relative_path": "output/phase4/CASE_MINDORO_RETRO_2023/qa_phase4_shoreline_impacts.png",
                "figure_group_code": "J",
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase4",
                "model_name": "openoil",
                "run_type": "shoreline_impact_summary",
                "date_token": "2023-03-03_to_2023-03-06",
                "scenario_id": "all_scenarios",
                "figure_slug": "shoreline_impacts",
                "notes": "Existing Mindoro Phase 4 shoreline impact QA figure reused from the stored Phase 4 bundle.",
            },
        ]
        for scenario_id in ("lighter_oil", "fixed_base_medium_heavy_proxy", "heavier_oil"):
            copy_specs.append(
                {
                    "source_relative_path": f"output/phase4/CASE_MINDORO_RETRO_2023/mass_budget_{scenario_id}.png",
                    "figure_group_code": "I",
                    "case_id": "CASE_MINDORO_RETRO_2023",
                    "phase_or_track": "phase4",
                    "model_name": "openoil",
                    "run_type": "oil_budget_timeseries",
                    "date_token": "2023-03-03_to_2023-03-06",
                    "scenario_id": scenario_id,
                    "figure_slug": "mass_budget_timeseries",
                    "notes": "Existing scenario-specific Phase 4 mass-budget figure reused from the stored Phase 4 outputs.",
                }
            )
        for spec in copy_specs:
            self._copy_existing_figure(**spec)

        self._generate_phase4_shoreline_arrival_summary()
        self._generate_phase4_segment_impact_map()

    def _build_figures_index_markdown(self) -> str:
        lines = [
            "# Trajectory Gallery Index",
            "",
            "This gallery is built from existing outputs, manifests, rasters, and NetCDFs only. No expensive scientific branch was rerun to generate these figures.",
            "",
            f"- Gallery root: `{_relative_to_repo(self.repo_root, self.output_dir)}`",
            f"- Figure count: `{len(self.figure_records)}`",
            "",
        ]
        for group_code in FIGURE_GROUPS:
            group = FIGURE_GROUPS[group_code]
            lines.append(f"## {group_code}. {group['label']}")
            lines.append("")
            group_records = sorted([record for record in self.figure_records if record.figure_group_code == group_code], key=lambda item: item.filename)
            if not group_records:
                lines.append("- No figure generated in this build.")
                lines.append("")
                continue
            for record in group_records:
                lines.append(
                    f"- `{record.filename}`: case=`{record.case_id}`, model=`{record.model_name}`, run_type=`{record.run_type}`, date=`{record.date_token}`, scenario=`{record.scenario_id or 'n/a'}`, panel_ready=`true`."
                )
                if record.notes:
                    lines.append(f"  Note: {record.notes}")
            lines.append("")
        if self.missing_optional_artifacts:
            lines.append("## Missing Optional Inputs")
            lines.append("")
            for item in self.missing_optional_artifacts:
                lines.append(f"- `{item['relative_path']}`: {item['notes']}")
            lines.append("")
        return "\n".join(lines)

    def _build_manifest(self, generated_at_utc: str) -> dict[str, Any]:
        group_counts = {code: len([record for record in self.figure_records if record.figure_group_code == code]) for code in FIGURE_GROUPS}
        case_summaries = []
        for case_id in sorted({record.case_id for record in self.figure_records}):
            case_records = [record for record in self.figure_records if record.case_id == case_id]
            case_summaries.append(
                {
                    "case_id": case_id,
                    "figure_count": len(case_records),
                    "figure_groups_generated": sorted({record.figure_group_code for record in case_records}),
                    "models": sorted({record.model_name for record in case_records}),
                    "scenario_ids": sorted({record.scenario_id for record in case_records if record.scenario_id}),
                }
            )
        return {
            "phase": PHASE,
            "generated_at_utc": generated_at_utc,
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "gallery_built_from_existing_outputs_only": True,
            "expensive_scientific_reruns_triggered": False,
            "source_indexes_reused": {
                "final_reproducibility_manifest": _relative_to_repo(self.repo_root, self.repo_root / FINAL_REPRO_MANIFEST_JSON),
                "final_phase_status_registry": _relative_to_repo(self.repo_root, self.repo_root / FINAL_PHASE_STATUS_CSV),
                "final_output_catalog": _relative_to_repo(self.repo_root, self.repo_root / FINAL_OUTPUT_CATALOG_CSV),
                "final_validation_manifest": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
                "phase4_run_manifest": _relative_to_repo(self.repo_root, self.repo_root / PHASE4_MANIFEST_JSON),
            },
            "figure_groups_requested": {code: spec for code, spec in FIGURE_GROUPS.items()},
            "figure_groups_generated": group_counts,
            "case_summaries": case_summaries,
            "panel_ready_group_codes": sorted(self.generated_group_codes),
            "missing_optional_artifacts": self.missing_optional_artifacts,
            "figures": [record.as_row() for record in self.figure_records],
        }

    def run(self) -> dict[str, Any]:
        generated_at_utc = pd.Timestamp.now(tz="UTC").isoformat()
        self._build_figures()
        figure_rows = [record.as_row() for record in sorted(self.figure_records, key=lambda item: item.filename)]
        manifest_path = self.output_dir / "trajectory_gallery_manifest.json"
        index_csv_path = self.output_dir / "trajectory_gallery_index.csv"
        figures_index_md_path = self.output_dir / "figures_index.md"
        _write_csv(
            index_csv_path,
            figure_rows,
            columns=["figure_id", "figure_group_code", "figure_group_id", "figure_group_label", "case_id", "phase_or_track", "model_name", "run_type", "date_token", "scenario_id", "figure_slug", "relative_path", "filename", "generation_mode", "ready_for_panel_presentation", "provisional_context", "source_paths", "notes"],
        )
        _write_text(figures_index_md_path, self._build_figures_index_markdown())
        _write_json(manifest_path, self._build_manifest(generated_at_utc))
        return {
            "output_dir": str(self.output_dir),
            "manifest_path": str(manifest_path),
            "index_csv": str(index_csv_path),
            "figures_index_md": str(figures_index_md_path),
            "figure_count": len(self.figure_records),
            "figure_group_counts": {code: len([record for record in self.figure_records if record.figure_group_code == code]) for code in FIGURE_GROUPS},
            "cases_with_visuals": sorted({record.case_id for record in self.figure_records}),
            "models_with_visuals": sorted({record.model_name for record in self.figure_records}),
            "scenario_ids_with_visuals": sorted({record.scenario_id for record in self.figure_records if record.scenario_id}),
            "missing_optional_artifacts": self.missing_optional_artifacts,
            "figure_rows": figure_rows,
        }


def run_trajectory_gallery_build(repo_root: str | Path = ".", output_dir: str | Path | None = None) -> dict[str, Any]:
    return TrajectoryGalleryBuildService(repo_root=repo_root, output_dir=output_dir).run()
