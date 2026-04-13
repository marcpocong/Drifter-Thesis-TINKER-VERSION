"""Curated final paper figure export for the prototype_2016 legacy bundle."""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import textwrap
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd
import rasterio
from matplotlib import pyplot as plt
from matplotlib.colors import to_rgba
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Polygon, Rectangle
import cartopy.crs as ccrs
from rasterio.transform import from_bounds

from src.helpers.plotting import (
    add_prototype_2016_geoaxes,
    bounds_from_track_dataframe,
    derive_prototype_2016_figure_bounds,
    figure_relative_inset_rect,
    plot_legacy_drifter_track_ensemble_overlay,
    plot_legacy_drifter_track_map,
    PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
    PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
    prototype_2016_rendering_metadata,
)
from src.services.figure_package_publication import (
    STYLE_CONFIG_PATH,
    apply_publication_typography,
    load_publication_style_config,
)
from src.services.prototype_pygnome_similarity_summary import (
    COMPARISON_TRACK_LABELS,
    MODEL_STYLES,
    PrototypePygnomeSimilaritySummaryService,
    REQUIRED_HOURS,
    REQUIRED_WINDOWS_KM,
)
from src.utils.io import load_drifter_data, select_drifter_of_record

matplotlib.use("Agg")

PHASE = "prototype_legacy_final_figures"
OUTPUT_DIR = Path("output") / "2016 Legacy Runs FINAL Figures"
PROTOTYPE_SIMILARITY_DIR = Path("output") / "prototype_2016_pygnome_similarity"
MANIFEST_FILENAME = "final_figure_manifest.json"
MISSING_FIGURES_FILENAME = "missing_figures.csv"
LEGACY_PACKAGE_MANIFEST_FILENAME = "legacy_final_output_manifest.json"
LEGACY_PACKAGE_REGISTRY_CSV_FILENAME = "prototype_2016_final_output_registry.csv"
LEGACY_PACKAGE_REGISTRY_JSON_FILENAME = "prototype_2016_final_output_registry.json"

logger = logging.getLogger(__name__)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8")) or {}


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _utc_now_iso() -> str:
    return pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


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


class PrototypeLegacyFinalFiguresService:
    def __init__(self, repo_root: str | Path = ".", output_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        if output_dir is None:
            self.output_dir = self.repo_root / OUTPUT_DIR
        else:
            resolved_output_dir = Path(output_dir)
            if not resolved_output_dir.is_absolute():
                resolved_output_dir = self.repo_root / resolved_output_dir
            self.output_dir = resolved_output_dir
        self.style = load_publication_style_config(self.repo_root / STYLE_CONFIG_PATH)
        self.font_family = apply_publication_typography(self.style, self.repo_root)
        self.prototype_helper = PrototypePygnomeSimilaritySummaryService(
            repo_root=self.repo_root,
            workflow_mode="prototype_2016",
        )
        self.case_ids = list(self.prototype_helper.case_ids)
        self.prototype_similarity_by_case = _read_csv(
            self.repo_root / PROTOTYPE_SIMILARITY_DIR / "prototype_pygnome_similarity_by_case.csv"
        )
        self.prototype_skipped_cases = _read_csv(
            self.repo_root / PROTOTYPE_SIMILARITY_DIR / "prototype_pygnome_skipped_cases.csv"
        )
        self._case_items: dict[str, dict[str, Any] | None] = {}
        self._case_item_errors: dict[str, BaseException] = {}
        self.figure_rows: list[dict[str, Any]] = []
        self.missing_rows: list[dict[str, Any]] = []
        self.package_registry_rows: list[dict[str, Any]] = []
        self.phase4_registry_rows: list[dict[str, Any]] = []
        self.extent_mode = PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST

    def _single_size(self) -> tuple[float, float]:
        values = (self.style.get("layout") or {}).get("single_size_inches") or [13, 8]
        return float(values[0]), float(values[1])

    def _dpi(self) -> int:
        return int((self.style.get("layout") or {}).get("dpi") or 220)

    def _case_output_dir(self, case_id: str) -> Path:
        return self.repo_root / "output" / case_id

    def _ensemble_dir(self, case_id: str) -> Path:
        return self._case_output_dir(case_id) / "ensemble"

    def _benchmark_dir(self, case_id: str) -> Path:
        return self._case_output_dir(case_id) / "benchmark"

    def _final_case_dir(self, case_id: str) -> Path:
        return self.output_dir / case_id

    def _case_start_utc(self, case_id: str) -> pd.Timestamp:
        token = str(case_id).replace("CASE_", "", 1)
        return pd.Timestamp(f"{token}T00:00:00Z")

    def _case_end_utc(self, case_id: str) -> pd.Timestamp:
        return self._case_start_utc(case_id) + pd.Timedelta(hours=72)

    def _load_case_item(self, case_id: str) -> dict[str, Any] | None:
        if case_id in self._case_items:
            return self._case_items[case_id]
        try:
            item = self.prototype_helper._load_case_artifacts(case_id)
            self._case_items[case_id] = item
            return item
        except Exception as exc:
            self._case_items[case_id] = None
            self._case_item_errors[case_id] = exc
            return None

    def _case_benchmark_skip_message(self, case_id: str) -> str:
        if not self.prototype_skipped_cases.empty:
            subset = self.prototype_skipped_cases[
                self.prototype_skipped_cases["case_id"].astype(str) == str(case_id)
            ]
            if not subset.empty:
                return str(subset.iloc[0].get("error_message") or "").strip()
        if case_id in self._case_item_errors:
            return str(self._case_item_errors[case_id])
        return ""

    def _missing_cause(self, *, category: str, case_id: str) -> str:
        if category == "prototype_similarity":
            return "prototype_similarity_summary_output_missing"
        if category == "phase2":
            return "phase2_output_missing"
        skip_message = self._case_benchmark_skip_message(case_id).lower()
        if "outside the benchmark grid" in skip_message or "skipped" in skip_message:
            return "unsupported_case_geometry_or_benchmark_skip"
        return "benchmark_or_pygnome_output_missing"

    def _record_figure(
        self,
        *,
        case_id: str,
        figure_id: str,
        output_path: Path,
        figure_type: str,
        source_paths: list[Path | str],
        notes: str,
        geometry_render_mode: str | None = None,
        density_render_mode: str | None = None,
        stored_geometry_status: str | None = None,
        extent_mode: str | None = None,
        plot_bounds_wgs84: tuple[float, float, float, float] | list[float] | None = None,
    ) -> None:
        self.figure_rows.append(
            {
                "case_id": case_id,
                "figure_id": figure_id,
                "figure_type": figure_type,
                "file_name": output_path.name,
                "relative_path": _relative_to_repo(self.repo_root, output_path),
                "source_paths": " | ".join(
                    dict.fromkeys(
                        _relative_to_repo(self.repo_root, Path(path))
                        if not isinstance(path, Path)
                        else _relative_to_repo(self.repo_root, path)
                        for path in source_paths
                    )
                ),
                "notes": notes,
                "extent_mode": extent_mode or "",
                "plot_bounds_wgs84": (
                    ",".join(f"{float(value):.4f}" for value in plot_bounds_wgs84)
                    if plot_bounds_wgs84 is not None
                    else ""
                ),
                "geometry_render_mode": geometry_render_mode or "",
                "density_render_mode": density_render_mode or "",
                "stored_geometry_status": stored_geometry_status or "",
                "status": "generated",
            }
        )

    def _record_missing(
        self,
        *,
        case_id: str,
        figure_id: str,
        figure_type: str,
        missing_sources: list[Path | str],
        category: str,
        notes: str,
    ) -> None:
        self.missing_rows.append(
            {
                "case_id": case_id,
                "figure_id": figure_id,
                "figure_type": figure_type,
                "missing_cause": self._missing_cause(category=category, case_id=case_id),
                "missing_sources": " | ".join(
                    dict.fromkeys(
                        _relative_to_repo(self.repo_root, Path(path))
                        if not isinstance(path, Path)
                        else _relative_to_repo(self.repo_root, path)
                        for path in missing_sources
                    )
                ),
                "notes": notes,
            }
        )

    def _drifter_csv_path(self, case_id: str) -> Path:
        return self.repo_root / "data" / "drifters" / case_id / "drifters_noaa.csv"

    def _load_drifter_track(self, case_id: str) -> tuple[dict[str, Any], pd.DataFrame]:
        drifter_csv = self._drifter_csv_path(case_id)
        if not drifter_csv.exists():
            raise FileNotFoundError(f"Missing drifter source file: {drifter_csv}")
        selection = select_drifter_of_record(load_drifter_data(drifter_csv))
        track_df = selection["drifter_df"].copy()
        track_df["time"] = pd.to_datetime(track_df["time"], utc=True, errors="coerce")
        track_df = track_df.dropna(subset=["time", "lat", "lon"]).copy()
        start_time = self._case_start_utc(case_id)
        end_time = self._case_end_utc(case_id)
        track_df = track_df.loc[(track_df["time"] >= start_time) & (track_df["time"] <= end_time)].copy()
        if track_df.empty:
            raise RuntimeError(
                f"The selected drifter-of-record track does not overlap the 72 h case window for {case_id}."
            )
        track_df["time"] = track_df["time"].dt.tz_convert("UTC").dt.tz_localize(None)
        return selection, track_df.sort_values("time").reset_index(drop=True)

    def _source_point(self, case_id: str) -> tuple[float, float]:
        selection, _ = self._load_drifter_track(case_id)
        return float(selection["start_lon"]), float(selection["start_lat"])

    def _display_bounds(self, case_id: str) -> tuple[float, float, float, float]:
        metadata = _read_json(self._ensemble_dir(case_id) / "metadata.json")
        grid = metadata.get("grid") or {}
        display_bounds = (
            metadata.get("display_bounds_wgs84")
            or (metadata.get("figure_rendering") or {}).get("display_bounds_wgs84")
            or grid.get("display_bounds_wgs84")
            or grid.get("extent")
            or []
        )
        if len(display_bounds) == 4:
            return tuple(float(value) for value in display_bounds)
        return tuple(float(value) for value in self.prototype_helper.domain_bounds)

    def _figure_crop_bounds(self, case_id: str) -> tuple[float, float, float, float]:
        item = self._load_case_item(case_id)
        if item is not None:
            return tuple(float(value) for value in item["crop_bounds"])
        return self._display_bounds(case_id)

    def _resolve_figure_bounds(
        self,
        case_id: str,
        *,
        raster_paths: list[Path | str | None] | tuple[Path | str | None, ...] = (),
        trajectory_points: list[tuple[float, float]] | tuple[tuple[float, float], ...] | None = None,
        source_point: tuple[float, float] | None = None,
    ) -> tuple[float, float, float, float]:
        return self.prototype_helper._resolve_plot_bounds(
            base_bounds=self._display_bounds(case_id),
            raster_paths=raster_paths,
            source_point=source_point,
            trajectory_points=trajectory_points,
        )

    def _raster_shape_bounds(self, path: Path) -> tuple[np.ndarray, tuple[float, float, float, float], str]:
        info = self.prototype_helper._load_raster_mask(path)
        return np.asarray(info["array"], dtype=np.float32), tuple(float(v) for v in info["bounds"]), str(info["crs"])

    def _write_temp_raster(
        self,
        path: Path,
        *,
        array: np.ndarray,
        bounds: tuple[float, float, float, float],
        crs: str,
    ) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        height, width = array.shape
        transform = from_bounds(*bounds, width=width, height=height)
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype="float32",
            crs=crs,
            transform=transform,
            nodata=0.0,
        ) as dataset:
            dataset.write(array.astype(np.float32), 1)
        return path

    def _build_composite_raster(
        self,
        paths: list[Path],
        *,
        density_mode: str,
    ) -> tuple[np.ndarray, tuple[float, float, float, float], str]:
        arrays: list[np.ndarray] = []
        bounds: tuple[float, float, float, float] | None = None
        crs: str | None = None
        for path in paths:
            array, current_bounds, current_crs = self._raster_shape_bounds(path)
            arrays.append(array)
            if bounds is None:
                bounds = current_bounds
                crs = current_crs
                continue
            if current_bounds != bounds or current_crs != crs or array.shape != arrays[0].shape:
                raise ValueError("Consolidated prototype rasters require matching grid shape, bounds, and CRS.")
        if not arrays or bounds is None or crs is None:
            raise ValueError("Composite raster build requires at least one input raster.")
        stack = np.stack(arrays, axis=0)
        if density_mode == "max":
            composite = np.nanmax(stack, axis=0)
        else:
            composite = np.clip(np.sum(np.clip(stack, a_min=0.0, a_max=None), axis=0), a_min=0.0, a_max=None)
        return composite.astype(np.float32), bounds, crs

    def _similarity_row(self, case_id: str, comparison_track_id: str) -> pd.Series | None:
        if self.prototype_similarity_by_case.empty:
            return None
        subset = self.prototype_similarity_by_case[
            (self.prototype_similarity_by_case["case_id"].astype(str) == str(case_id))
            & (self.prototype_similarity_by_case["comparison_track_id"].astype(str) == str(comparison_track_id))
        ]
        if subset.empty:
            return None
        return subset.iloc[0]

    def _fss_snapshot_values(self, item: dict[str, Any], *, comparison_track_id: str, hour: int) -> dict[int, float]:
        subset = item["fss_df"][
            (item["fss_df"]["comparison_track_id"].astype(str) == str(comparison_track_id))
            & (item["fss_df"]["hour"].astype(int) == int(hour))
        ].copy()
        if subset.empty:
            raise ValueError(
                f"Missing FSS rows for {item['case_id']} comparison_track_id={comparison_track_id} hour={hour}"
            )
        subset["window_km"] = subset["window_km"].astype(int)
        return {
            int(window): float(
                subset.loc[subset["window_km"] == int(window), "fss"].iloc[0]
            )
            for window in REQUIRED_WINDOWS_KM
        }

    def _mean_fss_values(self, case_id: str, *, comparison_track_id: str) -> dict[int, float]:
        row = self._similarity_row(case_id, comparison_track_id)
        if row is None:
            raise FileNotFoundError(
                "Missing prototype similarity summary row for "
                f"{case_id} comparison_track_id={comparison_track_id}"
            )
        values: dict[int, float] = {}
        for window in REQUIRED_WINDOWS_KM:
            column = f"mean_fss_{int(window)}km"
            values[int(window)] = float(row[column])
        return values

    def _format_values(self, values: dict[int, float]) -> str:
        return "/".join(f"{float(values[int(window)]):.3f}" for window in REQUIRED_WINDOWS_KM)

    def _score_box_lines(self, item: dict[str, Any], *, hour: int, case_id: str) -> list[str]:
        lines = []
        for comparison_track_id in ("ensemble_p50", "ensemble_p90"):
            label = COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id)
            snapshot_values = self._fss_snapshot_values(item, comparison_track_id=comparison_track_id, hour=hour)
            mean_values = self._mean_fss_values(case_id, comparison_track_id=comparison_track_id)
            lines.append(f"{label} FSS 1/3/5/10 km: {self._format_values(snapshot_values)}")
            lines.append(f"{label} mean FSS 1/3/5/10 km: {self._format_values(mean_values)}")
        lines.append(self.prototype_helper._pygnome_forcing_sentence(item))
        return lines

    def _overlay_track_polygons(
        self,
        ax: plt.Axes,
        path: Path,
        *,
        color: str,
        linewidth: float,
        label: str | None = None,
        fill_alpha: float = 0.16,
    ) -> bool:
        info = self.prototype_helper._load_raster_mask(path)
        polygons = info.get("footprint_polygons") or []
        drew_label = False
        if polygons:
            for coordinates in polygons:
                ax.add_patch(
                    Polygon(
                        coordinates,
                        closed=True,
                        facecolor=to_rgba(color, fill_alpha),
                        edgecolor=color,
                        linewidth=linewidth,
                        alpha=0.98,
                        zorder=7,
                        transform=ccrs.PlateCarree(),
                        label=label if not drew_label else None,
                    )
                )
                drew_label = True
            return True
        cell_boxes = info.get("positive_cell_boxes") or []
        if not cell_boxes:
            return False
        for idx, bounds in enumerate(cell_boxes):
            ax.add_patch(
                Rectangle(
                    (bounds[0], bounds[1]),
                    bounds[2] - bounds[0],
                    bounds[3] - bounds[1],
                    facecolor=to_rgba(color, fill_alpha),
                    edgecolor=color,
                    linewidth=linewidth,
                    zorder=7,
                    label=label if idx == 0 else None,
                    transform=ccrs.PlateCarree(),
                )
            )
        return True

    def _figure_frame(
        self,
        figure_title: str,
        subtitle: str,
        *,
        display_bounds: tuple[float, float, float, float],
    ) -> tuple[plt.Figure, plt.Axes]:
        fig = plt.figure(
            figsize=self._single_size(),
            dpi=self._dpi(),
            facecolor=(self.style.get("layout") or {}).get("figure_facecolor", "#ffffff"),
        )
        ax = add_prototype_2016_geoaxes(
            fig,
            [0.07, 0.12, 0.74, 0.76],
            display_bounds,
            show_grid_labels=True,
            add_north_arrow=True,
        )
        fig.suptitle(
            figure_title,
            x=0.07,
            y=0.965,
            ha="left",
            fontsize=float((self.style.get("typography") or {}).get("title_size") or 19),
            fontweight="bold",
        )
        fig.text(
            0.07,
            0.932,
            subtitle,
            ha="left",
            va="top",
            fontsize=float((self.style.get("typography") or {}).get("subtitle_size") or 10),
            color="#475569",
        )
        return fig, ax

    def _draw_side_panel(self, ax: plt.Axes, title: str, lines: list[str]) -> None:
        ax.axis("off")
        wrapped_lines = [textwrap.fill(str(line), width=28) for line in lines if str(line).strip()]
        ax.text(
            0.0,
            0.98,
            title,
            ha="left",
            va="top",
            fontsize=10.0,
            fontweight="bold",
            color="#0f172a",
            transform=ax.transAxes,
        )
        ax.text(
            0.0,
            0.92,
            "\n".join(wrapped_lines),
            ha="left",
            va="top",
            fontsize=8.4,
            color="#334155",
            transform=ax.transAxes,
            bbox={"boxstyle": "round,pad=0.42", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
        )

    def _phase2_source_paths(self, case_id: str, hour: int) -> dict[str, Path]:
        ensemble_dir = self._ensemble_dir(case_id)
        return {
            "probability": ensemble_dir / f"probability_{hour}h.tif",
            "p50": ensemble_dir / f"mask_p50_{hour}h.tif",
            "p90": ensemble_dir / f"mask_p90_{hour}h.tif",
        }

    def _require_paths(self, *paths: Path) -> None:
        missing = [str(path) for path in paths if not Path(path).exists()]
        if missing:
            raise FileNotFoundError("Missing required figure source(s): " + "; ".join(missing))

    def _phase2_note_lines(
        self,
        *,
        case_id: str,
        hour: int,
        consolidated: bool,
        probability_drawn: bool,
        p50_drawn: bool,
        p90_drawn: bool,
    ) -> list[str]:
        case_label = case_id.replace("CASE_", "")
        if consolidated:
            first_line = (
                f"{case_label} legacy support case. This consolidated panel redraws the stored 72 h forecast raster and footprint masks."
            )
        else:
            first_line = f"{case_label} legacy support case at T+{int(hour)} h."
        lines = [
            first_line,
            "Stored member-occupancy probability raster cells and exact p50/p90 occupancy footprint geometry are rendered directly. Empty stored layers are omitted.",
            "p50/p90 are exact valid-time member-occupancy footprints, not pooled-particle-density thresholds and not cumulative corridors.",
            "The drifter-of-record start point remains the authoritative prototype_2016 release reference.",
            "Legacy/debug support only; not final Chapter 3 evidence.",
        ]
        omitted_layers: list[str] = []
        if not probability_drawn:
            omitted_layers.append("member-occupancy probability raster")
        if not p50_drawn:
            omitted_layers.append("p50 mask")
        if not p90_drawn:
            omitted_layers.append("p90 mask")
        if omitted_layers:
            lines.insert(2, "Empty stored layer(s) omitted: " + ", ".join(omitted_layers) + ".")
        return lines

    def _pygnome_note_lines(self, item: dict[str, Any], *, hour: int) -> list[str]:
        return [
            f"{item['case_id'].replace('CASE_', '')} legacy support case at T+{int(hour)} h.",
            *self.prototype_helper._single_note_lines(item, hour, "pygnome"),
            "Legacy/debug support only; not final Chapter 3 evidence.",
        ]

    def _phase2_legend_handles(
        self,
        *,
        show_probability_cells: bool,
        show_p50: bool,
        show_p90: bool,
    ) -> list[Any]:
        handles: list[Any] = [
            Line2D(
                [0],
                [0],
                marker="*",
                linestyle="None",
                markerfacecolor="#1d9b1d",
                markeredgecolor="#111827",
                markersize=13,
                label="Drifter-of-record release point",
            )
        ]
        if show_probability_cells:
            handles.append(
                Patch(
                    facecolor=(0.545, 0.847, 0.804, 0.32),
                    edgecolor="none",
                    label="Stored member-occupancy probability cells",
                )
            )
        if show_p50:
            handles.append(
                Line2D(
                    [0],
                    [0],
                    color=MODEL_STYLES["opendrift_p50"]["color"],
                    linewidth=1.7,
                    label="Ensemble p50 footprint",
                )
            )
        if show_p90:
            handles.append(
                Line2D(
                    [0],
                    [0],
                    color=MODEL_STYLES["opendrift_p90"]["color"],
                    linewidth=1.7,
                    label="Ensemble p90 footprint",
                )
            )
        return handles

    def _pygnome_vs_ensemble_legend_handles(
        self,
        *,
        show_pygnome_cells: bool,
        show_pygnome_outline: bool,
        show_p50: bool,
        show_p90: bool,
    ) -> list[Any]:
        return [
            *self.prototype_helper._single_legend_handles(
                "pygnome",
                include_source_point=True,
                show_raster_cells=show_pygnome_cells,
                show_outline=show_pygnome_outline,
            ),
            *(
                [
                    Line2D(
                        [0],
                        [0],
                        color=MODEL_STYLES["opendrift_p50"]["color"],
                        linewidth=1.7,
                        label="Ensemble p50 footprint",
                    )
                ]
                if show_p50
                else []
            ),
            *(
                [
                    Line2D(
                        [0],
                        [0],
                        color=MODEL_STYLES["opendrift_p90"]["color"],
                        linewidth=1.7,
                        label="Ensemble p90 footprint",
                    )
                ]
                if show_p90
                else []
            ),
        ]

    def _render_phase2_probability_figure(
        self,
        *,
        case_id: str,
        hour: int,
        output_path: Path,
        title_suffix: str,
    ) -> dict[str, str]:
        source_paths = self._phase2_source_paths(case_id, hour)
        self._require_paths(source_paths["probability"], source_paths["p50"], source_paths["p90"])

        source_point = self._source_point(case_id)
        plot_bounds = self._resolve_figure_bounds(
            case_id,
            raster_paths=[
                source_paths["probability"],
                source_paths["p50"],
                source_paths["p90"],
            ],
            source_point=source_point,
        )
        case_label = case_id.replace("CASE_", "")
        fig, ax = self._figure_frame(
            f"{case_label} | {title_suffix}",
            "Prototype 2016 legacy support export | Arial publication styling | exact stored member-occupancy probability cells and mask geometry",
            display_bounds=plot_bounds,
        )
        side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])

        render_info = self.prototype_helper._render_model_footprint(
            ax,
            raster_path=source_paths["p50"],
            display_raster_path=source_paths["probability"],
            crop_bounds=plot_bounds,
            model_name="opendrift_p50",
            panel_title=f"{int(hour)} h ensemble support footprint",
            source_point=source_point,
        )
        p90_drawn = self._overlay_track_polygons(
            ax,
            source_paths["p90"],
            color=MODEL_STYLES["opendrift_p90"]["color"],
            linewidth=1.7,
            label="Ensemble p90 footprint",
        )
        locator_ax = add_prototype_2016_geoaxes(
            fig,
            figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
            self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
            show_grid_labels=False,
            add_scale_bar=False,
            add_north_arrow=False,
        )
        self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
        ax.legend(
            handles=self._phase2_legend_handles(
                show_probability_cells=(render_info["density_render_mode"] == "direct_raster"),
                show_p50=(render_info["stored_geometry_status"] == "nonempty"),
                show_p90=p90_drawn,
            ),
            loc="upper center",
            bbox_to_anchor=(0.5, -0.09),
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=8.3,
            ncol=2,
        )
        self._draw_side_panel(
            side_ax,
            "Interpretation",
            self._phase2_note_lines(
                case_id=case_id,
                hour=hour,
                consolidated=("consolidated" in title_suffix.lower()),
                probability_drawn=(render_info["density_render_mode"] == "direct_raster"),
                p50_drawn=(render_info["stored_geometry_status"] == "nonempty"),
                p90_drawn=p90_drawn,
            ),
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return {
            "geometry_render_mode": render_info["geometry_render_mode"],
            "density_render_mode": render_info["density_render_mode"],
            "stored_geometry_status": (
                "nonempty" if (render_info["stored_geometry_status"] == "nonempty" or p90_drawn) else "empty_stored_artifact"
            ),
            "extent_mode": self.extent_mode,
            "plot_bounds_wgs84": plot_bounds,
        }

    def _render_pygnome_single(self, *, case_id: str, hour: int, output_path: Path) -> dict[str, Any]:
        item = self._load_case_item(case_id)
        if item is None:
            raise RuntimeError(self._case_benchmark_skip_message(case_id) or f"Benchmark artifacts unavailable for {case_id}.")
        pair_row = item["pairings_by_hour"]["deterministic"][int(hour)]
        pygnome_footprint = Path(pair_row["pygnome_footprint_path_resolved"])
        pygnome_density = pair_row.get("pygnome_density_path_resolved")
        self._require_paths(pygnome_footprint)
        if pygnome_density:
            self._require_paths(Path(pygnome_density))

        source_point = self._source_point(case_id)
        plot_bounds = self._resolve_figure_bounds(
            case_id,
            raster_paths=[
                pygnome_footprint,
                Path(pygnome_density) if pygnome_density else None,
            ],
            source_point=source_point,
        )
        case_label = case_id.replace("CASE_", "")
        fig, ax = self._figure_frame(
            f"{case_label} | PyGNOME comparator {int(hour)} h",
            "Prototype 2016 legacy support export | deterministic PyGNOME comparator using exact stored raster geometry",
            display_bounds=plot_bounds,
        )
        side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])
        render_info = self.prototype_helper._render_model_footprint(
            ax,
            raster_path=pygnome_footprint,
            display_raster_path=Path(pygnome_density) if pygnome_density else None,
            crop_bounds=plot_bounds,
            model_name="pygnome",
            panel_title=f"{int(hour)} h deterministic PyGNOME footprint",
            source_point=source_point,
        )
        locator_ax = add_prototype_2016_geoaxes(
            fig,
            figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
            self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
            show_grid_labels=False,
            add_scale_bar=False,
            add_north_arrow=False,
        )
        self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
        ax.legend(
            handles=self.prototype_helper._single_legend_handles(
                "pygnome",
                include_source_point=source_point is not None,
                show_raster_cells=(render_info["density_render_mode"] == "direct_raster"),
                show_outline=(render_info["stored_geometry_status"] == "nonempty"),
            ),
            loc="upper center",
            bbox_to_anchor=(0.5, -0.09),
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=8.3,
            ncol=2,
        )
        self._draw_side_panel(side_ax, "Interpretation", self._pygnome_note_lines(item, hour=hour))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        render_info["extent_mode"] = self.extent_mode
        render_info["plot_bounds_wgs84"] = plot_bounds
        return render_info

    def _render_pygnome_vs_ensemble(
        self,
        *,
        case_id: str,
        hour: int,
        output_path: Path,
        pygnome_footprint_path: Path,
        pygnome_density_path: Path,
        p50_path: Path,
        p90_path: Path,
        note_suffix: str,
    ) -> dict[str, str]:
        item = self._load_case_item(case_id)
        if item is None:
            raise RuntimeError(self._case_benchmark_skip_message(case_id) or f"Benchmark artifacts unavailable for {case_id}.")
        self._require_paths(pygnome_footprint_path, p50_path, p90_path)
        if pygnome_density_path:
            self._require_paths(pygnome_density_path)

        hour_token = int("".join(char for char in str(note_suffix) if char.isdigit()) or "72")
        source_point = self._source_point(case_id)
        plot_bounds = self._resolve_figure_bounds(
            case_id,
            raster_paths=[
                pygnome_footprint_path,
                pygnome_density_path,
                p50_path,
                p90_path,
            ],
            source_point=source_point,
        )
        case_label = case_id.replace("CASE_", "")
        fig, ax = self._figure_frame(
            f"{case_label} | PyGNOME vs ensemble {note_suffix}",
            "Prototype 2016 legacy support export | deterministic PyGNOME over exact stored ensemble p50/p90 occupancy footprint geometry",
            display_bounds=plot_bounds,
        )
        side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])
        render_info = self.prototype_helper._render_model_footprint(
            ax,
            raster_path=pygnome_footprint_path,
            display_raster_path=pygnome_density_path,
            crop_bounds=plot_bounds,
            model_name="pygnome",
            panel_title=f"PyGNOME comparator vs ensemble footprints ({note_suffix})",
            source_point=source_point,
        )
        p50_drawn = self._overlay_track_polygons(
            ax,
            p50_path,
            color=MODEL_STYLES["opendrift_p50"]["color"],
            linewidth=1.6,
            label="Ensemble p50 footprint",
        )
        p90_drawn = self._overlay_track_polygons(
            ax,
            p90_path,
            color=MODEL_STYLES["opendrift_p90"]["color"],
            linewidth=1.8,
            label="Ensemble p90 footprint",
        )
        locator_ax = add_prototype_2016_geoaxes(
            fig,
            figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
            self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
            show_grid_labels=False,
            add_scale_bar=False,
            add_north_arrow=False,
        )
        self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
        ax.legend(
            handles=self._pygnome_vs_ensemble_legend_handles(
                show_pygnome_cells=(render_info["density_render_mode"] == "direct_raster"),
                show_pygnome_outline=(render_info["stored_geometry_status"] == "nonempty"),
                show_p50=p50_drawn,
                show_p90=p90_drawn,
            ),
            loc="upper center",
            bbox_to_anchor=(0.5, -0.09),
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=8.0,
            ncol=2,
        )
        self._draw_side_panel(
            side_ax,
            "FSS summary",
            self._score_box_lines(item, hour=hour_token, case_id=case_id),
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return {
            "geometry_render_mode": "exact_stored_raster",
            "density_render_mode": render_info["density_render_mode"],
            "stored_geometry_status": (
                "nonempty"
                if (render_info["stored_geometry_status"] == "nonempty" or p50_drawn or p90_drawn)
                else "empty_stored_artifact"
            ),
            "extent_mode": self.extent_mode,
            "plot_bounds_wgs84": plot_bounds,
        }

    def _render_consolidated_pygnome_products(self, *, case_id: str, case_dir: Path) -> None:
        item = self._load_case_item(case_id)
        if item is None:
            raise RuntimeError(self._case_benchmark_skip_message(case_id) or f"Benchmark artifacts unavailable for {case_id}.")

        pygnome_footprints = [
            Path(item["pairings_by_hour"]["deterministic"][hour]["pygnome_footprint_path_resolved"])
            for hour in REQUIRED_HOURS
        ]
        pygnome_density_paths = [
            Path(item["pairings_by_hour"]["deterministic"][hour]["pygnome_density_path_resolved"])
            for hour in REQUIRED_HOURS
            if item["pairings_by_hour"]["deterministic"][hour].get("pygnome_density_path_resolved")
        ]
        p50_paths = [
            Path(item["pairings_by_hour"]["ensemble_p50"][hour]["opendrift_footprint_path_resolved"])
            for hour in REQUIRED_HOURS
        ]
        p90_paths = [
            Path(item["pairings_by_hour"]["ensemble_p90"][hour]["opendrift_footprint_path_resolved"])
            for hour in REQUIRED_HOURS
        ]
        for path_group in (pygnome_footprints, p50_paths, p90_paths):
            self._require_paths(*path_group)
        if pygnome_density_paths:
            self._require_paths(*pygnome_density_paths)

        with tempfile.TemporaryDirectory(prefix=f"{case_id.lower()}_legacy_final_") as tmpdir:
            tmp_root = Path(tmpdir)
            pygnome_footprint_array, pygnome_bounds, pygnome_crs = self._build_composite_raster(
                pygnome_footprints,
                density_mode="sum",
            )
            pygnome_density_array, _, _ = self._build_composite_raster(
                pygnome_density_paths or pygnome_footprints,
                density_mode="max",
            )
            p50_array, p50_bounds, p50_crs = self._build_composite_raster(p50_paths, density_mode="sum")
            p90_array, p90_bounds, p90_crs = self._build_composite_raster(p90_paths, density_mode="sum")

            pygnome_footprint_tmp = self._write_temp_raster(
                tmp_root / "pygnome_footprint_72h.tif",
                array=pygnome_footprint_array,
                bounds=pygnome_bounds,
                crs=pygnome_crs,
            )
            pygnome_density_tmp = self._write_temp_raster(
                tmp_root / "pygnome_density_72h.tif",
                array=pygnome_density_array,
                bounds=pygnome_bounds,
                crs=pygnome_crs,
            )
            p50_tmp = self._write_temp_raster(
                tmp_root / "ensemble_p50_72h.tif",
                array=p50_array,
                bounds=p50_bounds,
                crs=p50_crs,
            )
            p90_tmp = self._write_temp_raster(
                tmp_root / "ensemble_p90_72h.tif",
                array=p90_array,
                bounds=p90_bounds,
                crs=p90_crs,
            )

            pygnome_single_output = case_dir / "pygnome_consolidated_72h.png"
            source_point = self._source_point(case_id)
            plot_bounds = self._resolve_figure_bounds(
                case_id,
                raster_paths=[pygnome_footprint_tmp, pygnome_density_tmp],
                source_point=source_point,
            )
            case_label = case_id.replace("CASE_", "")
            fig, ax = self._figure_frame(
                f"{case_label} | Consolidated PyGNOME comparator 72 h",
                "Prototype 2016 legacy support export | composite from stored 24/48/72 PyGNOME artifacts only",
                display_bounds=plot_bounds,
            )
            side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])
            render_info = self.prototype_helper._render_model_footprint(
                ax,
                raster_path=pygnome_footprint_tmp,
                display_raster_path=pygnome_density_tmp,
                crop_bounds=plot_bounds,
                model_name="pygnome",
                panel_title="Consolidated deterministic PyGNOME comparator footprint",
                source_point=source_point,
            )
            locator_ax = add_prototype_2016_geoaxes(
                fig,
                figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
                self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
                show_grid_labels=False,
                add_scale_bar=False,
                add_north_arrow=False,
            )
            self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
            ax.legend(
                handles=self.prototype_helper._single_legend_handles(
                    "pygnome",
                    include_source_point=source_point is not None,
                    show_raster_cells=(render_info["density_render_mode"] == "direct_raster"),
                    show_outline=(render_info["stored_geometry_status"] == "nonempty"),
                ),
                loc="upper center",
                bbox_to_anchor=(0.5, -0.09),
                frameon=True,
                framealpha=0.98,
                facecolor="#ffffff",
                edgecolor="#cbd5e1",
                fontsize=8.3,
                ncol=2,
            )
            self._draw_side_panel(
                side_ax,
                "Interpretation",
                [
                    f"{case_label} legacy support case. This consolidated PyGNOME panel unions the stored 24/48/72 comparator snapshots.",
                    "Stored PyGNOME raster cells and exact footprint outlines are rendered directly. Empty stored layers are omitted.",
                    self.prototype_helper._pygnome_forcing_sentence(item),
                    "Legacy/debug support only; not final Chapter 3 evidence.",
                ],
            )
            pygnome_single_output.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(pygnome_single_output, dpi=self._dpi())
            plt.close(fig)
            self._record_figure(
                case_id=case_id,
                figure_id="pygnome_consolidated_72h",
                output_path=pygnome_single_output,
                figure_type="pygnome_consolidated_map",
                source_paths=[*pygnome_footprints, *pygnome_density_paths],
                notes=(
                    "Consolidated deterministic PyGNOME comparator map built from stored 24/48/72 benchmark artifacts only. "
                    + self.prototype_helper._pygnome_forcing_sentence(item)
                ),
                geometry_render_mode=render_info["geometry_render_mode"],
                density_render_mode=render_info["density_render_mode"],
                stored_geometry_status=render_info["stored_geometry_status"],
                extent_mode=self.extent_mode,
                plot_bounds_wgs84=plot_bounds,
            )

            comparator_output = case_dir / "pygnome_vs_ensemble_consolidated_72h.png"
            comparator_render_info = self._render_pygnome_vs_ensemble(
                case_id=case_id,
                hour=72,
                output_path=comparator_output,
                pygnome_footprint_path=pygnome_footprint_tmp,
                pygnome_density_path=pygnome_density_tmp,
                p50_path=p50_tmp,
                p90_path=p90_tmp,
                note_suffix="consolidated 72 h",
            )
            self._record_figure(
                case_id=case_id,
                figure_id="pygnome_vs_ensemble_consolidated_72h",
                output_path=comparator_output,
                figure_type="pygnome_vs_ensemble_consolidated_overlay",
                source_paths=[*pygnome_footprints, *pygnome_density_paths, *p50_paths, *p90_paths],
                notes=(
                    "Consolidated deterministic PyGNOME vs ensemble p50/p90 occupancy overlay using stored 24/48/72 benchmark artifacts only. "
                    + self.prototype_helper._pygnome_forcing_sentence(item)
                ),
                geometry_render_mode=comparator_render_info["geometry_render_mode"],
                density_render_mode=comparator_render_info["density_render_mode"],
                stored_geometry_status=comparator_render_info["stored_geometry_status"],
                extent_mode=comparator_render_info.get("extent_mode"),
                plot_bounds_wgs84=comparator_render_info.get("plot_bounds_wgs84"),
            )

    def _export_case(self, case_id: str) -> None:
        case_dir = self._final_case_dir(case_id)
        case_dir.mkdir(parents=True, exist_ok=True)

        try:
            _, drifter_track_df = self._load_drifter_track(case_id)
            source_point = self._source_point(case_id)
            drifter_plot_bounds = self._resolve_figure_bounds(
                case_id,
                trajectory_points=list(zip(drifter_track_df["lon"], drifter_track_df["lat"])),
                source_point=source_point,
            )
            drifter_output = case_dir / "drifter_track_72h.png"
            plot_legacy_drifter_track_map(
                output_file=str(drifter_output),
                drifter_track_df=drifter_track_df,
                corners=list(self._display_bounds(case_id)),
                title=f"{case_id.replace('CASE_', '')} observed drifter-of-record track (72 h legacy support case)",
                extent_mode=self.extent_mode,
                locator_bounds=self._display_bounds(case_id),
            )
            self._record_figure(
                case_id=case_id,
                figure_id="drifter_track_72h",
                output_path=drifter_output,
                figure_type="drifter_track_map",
                source_paths=[self._drifter_csv_path(case_id)],
                notes="Observed drifter-of-record track over the 72 h prototype_2016 case window.",
                geometry_render_mode="observed_track_line",
                density_render_mode="not_applicable",
                stored_geometry_status="nonempty",
                extent_mode=self.extent_mode,
                plot_bounds_wgs84=drifter_plot_bounds,
            )
        except Exception as exc:
            logger.warning("Unable to export drifter-track final figure for %s: %s", case_id, exc)
            self._record_missing(
                case_id=case_id,
                figure_id="drifter_track_72h",
                figure_type="drifter_track_map",
                missing_sources=[self._drifter_csv_path(case_id)],
                category="phase2",
                notes=str(exc),
            )
            drifter_track_df = None

        p50_72h = self._ensemble_dir(case_id) / "mask_p50_72h.tif"
        p90_72h = self._ensemble_dir(case_id) / "mask_p90_72h.tif"
        if drifter_track_df is not None:
            try:
                self._require_paths(p50_72h, p90_72h)
                overlay_output = case_dir / "drifter_vs_ensemble_72h.png"
                plot_legacy_drifter_track_ensemble_overlay(
                    output_file=str(overlay_output),
                    drifter_track_df=drifter_track_df,
                    p50_mask_path=str(p50_72h),
                    p90_mask_path=str(p90_72h),
                    corners=list(self._display_bounds(case_id)),
                    title=f"{case_id.replace('CASE_', '')} drifter vs ensemble footprints (72 h legacy support case)",
                    extent_mode=self.extent_mode,
                    locator_bounds=self._display_bounds(case_id),
                )
                overlay_plot_bounds = self._resolve_figure_bounds(
                    case_id,
                    raster_paths=[p50_72h, p90_72h],
                    trajectory_points=list(zip(drifter_track_df["lon"], drifter_track_df["lat"])),
                    source_point=self._source_point(case_id),
                )
                self._record_figure(
                    case_id=case_id,
                    figure_id="drifter_vs_ensemble_72h",
                    output_path=overlay_output,
                    figure_type="drifter_track_ensemble_overlay",
                    source_paths=[self._drifter_csv_path(case_id), p50_72h, p90_72h],
                    notes="Observed drifter-of-record track overlaid on the stored 72 h ensemble p50/p90 member-occupancy footprints.",
                    geometry_render_mode="observed_track_line_plus_exact_stored_raster",
                    density_render_mode="not_applicable",
                    stored_geometry_status="mixed_nonempty_or_empty_stored_artifacts",
                    extent_mode=self.extent_mode,
                    plot_bounds_wgs84=overlay_plot_bounds,
                )
            except Exception as exc:
                logger.warning("Unable to export drifter-vs-ensemble final figure for %s: %s", case_id, exc)
                self._record_missing(
                    case_id=case_id,
                    figure_id="drifter_vs_ensemble_72h",
                    figure_type="drifter_track_ensemble_overlay",
                    missing_sources=[self._drifter_csv_path(case_id), p50_72h, p90_72h],
                    category="phase2",
                    notes=str(exc),
                )

        for hour in REQUIRED_HOURS:
            figure_id = f"ensemble_probability_{int(hour)}h"
            output_path = case_dir / f"{figure_id}.png"
            phase2_paths = self._phase2_source_paths(case_id, int(hour))
            try:
                render_info = self._render_phase2_probability_figure(
                    case_id=case_id,
                    hour=int(hour),
                    output_path=output_path,
                    title_suffix=f"Ensemble member-occupancy footprint {int(hour)} h",
                )
                self._record_figure(
                    case_id=case_id,
                    figure_id=figure_id,
                    output_path=output_path,
                    figure_type="ensemble_probability_map",
                    source_paths=list(phase2_paths.values()),
                    notes=f"Redrawn stored prototype_2016 ensemble member-occupancy support footprint for T+{int(hour)} h.",
                    geometry_render_mode=render_info["geometry_render_mode"],
                    density_render_mode=render_info["density_render_mode"],
                    stored_geometry_status=render_info["stored_geometry_status"],
                    extent_mode=render_info.get("extent_mode"),
                    plot_bounds_wgs84=render_info.get("plot_bounds_wgs84"),
                )
            except Exception as exc:
                logger.warning("Unable to export %s for %s: %s", figure_id, case_id, exc)
                self._record_missing(
                    case_id=case_id,
                    figure_id=figure_id,
                    figure_type="ensemble_probability_map",
                    missing_sources=list(phase2_paths.values()),
                    category="phase2",
                    notes=str(exc),
                )

        try:
            consolidated_output = case_dir / "ensemble_consolidated_72h.png"
            consolidated_render_info = self._render_phase2_probability_figure(
                case_id=case_id,
                hour=72,
                output_path=consolidated_output,
                title_suffix="Consolidated ensemble member-occupancy footprint 72 h",
            )
            self._record_figure(
                case_id=case_id,
                figure_id="ensemble_consolidated_72h",
                output_path=consolidated_output,
                figure_type="ensemble_consolidated_map",
                source_paths=list(self._phase2_source_paths(case_id, 72).values()),
                notes="Paper-style consolidated 72 h ensemble member-occupancy support footprint using the stored prototype legacy 72 h support products.",
                geometry_render_mode=consolidated_render_info["geometry_render_mode"],
                density_render_mode=consolidated_render_info["density_render_mode"],
                stored_geometry_status=consolidated_render_info["stored_geometry_status"],
                extent_mode=consolidated_render_info.get("extent_mode"),
                plot_bounds_wgs84=consolidated_render_info.get("plot_bounds_wgs84"),
            )
        except Exception as exc:
            logger.warning("Unable to export consolidated ensemble figure for %s: %s", case_id, exc)
            self._record_missing(
                case_id=case_id,
                figure_id="ensemble_consolidated_72h",
                figure_type="ensemble_consolidated_map",
                missing_sources=list(self._phase2_source_paths(case_id, 72).values()),
                category="phase2",
                notes=str(exc),
            )

        for hour in REQUIRED_HOURS:
            pygnome_output = case_dir / f"pygnome_{int(hour)}h.png"
            item = self._load_case_item(case_id)
            if item is None:
                missing_sources: list[Path | str] = [self._benchmark_dir(case_id) / "phase3a_pairing_manifest.csv"]
            else:
                pair_row = item["pairings_by_hour"]["deterministic"][int(hour)]
                missing_sources = [
                    Path(pair_row["pygnome_footprint_path_resolved"]),
                    Path(pair_row["pygnome_density_path_resolved"])
                    if pair_row.get("pygnome_density_path_resolved")
                    else self._benchmark_dir(case_id) / "pygnome",
                ]
            try:
                render_info = self._render_pygnome_single(case_id=case_id, hour=int(hour), output_path=pygnome_output)
                self._record_figure(
                    case_id=case_id,
                    figure_id=f"pygnome_{int(hour)}h",
                    output_path=pygnome_output,
                    figure_type="pygnome_single_map",
                    source_paths=missing_sources,
                    notes=(
                        f"Deterministic PyGNOME comparator export for T+{int(hour)} h. "
                        + self.prototype_helper._pygnome_forcing_sentence(item)
                    ),
                    geometry_render_mode=render_info["geometry_render_mode"],
                    density_render_mode=render_info["density_render_mode"],
                    stored_geometry_status=render_info["stored_geometry_status"],
                    extent_mode=render_info.get("extent_mode"),
                    plot_bounds_wgs84=render_info.get("plot_bounds_wgs84"),
                )
            except Exception as exc:
                logger.warning("Unable to export PyGNOME single figure for %s hour %s: %s", case_id, hour, exc)
                self._record_missing(
                    case_id=case_id,
                    figure_id=f"pygnome_{int(hour)}h",
                    figure_type="pygnome_single_map",
                    missing_sources=missing_sources,
                    category="benchmark",
                    notes=str(exc),
                )

        for hour in REQUIRED_HOURS:
            item = self._load_case_item(case_id)
            if item is None:
                self._record_missing(
                    case_id=case_id,
                    figure_id=f"pygnome_vs_ensemble_{int(hour)}h",
                    figure_type="pygnome_vs_ensemble_overlay",
                    missing_sources=[self._benchmark_dir(case_id) / "phase3a_pairing_manifest.csv"],
                    category="benchmark",
                    notes=self._case_benchmark_skip_message(case_id) or f"Benchmark artifacts unavailable for {case_id}.",
                )
                continue
            try:
                py_row = item["pairings_by_hour"]["deterministic"][int(hour)]
                p50_row = item["pairings_by_hour"]["ensemble_p50"][int(hour)]
                p90_row = item["pairings_by_hour"]["ensemble_p90"][int(hour)]
                output_path = case_dir / f"pygnome_vs_ensemble_{int(hour)}h.png"
                render_info = self._render_pygnome_vs_ensemble(
                    case_id=case_id,
                    hour=int(hour),
                    output_path=output_path,
                    pygnome_footprint_path=Path(py_row["pygnome_footprint_path_resolved"]),
                    pygnome_density_path=Path(py_row["pygnome_density_path_resolved"])
                    if py_row.get("pygnome_density_path_resolved")
                    else None,
                    p50_path=Path(p50_row["opendrift_footprint_path_resolved"]),
                    p90_path=Path(p90_row["opendrift_footprint_path_resolved"]),
                    note_suffix=f"{int(hour)} h",
                )
                self._record_figure(
                    case_id=case_id,
                    figure_id=f"pygnome_vs_ensemble_{int(hour)}h",
                    output_path=output_path,
                    figure_type="pygnome_vs_ensemble_overlay",
                    source_paths=[
                        Path(py_row["pygnome_footprint_path_resolved"]),
                        Path(py_row["pygnome_density_path_resolved"])
                        if py_row.get("pygnome_density_path_resolved")
                        else Path(py_row["pygnome_footprint_path_resolved"]),
                        Path(p50_row["opendrift_footprint_path_resolved"]),
                        Path(p90_row["opendrift_footprint_path_resolved"]),
                    ],
                    notes=(
                        f"Per-hour deterministic PyGNOME vs ensemble p50/p90 occupancy overlay for T+{int(hour)} h with snapshot and mean FSS notes. "
                        + self.prototype_helper._pygnome_forcing_sentence(item)
                    ),
                    geometry_render_mode=render_info["geometry_render_mode"],
                    density_render_mode=render_info["density_render_mode"],
                    stored_geometry_status=render_info["stored_geometry_status"],
                    extent_mode=render_info.get("extent_mode"),
                    plot_bounds_wgs84=render_info.get("plot_bounds_wgs84"),
                )
            except Exception as exc:
                logger.warning("Unable to export PyGNOME-vs-ensemble figure for %s hour %s: %s", case_id, hour, exc)
                self._record_missing(
                    case_id=case_id,
                    figure_id=f"pygnome_vs_ensemble_{int(hour)}h",
                    figure_type="pygnome_vs_ensemble_overlay",
                    missing_sources=[
                        self._benchmark_dir(case_id) / "phase3a_pairing_manifest.csv",
                        self._benchmark_dir(case_id) / "phase3a_fss_by_time_window.csv",
                        self.repo_root / PROTOTYPE_SIMILARITY_DIR / "prototype_pygnome_similarity_by_case.csv",
                    ],
                    category="benchmark",
                    notes=str(exc),
                )

        try:
            self._render_consolidated_pygnome_products(case_id=case_id, case_dir=case_dir)
        except Exception as exc:
            logger.warning("Unable to export consolidated PyGNOME products for %s: %s", case_id, exc)
            self._record_missing(
                case_id=case_id,
                figure_id="pygnome_consolidated_72h",
                figure_type="pygnome_consolidated_map",
                missing_sources=[
                    self._benchmark_dir(case_id) / "phase3a_pairing_manifest.csv",
                    self.repo_root / PROTOTYPE_SIMILARITY_DIR / "prototype_pygnome_similarity_by_case.csv",
                ],
                category="benchmark",
                notes=str(exc),
            )
            self._record_missing(
                case_id=case_id,
                figure_id="pygnome_vs_ensemble_consolidated_72h",
                figure_type="pygnome_vs_ensemble_consolidated_overlay",
                missing_sources=[
                    self._benchmark_dir(case_id) / "phase3a_pairing_manifest.csv",
                    self.repo_root / PROTOTYPE_SIMILARITY_DIR / "prototype_pygnome_similarity_by_case.csv",
                ],
                category="benchmark",
                notes=str(exc),
            )

    def _weathering_dir(self, case_id: str) -> Path:
        return self._case_output_dir(case_id) / "weathering"

    def _case_slug(self, case_id: str) -> str:
        return str(case_id).lower().replace("-", "_")

    def _package_subdir(self, *parts: str) -> Path:
        return self.output_dir.joinpath(*parts)

    def _reset_curated_package_dirs(self) -> None:
        for relative in ("publication", "scientific_source_pngs", "summary", "manifests", "phase5"):
            target = self.output_dir / relative
            if target.exists():
                shutil.rmtree(target)
        readme_path = self.output_dir / "README.md"
        if readme_path.exists():
            readme_path.unlink()

    def _register_package_artifact(
        self,
        *,
        final_path: Path,
        source_relative_path: str,
        phase_group: str,
        artifact_group: str,
        scientific_vs_display_only: str,
        copied_vs_regenerated: str,
        case_id: str = "",
        support_only: bool = True,
        comparator_only: bool = False,
        notes: str = "",
    ) -> None:
        self.package_registry_rows.append(
            {
                "final_relative_path": _relative_to_repo(self.repo_root, final_path),
                "source_relative_path": source_relative_path,
                "phase_group": phase_group,
                "artifact_group": artifact_group,
                "scientific_vs_display_only": scientific_vs_display_only,
                "copied_vs_regenerated": copied_vs_regenerated,
                "case_id": case_id,
                "support_only": bool(support_only),
                "comparator_only": bool(comparator_only),
                "notes": notes,
            }
        )

    def _copy_and_register(
        self,
        source_path: Path,
        destination_path: Path,
        *,
        phase_group: str,
        artifact_group: str,
        scientific_vs_display_only: str,
        copied_vs_regenerated: str,
        case_id: str = "",
        support_only: bool = True,
        comparator_only: bool = False,
        notes: str = "",
    ) -> None:
        if not source_path.exists():
            raise FileNotFoundError(f"Missing source artifact: {source_path}")
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
        self._register_package_artifact(
            final_path=destination_path,
            source_relative_path=_relative_to_repo(self.repo_root, source_path),
            phase_group=phase_group,
            artifact_group=artifact_group,
            scientific_vs_display_only=scientific_vs_display_only,
            copied_vs_regenerated=copied_vs_regenerated,
            case_id=case_id,
            support_only=support_only,
            comparator_only=comparator_only,
            notes=notes,
        )

    def _write_and_register_text(
        self,
        destination_path: Path,
        *,
        text: str,
        source_relative_path: str,
        phase_group: str,
        artifact_group: str,
        scientific_vs_display_only: str,
        copied_vs_regenerated: str,
        case_id: str = "",
        support_only: bool = True,
        comparator_only: bool = False,
        notes: str = "",
    ) -> None:
        _write_text(destination_path, text)
        self._register_package_artifact(
            final_path=destination_path,
            source_relative_path=source_relative_path,
            phase_group=phase_group,
            artifact_group=artifact_group,
            scientific_vs_display_only=scientific_vs_display_only,
            copied_vs_regenerated=copied_vs_regenerated,
            case_id=case_id,
            support_only=support_only,
            comparator_only=comparator_only,
            notes=notes,
        )

    def _write_and_register_json(
        self,
        destination_path: Path,
        *,
        payload: dict[str, Any] | list[dict[str, Any]],
        source_relative_path: str,
        phase_group: str,
        artifact_group: str,
        scientific_vs_display_only: str,
        copied_vs_regenerated: str,
        case_id: str = "",
        support_only: bool = True,
        comparator_only: bool = False,
        notes: str = "",
    ) -> None:
        _write_json(destination_path, payload)
        self._register_package_artifact(
            final_path=destination_path,
            source_relative_path=source_relative_path,
            phase_group=phase_group,
            artifact_group=artifact_group,
            scientific_vs_display_only=scientific_vs_display_only,
            copied_vs_regenerated=copied_vs_regenerated,
            case_id=case_id,
            support_only=support_only,
            comparator_only=comparator_only,
            notes=notes,
        )

    def _write_and_register_csv(
        self,
        destination_path: Path,
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
        source_relative_path: str,
        phase_group: str,
        artifact_group: str,
        scientific_vs_display_only: str,
        copied_vs_regenerated: str,
        case_id: str = "",
        support_only: bool = True,
        comparator_only: bool = False,
        notes: str = "",
    ) -> None:
        _write_csv(destination_path, rows, columns)
        self._register_package_artifact(
            final_path=destination_path,
            source_relative_path=source_relative_path,
            phase_group=phase_group,
            artifact_group=artifact_group,
            scientific_vs_display_only=scientific_vs_display_only,
            copied_vs_regenerated=copied_vs_regenerated,
            case_id=case_id,
            support_only=support_only,
            comparator_only=comparator_only,
            notes=notes,
        )

    def _register_existing_phase5_exports(self, *, manifest_json: Path, missing_csv: Path) -> None:
        for row in self.figure_rows:
            self._register_package_artifact(
                final_path=self.repo_root / Path(row["relative_path"]),
                source_relative_path=str(row.get("source_paths") or ""),
                phase_group="phase5",
                artifact_group="prototype_2016_flat_case_exports",
                scientific_vs_display_only="display_only",
                copied_vs_regenerated="regenerated_from_stored_outputs",
                case_id=str(row.get("case_id") or ""),
                support_only=True,
                comparator_only="pygnome" in str(row.get("figure_type") or "").lower(),
                notes="Backward-compatible flat per-case export retained at the legacy final-output root.",
            )
        self._register_package_artifact(
            final_path=manifest_json,
            source_relative_path="",
            phase_group="phase5",
            artifact_group="prototype_2016_flat_case_exports",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="Compatibility manifest for the flat per-case legacy export set.",
        )
        self._register_package_artifact(
            final_path=missing_csv,
            source_relative_path="",
            phase_group="phase5",
            artifact_group="prototype_2016_flat_case_exports",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="Compatibility missing-figure report for the flat per-case legacy export set.",
        )

    def _publication_phase3a_sources(self, case_id: str) -> list[Path]:
        case_slug = self._case_slug(case_id)
        publication_root = self.repo_root / "output" / "figure_package_publication"
        figure_root = self.repo_root / PROTOTYPE_SIMILARITY_DIR / "figures"
        candidates = sorted(publication_root.glob(f"{case_slug}__prototype_pygnome_similarity_summary__*.png"))
        if candidates:
            return candidates
        fallback = sorted(figure_root.glob(f"{case_slug}__prototype_2016__*.png"))
        if fallback:
            return fallback
        raise FileNotFoundError(f"Missing Phase 3A publication figures for {case_id} in figure-package and similarity outputs.")

    def _scientific_phase3a_sources(self, case_id: str) -> list[Path]:
        case_slug = self._case_slug(case_id)
        figure_root = self.repo_root / PROTOTYPE_SIMILARITY_DIR / "figures"
        candidates = sorted(figure_root.glob(f"{case_slug}__prototype_2016__*.png"))
        if not candidates:
            raise FileNotFoundError(f"Missing Phase 3A scientific source PNGs for {case_id}: {figure_root}")
        return candidates

    def _copy_phase3a_package(self) -> None:
        publication_root = self._package_subdir("publication", "phase3a")
        scientific_root = self._package_subdir("scientific_source_pngs", "phase3a")
        summary_root = self._package_subdir("summary", "phase3a")
        publication_root.mkdir(parents=True, exist_ok=True)
        scientific_root.mkdir(parents=True, exist_ok=True)
        summary_root.mkdir(parents=True, exist_ok=True)

        for case_id in self.case_ids:
            for source_path in self._publication_phase3a_sources(case_id):
                self._copy_and_register(
                    source_path,
                    publication_root / case_id / source_path.name,
                    phase_group="phase3a",
                    artifact_group="publication_phase3a",
                    scientific_vs_display_only="display_only",
                    copied_vs_regenerated="copied_from_existing_output",
                    case_id=case_id,
                    support_only=True,
                    comparator_only=True,
                    notes="Legacy Phase 3A publication-facing OpenDrift/PyGNOME comparator figure copied from stored outputs.",
                )
            for source_path in self._scientific_phase3a_sources(case_id):
                self._copy_and_register(
                    source_path,
                    scientific_root / case_id / source_path.name,
                    phase_group="phase3a",
                    artifact_group="scientific_source_phase3a",
                    scientific_vs_display_only="scientific_source",
                    copied_vs_regenerated="copied_from_existing_output",
                    case_id=case_id,
                    support_only=True,
                    comparator_only=True,
                    notes="Exact stored Phase 3A scientific source PNG used by the legacy comparator package.",
                )
            case_summary_dir = summary_root / case_id
            for filename in (
                "phase3a_summary.csv",
                "phase3a_fss_by_time_window.csv",
                "phase3a_kl_by_time.csv",
                "phase3a_pairing_manifest.csv",
            ):
                source_path = self._benchmark_dir(case_id) / filename
                if source_path.exists():
                    self._copy_and_register(
                        source_path,
                        case_summary_dir / filename,
                        phase_group="phase3a",
                        artifact_group="summary_phase3a",
                        scientific_vs_display_only="scientific_source",
                        copied_vs_regenerated="copied_from_existing_output",
                        case_id=case_id,
                        support_only=True,
                        comparator_only="pairing" in filename or "fss" in filename or "kl" in filename,
                        notes="Per-case Phase 3A summary or pairing artifact copied from the stored benchmark package.",
                    )
            for relative in ("pygnome/pygnome_benchmark_metadata.json", "grid/grid.json"):
                source_path = self._benchmark_dir(case_id) / relative
                if source_path.exists():
                    self._copy_and_register(
                        source_path,
                        case_summary_dir / Path(relative).name,
                        phase_group="phase3a",
                        artifact_group="summary_phase3a",
                        scientific_vs_display_only="metadata",
                        copied_vs_regenerated="copied_from_existing_output",
                        case_id=case_id,
                        support_only=True,
                        comparator_only="pygnome" in relative,
                        notes="Per-case Phase 3A metadata copied from stored benchmark artifacts.",
                    )

        for filename in (
            "prototype_pygnome_case_registry.csv",
            "prototype_pygnome_figure_captions.md",
            "prototype_pygnome_figure_registry.csv",
            "prototype_pygnome_fss_by_case_window.csv",
            "prototype_pygnome_kl_by_case_hour.csv",
            "prototype_pygnome_similarity_by_case.csv",
            "prototype_pygnome_similarity_manifest.json",
            "prototype_pygnome_similarity_summary.md",
            "prototype_pygnome_skipped_cases.csv",
        ):
            source_path = self.repo_root / PROTOTYPE_SIMILARITY_DIR / filename
            if source_path.exists():
                self._copy_and_register(
                    source_path,
                    summary_root / filename,
                    phase_group="phase3a",
                    artifact_group="summary_phase3a",
                    scientific_vs_display_only="metadata" if source_path.suffix in {".md", ".json"} else "scientific_source",
                    copied_vs_regenerated="copied_from_existing_output",
                    support_only=True,
                    comparator_only=True,
                    notes="Cross-case Phase 3A legacy comparator summary artifact copied from the stored prototype similarity package.",
                )

        self._write_and_register_text(
            summary_root / "source_path_notes.md",
            text=textwrap.dedent(
                """
                # Prototype 2016 Phase 3A Source Notes

                - `publication/phase3a/` reuses the best available stored legacy support figures, preferring `output/figure_package_publication/` and falling back to `output/prototype_2016_pygnome_similarity/figures/` when needed.
                - `scientific_source_pngs/phase3a/` contains the exact stored PNGs from `output/prototype_2016_pygnome_similarity/figures/`.
                - `summary/phase3a/` preserves the cross-case similarity tables plus per-case benchmark pairing/FSS/KL artifacts when they are present.
                - Phase 3A remains comparator-only OpenDrift-vs-deterministic-PyGNOME support and is not a thesis-facing truth track.
                """
            ).strip(),
            source_relative_path="output/figure_package_publication | output/prototype_2016_pygnome_similarity | output/CASE_2016-*/benchmark",
            phase_group="phase3a",
            artifact_group="summary_phase3a",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=True,
            notes="Phase 3A source-path memo for the curated prototype_2016 package.",
        )

    def _render_shoreline_summary(
        self,
        *,
        case_id: str,
        variant: str,
        source_csv: Path,
        output_path: Path,
    ) -> None:
        df = _read_csv(source_csv)
        if df.empty:
            raise ValueError(f"Stored shoreline summary is empty: {source_csv}")
        required_cols = {"segment_id", "total_beached_kg"}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"Stored shoreline summary is missing required columns {required_cols}: {source_csv}")
        shoreline_df = df.copy()
        shoreline_df["total_beached_kg"] = pd.to_numeric(shoreline_df["total_beached_kg"], errors="coerce").fillna(0.0)
        if "first_arrival_h" in shoreline_df.columns:
            shoreline_df["first_arrival_h"] = pd.to_numeric(shoreline_df["first_arrival_h"], errors="coerce")
        else:
            shoreline_df["first_arrival_h"] = np.nan
        top_rows = shoreline_df.sort_values("total_beached_kg", ascending=False).head(8).iloc[::-1].copy()

        fig = plt.figure(
            figsize=(12, 6),
            dpi=self._dpi(),
            facecolor=str((self.style.get("layout") or {}).get("figure_facecolor") or "#ffffff"),
        )
        gs = fig.add_gridspec(1, 2, width_ratios=[1.5, 1.0], wspace=0.28)
        ax_bar = fig.add_subplot(gs[0, 0])
        ax_note = fig.add_subplot(gs[0, 1])
        bar_color = str(
            (self.style.get("palette") or {}).get("oil_lighter" if variant == "light" else "oil_heavier")
            or "#8c564b"
        )
        ax_bar.barh(top_rows["segment_id"].astype(str), top_rows["total_beached_kg"], color=bar_color, alpha=0.85)
        ax_bar.set_xlabel("Total beached mass (kg)")
        ax_bar.set_title(
            f"{case_id.replace('CASE_', '')} shoreline {variant} summary",
            fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11),
        )
        ax_bar.grid(True, axis="x", alpha=0.25)

        total_mass = float(shoreline_df["total_beached_kg"].sum())
        impacted_segments = int((shoreline_df["total_beached_kg"] > 0).sum())
        first_arrival = shoreline_df["first_arrival_h"].dropna()
        first_arrival_text = f"{float(first_arrival.min()):.1f} h" if not first_arrival.empty else "not recorded"
        ax_note.axis("off")
        note_lines = [
            f"Scenario: {variant}",
            f"Stored shoreline CSV: {source_csv.name}",
            f"Impacted segments: {impacted_segments}",
            f"Total beached mass: {total_mass:.2f} kg",
            f"First arrival: {first_arrival_text}",
            "",
            "This figure is a summary from the stored shoreline CSV.",
            "It does not invent a shoreline map or new geometry.",
        ]
        ax_note.text(
            0.0,
            1.0,
            "\n".join(note_lines),
            va="top",
            ha="left",
            fontsize=float((self.style.get("typography") or {}).get("body_size") or 9),
        )
        fig.suptitle(
            f"{case_id.replace('CASE_', '')} legacy Phase 4 shoreline summary ({variant})",
            fontsize=float((self.style.get("typography") or {}).get("title_size") or 18),
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, bbox_inches="tight")
        plt.close(fig)

    def _copy_phase4_package(self) -> None:
        publication_root = self._package_subdir("publication", "phase4")
        scientific_root = self._package_subdir("scientific_source_pngs", "phase4")
        summary_root = self._package_subdir("summary", "phase4")
        publication_root.mkdir(parents=True, exist_ok=True)
        scientific_root.mkdir(parents=True, exist_ok=True)
        summary_root.mkdir(parents=True, exist_ok=True)

        for case_id in self.case_ids:
            weathering_dir = self._weathering_dir(case_id)
            if not weathering_dir.exists():
                raise FileNotFoundError(f"Missing Phase 4 weathering directory for {case_id}: {weathering_dir}")
            publication_case_dir = publication_root / case_id
            scientific_case_dir = scientific_root / case_id
            summary_case_dir = summary_root / case_id
            for filename in ("mass_budget_comparison.png", "mass_budget_light.png", "mass_budget_heavy.png"):
                source_path = weathering_dir / filename
                self._copy_and_register(
                    source_path,
                    publication_case_dir / filename,
                    phase_group="phase4",
                    artifact_group="publication_phase4",
                    scientific_vs_display_only="display_only",
                    copied_vs_regenerated="copied_from_existing_output",
                    case_id=case_id,
                    support_only=True,
                    comparator_only=False,
                    notes="Legacy Phase 4 weathering/fate publication figure copied from the stored per-case weathering output.",
                )
                self._copy_and_register(
                    source_path,
                    scientific_case_dir / filename,
                    phase_group="phase4",
                    artifact_group="scientific_source_phase4",
                    scientific_vs_display_only="scientific_source",
                    copied_vs_regenerated="copied_from_existing_output",
                    case_id=case_id,
                    support_only=True,
                    comparator_only=False,
                    notes="Exact stored Phase 4 weathering/fate source PNG used by the curated legacy package.",
                )
            for variant in ("light", "heavy"):
                shoreline_csv = weathering_dir / f"shoreline_{variant}.csv"
                shoreline_png = publication_case_dir / f"shoreline_summary_{variant}.png"
                self._render_shoreline_summary(case_id=case_id, variant=variant, source_csv=shoreline_csv, output_path=shoreline_png)
                self._register_package_artifact(
                    final_path=shoreline_png,
                    source_relative_path=_relative_to_repo(self.repo_root, shoreline_csv),
                    phase_group="phase4",
                    artifact_group="publication_phase4",
                    scientific_vs_display_only="display_only",
                    copied_vs_regenerated="regenerated_from_stored_csv",
                    case_id=case_id,
                    support_only=True,
                    comparator_only=False,
                    notes="Honest lightweight shoreline summary rendered from the stored shoreline CSV only.",
                )
            for filename in ("budget_light.csv", "budget_heavy.csv", "shoreline_light.csv", "shoreline_heavy.csv"):
                source_path = weathering_dir / filename
                self._copy_and_register(
                    source_path,
                    summary_case_dir / filename,
                    phase_group="phase4",
                    artifact_group="summary_phase4",
                    scientific_vs_display_only="scientific_source",
                    copied_vs_regenerated="copied_from_existing_output",
                    case_id=case_id,
                    support_only=True,
                    comparator_only=False,
                    notes="Stored per-case Phase 4 budget or shoreline summary table copied into the curated legacy package.",
                )
                self.phase4_registry_rows.append(
                    {
                        "case_id": case_id,
                        "artifact_type": filename,
                        "source_relative_path": _relative_to_repo(self.repo_root, source_path),
                        "final_relative_path": _relative_to_repo(self.repo_root, summary_case_dir / filename),
                        "notes": "Stored Phase 4 case summary table copied into the curated package.",
                    }
                )

        self._write_and_register_csv(
            summary_root / "prototype_2016_phase4_registry.csv",
            rows=self.phase4_registry_rows,
            columns=[
                "case_id",
                "artifact_type",
                "source_relative_path",
                "final_relative_path",
                "notes",
            ],
            source_relative_path="output/CASE_2016-*/weathering",
            phase_group="phase4",
            artifact_group="summary_phase4",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="Aggregate registry for the copied prototype_2016 Phase 4 weathering/fate tables.",
        )

    def _write_curated_legacy_readme(self) -> None:
        readme_text = textwrap.dedent(
            """
            # Prototype 2016 Legacy Final Package

            This folder is the authoritative curated `prototype_2016` legacy support export. It is thesis-facing only as a legacy support package and does not replace the final regional Phase 1 study.

            ## What This Package Means

            - `prototype_2016` is legacy support-only.
            - The visible legacy support flow here is `Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`.
            - `Phase 3A` is comparator-only OpenDrift vs deterministic PyGNOME support.
            - `Phase 4` is the legacy weathering/fate family seeded from the selected drifter-of-record start.
            - `Phase 5` is this read-only packaging/export layer built from stored outputs.
            - There is no thesis-facing `Phase 3B` or `Phase 3C` in this lane.
            - This lane does not replace the final regional Phase 1 study.

            ## Folder Guide

            - `publication/phase3a/`: legacy Phase 3A support figures copied from the stored publication/similarity outputs.
            - `publication/phase4/`: legacy Phase 4 weathering/fate publication figures plus shoreline summary figures derived from stored shoreline CSVs only.
            - `scientific_source_pngs/phase3a/`: exact stored Phase 3A source PNGs.
            - `scientific_source_pngs/phase4/`: exact stored Phase 4 source PNGs.
            - `summary/phase3a/`: similarity/FSS/KL tables, per-case pairing artifacts, and source-path notes.
            - `summary/phase4/`: copied budget/shoreline CSVs plus a lightweight phase4 registry.
            - `manifests/`: machine-readable registries for this curated export.
            - `phase5/`: packaging notes describing what was copied vs regenerated.

            ## Compatibility Note

            The flat per-case PNG directories at this root (`CASE_2016-09-01/`, `CASE_2016-09-06/`, `CASE_2016-09-17/`) are preserved for backward compatibility. The structured subfolders above are the easier-to-browse authoritative package layout.
            """
        ).strip()
        self._write_and_register_text(
            self.output_dir / "README.md",
            text=readme_text,
            source_relative_path="output/prototype_2016_pygnome_similarity | output/CASE_2016-*/weathering | output/figure_package_publication",
            phase_group="phase5",
            artifact_group="prototype_legacy_phase5_packaging",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="Curated README for the authoritative prototype_2016 legacy final-output root.",
        )

    def _write_phase5_packaging_summary(self) -> None:
        copied_count = sum(1 for row in self.package_registry_rows if row["copied_vs_regenerated"].startswith("copied"))
        regenerated_count = sum(1 for row in self.package_registry_rows if row["copied_vs_regenerated"].startswith("regenerated"))
        text = textwrap.dedent(
            f"""
            # Prototype 2016 Packaging Summary

            This Phase 5 layer curates the stored `prototype_2016` outputs into one easy-to-browse legacy support package.

            - Scientific reruns performed here: none
            - What was copied: stored Phase 3A publication figures, stored Phase 3A source PNGs, stored per-case Phase 3A summaries, stored Phase 4 weathering PNGs, and stored Phase 4 budget/shoreline CSVs
            - What was lightly regenerated: shoreline summary PNGs from stored shoreline CSVs, package registries, package README, and packaging notes
            - What was retained for compatibility: the flat per-case PNG exports and their root compatibility manifest
            - Relation to repo-wide `phase5_sync`: this package is the dedicated `prototype_2016` Phase 5 legacy deliverable; `phase5_sync` is the broader cross-repo read-only reproducibility layer

            Counts:

            - Copied artifacts: {copied_count}
            - Regenerated metadata/display artifacts: {regenerated_count}
            - Flat per-case compatibility figures retained: {len(self.figure_rows)}
            """
        ).strip()
        self._write_and_register_text(
            self._package_subdir("phase5", "prototype_2016_packaging_summary.md"),
            text=text,
            source_relative_path="output/prototype_2016_pygnome_similarity | output/CASE_2016-*/weathering | output/figure_package_publication | output/2016 Legacy Runs FINAL Figures/final_figure_manifest.json",
            phase_group="phase5",
            artifact_group="prototype_legacy_phase5_packaging",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="Explicit Phase 5 packaging note for the curated prototype_2016 legacy package.",
        )

    def _write_curated_package_manifests(self, *, compatibility_manifest: Path, missing_csv: Path) -> dict[str, Path]:
        manifest_dir = self._package_subdir("manifests")
        manifest_dir.mkdir(parents=True, exist_ok=True)
        registry_columns = [
            "final_relative_path",
            "source_relative_path",
            "phase_group",
            "artifact_group",
            "scientific_vs_display_only",
            "copied_vs_regenerated",
            "case_id",
            "support_only",
            "comparator_only",
            "notes",
        ]
        registry_csv = manifest_dir / LEGACY_PACKAGE_REGISTRY_CSV_FILENAME
        registry_json = manifest_dir / LEGACY_PACKAGE_REGISTRY_JSON_FILENAME
        manifest_json = manifest_dir / LEGACY_PACKAGE_MANIFEST_FILENAME
        self._write_and_register_csv(
            registry_csv,
            rows=self.package_registry_rows,
            columns=registry_columns,
            source_relative_path="output/prototype_2016_pygnome_similarity | output/CASE_2016-*/weathering | output/figure_package_publication | output/2016 Legacy Runs FINAL Figures",
            phase_group="phase5",
            artifact_group="prototype_legacy_phase5_packaging",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="CSV registry for the authoritative curated prototype_2016 final-output package.",
        )
        self._write_and_register_json(
            registry_json,
            payload=self.package_registry_rows,
            source_relative_path=_relative_to_repo(self.repo_root, registry_csv),
            phase_group="phase5",
            artifact_group="prototype_legacy_phase5_packaging",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="JSON mirror of the curated prototype_2016 final-output registry.",
        )
        final_registry_count = len(self.package_registry_rows) + 1
        manifest_payload = {
            "phase": PHASE,
            "workflow_mode": "prototype_2016",
            "generated_at_utc": _utc_now_iso(),
            "authoritative_curated_root": _relative_to_repo(self.repo_root, self.output_dir),
            "compatibility_flat_manifest": _relative_to_repo(self.repo_root, compatibility_manifest),
            "compatibility_missing_figures_csv": _relative_to_repo(self.repo_root, missing_csv),
            "scientific_rerun_performed": False,
            "phase4_case_reruns_performed": [],
            "publication_phase3a_dir": _relative_to_repo(self.repo_root, self._package_subdir("publication", "phase3a")),
            "publication_phase4_dir": _relative_to_repo(self.repo_root, self._package_subdir("publication", "phase4")),
            "scientific_source_phase3a_dir": _relative_to_repo(self.repo_root, self._package_subdir("scientific_source_pngs", "phase3a")),
            "scientific_source_phase4_dir": _relative_to_repo(self.repo_root, self._package_subdir("scientific_source_pngs", "phase4")),
            "summary_phase3a_dir": _relative_to_repo(self.repo_root, self._package_subdir("summary", "phase3a")),
            "summary_phase4_dir": _relative_to_repo(self.repo_root, self._package_subdir("summary", "phase4")),
            "phase5_dir": _relative_to_repo(self.repo_root, self._package_subdir("phase5")),
            "registry_csv": _relative_to_repo(self.repo_root, registry_csv),
            "registry_json": _relative_to_repo(self.repo_root, registry_json),
            "registry_row_count": final_registry_count,
            "figure_count": len(self.figure_rows),
            "missing_figure_count": len(self.missing_rows),
            "notes": [
                "prototype_2016 remains legacy support-only and does not replace the final regional Phase 1 study.",
                "Phase 3A is comparator-only OpenDrift vs deterministic PyGNOME support.",
                "Phase 4 is the legacy weathering/fate family seeded from the selected drifter-of-record start.",
                "Phase 5 is this read-only packaging/export layer built from stored outputs only.",
                "No Phase 3B or Phase 3C exists in the thesis-facing prototype_2016 lane.",
            ],
        }
        self._write_and_register_json(
            manifest_json,
            payload=manifest_payload,
            source_relative_path=f"{_relative_to_repo(self.repo_root, compatibility_manifest)} | {_relative_to_repo(self.repo_root, missing_csv)} | {_relative_to_repo(self.repo_root, registry_csv)}",
            phase_group="phase5",
            artifact_group="prototype_legacy_phase5_packaging",
            scientific_vs_display_only="metadata",
            copied_vs_regenerated="regenerated_from_existing_metadata",
            support_only=True,
            comparator_only=False,
            notes="Top-level manifest for the curated prototype_2016 legacy Phase 5 package.",
        )
        return {
            "manifest_json": manifest_json,
            "registry_csv": registry_csv,
            "registry_json": registry_json,
        }

    def _build_curated_package(self, *, compatibility_manifest: Path, missing_csv: Path) -> dict[str, Path]:
        self.package_registry_rows = []
        self.phase4_registry_rows = []
        self._reset_curated_package_dirs()
        self._register_existing_phase5_exports(manifest_json=compatibility_manifest, missing_csv=missing_csv)
        self._copy_phase3a_package()
        self._copy_phase4_package()
        self._write_curated_legacy_readme()
        self._write_phase5_packaging_summary()
        return self._write_curated_package_manifests(
            compatibility_manifest=compatibility_manifest,
            missing_csv=missing_csv,
        )

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        for case_id in self.case_ids:
            logger.info("Exporting prototype_2016 final paper figures for %s", case_id)
            self._export_case(case_id)

        missing_csv = self.output_dir / MISSING_FIGURES_FILENAME
        manifest_json = self.output_dir / MANIFEST_FILENAME
        _write_csv(
            missing_csv,
            self.missing_rows,
            columns=[
                "case_id",
                "figure_id",
                "figure_type",
                "missing_cause",
                "missing_sources",
                "notes",
            ],
        )
        curated_manifest_paths = self._build_curated_package(
            compatibility_manifest=manifest_json,
            missing_csv=missing_csv,
        )
        case_rendering = {
            case_id: prototype_2016_rendering_metadata(self._figure_crop_bounds(case_id))
            for case_id in self.case_ids
        }
        manifest_payload = {
            "phase": PHASE,
            "workflow_mode": "prototype_2016",
            "generated_at_utc": _utc_now_iso(),
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "configured_case_ids": list(self.case_ids),
            "font_family": self.font_family,
            "extent_modes_supported": [
                PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
                PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
            ],
            "default_extent_mode": self.extent_mode,
            "rendering_profile": "prototype_2016_case_local_projected_v1",
            "map_projection": "local_azimuthal_equidistant",
            "case_rendering": case_rendering,
            "figure_count": len(self.figure_rows),
            "missing_figure_count": len(self.missing_rows),
            "figures": self.figure_rows,
            "missing_figures_csv": _relative_to_repo(self.repo_root, missing_csv),
            "missing_figures": self.missing_rows,
            "curated_package_readme": _relative_to_repo(self.repo_root, self.output_dir / "README.md"),
            "legacy_final_output_manifest_json": _relative_to_repo(
                self.repo_root,
                curated_manifest_paths["manifest_json"],
            ),
            "legacy_final_output_registry_csv": _relative_to_repo(
                self.repo_root,
                curated_manifest_paths["registry_csv"],
            ),
            "legacy_final_output_registry_json": _relative_to_repo(
                self.repo_root,
                curated_manifest_paths["registry_json"],
            ),
            "notes": [
                "Curated prototype_2016 final paper-figure export built from existing outputs only.",
                "Forecast figures use exact stored raster cells and exact stored footprint geometry only; empty stored layers are omitted.",
                "prototype_2016 p50/p90 products are exact valid-time member-occupancy footprints.",
                "This folder is the authoritative curated prototype_2016 legacy support package and does not replace the final regional Phase 1 study.",
                "PyGNOME remains comparator-only; matched grid wind/current forcing is used when available and degraded mode is surfaced explicitly otherwise.",
            ],
        }
        _write_json(manifest_json, manifest_payload)
        return {
            "phase": PHASE,
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "manifest_json": _relative_to_repo(self.repo_root, manifest_json),
            "missing_figures_csv": _relative_to_repo(self.repo_root, missing_csv),
            "legacy_final_output_manifest_json": _relative_to_repo(
                self.repo_root,
                curated_manifest_paths["manifest_json"],
            ),
            "legacy_final_output_registry_csv": _relative_to_repo(
                self.repo_root,
                curated_manifest_paths["registry_csv"],
            ),
            "legacy_final_output_registry_json": _relative_to_repo(
                self.repo_root,
                curated_manifest_paths["registry_json"],
            ),
            "configured_case_ids": list(self.case_ids),
            "figure_count": len(self.figure_rows),
            "missing_figure_count": len(self.missing_rows),
            "font_family": self.font_family,
            "generated_figures": [row["relative_path"] for row in self.figure_rows],
            "package_registry_count": len(self.package_registry_rows),
        }


def run_prototype_legacy_final_figures(
    repo_root: str | Path = ".",
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    return PrototypeLegacyFinalFiguresService(repo_root=repo_root, output_dir=output_dir).run()
