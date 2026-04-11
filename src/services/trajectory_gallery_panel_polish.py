"""Panel-ready polish layer for the read-only trajectory gallery."""

from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
import numpy as np
import pandas as pd
import xarray as xr
import yaml
from matplotlib import pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Rectangle

matplotlib.use("Agg")

PHASE = "trajectory_gallery_panel_polish"
RAW_GALLERY_DIR = Path("output") / "trajectory_gallery"
OUTPUT_DIR = Path("output") / "trajectory_gallery_panel"
STYLE_CONFIG_PATH = Path("config") / "panel_figure_style.yaml"
MINDORO_LABELS_PATH = Path("config") / "panel_map_labels_mindoro.csv"
DWH_LABELS_PATH = Path("config") / "panel_map_labels_dwh.csv"

MINDORO_FORECAST_MANIFEST = Path("output") / "CASE_MINDORO_RETRO_2023" / "forecast" / "forecast_manifest.json"
MINDORO_PHASE3B_SUMMARY = Path("output") / "CASE_MINDORO_RETRO_2023" / "phase3b" / "phase3b_summary.csv"
MINDORO_PYGNOME_RANKING = Path("output") / "CASE_MINDORO_RETRO_2023" / "pygnome_public_comparison" / "pygnome_public_comparison_model_ranking.csv"
MINDORO_PHASE4_MANIFEST = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_run_manifest.json"
MINDORO_PHASE4_SUMMARY = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_oil_budget_summary.csv"
MINDORO_PHASE4_SHORELINE_ARRIVAL = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_shoreline_arrival.csv"
MINDORO_PHASE4_SHORELINE_SEGMENTS = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_shoreline_segments.csv"

DWH_RUN_MANIFEST = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run" / "phase3c_run_manifest.json"
DWH_SUMMARY = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run" / "phase3c_summary.csv"
DWH_ENSEMBLE_SUMMARY = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_summary.csv"
DWH_ALL_RESULTS = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_all_results_table.csv"

BOARD_FAMILIES: dict[str, str] = {
    "A": "Mindoro strict March 6 forecast-vs-observation board",
    "B": "Mindoro March 4-6 event-corridor board",
    "C": "Mindoro OpenDrift vs PyGNOME comparison board",
    "D": "Mindoro trajectory board",
    "E": "Mindoro Phase 4 oil-budget board",
    "F": "Mindoro Phase 4 shoreline-arrival / shoreline-impact board",
    "G": "DWH deterministic forecast-vs-observation board",
    "H": "DWH deterministic vs ensemble board",
    "I": "DWH OpenDrift vs PyGNOME comparison board",
    "J": "DWH trajectory board",
}


@dataclass
class PanelFigureRecord:
    figure_id: str
    board_family_code: str
    board_family_label: str
    case_id: str
    phase_or_track: str
    date_token: str
    model_names: str
    run_type: str
    scenario_id: str
    variant: str
    relative_path: str
    file_path: str
    plain_language_interpretation: str
    recommended_for_main_defense: bool
    source_paths: str
    notes: str

    def as_row(self) -> dict[str, Any]:
        return {
            "figure_id": self.figure_id,
            "board_family_code": self.board_family_code,
            "board_family_label": self.board_family_label,
            "case_id": self.case_id,
            "phase_or_track": self.phase_or_track,
            "date_token": self.date_token,
            "model_names": self.model_names,
            "run_type": self.run_type,
            "scenario_id": self.scenario_id,
            "variant": self.variant,
            "relative_path": self.relative_path,
            "file_path": self.file_path,
            "plain_language_interpretation": self.plain_language_interpretation,
            "recommended_for_main_defense": self.recommended_for_main_defense,
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


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    df = df[columns]
    df.to_csv(path, index=False)


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


def _safe_token(value: str | None) -> str:
    text = str(value or "").strip().lower()
    chars = [char if char.isalnum() else "_" for char in text]
    normalized = "".join(chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def build_panel_figure_filename(
    *,
    case_id: str,
    phase_or_track: str,
    model_name: str,
    run_type: str,
    date_token: str,
    figure_slug: str,
    scenario_id: str = "",
    variant: str = "panel",
    extension: str = "png",
) -> str:
    tokens = [
        _safe_token(case_id),
        _safe_token(phase_or_track),
        _safe_token(model_name),
        _safe_token(run_type),
        _safe_token(date_token),
    ]
    if _safe_token(scenario_id):
        tokens.append(_safe_token(scenario_id))
    tokens.append(_safe_token(variant))
    tokens.append(_safe_token(figure_slug))
    return "__".join(token for token in tokens if token) + f".{extension.lstrip('.')}"


def load_panel_style_config(path: str | Path = STYLE_CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    payload = _read_yaml(config_path)
    if not payload:
        raise ValueError(f"Missing panel figure style configuration: {config_path}")
    for key in ("palette", "legend_labels", "layout", "typography"):
        if key not in payload:
            raise ValueError(f"Panel figure style configuration is missing '{key}': {config_path}")
    return payload


class TrajectoryGalleryPanelPolishService:
    def __init__(self, repo_root: str | Path = ".", output_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_DIR
        self.style = load_panel_style_config(self.repo_root / STYLE_CONFIG_PATH)
        self.mindoro_labels = _read_csv(self.repo_root / MINDORO_LABELS_PATH)
        self.dwh_labels = _read_csv(self.repo_root / DWH_LABELS_PATH)
        self.mindoro_forecast_manifest = _read_json(self.repo_root / MINDORO_FORECAST_MANIFEST)
        self.mindoro_phase4_manifest = _read_json(self.repo_root / MINDORO_PHASE4_MANIFEST)
        self.dwh_run_manifest = _read_json(self.repo_root / DWH_RUN_MANIFEST)
        self.panel_records: list[PanelFigureRecord] = []
        self.missing_optional_artifacts: list[dict[str, str]] = []

    def _record_missing(self, path: Path, notes: str) -> None:
        self.missing_optional_artifacts.append(
            {
                "relative_path": _relative_to_repo(self.repo_root, path),
                "notes": notes,
            }
        )

    def _resolve(self, relative_path: str | Path) -> Path:
        path = Path(relative_path)
        if not path.is_absolute():
            path = self.repo_root / path
        return path.resolve()

    def _image_slot(self, title: str, relative_path: str, notes: str = "") -> dict[str, str]:
        return {"kind": "image", "title": title, "relative_path": relative_path, "notes": notes}

    def _text_slot(self, title: str, text: str) -> dict[str, str]:
        return {"kind": "text", "title": title, "text": text}

    def _panel_colors(self) -> dict[str, str]:
        return self.style.get("palette", {})

    def _legend_labels(self) -> dict[str, str]:
        return self.style.get("legend_labels", {})

    def _slide_size(self) -> tuple[float, float]:
        layout = self.style.get("layout", {})
        values = layout.get("slide_size_inches") or [16, 9]
        return float(values[0]), float(values[1])

    def _dpi(self) -> int:
        return int((self.style.get("layout", {}) or {}).get("dpi") or 180)

    def _load_optional_outline(self, path: Path) -> gpd.GeoDataFrame | None:
        if not path.exists():
            self._record_missing(path, "Optional locator outline source missing for a panel board.")
            return None
        gdf = gpd.read_file(path)
        if gdf.empty:
            return None
        return gdf.to_crs("EPSG:4326")

    def _case_locator_context(self, case_id: str) -> tuple[pd.DataFrame, tuple[float, float, float, float], gpd.GeoDataFrame | None]:
        if case_id == "CASE_MINDORO_RETRO_2023":
            bounds = tuple((self.mindoro_forecast_manifest.get("grid") or {}).get("display_bounds_wgs84") or [120.85, 122.1, 12.2, 13.8])
            outline = self._load_optional_outline(self.repo_root / "data_processed" / "grids" / "shoreline_segments.gpkg")
            return self.mindoro_labels, (float(bounds[0]), float(bounds[1]), float(bounds[2]), float(bounds[3])), outline
        labels = self.dwh_labels
        outline = self._load_optional_outline(
            self.repo_root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_setup" / "shoreline_segments.gpkg"
        )
        if outline is not None and not outline.empty:
            min_x, min_y, max_x, max_y = outline.total_bounds
            bounds = (float(min_x), float(max_x), float(min_y), float(max_y))
        else:
            bounds = (-91.8, -87.0, 27.7, 30.4)
        return labels, bounds, outline

    def _add_locator(self, ax: plt.Axes, case_id: str) -> None:
        labels_df, bounds, outline = self._case_locator_context(case_id)
        min_lon, max_lon, min_lat, max_lat = bounds
        ax.set_facecolor("#ffffff")
        if outline is not None and not outline.empty:
            outline.boundary.plot(ax=ax, linewidth=0.4, color="#64748b", alpha=0.7, zorder=1)
        if not labels_df.empty:
            active = labels_df.loc[labels_df["enabled_yes_no"].astype(str).str.lower() == "yes"].copy()
            ax.scatter(active["lon"], active["lat"], s=14, color="#0f172a", zorder=3)
            for _, row in active.iterrows():
                ax.text(
                    float(row["lon"]),
                    float(row["lat"]),
                    str(row["label_text"]),
                    fontsize=6.8,
                    ha="left",
                    va="bottom",
                    color="#0f172a",
                    zorder=4,
                )
        ax.add_patch(
            Rectangle(
                (min_lon, min_lat),
                max_lon - min_lon,
                max_lat - min_lat,
                fill=False,
                linewidth=1.5,
                linestyle="--",
                edgecolor="#dc2626",
                zorder=5,
            )
        )
        padding_lon = max((max_lon - min_lon) * 0.6, 0.3)
        padding_lat = max((max_lat - min_lat) * 0.6, 0.25)
        ax.set_xlim(min_lon - padding_lon, max_lon + padding_lon)
        ax.set_ylim(min_lat - padding_lat, max_lat + padding_lat)
        ax.set_title("Locator", fontsize=10, loc="left", pad=4)
        ax.set_xlabel("Lon", fontsize=7)
        ax.set_ylabel("Lat", fontsize=7)
        ax.tick_params(labelsize=6)
        ax.annotate(
            "N",
            xy=(0.92, 0.88),
            xytext=(0.92, 0.70),
            xycoords="axes fraction",
            textcoords="axes fraction",
            arrowprops={"arrowstyle": "-|>", "color": "#111827", "lw": 1.1},
            ha="center",
            va="center",
            fontsize=8,
            color="#111827",
            fontweight="bold",
        )
        center_lat = (min_lat + max_lat) / 2.0
        km_per_deg_lon = max(1.0, 111.32 * np.cos(np.deg2rad(center_lat)))
        scale_km = 50.0 if case_id == "CASE_MINDORO_RETRO_2023" else 100.0
        scale_deg = scale_km / km_per_deg_lon
        x0 = min_lon - padding_lon * 0.35
        x1 = x0 + scale_deg
        y0 = min_lat - padding_lat * 0.10
        ax.plot([x0, x1], [y0, y0], color="#111827", linewidth=2.0, zorder=6)
        ax.text((x0 + x1) / 2.0, y0 + padding_lat * 0.05, f"{int(scale_km)} km", fontsize=6.5, ha="center")

    def _add_legend(self, ax: plt.Axes, legend_keys: list[str]) -> None:
        palette = self._panel_colors()
        labels = self._legend_labels()
        handles: list[Any] = []
        for key in legend_keys:
            color = palette.get(key, "#475569")
            label = labels.get(key, key.replace("_", " ").title())
            if key in {"source_point"}:
                handles.append(Line2D([0], [0], marker="o", color="white", markerfacecolor=color, markeredgecolor="#111827", markersize=8, linewidth=0, label=label))
            elif key in {"initialization_polygon", "validation_polygon", "corridor_hull"}:
                handles.append(Patch(facecolor="none", edgecolor=color, linewidth=2, label=label))
            else:
                handles.append(Line2D([0], [0], color=color, linewidth=2.5, label=label))
        ax.axis("off")
        legend = ax.legend(handles=handles, loc="upper left", frameon=True, fontsize=8, title="Visual grammar")
        legend.get_frame().set_facecolor((1, 1, 1, 0.95))
        legend.get_frame().set_edgecolor("#94a3b8")
        if legend.get_title():
            legend.get_title().set_fontsize(9)

    def _add_note_box(self, ax: plt.Axes, title: str, lines: list[str]) -> None:
        wrapped = "\n".join(textwrap.fill(line, width=34) for line in lines if str(line).strip())
        ax.axis("off")
        ax.text(
            0.0,
            1.0,
            title,
            ha="left",
            va="top",
            fontsize=10,
            fontweight="bold",
            color="#0f172a",
            transform=ax.transAxes,
        )
        ax.text(
            0.0,
            0.92,
            wrapped,
            ha="left",
            va="top",
            fontsize=8.4,
            color="#334155",
            transform=ax.transAxes,
            bbox={"boxstyle": "round,pad=0.45", "facecolor": "#f8fafc", "edgecolor": "#94a3b8"},
        )

    def _draw_slot(self, ax: plt.Axes, slot: dict[str, str]) -> list[str]:
        ax.set_facecolor("#ffffff")
        ax.set_title(str(slot.get("title") or ""), fontsize=10, loc="left", pad=5)
        source_paths: list[str] = []
        if slot.get("kind") == "image":
            relative_path = str(slot.get("relative_path") or "").strip()
            if not relative_path:
                ax.text(0.5, 0.5, "Source figure unavailable", ha="center", va="center", fontsize=11, color="#475569", transform=ax.transAxes)
                ax.set_xticks([])
                ax.set_yticks([])
                return source_paths
            source_path = self._resolve(relative_path)
            source_paths.append(_relative_to_repo(self.repo_root, source_path))
            if source_path.exists() and source_path.is_file():
                image = plt.imread(source_path)
                ax.imshow(image)
                ax.set_xticks([])
                ax.set_yticks([])
                for spine in ax.spines.values():
                    spine.set_visible(True)
                    spine.set_edgecolor("#cbd5e1")
            else:
                self._record_missing(source_path, str(slot.get("notes") or "Optional panel source image missing."))
                ax.text(0.5, 0.5, "Source figure unavailable", ha="center", va="center", fontsize=11, color="#475569", transform=ax.transAxes)
                ax.set_xticks([])
                ax.set_yticks([])
        else:
            ax.axis("off")
            ax.text(
                0.02,
                0.98,
                textwrap.fill(str(slot.get("text") or ""), width=48),
                ha="left",
                va="top",
                fontsize=9.5,
                color="#334155",
                transform=ax.transAxes,
                bbox={"boxstyle": "round,pad=0.5", "facecolor": "#f8fafc", "edgecolor": "#cbd5e1"},
            )
        return source_paths

    def _register_board(
        self,
        *,
        board_family_code: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        scenario_id: str,
        variant: str,
        destination: Path,
        interpretation: str,
        recommended: bool,
        source_paths: list[str],
        notes: str,
    ) -> PanelFigureRecord:
        record = PanelFigureRecord(
            figure_id=destination.stem,
            board_family_code=board_family_code,
            board_family_label=BOARD_FAMILIES[board_family_code],
            case_id=case_id,
            phase_or_track=phase_or_track,
            date_token=date_token,
            model_names=model_names,
            run_type=run_type,
            scenario_id=scenario_id,
            variant=variant,
            relative_path=_relative_to_repo(self.repo_root, destination),
            file_path=str(destination),
            plain_language_interpretation=interpretation,
            recommended_for_main_defense=recommended,
            source_paths=";".join(sorted({path for path in source_paths if path})),
            notes=notes,
        )
        self.panel_records.append(record)
        return record

    def _board_filename(
        self,
        *,
        case_id: str,
        phase_or_track: str,
        model_names: str,
        run_type: str,
        date_token: str,
        figure_slug: str,
        scenario_id: str = "",
        variant: str = "panel",
    ) -> Path:
        return self.output_dir / build_panel_figure_filename(
            case_id=case_id,
            phase_or_track=phase_or_track,
            model_name=model_names,
            run_type=run_type,
            date_token=date_token,
            scenario_id=scenario_id,
            variant=variant,
            figure_slug=figure_slug,
        )

    def _compose_board(
        self,
        *,
        board_family_code: str,
        board_title: str,
        subtitle: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        figure_slug: str,
        panels: list[dict[str, str]],
        legend_keys: list[str],
        metric_lines: list[str],
        interpretation: str,
        caption: str,
        notes: str,
        recommended: bool,
        scenario_id: str = "",
        variant: str = "panel",
    ) -> PanelFigureRecord:
        destination = self._board_filename(
            case_id=case_id,
            phase_or_track=phase_or_track,
            model_names=model_names,
            run_type=run_type,
            date_token=date_token,
            figure_slug=figure_slug,
            scenario_id=scenario_id,
            variant=variant,
        )
        destination.parent.mkdir(parents=True, exist_ok=True)

        width, height = self._slide_size()
        fig = plt.figure(figsize=(width, height), dpi=self._dpi(), facecolor=(self.style.get("layout") or {}).get("figure_facecolor") or "#f8fafc")
        grid = fig.add_gridspec(
            2,
            3,
            width_ratios=[1.0, 1.0, 0.62],
            height_ratios=[1.0, 1.0],
            left=0.04,
            right=0.985,
            top=0.86,
            bottom=0.13,
            wspace=0.16,
            hspace=0.22,
        )
        slot_axes = [
            fig.add_subplot(grid[0, 0]),
            fig.add_subplot(grid[0, 1]),
            fig.add_subplot(grid[1, 0]),
            fig.add_subplot(grid[1, 1]),
        ]
        side_grid = grid[:, 2].subgridspec(3, 1, height_ratios=[0.9, 1.2, 1.5], hspace=0.22)
        locator_ax = fig.add_subplot(side_grid[0, 0])
        legend_ax = fig.add_subplot(side_grid[1, 0])
        note_ax = fig.add_subplot(side_grid[2, 0])

        source_paths: list[str] = []
        padded_panels = panels[:4] + [self._text_slot("Support note", "No source panel was available for this slot.")] * max(0, 4 - len(panels))
        for ax, slot in zip(slot_axes, padded_panels):
            source_paths.extend(self._draw_slot(ax, slot))

        self._add_locator(locator_ax, case_id)
        self._add_legend(legend_ax, legend_keys)
        self._add_note_box(note_ax, "Key reading notes", metric_lines)

        fig.text(0.04, 0.94, board_title, fontsize=19, fontweight="bold", ha="left", va="top", color="#0f172a")
        fig.text(0.04, 0.905, subtitle, fontsize=10, ha="left", va="top", color="#475569")
        fig.text(
            0.04,
            0.055,
            textwrap.fill(caption, width=165),
            fontsize=9.2,
            ha="left",
            va="bottom",
            color="#334155",
            bbox={"boxstyle": "round,pad=0.42", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
        )
        fig.savefig(destination, dpi=self._dpi(), bbox_inches="tight")
        plt.close(fig)

        return self._register_board(
            board_family_code=board_family_code,
            case_id=case_id,
            phase_or_track=phase_or_track,
            date_token=date_token,
            model_names=model_names,
            run_type=run_type,
            scenario_id=scenario_id,
            variant=variant,
            destination=destination,
            interpretation=interpretation,
            recommended=recommended,
            source_paths=source_paths,
            notes=notes,
        )

    def _extract_opendrift_wgs84(
        self,
        nc_path: Path,
        sample_count: int = 80,
    ) -> tuple[list[np.ndarray], list[np.ndarray], np.ndarray, np.ndarray]:
        with xr.open_dataset(nc_path) as ds:
            lon = np.asarray(ds["lon"].values, dtype=float)
            lat = np.asarray(ds["lat"].values, dtype=float)
        sample_indices = np.linspace(0, lon.shape[0] - 1, num=max(1, min(sample_count, lon.shape[0])), dtype=int)
        track_lons: list[np.ndarray] = []
        track_lats: list[np.ndarray] = []
        for idx in sample_indices:
            mask = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
            if mask.any():
                track_lons.append(lon[idx][mask])
                track_lats.append(lat[idx][mask])
        centroid_lon = np.nanmean(lon, axis=0)
        centroid_lat = np.nanmean(lat, axis=0)
        mask = np.isfinite(centroid_lon) & np.isfinite(centroid_lat)
        return track_lons, track_lats, centroid_lon[mask], centroid_lat[mask]

    def _extract_pygnome_wgs84(
        self,
        nc_path: Path,
        sample_count: int = 80,
    ) -> tuple[list[np.ndarray], list[np.ndarray], np.ndarray, np.ndarray]:
        with xr.open_dataset(nc_path) as ds:
            particle_counts = np.asarray(ds["particle_count"].values, dtype=int)
            time_count = int(ds.sizes["time"])
            particle_count = int(np.nanmax(particle_counts))
            lon_flat = np.asarray(ds["longitude"].values, dtype=float)
            lat_flat = np.asarray(ds["latitude"].values, dtype=float)
        if time_count * particle_count != lon_flat.size:
            raise ValueError(f"Unexpected PyGNOME flattened track layout in {nc_path}")
        lon = lon_flat.reshape(time_count, particle_count).T
        lat = lat_flat.reshape(time_count, particle_count).T
        sample_indices = np.linspace(0, lon.shape[0] - 1, num=max(1, min(sample_count, lon.shape[0])), dtype=int)
        track_lons: list[np.ndarray] = []
        track_lats: list[np.ndarray] = []
        for idx in sample_indices:
            mask = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
            if mask.any():
                track_lons.append(lon[idx][mask])
                track_lats.append(lat[idx][mask])
        centroid_lon = np.nanmean(lon, axis=0)
        centroid_lat = np.nanmean(lat, axis=0)
        mask = np.isfinite(centroid_lon) & np.isfinite(centroid_lat)
        return track_lons, track_lats, centroid_lon[mask], centroid_lat[mask]

    def _plot_track_panel(
        self,
        *,
        destination: Path,
        case_id: str,
        title: str,
        nc_relative_path: str,
        model_key: str,
    ) -> Path | None:
        nc_path = self._resolve(nc_relative_path)
        if not nc_path.exists():
            self._record_missing(nc_path, f"Track source missing for {title}.")
            return None

        if model_key == "pygnome":
            track_lons, track_lats, centroid_lon, centroid_lat = self._extract_pygnome_wgs84(nc_path, sample_count=90)
        else:
            track_lons, track_lats, centroid_lon, centroid_lat = self._extract_opendrift_wgs84(nc_path, sample_count=90)
        if not track_lons or centroid_lon.size == 0:
            self._record_missing(nc_path, f"Track source did not expose usable coordinates for {title}.")
            return None

        colors = self._panel_colors()
        fig, ax = plt.subplots(figsize=(6.4, 4.6), dpi=self._dpi(), facecolor="#ffffff")
        labels_df, bounds, outline = self._case_locator_context(case_id)
        if outline is not None and not outline.empty:
            outline.boundary.plot(ax=ax, linewidth=0.45, color="#94a3b8", alpha=0.8, zorder=1)
        for lon_values, lat_values in zip(track_lons, track_lats):
            ax.plot(lon_values, lat_values, color=colors.get("ensemble_member_path", "#94a3b8"), linewidth=0.55, alpha=0.18, zorder=2)
        ax.plot(centroid_lon, centroid_lat, color=colors.get("centroid_path", "#0f172a"), linewidth=1.9, zorder=3)
        ax.scatter(centroid_lon[0], centroid_lat[0], color=colors.get("source_point", "#dc2626"), s=26, zorder=4)
        ax.scatter(centroid_lon[-1], centroid_lat[-1], color=colors.get("pygnome" if model_key == "pygnome" else "deterministic_opendrift", "#2563eb"), s=30, zorder=4)
        if not labels_df.empty:
            active = labels_df.loc[labels_df["enabled_yes_no"].astype(str).str.lower() == "yes"].copy()
            for _, row in active.iterrows():
                ax.text(float(row["lon"]), float(row["lat"]), str(row["label_text"]), fontsize=6.4, color="#334155")
        min_lon, max_lon, min_lat, max_lat = bounds
        pad_lon = max((max_lon - min_lon) * 0.08, 0.06)
        pad_lat = max((max_lat - min_lat) * 0.08, 0.06)
        ax.set_xlim(min_lon - pad_lon, max_lon + pad_lon)
        ax.set_ylim(min_lat - pad_lat, max_lat + pad_lat)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.grid(True, linestyle="--", linewidth=0.35, alpha=0.3)
        ax.set_title(title, fontsize=11, loc="left")
        fig.tight_layout()
        destination.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(destination, dpi=self._dpi(), bbox_inches="tight")
        plt.close(fig)
        return destination

    def _mindoro_strict_metrics(self) -> list[str]:
        summary = _read_csv(self.repo_root / MINDORO_PHASE3B_SUMMARY)
        if summary.empty:
            return [
                "Strict March 6 metrics summary CSV was not available.",
                "Interpret this board as a sparse stress test, not broad-support validation.",
            ]
        row = summary.iloc[0]
        return [
            f"Strict March 6 remains the hard sparse stress test. Forecast non-zero cells: {int(row.get('forecast_nonzero_cells', 0))}; observed cells: {int(row.get('obs_nonzero_cells', 0))}.",
            f"All FSS windows remain {float(row.get('fss_10km', 0.0)):.2f} or lower in this strict setup, so the key message is stress-test behavior rather than broad overlap skill.",
            "Use this board to explain why the panel should read March 6 as a demanding boundary case.",
        ]

    def _mindoro_pygnome_metrics(self) -> list[str]:
        ranking = _read_csv(self.repo_root / MINDORO_PYGNOME_RANKING)
        if ranking.empty:
            return ["Mindoro benchmark ranking CSV was not available."]
        top = ranking.sort_values("eventcorridor_rank").iloc[0]
        control = ranking.loc[ranking["track_id"] == "C1"].iloc[0] if (ranking["track_id"] == "C1").any() else None
        p50 = ranking.loc[ranking["track_id"] == "C2"].iloc[0] if (ranking["track_id"] == "C2").any() else None
        return [
            f"Across the broader March 4-6 event corridor, {top['model_name']} ranks first with mean FSS {float(top['eventcorridor_mean_fss']):.3f}.",
            f"OpenDrift deterministic event-corridor mean FSS: {float(control['eventcorridor_mean_fss']):.3f}." if control is not None else "",
            f"OpenDrift ensemble p50 event-corridor mean FSS: {float(p50['eventcorridor_mean_fss']):.3f}." if p50 is not None else "",
        ]

    def _dwh_event_metrics(self) -> dict[str, float]:
        table = _read_csv(self.repo_root / DWH_ALL_RESULTS)
        if table.empty:
            return {}
        event_rows = table.loc[table["pair_role"] == "event_corridor"].copy()
        metrics: dict[str, float] = {}
        for _, row in event_rows.iterrows():
            metrics[str(row["model_result"])] = float(row.get("mean_fss", np.nan))
        return metrics

    def _phase4_metric_lines(self) -> list[str]:
        summary = _read_csv(self.repo_root / MINDORO_PHASE4_SUMMARY)
        if summary.empty:
            return ["Mindoro Phase 4 summary CSV was not available."]
        lighter = summary.loc[summary["scenario_id"] == "lighter_oil"].iloc[0]
        base = summary.loc[summary["scenario_id"] == "fixed_base_medium_heavy_proxy"].iloc[0]
        heavier = summary.loc[summary["scenario_id"] == "heavier_oil"].iloc[0]
        return [
            f"Lighter oil reaches {float(lighter['final_evaporated_pct']):.2f}% evaporation and only {float(lighter['total_beached_kg']):.0f} kg beached.",
            f"The fixed base medium-heavy proxy is the flagged follow-up case because max mass-balance deviation reaches {float(base['max_mass_balance_deviation_pct']):.2f}%.",
            f"Heavier oil keeps the largest shoreline burden at about {float(heavier['total_beached_kg']):.0f} kg.",
        ]

    def _phase4_shoreline_lines(self) -> list[str]:
        arrival = _read_csv(self.repo_root / MINDORO_PHASE4_SHORELINE_ARRIVAL)
        if arrival.empty:
            return ["Mindoro Phase 4 shoreline arrival CSV was not available."]
        fastest = arrival.sort_values("first_shoreline_arrival_h").iloc[0]
        largest = arrival.sort_values("total_beached_kg", ascending=False).iloc[0]
        return [
            f"All three oil scenarios strand within about {float(fastest['first_shoreline_arrival_h']):.1f} hours in the current shoreline-aware replay.",
            f"The heaviest shoreline burden is {largest['oil_label']} with about {float(largest['total_beached_kg']):.0f} kg beached.",
            "These outcomes remain inherited-provisional because the upstream Phase 1/2 freeze story is not yet complete.",
        ]

    def _build_phase4_oil_budget_board(self) -> PanelFigureRecord | None:
        summary_png = "output/trajectory_gallery/case_mindoro_retro_2023__phase4__openoil__oil_budget_summary__2023_03_03_to_2023_03_06__all_scenarios__mass_budget_comparison.png"
        lighter_png = "output/trajectory_gallery/case_mindoro_retro_2023__phase4__openoil__oil_budget_timeseries__2023_03_03_to_2023_03_06__lighter_oil__mass_budget_timeseries.png"
        base_png = "output/trajectory_gallery/case_mindoro_retro_2023__phase4__openoil__oil_budget_timeseries__2023_03_03_to_2023_03_06__fixed_base_medium_heavy_proxy__mass_budget_timeseries.png"
        heavier_png = "output/trajectory_gallery/case_mindoro_retro_2023__phase4__openoil__oil_budget_timeseries__2023_03_03_to_2023_03_06__heavier_oil__mass_budget_timeseries.png"
        return self._compose_board(
            board_family_code="E",
            board_title="Mindoro Phase 4 oil-budget comparison",
            subtitle="Mindoro | 3-6 March 2023 | OpenOil replay on the current shoreline-aware transport branch",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            date_token="2023-03-03_to_2023-03-06",
            model_names="openoil",
            run_type="panel_board",
            figure_slug="mindoro_phase4_oil_budget_board",
            panels=[
                self._image_slot("All-scenario final budget snapshot", summary_png),
                self._image_slot("Light oil time series", lighter_png),
                self._image_slot("Base medium-heavy proxy time series", base_png),
                self._image_slot("Heavier oil time series", heavier_png),
            ],
            legend_keys=["oil_lighter", "oil_base", "oil_heavier"],
            metric_lines=self._phase4_metric_lines(),
            interpretation="Mindoro Phase 4 already supports clear oil-type comparisons on the currently reportable transport replay, with the medium-heavy proxy still flagged for mass-balance follow-up.",
            caption="This board compares how the three Mindoro Phase 4 oil scenarios weather over time. The light scenario evaporates fastest, while the heavier scenarios leave a larger shoreline burden. The medium-heavy proxy remains the one scenario that still needs a tighter mass-balance follow-up.",
            notes="Built from stored Phase 4 oil-budget PNG outputs only; no weathering rerun was triggered.",
            recommended=True,
            scenario_id="all_scenarios",
        )

    def _build_phase4_shoreline_board(self) -> PanelFigureRecord | None:
        arrival_png = "output/trajectory_gallery/case_mindoro_retro_2023__phase4__openoil__shoreline_arrival_summary__2023_03_03_to_2023_03_06__all_scenarios__scenario_arrival_bars.png"
        impact_png = "output/trajectory_gallery/case_mindoro_retro_2023__phase4__openoil__shoreline_impact_summary__2023_03_03_to_2023_03_06__all_scenarios__shoreline_impacts.png"
        segment_png = "output/trajectory_gallery/case_mindoro_retro_2023__phase4__openoil__shoreline_segment_impact_map__2023_03_03_to_2023_03_06__all_scenarios__segment_midpoint_impacts.png"
        return self._compose_board(
            board_family_code="F",
            board_title="Mindoro Phase 4 shoreline arrival and impact",
            subtitle="Mindoro | 3-6 March 2023 | shoreline-aware replay with canonical shoreline segments",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            date_token="2023-03-03_to_2023-03-06",
            model_names="openoil",
            run_type="panel_board",
            figure_slug="mindoro_phase4_shoreline_board",
            panels=[
                self._image_slot("Arrival timing summary", arrival_png),
                self._image_slot("Scenario shoreline impacts", impact_png),
                self._image_slot("Segment-level impact map", segment_png),
                self._text_slot(
                    "Panel reading guide",
                    "Read this board from left to right: arrival timing shows how quickly shoreline contact begins, the summary panel compares scenario totals, and the segment map shows where the beached mass clusters along the southeast Mindoro coast.",
                ),
            ],
            legend_keys=["oil_lighter", "oil_base", "oil_heavier"],
            metric_lines=self._phase4_shoreline_lines(),
            interpretation="Shoreline arrival outputs and scenario-specific segment impacts are available now for Mindoro, with heavier oils producing the strongest shoreline burden under the current replay.",
            caption="This board turns the Phase 4 shoreline tables into a plain-language map-and-bars summary. It shows that shoreline contact begins quickly in the current replay and that heavier oils leave the largest stranded burden along the shoreline segment network.",
            notes="Built from stored Phase 4 shoreline summary figures and CSV-derived gallery outputs only.",
            recommended=True,
            scenario_id="all_scenarios",
        )

    def _build_mindoro_strict_board(self) -> PanelFigureRecord | None:
        return self._compose_board(
            board_family_code="A",
            board_title="Mindoro strict March 6 forecast vs observation",
            subtitle="Mindoro | 6 March 2023 | strict sparse stress test | OpenDrift official products",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase3b_strict",
            date_token="2023-03-06",
            model_names="opendrift",
            run_type="panel_board",
            figure_slug="mindoro_strict_march6_board",
            panels=[
                self._image_slot("Observation vs ensemble p50", "output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_obsmask_vs_p50.png"),
                self._image_slot("Source, initialization, validation context", "output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_source_init_validation_overlay.png"),
                self._image_slot("Deterministic path context", "output/trajectory_gallery/case_mindoro_retro_2023__phase2_official__opendrift__deterministic_track_map__2023_03_03_to_2023_03_06__sampled_particle_paths.png"),
                self._text_slot("Why this board matters", "This is intentionally the hardest Mindoro validation slice. Sparse overlap here does not invalidate the broader workflow; it shows how the official products behave under the most demanding single-day public-observation target."),
            ],
            legend_keys=["observed_mask", "ensemble_p50", "source_point", "initialization_polygon", "validation_polygon", "deterministic_opendrift"],
            metric_lines=self._mindoro_strict_metrics(),
            interpretation="The strict March 6 board is presentation-ready as an honesty board: it shows a hard sparse case where the official products are intentionally stressed and should not be oversold.",
            caption="Use this board early in the defense to explain the strict March 6 test. It is the most demanding public-observation slice, so the panel should read it as a stress test rather than as a summary of overall Mindoro performance.",
            notes="Built from stored Mindoro strict Phase 3B QA figures and the existing raw deterministic track panel.",
            recommended=True,
        )

    def _build_mindoro_eventcorridor_board(self) -> PanelFigureRecord | None:
        return self._compose_board(
            board_family_code="B",
            board_title="Mindoro March 4-6 event corridor",
            subtitle="Mindoro | 4-6 March 2023 | broader public-support context",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase3b_support",
            date_token="2023-03-04_to_2023-03-06",
            model_names="opendrift_vs_pygnome",
            run_type="panel_board",
            figure_slug="mindoro_eventcorridor_board",
            panels=[
                self._image_slot("Daily public-observation comparison", "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/qa_pygnome_public_comparison_overlays.png"),
                self._image_slot("March 4-6 event-corridor union", "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/qa_pygnome_public_comparison_eventcorridor_overlay.png"),
                self._image_slot("Corridor and hull context", "output/trajectory_gallery/case_mindoro_retro_2023__phase2_phase3b__opendrift__corridor_hull_view__2023_03_06__p50_p90_hull_overlay.png"),
                self._text_slot("How to read this board", "The top row shows the broader public-observation framing that complements the strict March 6 test. The lower-left panel connects that support track back to the ensemble corridor geometry used in the transport story."),
            ],
            legend_keys=["observed_mask", "deterministic_opendrift", "ensemble_p50", "pygnome", "corridor_hull"],
            metric_lines=self._mindoro_pygnome_metrics(),
            interpretation="The Mindoro event-corridor board gives the panel a broader visual context than the strict March 6 board alone and shows why the support track is still scientifically informative.",
            caption="This board broadens the story from the strict March 6 snapshot to the full March 4-6 public-observation corridor. It helps the panel see where support-track agreement exists even though the single-day March 6 board is intentionally sparse.",
            notes="Built from stored Mindoro PyGNOME comparison QA figures and the raw corridor/hull gallery panel.",
            recommended=False,
        )

    def _build_mindoro_model_comparison_board(self) -> PanelFigureRecord | None:
        return self._compose_board(
            board_family_code="C",
            board_title="Mindoro OpenDrift vs PyGNOME comparison",
            subtitle="Mindoro | 4-6 March 2023 | comparator framing, not truth replacement",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase3a_benchmark",
            date_token="2023-03-04_to_2023-03-06",
            model_names="opendrift_vs_pygnome",
            run_type="panel_board",
            figure_slug="mindoro_model_comparison_board",
            panels=[
                self._image_slot("Observation | deterministic | p50 | PyGNOME", "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/qa_pygnome_public_comparison_overlays.png"),
                self._image_slot("Event-corridor comparison", "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/qa_pygnome_public_comparison_eventcorridor_overlay.png"),
                self._image_slot("Strict March 6 context", "output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_obsmask_vs_p50.png"),
                self._text_slot("Comparator interpretation", "PyGNOME is included as a cross-model comparator, not as truth. In the broader March 4-6 event corridor, PyGNOME ranks first within this local comparator bundle, while the official OpenDrift products remain the reportable transport path."),
            ],
            legend_keys=["observed_mask", "deterministic_opendrift", "ensemble_p50", "pygnome"],
            metric_lines=self._mindoro_pygnome_metrics(),
            interpretation="This board is ready for panel presentation because it makes the comparator role of PyGNOME explicit while preserving the official OpenDrift framing.",
            caption="Use this board when the panel asks how OpenDrift compares with PyGNOME on the same public-observation corridor. It is a comparator discussion board, not a model-truth board.",
            notes="Built from stored Mindoro public-comparison QA figures and the strict March 6 overlay.",
            recommended=True,
        )

    def _build_mindoro_trajectory_board(self) -> PanelFigureRecord | None:
        return self._compose_board(
            board_family_code="D",
            board_title="Mindoro transport trajectory views",
            subtitle="Mindoro | 3-6 March 2023 | deterministic path, sampled ensemble, and corridor view",
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase2_official",
            date_token="2023-03-03_to_2023-03-06",
            model_names="opendrift",
            run_type="panel_board",
            figure_slug="mindoro_trajectory_board",
            panels=[
                self._image_slot("Deterministic path", "output/trajectory_gallery/case_mindoro_retro_2023__phase2_official__opendrift__deterministic_track_map__2023_03_03_to_2023_03_06__sampled_particle_paths.png"),
                self._image_slot("Sampled ensemble member centroids", "output/trajectory_gallery/case_mindoro_retro_2023__phase2_official__opendrift__ensemble_sampled_member_centroids__2023_03_03_to_2023_03_06__member_centroid_paths.png"),
                self._image_slot("Centroid corridor and hull", "output/trajectory_gallery/case_mindoro_retro_2023__phase2_phase3b__opendrift__corridor_hull_view__2023_03_06__p50_p90_hull_overlay.png"),
                self._text_slot("Transport framing", "This board is useful before any score tables: it shows the transport path, the spread across sampled members, and the final corridor geometry that feeds later Phase 3 and Phase 4 interpretation."),
            ],
            legend_keys=["deterministic_opendrift", "ensemble_member_path", "centroid_path", "corridor_hull", "source_point"],
            metric_lines=[
                "This board is presentation-ready because it gives the panel an intuitive transport picture before any validation metrics.",
                "The trajectory family is scientifically usable, but it still inherits the not-yet-frozen Phase 2 baseline story.",
            ],
            interpretation="The Mindoro trajectory board is one of the clearest early-slide figures because it shows path, spread, and corridor structure without relying on score-table literacy.",
            caption="This board gives the panel an intuitive view of the Mindoro transport story. It shows where the deterministic path goes, how sampled ensemble members spread around that path, and how the corridor view summarizes the final footprint geometry.",
            notes="Built from the existing raw trajectory gallery panels.",
            recommended=True,
        )

    def _build_dwh_deterministic_board(self) -> PanelFigureRecord | None:
        summary = _read_csv(self.repo_root / DWH_SUMMARY)
        metric_lines = [
            "DWH is the rich-data external transfer-validation case and remains one of the strongest reportable parts of the project.",
        ]
        if not summary.empty:
            event = summary.loc[summary["pair_role"] == "event_corridor"].iloc[0]
            metric_lines.append(f"Deterministic event-corridor mean FSS is {float(np.mean([event['fss_1km'], event['fss_3km'], event['fss_5km'], event['fss_10km']])):.3f}.")
        metric_lines.append("This board is the easiest way to show the panel that the workflow transfers to a richer external case.")
        return self._compose_board(
            board_family_code="G",
            board_title="DWH deterministic forecast vs observation",
            subtitle="Deepwater Horizon | 21-23 May 2010 | external transfer-validation case",
            case_id="CASE_DWH_RETRO_2010_72H",
            phase_or_track="phase3c_external_case_run",
            date_token="2010-05-21_to_2010-05-23",
            model_names="opendrift",
            run_type="panel_board",
            figure_slug="dwh_deterministic_board",
            panels=[
                self._image_slot("Per-date deterministic overlays", "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/qa_phase3c_overlays.png"),
                self._image_slot("Event-corridor overlay", "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/qa_phase3c_eventcorridor_overlay.png"),
                self._image_slot("Deterministic path map", "output/trajectory_gallery/case_dwh_retro_2010_72h__phase3c_external_case_run__opendrift__deterministic_track_map__2010_05_20_to_2010_05_23__sampled_particle_paths.png"),
                self._text_slot("Board reading guide", "Read the DWH board as a transfer-validation success story: daily and event-corridor overlays show meaningful overlap against public observation masks, while the trajectory map shows the transport context behind those masks."),
            ],
            legend_keys=["observed_mask", "deterministic_opendrift", "source_point"],
            metric_lines=metric_lines,
            interpretation="The DWH deterministic board is panel-ready and supports the plain-language claim that the transport framework transfers successfully to a richer external case.",
            caption="This board shows the strongest single external-case validation story in the project. It pairs daily and event-corridor overlap views with the underlying deterministic transport path so non-technical readers can follow the transfer-validation narrative.",
            notes="Built from stored DWH deterministic QA figures and the raw deterministic track map.",
            recommended=True,
        )

    def _build_dwh_det_vs_ensemble_board(self) -> PanelFigureRecord | None:
        metrics = self._dwh_event_metrics()
        return self._compose_board(
            board_family_code="H",
            board_title="DWH deterministic vs ensemble comparison",
            subtitle="Deepwater Horizon | 21-23 May 2010 | deterministic, p50, and p90 comparison",
            case_id="CASE_DWH_RETRO_2010_72H",
            phase_or_track="phase3c_ensemble",
            date_token="2010-05-21_to_2010-05-23",
            model_names="opendrift",
            run_type="panel_board",
            figure_slug="dwh_deterministic_vs_ensemble_board",
            panels=[
                self._image_slot("Deterministic event-corridor reference", "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/qa_phase3c_eventcorridor_overlay.png"),
                self._image_slot("Per-date p50 and p90 overlays", "output/trajectory_gallery/case_dwh_retro_2010_72h__phase3c_ensemble__opendrift__ensemble_overlay__2010_05_21_to_2010_05_23__p50_p90_overlays.png"),
                self._image_slot("Event-corridor p50 and p90 overlay", "output/trajectory_gallery/case_dwh_retro_2010_72h__phase3c_ensemble__opendrift__ensemble_overlay__2010_05_21_to_2010_05_23__eventcorridor_overlay.png"),
                self._text_slot("Model-selection note", "On this DWH definition, the deterministic event corridor remains slightly ahead of p50 on mean FSS, while p90 shows the broadest but loosest envelope. This helps the panel understand what p50 and p90 mean visually."),
            ],
            legend_keys=["deterministic_opendrift", "ensemble_p50", "ensemble_p90", "observed_mask"],
            metric_lines=[
                f"Deterministic event-corridor mean FSS: {metrics.get('OpenDrift deterministic', float('nan')):.3f}.",
                f"Ensemble p50 event-corridor mean FSS: {metrics.get('OpenDrift ensemble p50', float('nan')):.3f}.",
                f"Ensemble p90 event-corridor mean FSS: {metrics.get('OpenDrift ensemble p90', float('nan')):.3f}.",
            ],
            interpretation="This board is ready for the panel because it explains the practical meaning of deterministic, p50, and p90 products side by side on the DWH case.",
            caption="This board explains how the DWH deterministic, p50, and p90 products differ. It is especially useful when the panel wants to understand why the ensemble products are not simply 'better' or 'worse' than the deterministic control.",
            notes="Built from stored DWH deterministic and ensemble QA figures plus the raw gallery overlays.",
            recommended=True,
        )

    def _build_dwh_model_comparison_board(self) -> PanelFigureRecord | None:
        metrics = self._dwh_event_metrics()
        return self._compose_board(
            board_family_code="I",
            board_title="DWH OpenDrift vs PyGNOME comparison",
            subtitle="Deepwater Horizon | 21-23 May 2010 | comparator framing on the rich-data case",
            case_id="CASE_DWH_RETRO_2010_72H",
            phase_or_track="phase3c_pygnome_comparator",
            date_token="2010-05-21_to_2010-05-23",
            model_names="opendrift_vs_pygnome",
            run_type="panel_board",
            figure_slug="dwh_model_comparison_board",
            panels=[
                self._image_slot("Per-date comparison panels", "output/trajectory_gallery/case_dwh_retro_2010_72h__phase3c_pygnome_comparator__opendrift_vs_pygnome__comparison_overlay__2010_05_21_to_2010_05_23__per_date_overlays.png"),
                self._image_slot("Event-corridor comparison", "output/trajectory_gallery/case_dwh_retro_2010_72h__phase3c_pygnome_comparator__opendrift_vs_pygnome__comparison_overlay__2010_05_21_to_2010_05_23__eventcorridor_overlay.png"),
                self._image_slot("Deterministic reference board", "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/qa_phase3c_eventcorridor_overlay.png"),
                self._text_slot("Comparator interpretation", "PyGNOME is a useful comparator on DWH, but the current OpenDrift products remain the reportable transport path. This board lets the panel compare both model families against the same public observation corridor."),
            ],
            legend_keys=["observed_mask", "deterministic_opendrift", "ensemble_p50", "pygnome"],
            metric_lines=[
                f"OpenDrift deterministic event-corridor mean FSS: {metrics.get('OpenDrift deterministic', float('nan')):.3f}.",
                f"OpenDrift ensemble p50 event-corridor mean FSS: {metrics.get('OpenDrift ensemble p50', float('nan')):.3f}.",
                f"PyGNOME deterministic event-corridor mean FSS: {metrics.get('PyGNOME deterministic', float('nan')):.3f}.",
            ],
            interpretation="The DWH model-comparison board is presentation-ready because it clearly positions PyGNOME as a comparator and keeps the OpenDrift-rich-data success story visible.",
            caption="Use this board when the panel wants a direct OpenDrift-versus-PyGNOME comparison on the DWH case. It keeps the comparator framing explicit while showing that both models are being judged against the same public observation masks.",
            notes="Built from stored DWH comparator QA figures and the deterministic event-corridor overlay.",
            recommended=True,
        )

    def _build_dwh_trajectory_board(self) -> PanelFigureRecord | None:
        pygnome_track_path = self._plot_track_panel(
            destination=self.output_dir / "_support" / "case_dwh_retro_2010_72h__phase3c_pygnome_comparator__pygnome__sampled_track_panel__2010_05_20_to_2010_05_23__support__dwh_pygnome_track.png",
            case_id="CASE_DWH_RETRO_2010_72H",
            title="PyGNOME sampled track view",
            nc_relative_path="output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/tracks/pygnome_dwh_phase3c.nc",
            model_key="pygnome",
        )
        panels = [
            self._image_slot("OpenDrift deterministic path", "output/trajectory_gallery/case_dwh_retro_2010_72h__phase3c_external_case_run__opendrift__deterministic_track_map__2010_05_20_to_2010_05_23__sampled_particle_paths.png"),
            self._image_slot("OpenDrift ensemble p50/p90 footprint context", "output/trajectory_gallery/case_dwh_retro_2010_72h__phase3c_ensemble__opendrift__ensemble_overlay__2010_05_21_to_2010_05_23__p50_p90_overlays.png"),
            self._image_slot("PyGNOME sampled trajectory view", _relative_to_repo(self.repo_root, pygnome_track_path) if pygnome_track_path else ""),
            self._text_slot("Why this board exists", "This is the cleanest trajectory-only board for DWH. It places the deterministic transport path, the ensemble footprint context, and the PyGNOME track family side by side so the panel can compare model behavior before looking at overlap scores."),
        ]
        return self._compose_board(
            board_family_code="J",
            board_title="DWH trajectory views",
            subtitle="Deepwater Horizon | 20-23 May 2010 | deterministic transport, ensemble footprint context, and PyGNOME tracks",
            case_id="CASE_DWH_RETRO_2010_72H",
            phase_or_track="phase3c_trajectories",
            date_token="2010-05-20_to_2010-05-23",
            model_names="opendrift_vs_pygnome",
            run_type="panel_board",
            figure_slug="dwh_trajectory_board",
            panels=panels,
            legend_keys=["deterministic_opendrift", "ensemble_p50", "ensemble_p90", "pygnome", "source_point"],
            metric_lines=[
                "This board is more about transport intuition than about headline metrics.",
                "Use it when the panel wants to see how the DWH particle clouds travel before the overlap products are thresholded or summarized.",
            ],
            interpretation="The DWH trajectory board is useful appendix-support material because it makes the model-path behavior visible before score comparisons.",
            caption="This board is the DWH trajectory-oriented companion to the score-based comparison boards. It gives the panel a more intuitive view of how the OpenDrift and PyGNOME particle families move over time.",
            notes="Built from the raw DWH deterministic track map, the raw ensemble overlay, and a new read-only sampled PyGNOME track panel derived from the stored comparator NetCDF.",
            recommended=False,
        )

    def _build_boards(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        builders = [
            self._build_mindoro_strict_board,
            self._build_mindoro_eventcorridor_board,
            self._build_mindoro_model_comparison_board,
            self._build_mindoro_trajectory_board,
            self._build_phase4_oil_budget_board,
            self._build_phase4_shoreline_board,
            self._build_dwh_deterministic_board,
            self._build_dwh_det_vs_ensemble_board,
            self._build_dwh_model_comparison_board,
            self._build_dwh_trajectory_board,
        ]
        for builder in builders:
            builder()

    def _build_captions_markdown(self) -> str:
        lines = [
            "# Panel Figure Captions",
            "",
            "These polished boards are read-only reinterpretations of existing outputs. No expensive scientific branch was rerun to generate this panel pack.",
            "",
        ]
        for record in sorted(self.panel_records, key=lambda item: item.figure_id):
            lines.append(f"## {record.board_family_code}. {record.board_family_label}")
            lines.append("")
            lines.append(f"- File: `{record.relative_path}`")
            lines.append(f"- Main-defense figure: `{str(record.recommended_for_main_defense).lower()}`")
            lines.append(f"- Interpretation: {record.plain_language_interpretation}")
            lines.append("")
        return "\n".join(lines)

    def _build_talking_points_markdown(self) -> str:
        recommended = [record for record in self.panel_records if record.recommended_for_main_defense]
        lines = [
            "# Panel Figure Talking Points",
            "",
            "Recommended first-pass figures for the main defense presentation:",
            "",
        ]
        for record in recommended:
            lines.append(f"- `{record.figure_id}`: {record.plain_language_interpretation}")
        lines.extend(
            [
                "",
                "Appendix-support figures:",
                "",
            ]
        )
        for record in self.panel_records:
            if not record.recommended_for_main_defense:
                lines.append(f"- `{record.figure_id}`: {record.plain_language_interpretation}")
        return "\n".join(lines)

    def _build_manifest(self, generated_at_utc: str) -> dict[str, Any]:
        return {
            "phase": PHASE,
            "generated_at_utc": generated_at_utc,
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "built_from_existing_outputs_only": True,
            "expensive_scientific_reruns_triggered": False,
            "side_by_side_comparison_boards_produced": True,
            "plain_language_captions_produced": True,
            "style_config_path": _relative_to_repo(self.repo_root, self.repo_root / STYLE_CONFIG_PATH),
            "label_config_paths": [
                _relative_to_repo(self.repo_root, self.repo_root / MINDORO_LABELS_PATH),
                _relative_to_repo(self.repo_root, self.repo_root / DWH_LABELS_PATH),
            ],
            "documented_visual_grammar": {
                "palette": self.style.get("palette", {}),
                "legend_labels": self.style.get("legend_labels", {}),
            },
            "board_families_requested": BOARD_FAMILIES,
            "board_families_generated": sorted({record.board_family_code for record in self.panel_records}),
            "recommended_main_defense_figures": [record.figure_id for record in self.panel_records if record.recommended_for_main_defense],
            "missing_optional_artifacts": self.missing_optional_artifacts,
            "figures": [record.as_row() for record in self.panel_records],
        }

    def run(self) -> dict[str, Any]:
        generated_at_utc = pd.Timestamp.now(tz="UTC").isoformat()
        self._build_boards()
        figure_rows = [record.as_row() for record in sorted(self.panel_records, key=lambda item: item.figure_id)]
        registry_path = self.output_dir / "panel_figure_registry.csv"
        manifest_path = self.output_dir / "panel_figure_manifest.json"
        captions_path = self.output_dir / "panel_figure_captions.md"
        talking_points_path = self.output_dir / "panel_figure_talking_points.md"
        _write_csv(
            registry_path,
            figure_rows,
            columns=[
                "figure_id",
                "board_family_code",
                "board_family_label",
                "case_id",
                "phase_or_track",
                "date_token",
                "model_names",
                "run_type",
                "scenario_id",
                "variant",
                "relative_path",
                "file_path",
                "plain_language_interpretation",
                "recommended_for_main_defense",
                "source_paths",
                "notes",
            ],
        )
        _write_json(manifest_path, self._build_manifest(generated_at_utc))
        _write_text(captions_path, self._build_captions_markdown())
        _write_text(talking_points_path, self._build_talking_points_markdown())
        return {
            "output_dir": str(self.output_dir),
            "registry_csv": str(registry_path),
            "manifest_json": str(manifest_path),
            "captions_md": str(captions_path),
            "talking_points_md": str(talking_points_path),
            "figure_count": len(self.panel_records),
            "board_families_generated": sorted({record.board_family_code for record in self.panel_records}),
            "recommended_main_defense_figures": [record.figure_id for record in self.panel_records if record.recommended_for_main_defense],
            "side_by_side_comparison_boards_produced": True,
            "plain_language_captions_produced": True,
            "missing_optional_artifacts": self.missing_optional_artifacts,
        }


def run_trajectory_gallery_panel_polish(
    repo_root: str | Path = ".",
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    return TrajectoryGalleryPanelPolishService(repo_root=repo_root, output_dir=output_dir).run()
