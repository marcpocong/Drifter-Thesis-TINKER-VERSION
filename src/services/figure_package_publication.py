"""Publication-grade figure package built from existing outputs only."""

from __future__ import annotations

import json
import shutil
import textwrap
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import yaml
from matplotlib import pyplot as plt
from matplotlib.colors import ListedColormap
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Rectangle
from pyproj import Transformer
from rasterio.plot import show
from rasterio.warp import transform_bounds
from shapely.geometry import MultiPoint

from src.core.artifact_status import artifact_status_columns, artifact_status_columns_for_key
from src.services.mindoro_primary_validation_metadata import (
    MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH,
    MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
    MINDORO_SHARED_IMAGERY_CAVEAT,
)

matplotlib.use("Agg")

PHASE = "figure_package_publication"
OUTPUT_DIR = Path("output") / "figure_package_publication"
LOCAL_FONT_DIR = Path("output") / "_local_fonts"
STYLE_CONFIG_PATH = Path("config") / "publication_figure_style.yaml"
MINDORO_LABELS_PATH = Path("config") / "publication_map_labels_mindoro.csv"
DWH_LABELS_PATH = Path("config") / "publication_map_labels_dwh.csv"

FINAL_REPRO_DIR = Path("output") / "final_reproducibility_package"
FINAL_PHASE_STATUS_CSV = FINAL_REPRO_DIR / "final_phase_status_registry.csv"

MINDORO_FORECAST_MANIFEST = Path("output") / "CASE_MINDORO_RETRO_2023" / "forecast" / "forecast_manifest.json"
MINDORO_PHASE3B_SUMMARY = Path("output") / "CASE_MINDORO_RETRO_2023" / "phase3b" / "phase3b_summary.csv"
MINDORO_REINIT_SUMMARY = (
    Path("output") / "CASE_MINDORO_RETRO_2023" / "phase3b_extended_public_scored_march13_14_reinit" / "march13_14_reinit_summary.csv"
)
MINDORO_REINIT_RUN_MANIFEST = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit"
    / "march13_14_reinit_run_manifest.json"
)
MINDORO_REINIT_CROSSMODEL_SUMMARY = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison"
    / "march13_14_reinit_crossmodel_summary.csv"
)
MINDORO_REINIT_CROSSMODEL_RUN_MANIFEST = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison"
    / "march13_14_reinit_crossmodel_run_manifest.json"
)
MINDORO_PHASE4_DIR = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023"
MINDORO_PHASE4_MANIFEST = MINDORO_PHASE4_DIR / "phase4_run_manifest.json"
PHASE4_CROSSMODEL_AUDIT_DIR = Path("output") / "phase4_crossmodel_comparability_audit"
PHASE4_CROSSMODEL_MATRIX = PHASE4_CROSSMODEL_AUDIT_DIR / "phase4_crossmodel_comparability_matrix.csv"
PHASE4_CROSSMODEL_VERDICT = PHASE4_CROSSMODEL_AUDIT_DIR / "phase4_crossmodel_final_verdict.md"
PHASE4_CROSSMODEL_BLOCKERS = PHASE4_CROSSMODEL_AUDIT_DIR / "phase4_crossmodel_blockers.md"

DWH_SETUP_DIR = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_setup"
DWH_RUN_DIR = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run"
DWH_ENSEMBLE_DIR = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_ensemble_comparison"
DWH_PYGNOME_DIR = Path("output") / "CASE_DWH_RETRO_2010_72H" / "phase3c_dwh_pygnome_comparator"
DWH_RUN_MANIFEST = DWH_RUN_DIR / "phase3c_run_manifest.json"
DWH_SUMMARY = DWH_RUN_DIR / "phase3c_summary.csv"
DWH_ALL_RESULTS = DWH_PYGNOME_DIR / "phase3c_dwh_all_results_table.csv"
PREFERRED_PROTOTYPE_SIMILARITY_DIR = Path("output") / "prototype_2021_pygnome_similarity"
LEGACY_PROTOTYPE_SIMILARITY_DIR = Path("output") / "prototype_2016_pygnome_similarity"

FIGURE_FAMILIES: dict[str, str] = {
    "A": "Mindoro March 13 -> March 14 primary validation package",
    "B": "Mindoro March 13 -> March 14 cross-model publication package",
    "C": "Mindoro legacy March 6 honesty / limitations package",
    "D": "Mindoro trajectory publication package",
    "E": "Mindoro Phase 4 OpenDrift-only publication package",
    "F": "Mindoro Phase 4 cross-model deferred note package",
    "G": "DWH deterministic publication package",
    "H": "DWH deterministic vs ensemble publication package",
    "I": "DWH OpenDrift vs PyGNOME publication package",
    "J": "DWH trajectory publication package",
    "K": "Prototype accepted-segment OpenDrift vs PyGNOME support package",
}


@dataclass
class PublicationFigureRecord:
    figure_id: str
    figure_family_code: str
    figure_family_label: str
    case_id: str
    phase_or_track: str
    date_token: str
    model_names: str
    run_type: str
    scenario_id: str
    view_type: str
    variant: str
    relative_path: str
    file_path: str
    pixel_width: int
    pixel_height: int
    short_plain_language_interpretation: str
    recommended_for_main_defense: bool
    recommended_for_paper: bool
    source_paths: str
    notes: str
    status_key: str
    status_label: str
    status_role: str
    status_reportability: str
    status_official_status: str
    status_frozen_status: str
    status_provenance: str
    status_panel_text: str
    status_dashboard_summary: str

    def as_row(self) -> dict[str, Any]:
        return {
            "figure_id": self.figure_id,
            "figure_family_code": self.figure_family_code,
            "figure_family_label": self.figure_family_label,
            "case_id": self.case_id,
            "phase_or_track": self.phase_or_track,
            "date_token": self.date_token,
            "model_names": self.model_names,
            "run_type": self.run_type,
            "scenario_id": self.scenario_id,
            "view_type": self.view_type,
            "variant": self.variant,
            "relative_path": self.relative_path,
            "file_path": self.file_path,
            "pixel_width": self.pixel_width,
            "pixel_height": self.pixel_height,
            "short_plain_language_interpretation": self.short_plain_language_interpretation,
            "recommended_for_main_defense": self.recommended_for_main_defense,
            "recommended_for_paper": self.recommended_for_paper,
            "source_paths": self.source_paths,
            "notes": self.notes,
            "status_key": self.status_key,
            "status_label": self.status_label,
            "status_role": self.status_role,
            "status_reportability": self.status_reportability,
            "status_official_status": self.status_official_status,
            "status_frozen_status": self.status_frozen_status,
            "status_provenance": self.status_provenance,
            "status_panel_text": self.status_panel_text,
            "status_dashboard_summary": self.status_dashboard_summary,
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


def build_publication_figure_filename(
    *,
    case_id: str,
    phase_or_track: str,
    model_name: str,
    run_type: str,
    date_token: str,
    figure_slug: str,
    view_type: str,
    scenario_id: str = "",
    variant: str = "",
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
    tokens.append(_safe_token(view_type))
    variant_token = _safe_token(variant)
    if variant_token:
        tokens.append(variant_token)
    tokens.append(_safe_token(figure_slug))
    return "__".join(token for token in tokens if token) + f".{extension.lstrip('.')}"


def load_publication_style_config(path: str | Path = STYLE_CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    payload = _read_yaml(config_path)
    if not payload:
        raise ValueError(f"Missing publication figure style configuration: {config_path}")
    for key in ("palette", "legend_labels", "layout", "typography", "crop_rules", "locator_rules"):
        if key not in payload:
            raise ValueError(f"Publication figure style configuration is missing '{key}': {config_path}")
    return payload


def apply_publication_typography(style: dict[str, Any], repo_root: str | Path) -> str:
    repo_root_path = Path(repo_root).resolve()
    typography = style.get("typography") or {}
    font_family = str(typography.get("font_family") or "Arial").strip() or "Arial"
    fallbacks = [
        str(item).strip()
        for item in (typography.get("font_fallbacks") or ["DejaVu Sans", "Liberation Sans", "sans-serif"])
        if str(item).strip()
    ]
    candidate_paths: list[Path] = []
    for value in typography.get("font_paths") or []:
        path = Path(value)
        if not path.is_absolute():
            path = repo_root_path / path
        candidate_paths.append(path.resolve())
    local_font_dir = repo_root_path / LOCAL_FONT_DIR
    if local_font_dir.exists():
        candidate_paths.extend(sorted(local_font_dir.glob("*.ttf")))
        candidate_paths.extend(sorted(local_font_dir.glob("*.otf")))
    seen: set[Path] = set()
    for path in candidate_paths:
        if not path.exists():
            continue
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        try:
            matplotlib.font_manager.fontManager.addfont(str(resolved))
        except Exception:
            continue
    resolved_family = font_family
    try:
        matplotlib.font_manager.findfont(font_family, fallback_to_default=False)
    except Exception:
        resolved_family = fallbacks[0] if fallbacks else "sans-serif"
    sans_serif = [resolved_family]
    for candidate in [font_family, *fallbacks]:
        if candidate and candidate not in sans_serif:
            sans_serif.append(candidate)
    matplotlib.rcParams["font.family"] = [resolved_family]
    matplotlib.rcParams["font.sans-serif"] = sans_serif
    return resolved_family


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(result):
        return default
    return result


def _optional_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(result):
        return None
    return result


class FigurePackagePublicationService:
    def __init__(self, repo_root: str | Path = ".", output_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_DIR
        self.style = load_publication_style_config(self.repo_root / STYLE_CONFIG_PATH)
        self._configure_typography()
        self.mindoro_labels = _read_csv(self.repo_root / MINDORO_LABELS_PATH)
        self.dwh_labels = _read_csv(self.repo_root / DWH_LABELS_PATH)
        self.mindoro_forecast_manifest = _read_json(self.repo_root / MINDORO_FORECAST_MANIFEST)
        self.mindoro_phase3b_summary = _read_csv(self.repo_root / MINDORO_PHASE3B_SUMMARY)
        self.mindoro_reinit_summary = _read_csv(self.repo_root / MINDORO_REINIT_SUMMARY)
        self.mindoro_reinit_manifest = _read_json(self.repo_root / MINDORO_REINIT_RUN_MANIFEST)
        self.mindoro_phase1_confirmation_candidate = _read_yaml(
            self.repo_root / MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH
        )
        self.mindoro_reinit_crossmodel_summary = _read_csv(self.repo_root / MINDORO_REINIT_CROSSMODEL_SUMMARY)
        self.mindoro_reinit_crossmodel_manifest = _read_json(self.repo_root / MINDORO_REINIT_CROSSMODEL_RUN_MANIFEST)
        self.mindoro_phase4_manifest = _read_json(self.repo_root / MINDORO_PHASE4_MANIFEST)
        self.dwh_run_manifest = _read_json(self.repo_root / DWH_RUN_MANIFEST)
        self.dwh_summary = _read_csv(self.repo_root / DWH_SUMMARY)
        self.dwh_all_results = _read_csv(self.repo_root / DWH_ALL_RESULTS)
        self.prototype_similarity_dir, self.prototype_similarity_registry_path = self._resolve_prototype_support_dir()
        self.prototype_similarity_figure_registry = _read_csv(self.prototype_similarity_registry_path)
        self.final_phase_status = _read_csv(self.repo_root / FINAL_PHASE_STATUS_CSV)
        self.figure_records: list[PublicationFigureRecord] = []
        self.missing_optional_artifacts: list[dict[str, str]] = []
        self._raster_cache: dict[Path, dict[str, Any]] = {}
        self._vector_cache: dict[tuple[Path, str], gpd.GeoDataFrame] = {}

    def _resolve_prototype_support_dir(self) -> tuple[Path, Path]:
        preferred_dir = self.repo_root / PREFERRED_PROTOTYPE_SIMILARITY_DIR
        preferred_registry = preferred_dir / "prototype_pygnome_figure_registry.csv"
        if preferred_registry.exists():
            return preferred_dir, preferred_registry
        legacy_dir = self.repo_root / LEGACY_PROTOTYPE_SIMILARITY_DIR
        legacy_registry = legacy_dir / "prototype_pygnome_figure_registry.csv"
        return legacy_dir, legacy_registry

    def _configure_typography(self) -> None:
        apply_publication_typography(self.style, self.repo_root)

    def _record_missing(self, path: Path, notes: str) -> None:
        entry = {"relative_path": _relative_to_repo(self.repo_root, path), "notes": notes}
        if entry not in self.missing_optional_artifacts:
            self.missing_optional_artifacts.append(entry)

    def _resolve(self, value: str | Path) -> Path:
        path = Path(value)
        if not path.is_absolute():
            path = self.repo_root / path
        return path.resolve()

    def _palette(self) -> dict[str, str]:
        return self.style.get("palette", {})

    def _legend_labels(self) -> dict[str, str]:
        return self.style.get("legend_labels", {})

    def _board_size(self) -> tuple[float, float]:
        values = (self.style.get("layout") or {}).get("board_size_inches") or [16, 9]
        return float(values[0]), float(values[1])

    def _single_size(self) -> tuple[float, float]:
        values = (self.style.get("layout") or {}).get("single_size_inches") or [13, 8]
        return float(values[0]), float(values[1])

    def _dpi(self) -> int:
        return int((self.style.get("layout") or {}).get("dpi") or 220)

    def _crop_rules(self) -> dict[str, Any]:
        return self.style.get("crop_rules", {})

    def _locator_rules(self) -> dict[str, Any]:
        return self.style.get("locator_rules", {})

    def _load_raster_mask(self, path: Path) -> dict[str, Any] | None:
        if path in self._raster_cache:
            return self._raster_cache[path]
        if not path.exists():
            self._record_missing(path, "Optional raster source missing for the publication figure package.")
            return None
        with rasterio.open(path) as dataset:
            array = dataset.read(1)
            valid_mask = np.isfinite(array) & (array > 0)
            nonzero_bounds: tuple[float, float, float, float] | None = None
            if valid_mask.any():
                rows, cols = np.where(valid_mask)
                xs, ys = rasterio.transform.xy(dataset.transform, rows, cols)
                nonzero_bounds = (
                    float(np.min(xs)),
                    float(np.min(ys)),
                    float(np.max(xs)),
                    float(np.max(ys)),
                )
            x_coords = dataset.bounds.left + (np.arange(dataset.width) + 0.5) * dataset.res[0]
            y_coords = dataset.bounds.top - (np.arange(dataset.height) + 0.5) * abs(dataset.res[1])
            info = {
                "path": path,
                "array": array,
                "mask": valid_mask,
                "transform": dataset.transform,
                "bounds": tuple(dataset.bounds),
                "nonzero_bounds": nonzero_bounds,
                "crs": str(dataset.crs),
                "x_coords": np.asarray(x_coords, dtype=float),
                "y_coords": np.asarray(y_coords, dtype=float),
            }
        self._raster_cache[path] = info
        return info

    def _load_vector(self, path: Path, target_crs: str) -> gpd.GeoDataFrame | None:
        key = (path.resolve(), target_crs)
        if key in self._vector_cache:
            return self._vector_cache[key]
        if not path.exists():
            self._record_missing(path, "Optional vector source missing for the publication figure package.")
            return None
        gdf = gpd.read_file(path)
        if gdf.empty:
            return None
        if target_crs and gdf.crs and str(gdf.crs) != target_crs:
            gdf = gdf.to_crs(target_crs)
        self._vector_cache[key] = gdf
        return gdf

    def _phase_status_flag(self, phase_id: str, track_id: str, column: str, default: Any = "") -> Any:
        if self.final_phase_status.empty:
            return default
        mask = (self.final_phase_status["phase_id"] == phase_id) & (self.final_phase_status["track_id"] == track_id)
        if not mask.any():
            return default
        row = self.final_phase_status.loc[mask].iloc[0]
        return row.get(column, default)

    def _projected_bounds_from_grid(
        self,
        grid: dict[str, Any],
        default: tuple[float, float, float, float],
    ) -> tuple[float, float, float, float]:
        explicit = (
            _optional_float(grid.get("min_x")),
            _optional_float(grid.get("min_y")),
            _optional_float(grid.get("max_x")),
            _optional_float(grid.get("max_y")),
        )
        if all(value is not None for value in explicit):
            min_x, min_y, max_x, max_y = explicit
            return float(min_x), float(min_y), float(max_x), float(max_y)

        extent = grid.get("extent") or []
        if not isinstance(extent, (list, tuple)) or len(extent) != 4:
            return default
        values = [_optional_float(value) for value in extent]
        if any(value is None for value in values):
            return default

        candidates = [
            (float(values[0]), float(values[1]), float(values[2]), float(values[3])),
            (float(values[0]), float(values[2]), float(values[1]), float(values[3])),
        ]
        expected_width = None
        expected_height = None
        width_cells = _optional_float(grid.get("width"))
        height_cells = _optional_float(grid.get("height"))
        res_x = _optional_float(grid.get("resolution_x"))
        res_y = _optional_float(grid.get("resolution_y"))
        resolution = _optional_float(grid.get("resolution"))
        if width_cells is not None and (res_x is not None or resolution is not None):
            expected_width = width_cells * float(res_x if res_x is not None else resolution)
        if height_cells is not None and (res_y is not None or resolution is not None):
            expected_height = height_cells * float(res_y if res_y is not None else resolution)

        scored: list[tuple[float, tuple[float, float, float, float]]] = []
        for candidate in candidates:
            min_x, min_y, max_x, max_y = candidate
            if max_x <= min_x or max_y <= min_y:
                continue
            width = max_x - min_x
            height = max_y - min_y
            if expected_width is not None and expected_height is not None:
                score = abs(width - expected_width) + abs(height - expected_height)
            else:
                score = width + height
            scored.append((score, candidate))
        if not scored:
            return default
        return min(scored, key=lambda item: item[0])[1]

    def _case_context(self, case_id: str) -> dict[str, Any]:
        if case_id == "CASE_MINDORO_RETRO_2023":
            grid = self.mindoro_forecast_manifest.get("grid") or {}
            source_geometry = self.mindoro_forecast_manifest.get("source_geometry") or {}
            full_bounds = self._projected_bounds_from_grid(
                grid,
                (274000.0, 1355000.0, 398000.0, 1524000.0),
            )
            full_bounds_wgs84 = tuple(grid.get("display_bounds_wgs84") or [120.9096, 122.0622, 12.2494, 13.7837])
            return {
                "case_label": "Mindoro",
                "projected_crs": str(grid.get("crs") or "EPSG:32651"),
                "full_bounds": full_bounds,
                "full_bounds_wgs84": full_bounds_wgs84,
                "land_mask_path": self._resolve(grid.get("land_mask_path") or "data_processed/grids/land_mask.tif"),
                "shoreline_path": self._resolve(grid.get("shoreline_segments_path") or "data_processed/grids/shoreline_segments.gpkg"),
                "source_point_path": self._resolve(source_geometry.get("source_point") or "data/arcgis/CASE_MINDORO_RETRO_2023/source_point_metadata_processed.gpkg"),
                "init_polygon_path": self._resolve(source_geometry.get("initialization_polygon") or "data/arcgis/CASE_MINDORO_RETRO_2023/seed_polygon_mar3_processed.gpkg"),
                "validation_polygon_path": self._resolve(source_geometry.get("validation_polygon") or "data/arcgis/CASE_MINDORO_RETRO_2023/validation_polygon_mar6_processed.gpkg"),
                "labels_df": self.mindoro_labels.copy(),
            }
        land_mask_path = self._resolve(DWH_SETUP_DIR / "land_mask.tif")
        land_info = self._load_raster_mask(land_mask_path)
        full_bounds = tuple(land_info["bounds"]) if land_info else (-70000.0, 2933000.0, 754000.0, 3398000.0)
        full_bounds_wgs84 = transform_bounds("EPSG:32616", "EPSG:4326", *full_bounds, densify_pts=21)
        return {
            "case_label": "Deepwater Horizon",
            "projected_crs": "EPSG:32616",
            "full_bounds": full_bounds,
            "full_bounds_wgs84": full_bounds_wgs84,
            "land_mask_path": land_mask_path,
            "shoreline_path": self._resolve(DWH_SETUP_DIR / "shoreline_segments.gpkg"),
            "source_point_path": self._resolve(DWH_SETUP_DIR / "processed" / "layer_00_dwh_wellhead_processed.gpkg"),
            "init_polygon_path": self._resolve(DWH_SETUP_DIR / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg"),
            "validation_polygon_path": None,
            "labels_df": self.dwh_labels.copy(),
        }

    def _legend_handles(self, legend_keys: list[str]) -> list[Any]:
        palette = self._palette()
        labels = self._legend_labels()
        handles: list[Any] = []
        for key in legend_keys:
            color = palette.get(key, "#475569")
            label = labels.get(key, key.replace("_", " ").title())
            if key == "source_point":
                handles.append(
                    Line2D(
                        [0],
                        [0],
                        marker="*",
                        color="white",
                        markerfacecolor=color,
                        markeredgecolor="#111827",
                        markersize=10,
                        linewidth=0,
                        label=label,
                    )
                )
            elif key in {"initialization_polygon", "validation_polygon", "corridor_hull"}:
                handles.append(Patch(facecolor="none", edgecolor=color, linewidth=2, label=label))
            else:
                handles.append(Patch(facecolor=color, edgecolor=color, alpha=0.7, label=label))
        return handles

    def _add_legend(self, ax: plt.Axes, legend_keys: list[str]) -> None:
        ax.axis("off")
        handles = self._legend_handles(legend_keys)
        legend = ax.legend(
            handles=handles,
            loc="upper left",
            frameon=True,
            fontsize=float((self.style.get("typography") or {}).get("note_size") or 8),
            title="Legend",
        )
        legend.get_frame().set_facecolor((1, 1, 1, 0.98))
        legend.get_frame().set_edgecolor((self.style.get("layout") or {}).get("legend_edgecolor") or "#94a3b8")
        if legend.get_title():
            legend.get_title().set_fontsize(float((self.style.get("typography") or {}).get("legend_title_size") or 9))

    def _add_note_box(self, ax: plt.Axes, title: str, lines: list[str]) -> None:
        ax.axis("off")
        wrapped = "\n".join(textwrap.fill(line, width=40) for line in lines if str(line).strip())
        ax.text(
            0.0,
            1.0,
            title,
            ha="left",
            va="top",
            fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11),
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
            fontsize=float((self.style.get("typography") or {}).get("note_size") or 8),
            color="#334155",
            transform=ax.transAxes,
            bbox={"boxstyle": "round,pad=0.45", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
        )

    def _add_geographic_labels(self, ax: plt.Axes, case_id: str, target_crs: str) -> None:
        labels_df = self._case_context(case_id).get("labels_df", pd.DataFrame())
        if labels_df.empty:
            return
        active = labels_df.loc[labels_df["enabled_yes_no"].astype(str).str.lower() == "yes"].copy()
        if active.empty:
            return
        transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
        for _, row in active.iterrows():
            x_value, y_value = transformer.transform(float(row["lon"]), float(row["lat"]))
            ax.text(
                float(x_value),
                float(y_value),
                str(row["label_text"]),
                fontsize=7,
                color="#334155",
                ha="left",
                va="bottom",
                zorder=8,
                bbox={"boxstyle": "round,pad=0.18", "facecolor": (1, 1, 1, 0.7), "edgecolor": "none"},
            )

    def _draw_land_and_shoreline(self, ax: plt.Axes, case_id: str, target_crs: str) -> None:
        context = self._case_context(case_id)
        palette = self._palette()
        land_info = self._load_raster_mask(context["land_mask_path"])
        if land_info is not None and land_info["mask"].any():
            land_mask = np.ma.masked_where(~land_info["mask"], land_info["mask"].astype(float))
            show(
                land_mask,
                transform=land_info["transform"],
                ax=ax,
                cmap=ListedColormap([palette.get("background_land", "#e6dfd1")]),
                alpha=0.85,
                zorder=1,
            )
        shoreline = self._load_vector(context["shoreline_path"], target_crs)
        if shoreline is not None and not shoreline.empty:
            shoreline.boundary.plot(
                ax=ax,
                color=palette.get("shoreline", "#8b8178"),
                linewidth=0.35 if case_id == "CASE_DWH_RETRO_2010_72H" else 0.45,
                alpha=0.9,
                zorder=2,
            )

    def _draw_mask_layer(self, ax: plt.Axes, layer: dict[str, Any], target_crs: str) -> tuple[list[str], tuple[float, float, float, float] | None]:
        source_paths: list[str] = []
        path = self._resolve(layer["path"])
        info = self._load_raster_mask(path)
        if info is None:
            return source_paths, None
        if info["crs"] != target_crs:
            self._record_missing(path, f"Raster CRS mismatch for publication figure layer: expected {target_crs}, found {info['crs']}.")
            return source_paths, None
        if info["nonzero_bounds"] is None:
            return source_paths, None
        source_paths.append(_relative_to_repo(self.repo_root, path))
        color = self._palette().get(str(layer["legend_key"]), "#475569")
        if bool(layer.get("fill", True)):
            masked = np.ma.masked_where(~info["mask"], info["mask"].astype(float))
            show(
                masked,
                transform=info["transform"],
                ax=ax,
                cmap=ListedColormap([color]),
                alpha=float(layer.get("alpha", 0.3)),
                zorder=int(layer.get("zorder", 4)),
            )
        if bool(layer.get("outline", True)):
            ax.contour(
                info["x_coords"],
                info["y_coords"],
                info["mask"].astype(float),
                levels=[0.5],
                colors=[color],
                linewidths=float(layer.get("linewidth", 1.4)),
                linestyles=str(layer.get("linestyle", "solid")),
                alpha=0.95,
                zorder=int(layer.get("zorder", 4)) + 1,
            )
        return source_paths, info["nonzero_bounds"]

    def _draw_case_geometry(
        self,
        ax: plt.Axes,
        case_id: str,
        target_crs: str,
        *,
        show_source: bool,
        show_init: bool,
        show_validation: bool,
    ) -> tuple[list[str], list[tuple[float, float, float, float]]]:
        context = self._case_context(case_id)
        source_paths: list[str] = []
        crop_bounds: list[tuple[float, float, float, float]] = []
        if show_init and context.get("init_polygon_path"):
            gdf = self._load_vector(context["init_polygon_path"], target_crs)
            if gdf is not None and not gdf.empty:
                gdf.boundary.plot(ax=ax, color=self._palette().get("initialization_polygon", "#d97706"), linewidth=1.3, linestyle="--", alpha=0.95, zorder=6)
                source_paths.append(_relative_to_repo(self.repo_root, context["init_polygon_path"]))
                crop_bounds.append(tuple(gdf.total_bounds))
        if show_validation and context.get("validation_polygon_path"):
            gdf = self._load_vector(Path(context["validation_polygon_path"]), target_crs)
            if gdf is not None and not gdf.empty:
                gdf.boundary.plot(ax=ax, color=self._palette().get("validation_polygon", "#0f172a"), linewidth=1.0, linestyle="-.", alpha=0.85, zorder=6)
                source_paths.append(_relative_to_repo(self.repo_root, Path(context["validation_polygon_path"])))
                crop_bounds.append(tuple(gdf.total_bounds))
        if show_source and context.get("source_point_path"):
            gdf = self._load_vector(context["source_point_path"], target_crs)
            if gdf is not None and not gdf.empty:
                gdf.plot(ax=ax, color=self._palette().get("source_point", "#b42318"), marker="*", markersize=120, edgecolor="#111827", linewidth=0.4, zorder=7)
                point = gdf.geometry.iloc[0]
                ax.text(point.x, point.y, " Source", fontsize=7, fontweight="bold", color="#7f1d1d", ha="left", va="bottom", zorder=8)
                source_paths.append(_relative_to_repo(self.repo_root, context["source_point_path"]))
                crop_bounds.append(tuple(gdf.total_bounds))
        return source_paths, crop_bounds

    def _extract_track_arrays(self, path: Path, *, model_kind: str, target_crs: str, sample_count: int) -> dict[str, Any] | None:
        if not path.exists():
            self._record_missing(path, "Track file missing for publication figure package.")
            return None
        transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
        if model_kind == "pygnome":
            with xr.open_dataset(path) as ds:
                particle_counts = np.asarray(ds["particle_count"].values, dtype=int)
                time_count = int(ds.sizes["time"])
                particle_count = int(np.nanmax(particle_counts))
                lon_flat = np.asarray(ds["longitude"].values, dtype=float)
                lat_flat = np.asarray(ds["latitude"].values, dtype=float)
            if particle_count <= 0 or time_count <= 0 or lon_flat.size != time_count * particle_count:
                self._record_missing(path, "Unexpected PyGNOME track layout in NetCDF.")
                return None
            lon = lon_flat.reshape(time_count, particle_count).T
            lat = lat_flat.reshape(time_count, particle_count).T
        else:
            with xr.open_dataset(path) as ds:
                lon = np.asarray(ds["lon"].values, dtype=float)
                lat = np.asarray(ds["lat"].values, dtype=float)
            if lon.ndim != 2 or lat.ndim != 2:
                self._record_missing(path, "Unexpected OpenDrift track layout in NetCDF.")
                return None
        sample_indices = np.linspace(0, lon.shape[0] - 1, num=max(1, min(sample_count, lon.shape[0])), dtype=int)
        sample_x: list[np.ndarray] = []
        sample_y: list[np.ndarray] = []
        final_points: list[tuple[float, float]] = []
        for idx in sample_indices:
            mask = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
            if not mask.any():
                continue
            x_values, y_values = transformer.transform(lon[idx][mask], lat[idx][mask])
            sample_x.append(np.asarray(x_values, dtype=float))
            sample_y.append(np.asarray(y_values, dtype=float))
            final_points.append((float(x_values[-1]), float(y_values[-1])))
        centroid_lon = np.nanmean(lon, axis=0)
        centroid_lat = np.nanmean(lat, axis=0)
        centroid_mask = np.isfinite(centroid_lon) & np.isfinite(centroid_lat)
        if not centroid_mask.any():
            self._record_missing(path, "Track file did not expose usable centroid coordinates.")
            return None
        centroid_x, centroid_y = transformer.transform(centroid_lon[centroid_mask], centroid_lat[centroid_mask])
        merged_x = np.concatenate(sample_x + [np.asarray(centroid_x, dtype=float)]) if sample_x else np.asarray(centroid_x, dtype=float)
        merged_y = np.concatenate(sample_y + [np.asarray(centroid_y, dtype=float)]) if sample_y else np.asarray(centroid_y, dtype=float)
        bounds = (float(np.nanmin(merged_x)), float(np.nanmin(merged_y)), float(np.nanmax(merged_x)), float(np.nanmax(merged_y)))
        return {
            "sample_x": sample_x,
            "sample_y": sample_y,
            "centroid_x": np.asarray(centroid_x, dtype=float),
            "centroid_y": np.asarray(centroid_y, dtype=float),
            "final_points": final_points,
            "bounds": bounds,
            "source_path": _relative_to_repo(self.repo_root, path),
        }

    def _union_bounds(self, bounds_list: list[tuple[float, float, float, float] | None]) -> tuple[float, float, float, float] | None:
        valid = [bounds for bounds in bounds_list if bounds is not None]
        if not valid:
            return None
        return (
            float(min(bounds[0] for bounds in valid)),
            float(min(bounds[1] for bounds in valid)),
            float(max(bounds[2] for bounds in valid)),
            float(max(bounds[3] for bounds in valid)),
        )

    def _expand_crop_bounds(self, bounds: tuple[float, float, float, float], *, view_type: str) -> tuple[float, float, float, float]:
        min_x, min_y, max_x, max_y = bounds
        crop_rules = self._crop_rules()
        pad_fraction = float(crop_rules.get("close_padding_fraction") if view_type == "close" else crop_rules.get("zoom_padding_fraction") or 0.18)
        min_pad = float(crop_rules.get("minimum_padding_m") or 4000.0)
        min_span = float(crop_rules.get("minimum_crop_span_m") or 12000.0)
        width = max(max_x - min_x, 1.0)
        height = max(max_y - min_y, 1.0)
        pad_x = max(min_pad, width * pad_fraction)
        pad_y = max(min_pad, height * pad_fraction)
        width_with_pad = width + (2.0 * pad_x)
        height_with_pad = height + (2.0 * pad_y)
        if width_with_pad < min_span:
            pad_x += (min_span - width_with_pad) / 2.0
        if height_with_pad < min_span:
            pad_y += (min_span - height_with_pad) / 2.0
        return (min_x - pad_x, min_y - pad_y, max_x + pad_x, max_y + pad_y)

    def _case_wgs84_bounds(self, case_id: str) -> tuple[float, float, float, float]:
        context = self._case_context(case_id)
        return tuple(float(value) for value in context.get("full_bounds_wgs84"))

    def _preferred_scale_km(self, case_id: str, width_m: float) -> float:
        locator_rules = self._locator_rules()
        preferred_scale_km = _optional_float(
            locator_rules.get("mindoro_scale_km") if case_id == "CASE_MINDORO_RETRO_2023" else locator_rules.get("dwh_scale_km")
        )
        if preferred_scale_km is not None and (preferred_scale_km * 1000.0) <= (width_m * 0.60):
            return preferred_scale_km
        if width_m <= 25000:
            return 5.0
        if width_m <= 60000:
            return 10.0
        if width_m <= 150000:
            return 25.0
        return 100.0

    def _geographic_aspect(self, latitude: float) -> float:
        cosine = abs(float(np.cos(np.deg2rad(latitude))))
        return 1.0 / max(cosine, 1e-6)

    def _figure_pixel_size(self, fig: plt.Figure) -> tuple[int, int]:
        fig.canvas.draw()
        width, height = fig.canvas.get_width_height()
        return int(width), int(height)

    def _add_locator(self, ax: plt.Axes, case_id: str, crop_bounds: tuple[float, float, float, float] | None, target_crs: str) -> None:
        context = self._case_context(case_id)
        locator_outline = self._load_vector(context["shoreline_path"], "EPSG:4326")
        if locator_outline is not None and not locator_outline.empty:
            locator_outline.boundary.plot(ax=ax, color=self._palette().get("shoreline", "#8b8178"), linewidth=0.35, alpha=0.9, zorder=1)
        labels_df = context.get("labels_df", pd.DataFrame())
        if not labels_df.empty:
            active = labels_df.loc[labels_df["enabled_yes_no"].astype(str).str.lower() == "yes"].copy()
            ax.scatter(active["lon"], active["lat"], s=12, color="#0f172a", zorder=3)
            for _, row in active.iterrows():
                ax.text(float(row["lon"]), float(row["lat"]), str(row["label_text"]), fontsize=6.2, color="#0f172a", ha="left", va="bottom", zorder=4)
        if crop_bounds is not None:
            min_lon, min_lat, max_lon, max_lat = transform_bounds(target_crs, "EPSG:4326", *crop_bounds, densify_pts=21)
            ax.add_patch(Rectangle((min_lon, min_lat), max_lon - min_lon, max_lat - min_lat, fill=False, linewidth=1.4, linestyle="--", edgecolor="#b42318", zorder=5))
        full_bounds = self._case_wgs84_bounds(case_id)
        pad_fraction = float(self._locator_rules().get("locator_padding_fraction") or 0.55)
        lon_span = full_bounds[1] - full_bounds[0]
        lat_span = full_bounds[3] - full_bounds[2]
        ax.set_xlim(full_bounds[0] - (lon_span * pad_fraction), full_bounds[1] + (lon_span * pad_fraction))
        ax.set_ylim(full_bounds[2] - (lat_span * pad_fraction), full_bounds[3] + (lat_span * pad_fraction))
        center_lat = ((full_bounds[2] - (lat_span * pad_fraction)) + (full_bounds[3] + (lat_span * pad_fraction))) / 2.0
        ax.set_aspect(self._geographic_aspect(center_lat), adjustable="box")
        ax.set_title("Locator", fontsize=10, loc="left")
        ax.set_xlabel("Longitude", fontsize=7)
        ax.set_ylabel("Latitude", fontsize=7)
        ax.tick_params(labelsize=6)
        ax.annotate("N", xy=(0.92, 0.86), xytext=(0.92, 0.68), xycoords="axes fraction", textcoords="axes fraction", arrowprops={"arrowstyle": "-|>", "color": "#111827", "lw": 1.1}, ha="center", va="center", fontsize=8, color="#111827", fontweight="bold")

    def _add_scale_bar(self, ax: plt.Axes, bounds: tuple[float, float, float, float], case_id: str) -> None:
        min_x, min_y, max_x, max_y = bounds
        width = max_x - min_x
        height = max_y - min_y
        scale_km = self._preferred_scale_km(case_id, width)
        length_m = float(scale_km) * 1000.0
        x0 = min_x + (width * 0.05)
        y0 = min_y + (height * 0.05)
        ax.plot([x0, x0 + length_m], [y0, y0], color="#111827", linewidth=2.4, zorder=9)
        scale_label = int(scale_km) if float(scale_km).is_integer() else scale_km
        ax.text(x0 + (length_m / 2.0), y0 + (height * 0.02), f"{scale_label} km", fontsize=7, color="#111827", ha="center", va="bottom")

    def _add_north_arrow(self, ax: plt.Axes) -> None:
        ax.annotate("N", xy=(0.95, 0.90), xytext=(0.95, 0.72), xycoords="axes fraction", textcoords="axes fraction", arrowprops={"arrowstyle": "-|>", "color": "#111827", "lw": 1.3}, ha="center", va="center", fontsize=9, color="#111827", fontweight="bold")

    def _apply_map_axes_style(self, ax: plt.Axes, target_crs: str, bounds: tuple[float, float, float, float], case_id: str) -> None:
        ax.set_facecolor((self.style.get("layout") or {}).get("axes_facecolor") or "#f7fbfd")
        ax.grid(True, linestyle="--", linewidth=0.35, color=(self.style.get("layout") or {}).get("grid_color") or "#cbd5e1", alpha=0.45)
        if "326" in target_crs or "3857" in target_crs:
            ax.set_xlabel(f"{target_crs} Easting (m)")
            ax.set_ylabel(f"{target_crs} Northing (m)")
            ax.set_aspect("equal", adjustable="box")
        else:
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
        ax.set_xlim(bounds[0], bounds[2])
        ax.set_ylim(bounds[1], bounds[3])
        self._add_scale_bar(ax, bounds, case_id)
        self._add_north_arrow(ax)

    def _render_spatial_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        case_id = str(spec["case_id"])
        context = self._case_context(case_id)
        target_crs = context["projected_crs"]
        source_paths: list[str] = []
        crop_candidates: list[tuple[float, float, float, float] | None] = []
        self._draw_land_and_shoreline(ax, case_id, target_crs)
        geometry_paths, geometry_bounds = self._draw_case_geometry(
            ax,
            case_id,
            target_crs,
            show_source=bool(spec.get("show_source", True)),
            show_init=bool(spec.get("show_init", False)),
            show_validation=bool(spec.get("show_validation", False)),
        )
        source_paths.extend(geometry_paths)
        if bool(spec.get("include_source_in_crop", False)) or bool(spec.get("include_init_in_crop", False)) or bool(spec.get("include_validation_in_crop", False)):
            crop_candidates.extend(geometry_bounds)
        for layer in spec.get("raster_layers", []):
            layer_paths, layer_bounds = self._draw_mask_layer(ax, layer, target_crs)
            source_paths.extend(layer_paths)
            if bool(layer.get("include_in_crop", True)):
                crop_candidates.append(layer_bounds)
        if bool(spec.get("show_labels", True)):
            self._add_geographic_labels(ax, case_id, target_crs)
        crop_bounds = self._union_bounds(crop_candidates)
        if spec.get("view_type") == "locator":
            crop_bounds = tuple(float(value) for value in context["full_bounds"])
        elif crop_bounds is None:
            crop_bounds = tuple(float(value) for value in context["full_bounds"])
        else:
            crop_bounds = self._expand_crop_bounds(crop_bounds, view_type=str(spec.get("view_type") or "zoom"))
        self._apply_map_axes_style(ax, target_crs, crop_bounds, case_id)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        return {"source_paths": source_paths, "crop_bounds": crop_bounds, "target_crs": target_crs}

    def _render_deterministic_track_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        case_id = str(spec["case_id"])
        context = self._case_context(case_id)
        target_crs = context["projected_crs"]
        self._draw_land_and_shoreline(ax, case_id, target_crs)
        source_paths, geometry_bounds = self._draw_case_geometry(ax, case_id, target_crs, show_source=True, show_init=False, show_validation=False)
        track_info = self._extract_track_arrays(self._resolve(spec["track_path"]), model_kind=str(spec["model_kind"]), target_crs=target_crs, sample_count=int(spec.get("sample_count", 40)))
        crop_candidates: list[tuple[float, float, float, float] | None] = geometry_bounds
        if track_info is not None:
            color = self._palette().get(str(spec.get("legend_key") or "deterministic_opendrift"), "#165ba8")
            sample_color = self._palette().get("ensemble_member_path", "#94a3b8")
            for x_values, y_values in zip(track_info["sample_x"], track_info["sample_y"]):
                ax.plot(x_values, y_values, color=sample_color, linewidth=0.55, alpha=0.22, zorder=4)
            ax.plot(track_info["centroid_x"], track_info["centroid_y"], color=color, linewidth=2.0, zorder=5)
            ax.scatter(track_info["centroid_x"][0], track_info["centroid_y"][0], color=self._palette().get("source_point", "#b42318"), s=28, zorder=6)
            ax.scatter(track_info["centroid_x"][-1], track_info["centroid_y"][-1], color=color, s=30, zorder=6)
            source_paths.append(track_info["source_path"])
            crop_candidates.append(track_info["bounds"])
        crop_bounds = self._union_bounds(crop_candidates) or tuple(float(value) for value in context["full_bounds"])
        crop_bounds = self._expand_crop_bounds(crop_bounds, view_type=str(spec.get("view_type") or "zoom"))
        self._add_geographic_labels(ax, case_id, target_crs)
        self._apply_map_axes_style(ax, target_crs, crop_bounds, case_id)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        return {"source_paths": source_paths, "crop_bounds": crop_bounds, "target_crs": target_crs}

    def _render_sampled_ensemble_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        case_id = str(spec["case_id"])
        context = self._case_context(case_id)
        target_crs = context["projected_crs"]
        self._draw_land_and_shoreline(ax, case_id, target_crs)
        source_paths, geometry_bounds = self._draw_case_geometry(ax, case_id, target_crs, show_source=True, show_init=False, show_validation=False)
        all_bounds: list[tuple[float, float, float, float] | None] = geometry_bounds
        centroid_lines_x: list[np.ndarray] = []
        centroid_lines_y: list[np.ndarray] = []
        selected_paths = self._sample_paths([self._resolve(path) for path in spec.get("track_paths", [])], int(spec.get("member_sample_count", 12)))
        for path in selected_paths:
            track_info = self._extract_track_arrays(path, model_kind="opendrift", target_crs=target_crs, sample_count=4)
            if track_info is None:
                continue
            ax.plot(track_info["centroid_x"], track_info["centroid_y"], color=self._palette().get("ensemble_member_path", "#94a3b8"), linewidth=0.9, alpha=0.7, zorder=4)
            ax.scatter(track_info["centroid_x"][-1], track_info["centroid_y"][-1], color=self._palette().get("ensemble_member_path", "#94a3b8"), s=16, zorder=5)
            source_paths.append(track_info["source_path"])
            centroid_lines_x.append(track_info["centroid_x"])
            centroid_lines_y.append(track_info["centroid_y"])
            all_bounds.append(track_info["bounds"])
        if centroid_lines_x:
            max_len = max(len(line) for line in centroid_lines_x)
            x_stack = np.full((len(centroid_lines_x), max_len), np.nan, dtype=float)
            y_stack = np.full((len(centroid_lines_y), max_len), np.nan, dtype=float)
            for idx, line in enumerate(centroid_lines_x):
                x_stack[idx, : len(line)] = line
            for idx, line in enumerate(centroid_lines_y):
                y_stack[idx, : len(line)] = line
            merged_x = np.nanmean(x_stack, axis=0)
            merged_y = np.nanmean(y_stack, axis=0)
            valid = np.isfinite(merged_x) & np.isfinite(merged_y)
            ax.plot(merged_x[valid], merged_y[valid], color=self._palette().get("centroid_path", "#111827"), linewidth=2.0, zorder=5)
        crop_bounds = self._union_bounds(all_bounds) or tuple(float(value) for value in context["full_bounds"])
        crop_bounds = self._expand_crop_bounds(crop_bounds, view_type=str(spec.get("view_type") or "zoom"))
        self._add_geographic_labels(ax, case_id, target_crs)
        self._apply_map_axes_style(ax, target_crs, crop_bounds, case_id)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        return {"source_paths": source_paths, "crop_bounds": crop_bounds, "target_crs": target_crs}

    def _render_corridor_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        case_id = str(spec["case_id"])
        context = self._case_context(case_id)
        target_crs = context["projected_crs"]
        self._draw_land_and_shoreline(ax, case_id, target_crs)
        source_paths, geometry_bounds = self._draw_case_geometry(
            ax,
            case_id,
            target_crs,
            show_source=True,
            show_init=bool(spec.get("show_init", False)),
            show_validation=bool(spec.get("show_validation", False)),
        )
        all_bounds: list[tuple[float, float, float, float] | None] = list(geometry_bounds)
        for layer in spec.get("raster_layers", []):
            layer_paths, layer_bounds = self._draw_mask_layer(ax, layer, target_crs)
            source_paths.extend(layer_paths)
            if bool(layer.get("include_in_crop", True)):
                all_bounds.append(layer_bounds)
        centroid_lines_x: list[np.ndarray] = []
        centroid_lines_y: list[np.ndarray] = []
        final_points: list[tuple[float, float]] = []
        for path in self._sample_paths([self._resolve(path) for path in spec.get("track_paths", [])], int(spec.get("member_sample_count", 12))):
            model_kind = "pygnome" if "pygnome" in path.name.lower() else "opendrift"
            track_info = self._extract_track_arrays(path, model_kind=model_kind, target_crs=target_crs, sample_count=4)
            if track_info is None:
                continue
            ax.plot(
                track_info["centroid_x"],
                track_info["centroid_y"],
                color=self._palette().get("ensemble_member_path", "#94a3b8"),
                linewidth=0.8,
                alpha=0.45,
                zorder=4,
            )
            centroid_lines_x.append(track_info["centroid_x"])
            centroid_lines_y.append(track_info["centroid_y"])
            if track_info["final_points"]:
                final_points.append(track_info["final_points"][-1])
            source_paths.append(track_info["source_path"])
            all_bounds.append(track_info["bounds"])
        if centroid_lines_x:
            max_len = max(len(line) for line in centroid_lines_x)
            x_stack = np.full((len(centroid_lines_x), max_len), np.nan, dtype=float)
            y_stack = np.full((len(centroid_lines_y), max_len), np.nan, dtype=float)
            for idx, line in enumerate(centroid_lines_x):
                x_stack[idx, : len(line)] = line
            for idx, line in enumerate(centroid_lines_y):
                y_stack[idx, : len(line)] = line
            centroid_x = np.nanmean(x_stack, axis=0)
            centroid_y = np.nanmean(y_stack, axis=0)
            valid = np.isfinite(centroid_x) & np.isfinite(centroid_y)
            ax.plot(
                centroid_x[valid],
                centroid_y[valid],
                color=self._palette().get("centroid_path", "#111827"),
                linewidth=2.2,
                zorder=6,
            )
        if len(final_points) >= 2:
            hull = MultiPoint(final_points).convex_hull
            if hull.geom_type == "LineString":
                hull = hull.buffer(2500.0)
            elif hull.geom_type == "Point":
                hull = hull.buffer(3000.0)
            hull_gdf = gpd.GeoDataFrame(geometry=[hull], crs=target_crs)
            hull_gdf.boundary.plot(
                ax=ax,
                color=self._palette().get("corridor_hull", "#c2410c"),
                linewidth=1.8,
                linestyle="-.",
                alpha=0.95,
                zorder=6,
            )
            hull_gdf.plot(
                ax=ax,
                color=self._palette().get("corridor_hull", "#c2410c"),
                alpha=0.06,
                zorder=5,
            )
            all_bounds.append(tuple(hull_gdf.total_bounds))
        crop_bounds = self._union_bounds(all_bounds) or tuple(float(value) for value in context["full_bounds"])
        crop_bounds = self._expand_crop_bounds(crop_bounds, view_type=str(spec.get("view_type") or "zoom"))
        self._add_geographic_labels(ax, case_id, target_crs)
        self._apply_map_axes_style(ax, target_crs, crop_bounds, case_id)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        return {"source_paths": source_paths, "crop_bounds": crop_bounds, "target_crs": target_crs}

    def _render_oil_budget_summary_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        path = self._resolve(spec["csv_path"])
        df = _read_csv(path)
        if df.empty:
            self._record_missing(path, "Phase 4 oil-budget summary CSV was missing for publication figures.")
            ax.text(0.5, 0.5, "Oil-budget summary unavailable", ha="center", va="center", fontsize=12)
            ax.axis("off")
            return {"source_paths": [], "crop_bounds": None, "target_crs": None}
        palette = self._palette()
        color_map = {
            "lighter_oil": palette.get("oil_lighter", "#f28c28"),
            "fixed_base_medium_heavy_proxy": palette.get("oil_base", "#8c564b"),
            "heavier_oil": palette.get("oil_heavier", "#4b0082"),
        }
        scenarios = df["scenario_id"].tolist()
        colors = [color_map.get(str(item), "#64748b") for item in scenarios]
        x = np.arange(len(df))
        bars = ax.bar(x, df["total_beached_kg"] / 1000.0, color=colors, edgecolor="#0f172a", alpha=0.88, width=0.58)
        for bar, label in zip(bars, df["oil_label"]):
            ax.text(bar.get_x() + bar.get_width() / 2.0, bar.get_height() + 0.15, str(label), ha="center", va="bottom", fontsize=8, rotation=0)
        ax2 = ax.twinx()
        ax2.plot(x, df["final_evaporated_pct"], color="#111827", marker="o", linewidth=2.0, label="Final evaporated %")
        ax.set_xticks(x)
        ax.set_xticklabels(["Light", "Base", "Heavy"])
        ax.set_ylabel("Total beached mass (tonnes)")
        ax2.set_ylabel("Final evaporated (%)")
        ax.grid(True, axis="y", linestyle="--", linewidth=0.35, alpha=0.45)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        return {"source_paths": [_relative_to_repo(self.repo_root, path)], "crop_bounds": None, "target_crs": None}

    def _render_oil_budget_timeseries_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        path = self._resolve(spec["csv_path"])
        df = _read_csv(path)
        if df.empty:
            self._record_missing(path, "Scenario oil-budget timeseries CSV was missing for publication figures.")
            ax.text(0.5, 0.5, "Oil-budget timeseries unavailable", ha="center", va="center", fontsize=12)
            ax.axis("off")
            return {"source_paths": [], "crop_bounds": None, "target_crs": None}
        hours = pd.to_numeric(df.get("hours_elapsed"), errors="coerce").fillna(0.0)
        ax.plot(hours, df["surface_pct"], color="#165ba8", linewidth=2.0, label="Surface oil (%)")
        ax.plot(hours, df["evaporated_pct"], color="#f28c28", linewidth=2.0, label="Evaporated (%)")
        ax.plot(hours, df["dispersed_pct"], color="#4f46e5", linewidth=2.0, label="Dispersed (%)")
        ax.plot(hours, df["beached_pct"], color="#8c564b", linewidth=2.0, label="Beached (%)")
        ax.set_xlabel("Hours since release")
        ax.set_ylabel("Percent of initial mass")
        ax.set_ylim(bottom=0)
        ax.grid(True, linestyle="--", linewidth=0.35, alpha=0.45)
        ax.legend(loc="upper right", frameon=True, fontsize=8)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        return {"source_paths": [_relative_to_repo(self.repo_root, path)], "crop_bounds": None, "target_crs": None}

    def _render_shoreline_arrival_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        path = self._resolve(spec["csv_path"])
        df = _read_csv(path)
        if df.empty:
            self._record_missing(path, "Phase 4 shoreline-arrival CSV was missing for publication figures.")
            ax.text(0.5, 0.5, "Shoreline-arrival summary unavailable", ha="center", va="center", fontsize=12)
            ax.axis("off")
            return {"source_paths": [], "crop_bounds": None, "target_crs": None}
        palette = self._palette()
        color_map = {
            "lighter_oil": palette.get("oil_lighter", "#f28c28"),
            "fixed_base_medium_heavy_proxy": palette.get("oil_base", "#8c564b"),
            "heavier_oil": palette.get("oil_heavier", "#4b0082"),
        }
        plot_df = df.copy().sort_values("first_shoreline_arrival_h")
        y = np.arange(len(plot_df))
        ax.barh(
            y,
            plot_df["first_shoreline_arrival_h"],
            color=[color_map.get(str(item), "#64748b") for item in plot_df["scenario_id"]],
            edgecolor="#0f172a",
            alpha=0.88,
        )
        ax.set_yticks(y)
        ax.set_yticklabels(plot_df["oil_label"])
        ax.set_xlabel("First shoreline arrival (hours)")
        ax.set_ylabel("Oil scenario")
        ax.grid(True, axis="x", linestyle="--", linewidth=0.35, alpha=0.45)
        for idx, row in plot_df.reset_index(drop=True).iterrows():
            ax.text(
                float(row["first_shoreline_arrival_h"]) + 0.35,
                idx,
                f"{float(row['total_beached_kg']) / 1000.0:.1f} t beached",
                va="center",
                fontsize=8,
                color="#334155",
            )
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        return {"source_paths": [_relative_to_repo(self.repo_root, path)], "crop_bounds": None, "target_crs": None}

    def _render_shoreline_segment_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        case_id = str(spec["case_id"])
        context = self._case_context(case_id)
        target_crs = context["projected_crs"]
        self._draw_land_and_shoreline(ax, case_id, target_crs)
        source_paths, geometry_bounds = self._draw_case_geometry(ax, case_id, target_crs, show_source=True, show_init=False, show_validation=False)
        path = self._resolve(spec["csv_path"])
        df = _read_csv(path)
        if df.empty:
            self._record_missing(path, "Phase 4 shoreline-segment CSV was missing for publication figures.")
            ax.text(0.5, 0.5, "Shoreline-segment impacts unavailable", ha="center", va="center", fontsize=12)
            ax.axis("off")
            return {"source_paths": source_paths, "crop_bounds": None, "target_crs": target_crs}
        scenario_id = str(spec.get("scenario_id") or "all_scenarios")
        if scenario_id and scenario_id != "all_scenarios":
            df = df.loc[df["scenario_id"] == scenario_id].copy()
        if df.empty:
            ax.text(0.5, 0.5, "No shoreline segment impacts found", ha="center", va="center", fontsize=12)
            ax.axis("off")
            return {"source_paths": source_paths, "crop_bounds": None, "target_crs": target_crs}
        if scenario_id == "all_scenarios":
            dominant = (
                df.sort_values("total_beached_kg", ascending=False)
                .drop_duplicates("segment_id")[["segment_id", "scenario_id"]]
                .rename(columns={"scenario_id": "dominant_scenario_id"})
            )
            plot_df = (
                df.groupby("segment_id", as_index=False)
                .agg(
                    total_beached_kg=("total_beached_kg", "sum"),
                    segment_midpoint_lon=("segment_midpoint_lon", "first"),
                    segment_midpoint_lat=("segment_midpoint_lat", "first"),
                )
                .merge(dominant, on="segment_id", how="left")
            )
            color_values = plot_df["dominant_scenario_id"].fillna("fixed_base_medium_heavy_proxy")
        else:
            plot_df = (
                df.groupby("segment_id", as_index=False)
                .agg(
                    total_beached_kg=("total_beached_kg", "sum"),
                    segment_midpoint_lon=("segment_midpoint_lon", "first"),
                    segment_midpoint_lat=("segment_midpoint_lat", "first"),
                )
            )
            color_values = pd.Series([scenario_id] * len(plot_df))
        transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
        xs, ys = transformer.transform(plot_df["segment_midpoint_lon"].to_numpy(), plot_df["segment_midpoint_lat"].to_numpy())
        palette = self._palette()
        color_map = {
            "lighter_oil": palette.get("oil_lighter", "#f28c28"),
            "fixed_base_medium_heavy_proxy": palette.get("oil_base", "#8c564b"),
            "heavier_oil": palette.get("oil_heavier", "#4b0082"),
        }
        max_mass = max(float(plot_df["total_beached_kg"].max()), 1.0)
        sizes = 28.0 + (np.asarray(plot_df["total_beached_kg"], dtype=float) / max_mass) * 220.0
        ax.scatter(
            xs,
            ys,
            s=sizes,
            c=[color_map.get(str(item), "#64748b") for item in color_values],
            alpha=0.82,
            edgecolor="#111827",
            linewidth=0.35,
            zorder=7,
        )
        label_df = plot_df.sort_values("total_beached_kg", ascending=False).head(4).copy()
        label_xs, label_ys = transformer.transform(label_df["segment_midpoint_lon"].to_numpy(), label_df["segment_midpoint_lat"].to_numpy())
        for x_value, y_value, row in zip(label_xs, label_ys, label_df.to_dict(orient="records")):
            ax.text(
                float(x_value),
                float(y_value),
                f"{row['segment_id']}\n{float(row['total_beached_kg'])/1000.0:.1f} t",
                fontsize=7,
                color="#111827",
                ha="left",
                va="bottom",
                zorder=8,
                bbox={"boxstyle": "round,pad=0.15", "facecolor": (1, 1, 1, 0.72), "edgecolor": "none"},
            )
        crop_bounds = self._union_bounds(
            geometry_bounds + [(
                float(np.nanmin(xs)),
                float(np.nanmin(ys)),
                float(np.nanmax(xs)),
                float(np.nanmax(ys)),
            )]
        ) or tuple(float(value) for value in context["full_bounds"])
        crop_bounds = self._expand_crop_bounds(crop_bounds, view_type=str(spec.get("view_type") or "close"))
        self._add_geographic_labels(ax, case_id, target_crs)
        self._apply_map_axes_style(ax, target_crs, crop_bounds, case_id)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        source_paths.append(_relative_to_repo(self.repo_root, path))
        return {"source_paths": source_paths, "crop_bounds": crop_bounds, "target_crs": target_crs}

    def _render_text_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        self._add_note_box(ax, str(spec.get("panel_title") or "How to read this figure"), [str(item) for item in spec.get("note_lines", [])])
        source_paths = [_relative_to_repo(self.repo_root, self._resolve(item)) for item in spec.get("source_paths", [])]
        return {"source_paths": source_paths, "crop_bounds": None, "target_crs": None}

    def _render_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        renderer = str(spec["renderer"])
        if renderer == "spatial":
            return self._render_spatial_panel(ax, spec)
        if renderer == "track":
            return self._render_deterministic_track_panel(ax, spec)
        if renderer == "ensemble_track":
            return self._render_sampled_ensemble_panel(ax, spec)
        if renderer == "corridor":
            return self._render_corridor_panel(ax, spec)
        if renderer == "oil_budget_summary":
            return self._render_oil_budget_summary_panel(ax, spec)
        if renderer == "oil_budget_timeseries":
            return self._render_oil_budget_timeseries_panel(ax, spec)
        if renderer == "shoreline_arrival":
            return self._render_shoreline_arrival_panel(ax, spec)
        if renderer == "shoreline_segment":
            return self._render_shoreline_segment_panel(ax, spec)
        if renderer == "text":
            return self._render_text_panel(ax, spec)
        raise ValueError(f"Unsupported publication figure renderer: {renderer}")

    def _sample_paths(self, paths: list[Path], sample_count: int) -> list[Path]:
        valid = [path for path in paths if path.exists()]
        if not valid:
            return []
        if len(valid) <= sample_count:
            return valid
        indices = np.linspace(0, len(valid) - 1, num=max(1, sample_count), dtype=int)
        return [valid[idx] for idx in sorted(set(indices.tolist()))]

    def _figure_path(self, spec: dict[str, Any]) -> Path:
        return self.output_dir / build_publication_figure_filename(
            case_id=str(spec["case_id"]),
            phase_or_track=str(spec["phase_or_track"]),
            model_name=str(spec["model_names"]),
            run_type=str(spec["run_type"]),
            date_token=str(spec["date_token"]),
            scenario_id=str(spec.get("scenario_id") or ""),
            view_type=str(spec["view_type"]),
            variant=str(spec.get("variant") or ""),
            figure_slug=str(spec["figure_slug"]),
        )

    def _register_figure(
        self,
        spec: dict[str, Any],
        path: Path,
        source_paths: list[str],
        *,
        pixel_width: int,
        pixel_height: int,
    ) -> PublicationFigureRecord:
        relative_path = _relative_to_repo(self.repo_root, path)
        status_override = str(spec.get("status_key_override") or "").strip()
        if status_override:
            status = artifact_status_columns_for_key(status_override)
        else:
            status = artifact_status_columns(
                {
                    "case_id": str(spec["case_id"]),
                    "phase_or_track": str(spec["phase_or_track"]),
                    "run_type": str(spec["run_type"]),
                    "figure_slug": str(spec.get("figure_slug") or ""),
                    "relative_path": relative_path,
                    "notes": str(spec.get("notes") or ""),
                    "short_plain_language_interpretation": str(spec["short_plain_language_interpretation"]),
                    "legacy_debug_only": bool(spec.get("legacy_debug_only")),
                }
            )
        record = PublicationFigureRecord(
            figure_id=path.stem,
            figure_family_code=str(spec["figure_family_code"]),
            figure_family_label=FIGURE_FAMILIES[str(spec["figure_family_code"])],
            case_id=str(spec["case_id"]),
            phase_or_track=str(spec["phase_or_track"]),
            date_token=str(spec["date_token"]),
            model_names=str(spec["model_names"]),
            run_type=str(spec["run_type"]),
            scenario_id=str(spec.get("scenario_id") or ""),
            view_type=str(spec["view_type"]),
            variant=str(spec.get("variant") or ""),
            relative_path=relative_path,
            file_path=str(path),
            pixel_width=int(pixel_width),
            pixel_height=int(pixel_height),
            short_plain_language_interpretation=str(spec["short_plain_language_interpretation"]),
            recommended_for_main_defense=bool(spec.get("recommended_for_main_defense")),
            recommended_for_paper=bool(spec.get("recommended_for_paper")),
            source_paths=" | ".join(path_value for path_value in source_paths if path_value),
            notes=str(spec.get("notes") or ""),
            status_key=status["status_key"],
            status_label=status["status_label"],
            status_role=status["status_role"],
            status_reportability=status["status_reportability"],
            status_official_status=status["status_official_status"],
            status_frozen_status=status["status_frozen_status"],
            status_provenance=status["status_provenance"],
            status_panel_text=status["status_panel_text"],
            status_dashboard_summary=status["status_dashboard_summary"],
        )
        self.figure_records.append(record)
        return record

    def _image_pixel_size(self, path: Path) -> tuple[int, int]:
        image = plt.imread(path)
        return int(image.shape[1]), int(image.shape[0])

    def _save_external_image_figure(self, spec: dict[str, Any]) -> PublicationFigureRecord:
        source_path = self._resolve(str(spec["source_image_path"]))
        if not source_path.exists():
            raise FileNotFoundError(f"External image source missing for publication figure: {source_path}")
        output_path = self._figure_path(spec)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, output_path)
        pixel_width, pixel_height = self._image_pixel_size(output_path)
        source_paths = [str(Path(str(spec["source_image_path"])).as_posix())]
        source_paths.extend(str(item) for item in spec.get("source_paths", []) if str(item))
        return self._register_figure(
            spec,
            output_path,
            list(dict.fromkeys(source_paths)),
            pixel_width=pixel_width,
            pixel_height=pixel_height,
        )

    def _save_single_figure(self, spec: dict[str, Any]) -> PublicationFigureRecord:
        if str(spec.get("renderer")) == "external_image":
            return self._save_external_image_figure(spec)
        output_path = self._figure_path(spec)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        is_spatial = str(spec["renderer"]) in {"spatial", "track", "ensemble_track", "corridor", "shoreline_segment"}
        fig = plt.figure(figsize=self._single_size(), dpi=self._dpi(), facecolor=(self.style.get("layout") or {}).get("figure_facecolor") or "#ffffff")
        if is_spatial:
            grid = fig.add_gridspec(3, 2, width_ratios=[3.8, 1.35], height_ratios=[1.15, 0.8, 1.25], left=0.05, right=0.98, top=0.90, bottom=0.07, wspace=0.16, hspace=0.22)
            main_ax = fig.add_subplot(grid[:, 0])
            locator_ax = fig.add_subplot(grid[0, 1])
            legend_ax = fig.add_subplot(grid[1, 1])
            note_ax = fig.add_subplot(grid[2, 1])
            panel_title = spec.get("figure_title")
            if "map_panel_title" in spec:
                panel_title = spec.get("map_panel_title") or ""
            render_info = self._render_panel(main_ax, dict(spec, panel_title=panel_title))
            self._add_locator(locator_ax, str(spec["case_id"]), render_info.get("crop_bounds"), str(render_info.get("target_crs") or self._case_context(str(spec["case_id"]))["projected_crs"]))
            self._add_legend(legend_ax, [str(item) for item in spec.get("legend_keys", [])])
            self._add_note_box(note_ax, str(spec.get("note_box_title") or "How to read this figure"), [str(item) for item in spec.get("note_lines", [])])
        else:
            grid = fig.add_gridspec(2, 2, width_ratios=[3.3, 1.4], height_ratios=[1.0, 1.0], left=0.05, right=0.98, top=0.90, bottom=0.07, wspace=0.18, hspace=0.24)
            main_ax = fig.add_subplot(grid[:, 0])
            info_ax = fig.add_subplot(grid[0, 1])
            note_ax = fig.add_subplot(grid[1, 1])
            panel_title = spec.get("figure_title")
            if "map_panel_title" in spec:
                panel_title = spec.get("map_panel_title") or ""
            render_info = self._render_panel(main_ax, dict(spec, panel_title=panel_title))
            self._add_note_box(
                info_ax,
                str(spec.get("subtitle_box_title") or "Context"),
                [
                    str(spec.get("subtitle") or ""),
                    f"Case: {spec['case_id']}",
                    f"Track: {spec['phase_or_track']}",
                ],
            )
            self._add_note_box(note_ax, str(spec.get("note_box_title") or "How to read this figure"), [str(item) for item in spec.get("note_lines", [])])
        fig.suptitle(str(spec["figure_title"]), x=0.05, y=0.965, ha="left", fontsize=float((self.style.get("typography") or {}).get("title_size") or 19), fontweight="bold")
        fig.text(0.05, 0.932, str(spec["subtitle"]), ha="left", va="top", fontsize=float((self.style.get("typography") or {}).get("subtitle_size") or 10), color="#475569")
        pixel_width, pixel_height = self._figure_pixel_size(fig)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return self._register_figure(
            spec,
            output_path,
            [str(item) for item in render_info.get("source_paths", [])],
            pixel_width=pixel_width,
            pixel_height=pixel_height,
        )

    def _save_board_figure(self, spec: dict[str, Any], saved_figures: dict[str, PublicationFigureRecord]) -> PublicationFigureRecord:
        output_path = self._figure_path(spec)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        panels = list(spec.get("panels", []))
        panel_count = max(1, len(panels))
        if panel_count <= 3:
            cols = panel_count
        elif panel_count == 4:
            cols = 2
        else:
            cols = 3
        rows = int(np.ceil(panel_count / cols))
        fig = plt.figure(figsize=self._board_size(), dpi=self._dpi(), facecolor=(self.style.get("layout") or {}).get("figure_facecolor") or "#ffffff")
        grid = fig.add_gridspec(rows + 1, cols, height_ratios=([1.0] * rows) + [0.55], left=0.04, right=0.98, top=0.90, bottom=0.06, wspace=0.10, hspace=0.14)
        source_paths: list[str] = []
        for idx, panel in enumerate(panels):
            row_idx = idx // cols
            col_idx = idx % cols
            ax = fig.add_subplot(grid[row_idx, col_idx])
            ax.axis("off")
            source_spec_id = str(panel.get("source_spec_id") or "")
            if source_spec_id:
                source_record = saved_figures.get(source_spec_id)
                if source_record is None:
                    ax.text(0.5, 0.5, f"Missing figure\n{source_spec_id}", ha="center", va="center", fontsize=11)
                else:
                    image = plt.imread(source_record.file_path)
                    ax.imshow(image)
                    source_paths.append(source_record.relative_path)
            else:
                ax.text(
                    0.03,
                    0.96,
                    textwrap.fill(str(panel.get("text") or ""), width=46),
                    ha="left",
                    va="top",
                    fontsize=11,
                    color="#334155",
                    transform=ax.transAxes,
                    bbox={"boxstyle": "round,pad=0.45", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
                )
            ax.set_title(str(panel.get("panel_title") or ""), loc="left", fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), fontweight="bold")
        note_ax = fig.add_subplot(grid[rows, :])
        self._add_note_box(note_ax, str(spec.get("note_box_title") or "Board reading guide"), [str(item) for item in spec.get("note_lines", [])])
        fig.suptitle(str(spec["figure_title"]), x=0.04, y=0.965, ha="left", fontsize=float((self.style.get("typography") or {}).get("title_size") or 19), fontweight="bold")
        fig.text(0.04, 0.932, str(spec["subtitle"]), ha="left", va="top", fontsize=float((self.style.get("typography") or {}).get("subtitle_size") or 10), color="#475569")
        pixel_width, pixel_height = self._figure_pixel_size(fig)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return self._register_figure(
            spec,
            output_path,
            source_paths,
            pixel_width=pixel_width,
            pixel_height=pixel_height,
        )

    def _spatial_spec(
        self,
        *,
        spec_id: str,
        figure_family_code: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        view_type: str,
        variant: str,
        figure_slug: str,
        figure_title: str,
        subtitle: str,
        interpretation: str,
        notes: str,
        note_lines: list[str],
        legend_keys: list[str],
        raster_layers: list[dict[str, Any]],
        recommended_for_main_defense: bool = False,
        recommended_for_paper: bool = True,
        scenario_id: str = "",
        map_panel_title: str | None = None,
        show_source: bool = True,
        show_init: bool = False,
        show_validation: bool = False,
        include_source_in_crop: bool = True,
        include_init_in_crop: bool = False,
        include_validation_in_crop: bool = False,
        status_key_override: str = "",
    ) -> dict[str, Any]:
        return {
            "spec_id": spec_id,
            "renderer": "spatial",
            "figure_family_code": figure_family_code,
            "case_id": case_id,
            "phase_or_track": phase_or_track,
            "date_token": date_token,
            "model_names": model_names,
            "run_type": run_type,
            "scenario_id": scenario_id,
            "view_type": view_type,
            "variant": variant,
            "figure_slug": figure_slug,
            "figure_title": figure_title,
            "subtitle": subtitle,
            "legend_keys": legend_keys,
            "note_lines": note_lines,
            "note_box_title": "Plain-language reading guide",
            "short_plain_language_interpretation": interpretation,
            "recommended_for_main_defense": recommended_for_main_defense,
            "recommended_for_paper": recommended_for_paper,
            "notes": notes,
            "raster_layers": raster_layers,
            "map_panel_title": map_panel_title,
            "show_source": show_source,
            "show_init": show_init,
            "show_validation": show_validation,
            "include_source_in_crop": include_source_in_crop,
            "include_init_in_crop": include_init_in_crop,
            "include_validation_in_crop": include_validation_in_crop,
            "status_key_override": status_key_override,
        }

    def _track_spec(
        self,
        *,
        spec_id: str,
        figure_family_code: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        view_type: str,
        variant: str,
        figure_slug: str,
        figure_title: str,
        subtitle: str,
        interpretation: str,
        notes: str,
        note_lines: list[str],
        legend_keys: list[str],
        renderer: str,
        track_path: str = "",
        track_paths: list[str] | None = None,
        model_kind: str = "opendrift",
        member_sample_count: int = 12,
        raster_layers: list[dict[str, Any]] | None = None,
        status_key_override: str = "",
    ) -> dict[str, Any]:
        payload = {
            "spec_id": spec_id,
            "renderer": renderer,
            "figure_family_code": figure_family_code,
            "case_id": case_id,
            "phase_or_track": phase_or_track,
            "date_token": date_token,
            "model_names": model_names,
            "run_type": run_type,
            "scenario_id": "",
            "view_type": view_type,
            "variant": variant,
            "figure_slug": figure_slug,
            "figure_title": figure_title,
            "subtitle": subtitle,
            "legend_keys": legend_keys,
            "note_lines": note_lines,
            "note_box_title": "Plain-language reading guide",
            "short_plain_language_interpretation": interpretation,
            "recommended_for_main_defense": False,
            "recommended_for_paper": True,
            "notes": notes,
            "model_kind": model_kind,
            "member_sample_count": member_sample_count,
            "raster_layers": raster_layers or [],
            "status_key_override": status_key_override,
        }
        if track_path:
            payload["track_path"] = track_path
        if track_paths:
            payload["track_paths"] = track_paths
        return payload

    def _chart_spec(
        self,
        *,
        spec_id: str,
        figure_family_code: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        figure_slug: str,
        figure_title: str,
        subtitle: str,
        interpretation: str,
        notes: str,
        note_lines: list[str],
        renderer: str,
        csv_path: str,
        scenario_id: str = "",
        recommended_for_main_defense: bool = False,
        recommended_for_paper: bool = True,
        view_type: str = "single",
        variant: str = "paper",
    ) -> dict[str, Any]:
        return {
            "spec_id": spec_id,
            "renderer": renderer,
            "figure_family_code": figure_family_code,
            "case_id": case_id,
            "phase_or_track": phase_or_track,
            "date_token": date_token,
            "model_names": model_names,
            "run_type": run_type,
            "scenario_id": scenario_id,
            "view_type": view_type,
            "variant": variant,
            "figure_slug": figure_slug,
            "figure_title": figure_title,
            "subtitle": subtitle,
            "legend_keys": [],
            "note_lines": note_lines,
            "note_box_title": "Plain-language reading guide",
            "short_plain_language_interpretation": interpretation,
            "recommended_for_main_defense": recommended_for_main_defense,
            "recommended_for_paper": recommended_for_paper,
            "notes": notes,
            "csv_path": csv_path,
        }

    def _text_spec(
        self,
        *,
        spec_id: str,
        figure_family_code: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        figure_slug: str,
        figure_title: str,
        subtitle: str,
        interpretation: str,
        notes: str,
        note_lines: list[str],
        scenario_id: str = "",
        recommended_for_main_defense: bool = False,
        recommended_for_paper: bool = False,
        view_type: str = "single",
        variant: str = "paper",
        source_paths: list[str] | None = None,
    ) -> dict[str, Any]:
        return {
            "spec_id": spec_id,
            "renderer": "text",
            "figure_family_code": figure_family_code,
            "case_id": case_id,
            "phase_or_track": phase_or_track,
            "date_token": date_token,
            "model_names": model_names,
            "run_type": run_type,
            "scenario_id": scenario_id,
            "view_type": view_type,
            "variant": variant,
            "figure_slug": figure_slug,
            "figure_title": figure_title,
            "subtitle": subtitle,
            "legend_keys": [],
            "note_lines": note_lines,
            "note_box_title": "Plain-language reading guide",
            "short_plain_language_interpretation": interpretation,
            "recommended_for_main_defense": recommended_for_main_defense,
            "recommended_for_paper": recommended_for_paper,
            "notes": notes,
            "source_paths": source_paths or [],
        }

    def _image_spec(
        self,
        *,
        spec_id: str,
        figure_family_code: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        view_type: str,
        variant: str,
        figure_slug: str,
        figure_title: str,
        subtitle: str,
        interpretation: str,
        notes: str,
        source_image_path: str,
        source_paths: list[str] | None = None,
        recommended_for_main_defense: bool = False,
        recommended_for_paper: bool = False,
        legacy_debug_only: bool = False,
    ) -> dict[str, Any]:
        return {
            "spec_id": spec_id,
            "renderer": "external_image",
            "figure_family_code": figure_family_code,
            "case_id": case_id,
            "phase_or_track": phase_or_track,
            "date_token": date_token,
            "model_names": model_names,
            "run_type": run_type,
            "scenario_id": "",
            "view_type": view_type,
            "variant": variant,
            "figure_slug": figure_slug,
            "figure_title": figure_title,
            "subtitle": subtitle,
            "legend_keys": [],
            "note_lines": [],
            "short_plain_language_interpretation": interpretation,
            "recommended_for_main_defense": recommended_for_main_defense,
            "recommended_for_paper": recommended_for_paper,
            "legacy_debug_only": legacy_debug_only,
            "notes": notes,
            "source_image_path": source_image_path,
            "source_paths": source_paths or [],
        }

    def _board_spec(
        self,
        *,
        spec_id: str,
        figure_family_code: str,
        case_id: str,
        phase_or_track: str,
        date_token: str,
        model_names: str,
        run_type: str,
        figure_slug: str,
        figure_title: str,
        subtitle: str,
        interpretation: str,
        notes: str,
        note_lines: list[str],
        panels: list[dict[str, Any]],
        scenario_id: str = "",
        recommended_for_main_defense: bool = True,
        recommended_for_paper: bool = False,
        status_key_override: str = "",
    ) -> dict[str, Any]:
        return {
            "spec_id": spec_id,
            "figure_family_code": figure_family_code,
            "case_id": case_id,
            "phase_or_track": phase_or_track,
            "date_token": date_token,
            "model_names": model_names,
            "run_type": run_type,
            "scenario_id": scenario_id,
            "view_type": "board",
            "variant": "slide",
            "figure_slug": figure_slug,
            "figure_title": figure_title,
            "subtitle": subtitle,
            "note_lines": note_lines,
            "note_box_title": "Board reading guide",
            "short_plain_language_interpretation": interpretation,
            "recommended_for_main_defense": recommended_for_main_defense,
            "recommended_for_paper": recommended_for_paper,
            "notes": notes,
            "panels": panels,
            "status_key_override": status_key_override,
        }

    def _normalize_date_token(self, value: Any) -> str:
        return str(value or "").strip().replace("_to_", "/")

    def _format_score_value(self, value: float | None) -> str:
        if value is None:
            return "n/a"
        rounded = Decimal(str(value)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        return f"{rounded:.3f}"

    def _compose_note_lines(self, context_lines: list[str], score_lines: list[str] | None = None) -> list[str]:
        lines = [str(line) for line in context_lines if str(line).strip()]
        for line in score_lines or []:
            if str(line).strip():
                lines.append(str(line))
        return lines

    def _row_mean_fss(self, row: pd.Series | dict[str, Any] | None) -> float | None:
        if row is None:
            return None
        if isinstance(row, dict):
            getter = row.get
        else:
            getter = row.get
        mean_value = _optional_float(getter("mean_fss"))
        if mean_value is not None:
            return mean_value
        values = [_optional_float(getter(f"fss_{window}km")) for window in (1, 3, 5, 10)]
        finite = [value for value in values if value is not None]
        if not finite:
            return None
        return float(np.mean(finite))

    def _format_fss_line(self, row: pd.Series | dict[str, Any] | None, label: str = "") -> str:
        prefix = f"{label} " if label else ""
        if row is None:
            return f"{prefix}metric unavailable in stored summary."
        if isinstance(row, dict):
            getter = row.get
        else:
            getter = row.get
        values = [_optional_float(getter(f"fss_{window}km")) for window in (1, 3, 5, 10)]
        if not any(value is not None for value in values):
            return f"{prefix}metric unavailable in stored summary."
        mean_value = self._row_mean_fss(row)
        formatted_values = "/".join(self._format_score_value(value) for value in values)
        return f"{prefix}FSS(1/3/5/10 km): {formatted_values}; mean: {self._format_score_value(mean_value)}."

    def _mindoro_primary_row(self) -> pd.Series | None:
        summary = self.mindoro_reinit_summary
        if summary.empty:
            return None
        if "branch_id" in summary.columns:
            row = summary.loc[summary["branch_id"].astype(str) == "R1_previous"]
            if not row.empty:
                return row.iloc[0]
        return summary.iloc[0]

    def _mindoro_primary_branch_row(self, branch_id: str) -> pd.Series | None:
        summary = self.mindoro_reinit_summary
        if summary.empty:
            return None
        if "branch_id" in summary.columns:
            row = summary.loc[summary["branch_id"].astype(str) == str(branch_id)]
            if not row.empty:
                return row.iloc[0]
        return self._mindoro_primary_row()

    def _mindoro_strict_row(self) -> pd.Series | None:
        summary = self.mindoro_phase3b_summary
        if summary.empty:
            return None
        if "pair_id" in summary.columns:
            row = summary.loc[summary["pair_id"].astype(str) == "official_primary_march6"]
            if not row.empty:
                return row.iloc[0]
        return summary.iloc[0]

    def _mindoro_crossmodel_row(self, model_key: str) -> pd.Series | None:
        summary = self.mindoro_reinit_crossmodel_summary
        if summary.empty:
            return None
        track_map = {
            "r1": "R1_previous_reinit_p50",
            "ensemble": "R1_previous_reinit_p50",
            "r0": "R0_reinit_p50",
            "deterministic": "R0_reinit_p50",
            "pygnome": "pygnome_reinit_deterministic",
        }
        track_id = track_map.get(model_key, "")
        if track_id and "track_id" in summary.columns:
            row = summary.loc[summary["track_id"].astype(str) == track_id]
            if not row.empty:
                return row.iloc[0]
        model_map = {
            "r1": "OpenDrift R1 previous reinit p50",
            "ensemble": "OpenDrift R1 previous reinit p50",
            "r0": "OpenDrift R0 reinit p50",
            "deterministic": "OpenDrift R0 reinit p50",
            "pygnome": "PyGNOME deterministic March 13 reinit comparator",
        }
        model_name = model_map.get(model_key, "")
        if model_name and "model_name" in summary.columns:
            row = summary.loc[summary["model_name"].astype(str) == model_name]
            if not row.empty:
                return row.iloc[0]
        return summary.iloc[0]

    def _dwh_deterministic_row(self, date_token: str) -> pd.Series | None:
        summary = self.dwh_summary
        if summary.empty:
            return None
        normalized_date = self._normalize_date_token(date_token)
        pair_role = "event_corridor" if "/" in normalized_date else "per_date"
        filters = pd.Series(True, index=summary.index)
        if "pair_role" in summary.columns:
            filters &= summary["pair_role"].astype(str) == pair_role
        if "pairing_date_utc" in summary.columns:
            filters &= summary["pairing_date_utc"].map(self._normalize_date_token) == normalized_date
        row = summary.loc[filters]
        if not row.empty:
            return row.iloc[0]
        if "pair_role" in summary.columns:
            fallback = summary.loc[summary["pair_role"].astype(str) == pair_role]
            if not fallback.empty:
                return fallback.iloc[0]
        return summary.iloc[0]

    def _dwh_event_model_row(self, model_key: str) -> pd.Series | None:
        summary = self.dwh_all_results
        if summary.empty:
            return None
        filters = pd.Series(True, index=summary.index)
        if "pair_role" in summary.columns:
            filters &= summary["pair_role"].astype(str) == "event_corridor"
        model_map = {
            "deterministic": "OpenDrift deterministic",
            "p50": "OpenDrift ensemble p50",
            "p90": "OpenDrift ensemble p90",
            "pygnome": "PyGNOME deterministic",
        }
        model_name = model_map.get(model_key, "")
        if model_name and "model_result" in summary.columns:
            row = summary.loc[filters & (summary["model_result"].astype(str) == model_name)]
            if not row.empty:
                return row.iloc[0]
        row = summary.loc[filters]
        if not row.empty:
            return row.iloc[0]
        return summary.iloc[0]

    def _mindoro_primary_context_lines(self) -> list[str]:
        row = self._mindoro_primary_row()
        if row is None:
            return ["The March 13 -> March 14 reinit summary CSV was not available, so this figure should be read visually."]
        confirmation_recipe = str(self.mindoro_phase1_confirmation_candidate.get("selected_recipe", "") or "")
        return [
            f"{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE} is now carried by the March 13 -> March 14 promoted Mindoro validation pair, seeded from the March 13 NOAA polygon and scored against the March 14 NOAA target.",
            MINDORO_SHARED_IMAGERY_CAVEAT,
            (
                f"The separate focused 2016-2023 Mindoro drifter rerun selected the same {confirmation_recipe} recipe used by the stored B1 run and now serves as the active B1 recipe-provenance lane."
                if confirmation_recipe
                else "The separate focused 2016-2023 Mindoro drifter provenance artifact was unavailable."
            ),
            f"The promoted OpenDrift R1 previous reinit p50 row reaches FSS beyond zero at 3/5/10 km with {int(row.get('forecast_nonzero_cells', 0))} forecast cells against {int(row.get('obs_nonzero_cells', 0))} observed cells.",
        ]

    def _mindoro_strict_context_lines(self) -> list[str]:
        row = self._mindoro_strict_row()
        if row is None:
            return ["The strict March 6 summary CSV was not available, so this figure should be read visually."]
        return [
            f"March 6 remains the legacy sparse-reference honesty figure: the observed footprint is only {int(row.get('obs_nonzero_cells', 0))} non-zero cells.",
            f"The official p50 product is intentionally sparse here, with {int(row.get('forecast_nonzero_cells', 0))} forecast cells in this strict slice.",
            "Use this family to explain why March 6 remains visible for methods honesty even though it is no longer the promoted primary validation row.",
        ]

    def _mindoro_crossmodel_context_lines(self) -> list[str]:
        summary = self.mindoro_reinit_crossmodel_summary
        if summary.empty:
            return ["The March 13 -> March 14 cross-model summary CSV was not available."]
        ranking = summary.copy()
        if "mean_fss" not in ranking.columns:
            ranking["mean_fss"] = ranking.apply(self._row_mean_fss, axis=1)
        if "track_tie_break_order" not in ranking.columns:
            ranking["track_tie_break_order"] = range(len(ranking))
        top = ranking.sort_values(
            ["mean_fss", "fss_1km", "iou", "nearest_distance_to_obs_m", "track_tie_break_order"],
            ascending=[False, False, False, True, True],
        ).iloc[0]
        return [
            "This cross-model lane reuses the completed March 13 -> March 14 reinit outputs and adds one deterministic PyGNOME surrogate seeded from the same March 13 NOAA polygon.",
            f"{top['model_name']} currently ranks first in the promoted local cross-model bundle under the current case definition.",
            "PyGNOME remains comparator-only here, and the shared-imagery caveat still applies to the March 13 -> March 14 pair.",
        ]

    def _mindoro_comparison_context_lines(self) -> list[str]:
        return [
            "This comparison keeps the promoted OpenDrift reinit and PyGNOME comparator views side by side without treating PyGNOME as truth.",
            "In this lane, the score lines come from the March 14 NOAA target while the seed geometry comes from the March 13 NOAA polygon.",
            MINDORO_SHARED_IMAGERY_CAVEAT,
        ]

    def _dwh_deterministic_context_lines(self) -> list[str]:
        return [
            "DWH is a separate external transfer-validation/support case; Mindoro remains the main Philippine thesis case.",
            "The DWH forcing rule is readiness-gated rather than Phase 1 drifter-selected baseline logic, and the current stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
        ]

    def _dwh_model_context_lines(self) -> list[str]:
        return [
            "Observed DWH daily masks remain truth throughout these figures; PyGNOME is comparator-only.",
            "Each score line belongs to the matching model panel only; the observed corridor panel is included for visual reference.",
        ]

    def _dwh_trajectory_context_lines(self) -> list[str]:
        return [
            "These trajectory views are appendix/support material for the separate DWH transfer-validation case rather than the main thesis case.",
            "Use the scored footprint figures when the panel asks about validation overlap rather than transport shape.",
        ]

    def _mindoro_primary_score_line(self, label: str = "") -> str:
        return self._format_fss_line(self._mindoro_primary_row(), label)

    def _mindoro_strict_score_line(self, label: str = "") -> str:
        return self._format_fss_line(self._mindoro_strict_row(), label)

    def _mindoro_crossmodel_score_line(self, model_key: str, label: str = "") -> str:
        return self._format_fss_line(self._mindoro_crossmodel_row(model_key), label)

    def _mindoro_primary_branch_score_line(self, branch_id: str, label: str = "") -> str:
        return self._format_fss_line(self._mindoro_primary_branch_row(branch_id), label)

    def _dwh_deterministic_score_line(self, date_token: str, label: str = "") -> str:
        return self._format_fss_line(self._dwh_deterministic_row(date_token), label)

    def _dwh_event_score_line(self, model_key: str, label: str = "") -> str:
        return self._format_fss_line(self._dwh_event_model_row(model_key), label)

    def _mindoro_primary_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_primary_context_lines(), list(score_lines))

    def _mindoro_strict_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_strict_context_lines(), list(score_lines))

    def _mindoro_crossmodel_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_crossmodel_context_lines(), list(score_lines))

    def _mindoro_comparison_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_comparison_context_lines(), list(score_lines))

    def _stored_empty_forecast_line(self, row: pd.Series | dict[str, Any] | None, label: str = "") -> str:
        if row is None:
            return ""
        if isinstance(row, dict):
            getter = row.get
        else:
            getter = row.get
        reason = (
            str(getter("empty_forecast_reason_survival") or "").strip()
            or str(getter("empty_forecast_reason") or "").strip()
        )
        if not reason:
            return ""
        prefix = f"{label} " if label else ""
        plain_reason = reason.replace("_", " ")
        return f"{prefix}Stored summary note: {plain_reason}."

    def _mindoro_reinit_seed_mask_path(self) -> str:
        seed_release = self.mindoro_reinit_manifest.get("seed_release") or {}
        return str(
            seed_release.get("seed_mask_path")
            or (
                Path("output")
                / "CASE_MINDORO_RETRO_2023"
                / "phase3b_extended_public_scored_march13_14_reinit"
                / "march13_seed_mask_on_grid.tif"
            )
        )

    def _mindoro_reinit_target_mask_path(self) -> str:
        target_source = self.mindoro_reinit_manifest.get("selected_target_source") or {}
        return str(
            target_source.get("extended_obs_mask")
            or (
                Path("output")
                / "CASE_MINDORO_RETRO_2023"
                / "phase3b_extended_public"
                / "accepted_obs_masks"
                / "10b37c42a9754363a5f7b14199b077e6.tif"
            )
        )

    def _mindoro_primary_publication_specs(self) -> list[dict[str, Any]]:
        subtitle = (
            f"{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE} | Mindoro | 13-14 March 2023 | "
            "shared-imagery caveat explicit"
        )
        seed_mask = self._mindoro_reinit_seed_mask_path()
        target_mask = self._mindoro_reinit_target_mask_path()
        r1_row = self._mindoro_primary_branch_row("R1_previous")
        r0_row = self._mindoro_primary_branch_row("R0")
        r1_mask = str(r1_row.get("forecast_path") or r1_row.get("march14_forecast_path") or "") if r1_row is not None else ""
        r0_mask = str(r0_row.get("forecast_path") or r0_row.get("march14_forecast_path") or "") if r0_row is not None else ""
        return [
            self._spatial_spec(
                spec_id="mindoro_primary_seed_mask",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_reinit_primary",
                date_token="2023-03-13",
                model_names="observation",
                run_type="single_seed_observation",
                view_type="single",
                variant="paper",
                figure_slug="march13_seed_mask_on_grid",
                figure_title="Mindoro March 13 seed mask on grid",
                map_panel_title="",
                subtitle=subtitle,
                interpretation=(
                    "This figure redraws the stored March 13 seed geometry on the scoring grid so "
                    f"{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE.lower()} starts from a "
                    "publication-grade observation panel rather than a QA screenshot."
                ),
                notes=(
                    "Built from the stored March 13 seed mask raster, shoreline context, and "
                    "source-point geometry only; no scientific rerun was triggered, and the later "
                    "2016-2023 Mindoro-focused drifter rerun now provides the active B1 recipe-provenance story."
                ),
                note_lines=self._mindoro_primary_note_lines(
                    "Seed geometry is shown on the same canonical grid used downstream for the March 14 comparison."
                ),
                legend_keys=["initialization_polygon", "source_point"],
                raster_layers=[
                    {
                        "path": seed_mask,
                        "legend_key": "initialization_polygon",
                        "alpha": 0.24,
                        "linewidth": 1.5,
                        "linestyle": "--",
                        "zorder": 5,
                    }
                ],
                show_source=True,
                include_source_in_crop=False,
            ),
            self._spatial_spec(
                spec_id="mindoro_primary_seed_vs_target",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_reinit_primary",
                date_token="2023-03-13_to_2023-03-14",
                model_names="observation",
                run_type="single_seed_target_compare",
                view_type="single",
                variant="paper",
                figure_slug="march13_seed_vs_march14_target",
                figure_title="Mindoro March 13 seed vs March 14 target",
                map_panel_title="",
                subtitle=subtitle,
                interpretation=(
                    "This figure makes the promoted March 13 seed and March 14 observation geometry "
                    f"explicit before any model overlay is shown for {MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE.lower()}, "
                    "using the stored on-grid rasters rather than the earlier QA composite."
                ),
                notes=(
                    "Built from the stored March 13 seed mask and March 14 observation mask only, "
                    "with the shared-imagery caveat and the separate focused Phase 1 provenance note carried into the note box."
                ),
                note_lines=self._mindoro_primary_note_lines(
                    "Orange shows the March 13 seed geometry and dark slate shows the March 14 observation target."
                ),
                legend_keys=["initialization_polygon", "observed_mask", "source_point"],
                raster_layers=[
                    {
                        "path": seed_mask,
                        "legend_key": "initialization_polygon",
                        "alpha": 0.18,
                        "linewidth": 1.5,
                        "linestyle": "--",
                        "zorder": 5,
                    },
                    {
                        "path": target_mask,
                        "legend_key": "observed_mask",
                        "alpha": 0.28,
                        "linewidth": 1.3,
                        "zorder": 6,
                    },
                ],
                show_source=True,
                include_source_in_crop=False,
            ),
            self._spatial_spec(
                spec_id="mindoro_primary_r1_overlay",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_reinit_primary",
                date_token="2023-03-14",
                model_names="opendrift",
                run_type="single_primary_overlay",
                view_type="single",
                variant="paper",
                figure_slug="march14_r1_previous_overlay",
                figure_title="Mindoro March 14 promoted OpenDrift R1_previous",
                map_panel_title="",
                subtitle=subtitle,
                interpretation=(
                    "This is the promoted March 14 Mindoro validation overlay for "
                    f"{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE.lower()}, rebuilt from stored "
                    "rasters with the same publication grammar used by the DWH boards."
                ),
                notes=(
                    "Built from the stored March 14 observation mask, the stored March 13 seed mask "
                    "outline, and the stored OpenDrift R1 previous p50 raster only; the later "
                    "2016-2023 Mindoro-focused drifter rerun selected the same recipe without rewriting stored run provenance."
                ),
                note_lines=self._mindoro_primary_note_lines(
                    self._mindoro_primary_branch_score_line("R1_previous", "OpenDrift R1 previous reinit p50")
                ),
                legend_keys=["observed_mask", "ensemble_p50", "initialization_polygon", "source_point"],
                raster_layers=[
                    {
                        "path": target_mask,
                        "legend_key": "observed_mask",
                        "alpha": 0.34,
                        "linewidth": 1.1,
                        "zorder": 5,
                    },
                    {
                        "path": seed_mask,
                        "legend_key": "initialization_polygon",
                        "fill": False,
                        "outline": True,
                        "linewidth": 1.2,
                        "linestyle": "--",
                        "zorder": 6,
                    },
                    {
                        "path": r1_mask,
                        "legend_key": "ensemble_p50",
                        "alpha": 0.26,
                        "linewidth": 1.4,
                        "zorder": 7,
                    },
                ],
                show_source=True,
                include_source_in_crop=False,
                recommended_for_main_defense=True,
                recommended_for_paper=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_primary_r0_overlay",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_reinit_primary",
                date_token="2023-03-14",
                model_names="opendrift",
                run_type="single_primary_overlay",
                view_type="single",
                variant="paper",
                figure_slug="march14_r0_overlay",
                figure_title="Mindoro March 14 OpenDrift R0 branch",
                map_panel_title="",
                subtitle=subtitle,
                interpretation=(
                    "This companion figure keeps the stored R0 branch visible in the same publication "
                    "grammar so the promoted R1 result can be compared against it honestly within the "
                    "final B1 Phase 3B framing."
                ),
                notes=(
                    "Built from the stored March 14 observation mask, the stored March 13 seed mask "
                    "outline, and the stored OpenDrift R0 p50 raster only."
                ),
                note_lines=self._mindoro_primary_note_lines(
                    self._mindoro_primary_branch_score_line("R0", "OpenDrift R0 reinit p50"),
                    self._stored_empty_forecast_line(r0_row, "OpenDrift R0 reinit p50"),
                ),
                legend_keys=["observed_mask", "ensemble_p50", "initialization_polygon", "source_point"],
                raster_layers=[
                    {
                        "path": target_mask,
                        "legend_key": "observed_mask",
                        "alpha": 0.34,
                        "linewidth": 1.1,
                        "zorder": 5,
                    },
                    {
                        "path": seed_mask,
                        "legend_key": "initialization_polygon",
                        "fill": False,
                        "outline": True,
                        "linewidth": 1.2,
                        "linestyle": "--",
                        "zorder": 6,
                    },
                    {
                        "path": r0_mask,
                        "legend_key": "ensemble_p50",
                        "alpha": 0.26,
                        "linewidth": 1.4,
                        "zorder": 7,
                    },
                ],
                show_source=True,
                include_source_in_crop=False,
            ),
        ]

    def _mindoro_crossmodel_publication_specs(self) -> list[dict[str, Any]]:
        subtitle = "Mindoro | 13-14 March 2023 | cross-model comparator on the March 14 NOAA target | shared March 12 imagery caveat"
        seed_mask = self._mindoro_reinit_seed_mask_path()
        target_mask = self._mindoro_reinit_target_mask_path()
        r1_row = self._mindoro_crossmodel_row("r1")
        r0_row = self._mindoro_crossmodel_row("r0")
        pygnome_row = self._mindoro_crossmodel_row("pygnome")
        r1_mask = str(r1_row.get("forecast_path") or "") if r1_row is not None else ""
        r0_mask = str(r0_row.get("forecast_path") or "") if r0_row is not None else ""
        pygnome_mask = str(pygnome_row.get("forecast_path") or "") if pygnome_row is not None else ""
        return [
            self._spatial_spec(
                spec_id="mindoro_crossmodel_r1_overlay",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3a_reinit_crossmodel",
                date_token="2023-03-14",
                model_names="opendrift",
                run_type="single_model_overlay",
                view_type="single",
                variant="paper",
                figure_slug="march14_crossmodel_r1_overlay",
                figure_title="Mindoro March 14 OpenDrift R1_previous",
                map_panel_title="",
                subtitle=subtitle,
                interpretation="This figure shows the top-ranked OpenDrift model in the stored March 14 cross-model bundle, redrawn from the saved rasters instead of copied from QA.",
                notes="Built from the stored March 14 observation mask, March 13 seed mask outline, and the stored OpenDrift R1 previous p50 raster only.",
                note_lines=self._mindoro_crossmodel_note_lines(
                    self._mindoro_crossmodel_score_line("r1", "OpenDrift R1 previous reinit p50")
                ),
                legend_keys=["observed_mask", "ensemble_p50", "initialization_polygon", "source_point"],
                raster_layers=[
                    {
                        "path": target_mask,
                        "legend_key": "observed_mask",
                        "alpha": 0.34,
                        "linewidth": 1.1,
                        "zorder": 5,
                    },
                    {
                        "path": seed_mask,
                        "legend_key": "initialization_polygon",
                        "fill": False,
                        "outline": True,
                        "linewidth": 1.2,
                        "linestyle": "--",
                        "zorder": 6,
                    },
                    {
                        "path": r1_mask,
                        "legend_key": "ensemble_p50",
                        "alpha": 0.26,
                        "linewidth": 1.4,
                        "zorder": 7,
                    },
                ],
                show_source=True,
                include_source_in_crop=False,
                recommended_for_main_defense=True,
                recommended_for_paper=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_crossmodel_r0_overlay",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3a_reinit_crossmodel",
                date_token="2023-03-14",
                model_names="opendrift",
                run_type="single_model_overlay",
                view_type="single",
                variant="paper",
                figure_slug="march14_crossmodel_r0_overlay",
                figure_title="Mindoro March 14 OpenDrift R0",
                map_panel_title="",
                subtitle=subtitle,
                interpretation="This figure keeps the baseline OpenDrift branch visible in the promoted March 14 cross-model discussion using the same publication-grade map grammar as the DWH family.",
                notes="Built from the stored March 14 observation mask, March 13 seed mask outline, and the stored OpenDrift R0 p50 raster only.",
                note_lines=self._mindoro_crossmodel_note_lines(
                    self._mindoro_crossmodel_score_line("r0", "OpenDrift R0 reinit p50"),
                    self._stored_empty_forecast_line(r0_row, "OpenDrift R0 reinit p50"),
                ),
                legend_keys=["observed_mask", "ensemble_p50", "initialization_polygon", "source_point"],
                raster_layers=[
                    {
                        "path": target_mask,
                        "legend_key": "observed_mask",
                        "alpha": 0.34,
                        "linewidth": 1.1,
                        "zorder": 5,
                    },
                    {
                        "path": seed_mask,
                        "legend_key": "initialization_polygon",
                        "fill": False,
                        "outline": True,
                        "linewidth": 1.2,
                        "linestyle": "--",
                        "zorder": 6,
                    },
                    {
                        "path": r0_mask,
                        "legend_key": "ensemble_p50",
                        "alpha": 0.26,
                        "linewidth": 1.4,
                        "zorder": 7,
                    },
                ],
                show_source=True,
                include_source_in_crop=False,
            ),
            self._spatial_spec(
                spec_id="mindoro_crossmodel_pygnome_overlay",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3a_reinit_crossmodel",
                date_token="2023-03-14",
                model_names="pygnome",
                run_type="single_model_overlay",
                view_type="single",
                variant="paper",
                figure_slug="march14_crossmodel_pygnome_overlay",
                figure_title="Mindoro March 14 PyGNOME comparator",
                map_panel_title="",
                subtitle=subtitle,
                interpretation="This figure preserves the PyGNOME comparator in the promoted March 14 lane without treating it as truth, rebuilt from the stored comparator mask rather than pasted from QA.",
                notes="Built from the stored March 14 observation mask, March 13 seed mask outline, and the stored PyGNOME comparator raster only.",
                note_lines=self._mindoro_crossmodel_note_lines(
                    self._mindoro_crossmodel_score_line("pygnome", "PyGNOME comparator")
                ),
                legend_keys=["observed_mask", "pygnome", "initialization_polygon", "source_point"],
                raster_layers=[
                    {
                        "path": target_mask,
                        "legend_key": "observed_mask",
                        "alpha": 0.34,
                        "linewidth": 1.1,
                        "zorder": 5,
                    },
                    {
                        "path": seed_mask,
                        "legend_key": "initialization_polygon",
                        "fill": False,
                        "outline": True,
                        "linewidth": 1.2,
                        "linestyle": "--",
                        "zorder": 6,
                    },
                    {
                        "path": pygnome_mask,
                        "legend_key": "pygnome",
                        "alpha": 0.24,
                        "linewidth": 1.4,
                        "zorder": 7,
                    },
                ],
                show_source=True,
                include_source_in_crop=False,
            ),
        ]

    def _mindoro_legacy_publication_specs(self) -> list[dict[str, Any]]:
        subtitle = "Mindoro | 6 March 2023 | legacy sparse-reference honesty view"
        return [
            self._image_spec(
                spec_id="mindoro_legacy_strict_overlay",
                figure_family_code="C",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_legacy_strict",
                date_token="2023-03-06",
                model_names="opendrift",
                run_type="single_legacy_overlay",
                view_type="single",
                variant="paper",
                figure_slug="legacy_strict_overlay",
                figure_title="Mindoro legacy March 6 strict overlay",
                subtitle=subtitle,
                interpretation="This figure is kept as the legacy sparse-reference honesty view rather than the promoted primary validation image.",
                notes="Copied from the stored strict March 6 QA bundle and retained as an honesty-only legacy figure.",
                source_image_path="output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_obsmask_vs_p50.png",
            ),
            self._image_spec(
                spec_id="mindoro_legacy_strict_context",
                figure_family_code="C",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_legacy_strict",
                date_token="2023-03-03_to_2023-03-06",
                model_names="opendrift",
                run_type="single_legacy_context",
                view_type="single",
                variant="paper",
                figure_slug="legacy_strict_context",
                figure_title="Mindoro legacy March 6 source/initialization/validation context",
                subtitle=subtitle,
                interpretation="This context figure explains how the legacy March 6 slice fits into the older Mindoro framing without pretending it is still the main result.",
                notes="Copied from the stored strict March 6 QA bundle and retained as an honesty-only legacy context figure.",
                source_image_path="output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_source_init_validation_overlay.png",
            ),
        ]

    def _mindoro_promoted_board_specs(self) -> list[dict[str, Any]]:
        primary_subtitle = (
            f"{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE} | Mindoro | 13-14 March 2023 | shared-imagery caveat explicit"
        )
        crossmodel_subtitle = (
            "Mindoro | 13-14 March 2023 | promoted cross-model comparator on the March 14 NOAA target | shared-imagery caveat explicit"
        )
        legacy_subtitle = "Mindoro | 6 March 2023 | legacy sparse-reference honesty view"
        return [
            self._board_spec(
                spec_id="mindoro_primary_board",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_reinit_primary",
                date_token="2023-03-13_to_2023-03-14",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="mindoro_primary_validation_board",
                figure_title="Mindoro March 13 -> March 14 primary validation board",
                subtitle=primary_subtitle,
                interpretation=(
                    "This is now the main Mindoro presentation board for Phase 3B observation-based spatial validation "
                    "using public Mindoro spill extents because it centers the promoted March 13 -> March 14 validation "
                    "pair, the best OpenDrift result, and the shared-imagery caveat without rewriting the stored run provenance."
                ),
                notes="Board assembled from publication-grade March 13 -> March 14 singles rebuilt from stored rasters and vectors only.",
                note_lines=self._mindoro_primary_note_lines(
                    self._mindoro_primary_branch_score_line("R1_previous", "OpenDrift R1 previous reinit p50"),
                    self._mindoro_primary_branch_score_line("R0", "OpenDrift R0 reinit p50"),
                    self._stored_empty_forecast_line(self._mindoro_primary_branch_row("R0"), "OpenDrift R0 reinit p50"),
                ),
                panels=[
                    {"panel_title": "March 13 seed vs March 14 target", "source_spec_id": "mindoro_primary_seed_vs_target"},
                    {"panel_title": "Promoted R1 previous reinit overlay", "source_spec_id": "mindoro_primary_r1_overlay"},
                    {"panel_title": "R0 baseline branch overlay", "source_spec_id": "mindoro_primary_r0_overlay"},
                    {"panel_title": "March 13 seed mask on grid", "source_spec_id": "mindoro_primary_seed_mask"},
                ],
                recommended_for_main_defense=True,
            ),
            self._board_spec(
                spec_id="mindoro_crossmodel_board",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3a_reinit_crossmodel",
                date_token="2023-03-14",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="mindoro_crossmodel_board",
                figure_title="Mindoro March 13 -> March 14 cross-model comparator board",
                subtitle=crossmodel_subtitle,
                interpretation="This board answers the cross-model question on the promoted March 14 target without treating PyGNOME as truth and without upgrading the shared-imagery pair into an independent day-to-day validation claim.",
                notes="Board assembled from publication-grade March 13 -> March 14 singles rebuilt from stored rasters only; PyGNOME remains comparator-only.",
                note_lines=self._mindoro_comparison_note_lines(
                    self._mindoro_crossmodel_score_line("r1", "OpenDrift R1 previous reinit p50"),
                    self._mindoro_crossmodel_score_line("r0", "OpenDrift R0 reinit p50"),
                    self._mindoro_crossmodel_score_line("pygnome", "PyGNOME comparator"),
                    self._stored_empty_forecast_line(self._mindoro_crossmodel_row("r0"), "OpenDrift R0 reinit p50"),
                ),
                panels=[
                    {"panel_title": "March 14 observation reference context", "source_spec_id": "mindoro_primary_seed_vs_target"},
                    {"panel_title": "OpenDrift R1 previous reinit p50", "source_spec_id": "mindoro_crossmodel_r1_overlay"},
                    {"panel_title": "OpenDrift R0 reinit p50", "source_spec_id": "mindoro_crossmodel_r0_overlay"},
                    {"panel_title": "PyGNOME deterministic comparator", "source_spec_id": "mindoro_crossmodel_pygnome_overlay"},
                ],
                recommended_for_main_defense=True,
            ),
            self._board_spec(
                spec_id="mindoro_legacy_board",
                figure_family_code="C",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_legacy_strict",
                date_token="2023-03-06",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="mindoro_legacy_march6_board",
                figure_title="Mindoro legacy March 6 honesty / limitations board",
                subtitle=legacy_subtitle,
                interpretation="This board preserves the March 6 sparse-reference material transparently as an honesty-only legacy result, but it is no longer the canonical main-validation board.",
                notes="Board assembled from the stored strict March 6 QA figures only and retained as an honesty-only legacy board.",
                note_lines=self._mindoro_strict_note_lines(
                    self._mindoro_strict_score_line("Legacy strict March 6")
                ),
                panels=[
                    {"panel_title": "Legacy strict overlay", "source_spec_id": "mindoro_legacy_strict_overlay"},
                    {"panel_title": "Legacy source/init/validation context", "source_spec_id": "mindoro_legacy_strict_context"},
                ],
                recommended_for_main_defense=False,
            ),
        ]

    def _dwh_deterministic_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._dwh_deterministic_context_lines(), list(score_lines))

    def _dwh_model_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._dwh_model_context_lines(), list(score_lines))

    def _dwh_trajectory_note_lines(self) -> list[str]:
        return self._compose_note_lines(self._dwh_trajectory_context_lines())

    def _phase4_lines(self) -> list[str]:
        summary = _read_csv(self.repo_root / (MINDORO_PHASE4_DIR / "phase4_oil_budget_summary.csv"))
        if summary.empty:
            return ["The Phase 4 oil-budget summary CSV was not available."]
        base = summary.loc[summary["scenario_id"] == "fixed_base_medium_heavy_proxy"]
        heavy = summary.loc[summary["scenario_id"] == "heavier_oil"]
        light = summary.loc[summary["scenario_id"] == "lighter_oil"]
        return [
            f"Light oil evaporates fastest, reaching {float(light.iloc[0]['final_evaporated_pct']):.2f}% by the end of the replay." if not light.empty else "Light oil remains the fastest-weathering scenario in the current replay.",
            f"The fixed base medium-heavy proxy is still the follow-up case because mass-balance deviation reaches {float(base.iloc[0]['max_mass_balance_deviation_pct']):.2f}%." if not base.empty else "The medium-heavy proxy remains the follow-up scenario.",
            f"Heavier oil leaves the largest shoreline burden at about {float(heavy.iloc[0]['total_beached_kg'])/1000.0:.1f} tonnes beached." if not heavy.empty else "Heavier oil leaves the largest shoreline burden in the current replay.",
        ]

    def _phase4_deferred_note_lines(self) -> list[str]:
        matrix = _read_csv(self.repo_root / PHASE4_CROSSMODEL_MATRIX)
        not_comparable = int((matrix.get("classification", pd.Series(dtype=str)) == "not_comparable_honestly").sum()) if not matrix.empty else 0
        blocker_text = ""
        blockers_path = self.repo_root / PHASE4_CROSSMODEL_BLOCKERS
        if blockers_path.exists():
            blocker_lines = [line.strip("- ").strip() for line in blockers_path.read_text(encoding="utf-8").splitlines() if line.strip().startswith("-")]
            if blocker_lines:
                blocker_text = blocker_lines[0]
        if not blocker_text:
            blocker_text = "The current Mindoro PyGNOME benchmark is transport-only with weathering disabled, so it cannot support Phase 4 fate or shoreline comparison."
        quantity_text = f"All {not_comparable} audited Phase 4 quantities were classified as not honestly comparable with the current stored PyGNOME outputs." if not_comparable else "The current stored outputs do not support an honest Phase 4 OpenDrift-versus-PyGNOME comparison."
        return [
            quantity_text,
            blocker_text,
            "Future work: add a matched Mindoro PyGNOME Phase 4 output family with the same oil scenarios, weathering compartments, shoreline-arrival timing, and shoreline-segment tables used by the OpenDrift Phase 4 workflow.",
        ]

    def _mindoro_member_paths(self) -> list[str]:
        return [str(_relative_to_repo(self.repo_root, path)) for path in sorted((self.repo_root / "output" / "CASE_MINDORO_RETRO_2023" / "ensemble").glob("member_*.nc"))]

    def _dwh_member_paths(self) -> list[str]:
        return [str(_relative_to_repo(self.repo_root, path)) for path in sorted((self.repo_root / DWH_ENSEMBLE_DIR / "tracks").glob("member_*.nc"))]

    def _mindoro_publication_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        singles = []
        singles.extend(self._mindoro_primary_publication_specs())
        singles.extend(self._mindoro_crossmodel_publication_specs())
        singles.extend(self._mindoro_legacy_publication_specs())
        boards = self._mindoro_promoted_board_specs()
        return singles, boards

        strict_obs = "data/arcgis/CASE_MINDORO_RETRO_2023/obs_mask_2023-03-06.tif"
        strict_p50 = "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p50_2023-03-06_datecomposite.tif"
        event_obs = "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/observations/eventcorridor_obs_union_2023-03-04_to_2023-03-06.tif"
        event_det = "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/products/C1_od_deterministic/od_control_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif"
        event_ensemble = "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/products/C2_od_ensemble_consolidated/od_ensemble_consolidated_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif"
        event_pygnome = "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/products/C3_pygnome_deterministic/pygnome_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif"
        deterministic_track = "output/CASE_MINDORO_RETRO_2023/forecast/deterministic_control_cmems_era5.nc"
        pygnome_track = "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/products/C3_pygnome_deterministic/pygnome_deterministic_control.nc"
        strict_score_line = self._mindoro_strict_score_line()
        event_det_score_line = self._mindoro_event_score_line("deterministic")
        event_ensemble_score_line = self._mindoro_event_score_line("ensemble")
        event_pygnome_score_line = self._mindoro_event_score_line("pygnome")
        note_lines_strict_observation = self._mindoro_strict_note_lines()
        note_lines_strict_scored = self._mindoro_strict_note_lines(strict_score_line)
        note_lines_event_observation = self._mindoro_event_note_lines()
        note_lines_event_det = self._mindoro_event_note_lines(event_det_score_line)
        note_lines_event_ensemble = self._mindoro_event_note_lines(event_ensemble_score_line)
        note_lines_comparison_det = self._mindoro_comparison_note_lines(event_det_score_line)
        note_lines_comparison_ensemble = self._mindoro_comparison_note_lines(event_ensemble_score_line)
        note_lines_comparison_pygnome = self._mindoro_comparison_note_lines(event_pygnome_score_line)
        note_lines_comparison_overlay = self._mindoro_comparison_note_lines(
            self._mindoro_event_score_line("ensemble", "OpenDrift consolidated ensemble trajectory"),
            self._mindoro_event_score_line("pygnome", "PyGNOME comparator"),
        )
        common_subtitle_strict = "Mindoro | 6 March 2023 | legacy sparse-reference honesty view"
        common_subtitle_event = "Mindoro | 4-6 March 2023 | March 5-inclusive public-comparison corridor"

        singles = [
            self._spatial_spec(
                spec_id="mindoro_strict_obs_zoom",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_strict",
                date_token="2023-03-06",
                model_names="observation",
                run_type="single_observation",
                view_type="zoom",
                variant="paper",
                figure_slug="strict_observation",
                figure_title="Mindoro strict March 6 observation",
                subtitle=common_subtitle_strict,
                interpretation="This figure isolates the tiny strict March 6 observed target so the panel can see what the forecast is being judged against.",
                notes="Built from the stored March 6 observed mask only.",
                note_lines=note_lines_strict_observation,
                legend_keys=["observed_mask", "validation_polygon"],
                raster_layers=[{"path": strict_obs, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5}],
                show_source=False,
                show_init=False,
                show_validation=True,
                include_validation_in_crop=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_strict_forecast_zoom",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_strict",
                date_token="2023-03-06",
                model_names="opendrift",
                run_type="single_forecast",
                view_type="zoom",
                variant="paper",
                figure_slug="strict_official_p50",
                figure_title="Mindoro strict March 6 official forecast",
                subtitle=common_subtitle_strict,
                interpretation="This figure shows the official OpenDrift ensemble p50 product on its own, without the observation overlaid.",
                notes="Built from the stored OpenDrift ensemble p50 March 6 raster.",
                note_lines=note_lines_strict_scored,
                legend_keys=["ensemble_p50", "source_point", "validation_polygon"],
                raster_layers=[{"path": strict_p50, "legend_key": "ensemble_p50", "alpha": 0.38, "zorder": 5}],
                show_source=True,
                show_validation=True,
                include_source_in_crop=True,
                include_validation_in_crop=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_strict_overlay_zoom",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_strict",
                date_token="2023-03-06",
                model_names="opendrift",
                run_type="single_overlay",
                view_type="zoom",
                variant="paper",
                figure_slug="strict_obs_vs_p50",
                figure_title="Mindoro strict March 6 observation vs official forecast",
                subtitle=common_subtitle_strict,
                interpretation="This zoom view is the core strict March 6 comparison figure and should be paired with the dedicated close-up.",
                notes="Built from the stored March 6 observed mask and official p50 raster.",
                note_lines=note_lines_strict_scored,
                legend_keys=["observed_mask", "ensemble_p50", "source_point", "validation_polygon"],
                raster_layers=[
                    {"path": strict_obs, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5},
                    {"path": strict_p50, "legend_key": "ensemble_p50", "alpha": 0.32, "zorder": 6},
                ],
                show_source=True,
                show_validation=True,
                include_source_in_crop=True,
                include_validation_in_crop=True,
                recommended_for_main_defense=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_strict_overlay_locator",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_strict",
                date_token="2023-03-06",
                model_names="opendrift",
                run_type="single_overlay",
                view_type="locator",
                variant="paper",
                figure_slug="strict_obs_vs_p50",
                figure_title="Mindoro strict March 6 regional context",
                subtitle=common_subtitle_strict,
                interpretation="This locator/context figure shows where the strict March 6 overlap problem sits within the wider Mindoro domain.",
                notes="Built from the same stored March 6 rasters with a forced regional locator view.",
                note_lines=note_lines_strict_scored,
                legend_keys=["observed_mask", "ensemble_p50", "source_point", "validation_polygon"],
                raster_layers=[
                    {"path": strict_obs, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5},
                    {"path": strict_p50, "legend_key": "ensemble_p50", "alpha": 0.32, "zorder": 6},
                ],
                show_source=True,
                show_validation=True,
                include_source_in_crop=True,
                include_validation_in_crop=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_strict_overlay_close",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_strict",
                date_token="2023-03-06",
                model_names="opendrift",
                run_type="single_overlay",
                view_type="close",
                variant="paper",
                figure_slug="strict_obs_vs_p50",
                figure_title="Mindoro strict March 6 close-up",
                subtitle=common_subtitle_strict,
                interpretation="This forced close-up is the figure the panel should use to inspect the strict March 6 overlap honestly.",
                notes="Built from the stored March 6 rasters with forced close-up padding rules.",
                note_lines=note_lines_strict_scored,
                legend_keys=["observed_mask", "ensemble_p50", "source_point", "validation_polygon"],
                raster_layers=[
                    {"path": strict_obs, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5},
                    {"path": strict_p50, "legend_key": "ensemble_p50", "alpha": 0.32, "zorder": 6},
                ],
                show_source=True,
                show_validation=True,
                include_source_in_crop=True,
                include_validation_in_crop=True,
                recommended_for_main_defense=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_event_observation_zoom",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_support",
                date_token="2023-03-04_to_2023-03-06",
                model_names="observation",
                run_type="single_observation",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_observation",
                figure_title="Mindoro March 4-6 observed event corridor",
                subtitle=common_subtitle_event,
                interpretation="This figure isolates the broader observed event corridor that complements the sparse strict March 6 target.",
                notes="Built from the stored Mindoro event-corridor observation raster.",
                note_lines=note_lines_event_observation,
                legend_keys=["observed_mask"],
                raster_layers=[{"path": event_obs, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5}],
                show_source=False,
            ),
            self._spatial_spec(
                spec_id="mindoro_event_deterministic_zoom",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_support",
                date_token="2023-03-04_to_2023-03-06",
                model_names="opendrift",
                run_type="single_forecast",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_deterministic",
                figure_title="Mindoro March 4-6 deterministic event corridor",
                subtitle=common_subtitle_event,
                interpretation="This figure shows the OpenDrift deterministic event corridor without the observed mask on top of it.",
                notes="Built from the stored deterministic event-corridor raster.",
                note_lines=note_lines_event_det,
                legend_keys=["deterministic_opendrift", "source_point"],
                raster_layers=[{"path": event_det, "legend_key": "deterministic_opendrift", "alpha": 0.34, "zorder": 5}],
                show_source=True,
            ),
            self._spatial_spec(
                spec_id="mindoro_event_ensemble_zoom",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_support",
                date_token="2023-03-04_to_2023-03-06",
                model_names="opendrift",
                run_type="single_forecast",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_ensemble_consolidated",
                figure_title="Mindoro March 4-6 consolidated ensemble corridor",
                subtitle=common_subtitle_event,
                interpretation="This figure shows the corridor obtained by consolidating all stored ensemble runs into one support trajectory.",
                notes="Built from the stored consolidated-ensemble event-corridor raster.",
                note_lines=note_lines_event_ensemble,
                legend_keys=["ensemble_consolidated", "source_point"],
                raster_layers=[{"path": event_ensemble, "legend_key": "ensemble_consolidated", "alpha": 0.32, "zorder": 5}],
                show_source=True,
            ),
        ]
        singles.extend(
            [
                self._spatial_spec(
                    spec_id="mindoro_event_overlay_locator",
                    figure_family_code="B",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3b_support",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="opendrift",
                    run_type="single_overlay",
                    view_type="locator",
                    variant="paper",
                    figure_slug="eventcorridor_obs_vs_ensemble_consolidated",
                    figure_title="Mindoro March 4-6 event corridor regional context",
                    subtitle=common_subtitle_event,
                    interpretation="This locator figure shows where the broader March 4-6 corridor sits within the full Mindoro domain.",
                    notes="Built from the stored observation and consolidated-ensemble event-corridor rasters.",
                    note_lines=note_lines_event_ensemble,
                    legend_keys=["observed_mask", "ensemble_consolidated", "source_point"],
                    raster_layers=[
                        {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                        {"path": event_ensemble, "legend_key": "ensemble_consolidated", "alpha": 0.30, "zorder": 6},
                    ],
                    show_source=True,
                ),
                self._spatial_spec(
                    spec_id="mindoro_event_overlay_close",
                    figure_family_code="B",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3b_support",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="opendrift",
                    run_type="single_overlay",
                    view_type="close",
                    variant="paper",
                    figure_slug="eventcorridor_obs_vs_ensemble_consolidated",
                    figure_title="Mindoro March 4-6 event corridor close-up",
                    subtitle=common_subtitle_event,
                    interpretation="This close-up makes the broader event-corridor overlap readable at the spill scale instead of the regional scale.",
                    notes="Built from the stored observation and consolidated-ensemble event-corridor rasters.",
                    note_lines=note_lines_event_ensemble,
                    legend_keys=["observed_mask", "ensemble_consolidated", "source_point"],
                    raster_layers=[
                        {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                        {"path": event_ensemble, "legend_key": "ensemble_consolidated", "alpha": 0.30, "zorder": 6},
                    ],
                    show_source=True,
                ),
                self._spatial_spec(
                    spec_id="mindoro_comparison_od_zoom",
                    figure_family_code="C",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3a_benchmark",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="opendrift",
                    run_type="single_model",
                    view_type="zoom",
                    variant="paper",
                    figure_slug="comparison_opendrift_deterministic",
                    figure_title="Mindoro OpenDrift deterministic vs observed corridor",
                    subtitle="Mindoro | 4-6 March 2023 | OpenDrift comparator view",
                    interpretation="This single figure shows the deterministic OpenDrift corridor against the observed corridor in a paper-friendly format.",
                    notes="Built from stored comparator rasters only.",
                    note_lines=note_lines_comparison_det,
                    legend_keys=["observed_mask", "deterministic_opendrift", "source_point"],
                    raster_layers=[
                        {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                        {"path": event_det, "legend_key": "deterministic_opendrift", "alpha": 0.28, "zorder": 6},
                    ],
                    show_source=True,
                ),
                self._spatial_spec(
                    spec_id="mindoro_comparison_ensemble_zoom",
                    figure_family_code="C",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3a_benchmark",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="opendrift",
                    run_type="single_model",
                    view_type="zoom",
                    variant="paper",
                    figure_slug="comparison_opendrift_ensemble_consolidated",
                    figure_title="Mindoro consolidated ensemble trajectory vs observed corridor",
                    subtitle="Mindoro | 4-6 March 2023 | OpenDrift comparator view",
                    interpretation="This single figure shows the consolidated ensemble trajectory corridor against the observed corridor.",
                    notes="Built from stored comparator rasters only.",
                    note_lines=note_lines_comparison_ensemble,
                    legend_keys=["observed_mask", "ensemble_consolidated", "source_point"],
                    raster_layers=[
                        {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                        {"path": event_ensemble, "legend_key": "ensemble_consolidated", "alpha": 0.28, "zorder": 6},
                    ],
                    show_source=True,
                ),
                self._spatial_spec(
                    spec_id="mindoro_comparison_pygnome_zoom",
                    figure_family_code="C",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3a_benchmark",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="pygnome",
                    run_type="single_model",
                    view_type="zoom",
                    variant="paper",
                    figure_slug="comparison_pygnome",
                    figure_title="Mindoro PyGNOME comparator vs observed corridor",
                    subtitle="Mindoro | 4-6 March 2023 | PyGNOME comparator view",
                    interpretation="This single figure shows the PyGNOME comparator corridor against the observed corridor with the same visual grammar used for OpenDrift.",
                    notes="Built from stored comparator rasters only.",
                    note_lines=note_lines_comparison_pygnome,
                    legend_keys=["observed_mask", "pygnome", "source_point"],
                    raster_layers=[
                        {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                        {"path": event_pygnome, "legend_key": "pygnome", "alpha": 0.28, "zorder": 6},
                    ],
                    show_source=True,
                ),
                self._spatial_spec(
                    spec_id="mindoro_comparison_overlay_close",
                    figure_family_code="C",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3a_benchmark",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="opendrift_vs_pygnome",
                    run_type="comparison_overlay",
                    view_type="close",
                    variant="paper",
                    figure_slug="comparison_overlay",
                    figure_title="Mindoro close-up: observed corridor, consolidated ensemble, and PyGNOME",
                    subtitle="Mindoro | 4-6 March 2023 | close crop around the overlap union",
                    interpretation="This close-up overlay is useful when the panel wants a compact direct comparison between the observed corridor, the consolidated ensemble corridor, and the PyGNOME comparator.",
                    notes="Built from stored comparator rasters only.",
                    note_lines=note_lines_comparison_overlay,
                    legend_keys=["observed_mask", "ensemble_consolidated", "pygnome", "source_point"],
                    raster_layers=[
                        {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5},
                        {"path": event_ensemble, "legend_key": "ensemble_consolidated", "alpha": 0.28, "zorder": 6},
                        {"path": event_pygnome, "legend_key": "pygnome", "alpha": 0.22, "zorder": 7},
                    ],
                    show_source=True,
                    recommended_for_main_defense=True,
                ),
                self._track_spec(
                    spec_id="mindoro_track_deterministic",
                    figure_family_code="D",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase2_official",
                    date_token="2023-03-03_to_2023-03-06",
                    model_names="opendrift",
                    run_type="single_trajectory",
                    view_type="zoom",
                    variant="paper",
                    figure_slug="deterministic_trajectory",
                    figure_title="Mindoro deterministic transport path",
                    subtitle="Mindoro | 3-6 March 2023 | deterministic OpenDrift trajectory",
                    interpretation="This trajectory figure gives the panel an intuitive picture of the deterministic transport path before any mask comparisons are introduced.",
                    notes="Built from the stored deterministic OpenDrift track NetCDF.",
                    note_lines=[
                        "This is the simplest transport-view figure in the package and is often the easiest place to start the transport story.",
                        "The source marker shows where the release begins; the dark line shows the deterministic centroid path through time.",
                    ],
                    legend_keys=["deterministic_opendrift", "source_point"],
                    renderer="track",
                    track_path=deterministic_track,
                    model_kind="opendrift",
                ),
                self._track_spec(
                    spec_id="mindoro_track_ensemble",
                    figure_family_code="D",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase2_official",
                    date_token="2023-03-03_to_2023-03-06",
                    model_names="opendrift",
                    run_type="single_trajectory",
                    view_type="zoom",
                    variant="paper",
                    figure_slug="ensemble_sampled_trajectory",
                    figure_title="Mindoro sampled ensemble trajectories",
                    subtitle="Mindoro | 3-6 March 2023 | sampled member centroids and mean path",
                    interpretation="This figure shows how the ensemble spreads around the main transport pathway while preserving a readable centroid path.",
                    notes="Built from stored OpenDrift ensemble member NetCDFs only.",
                    note_lines=[
                        "Only a sampled subset of member centroids is drawn so the panel can see spread without the figure turning into a blur.",
                        "The dark line is the mean centroid path across the sampled members.",
                    ],
                    legend_keys=["ensemble_member_path", "centroid_path", "source_point"],
                    renderer="ensemble_track",
                    track_paths=self._mindoro_member_paths(),
                ),
                self._track_spec(
                    spec_id="mindoro_track_corridor",
                    figure_family_code="D",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase2_phase3b",
                    date_token="2023-03-06",
                    model_names="opendrift",
                    run_type="single_trajectory",
                    view_type="close",
                    variant="paper",
                    figure_slug="corridor_hull_trajectory",
                    figure_title="Mindoro corridor, hull, and centroid path",
                    subtitle="Mindoro | 6 March 2023 | ensemble corridor geometry at the spill scale",
                    interpretation="This figure combines the p50/p90 corridor geometry with sampled centroid paths so the panel can connect trajectory spread to the final footprint masks.",
                    notes="Built from stored p50/p90 rasters and sampled ensemble tracks.",
                    note_lines=[
                        "The light trajectories show sampled member centroids, the dark line shows the average path, and the orange outline shows the hull of member end positions.",
                        "This figure helps translate the trajectory story into the Phase 3 footprint story.",
                    ],
                    legend_keys=["ensemble_member_path", "centroid_path", "corridor_hull", "ensemble_p50", "ensemble_p90", "source_point"],
                    renderer="corridor",
                    track_paths=self._mindoro_member_paths(),
                    raster_layers=[
                        {"path": strict_p50, "legend_key": "ensemble_p50", "alpha": 0.26, "zorder": 5},
                        {"path": "output/CASE_MINDORO_RETRO_2023/ensemble/mask_p90_2023-03-06T09-59-00Z.tif", "legend_key": "ensemble_p90", "alpha": 0.16, "zorder": 4},
                    ],
                ),
                self._track_spec(
                    spec_id="mindoro_track_pygnome",
                    figure_family_code="D",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3a_benchmark",
                    date_token="2023-03-03_to_2023-03-06",
                    model_names="pygnome",
                    run_type="single_trajectory",
                    view_type="zoom",
                    variant="paper",
                    figure_slug="pygnome_trajectory",
                    figure_title="Mindoro PyGNOME comparator trajectory",
                    subtitle="Mindoro | 3-6 March 2023 | PyGNOME comparator trajectory",
                    interpretation="This figure gives the panel a like-for-like PyGNOME trajectory picture using the same framing as the OpenDrift transport figures.",
                    notes="Built from the stored PyGNOME comparator track NetCDF.",
                    note_lines=[
                        "PyGNOME is included here as a comparator, not as truth.",
                        "Using the same visual grammar helps the panel compare path shape and endpoint region without learning a new legend.",
                    ],
                    legend_keys=["pygnome", "source_point"],
                    renderer="track",
                    track_path=pygnome_track,
                    model_kind="pygnome",
                ),
            ]
        )
        boards = [
            self._board_spec(
                spec_id="mindoro_strict_board",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_strict",
                date_token="2023-03-06",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="strict_board",
                figure_title="Mindoro strict March 6 publication board",
                subtitle=common_subtitle_strict,
                interpretation="This board is one of the most important defense slides because it makes the hard strict March 6 case readable.",
                notes="Board assembled from publication-grade single figures only.",
                note_lines=self._mindoro_strict_note_lines(
                    self._mindoro_strict_score_line("Official forecast"),
                    self._mindoro_strict_score_line("Zoomed overlay"),
                    self._mindoro_strict_score_line("Forced close-up"),
                ),
                panels=[
                    {"panel_title": "Observed target", "source_spec_id": "mindoro_strict_obs_zoom"},
                    {"panel_title": "Official forecast", "source_spec_id": "mindoro_strict_forecast_zoom"},
                    {"panel_title": "Zoomed overlay", "source_spec_id": "mindoro_strict_overlay_zoom"},
                    {"panel_title": "Forced close-up", "source_spec_id": "mindoro_strict_overlay_close"},
                ],
                recommended_for_main_defense=True,
            ),
        ]
        boards.extend(
            [
                self._board_spec(
                    spec_id="mindoro_event_board",
                    figure_family_code="B",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3b_support",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="opendrift",
                    run_type="comparison_board",
                    figure_slug="eventcorridor_board",
                    figure_title="Mindoro March 4-6 event-corridor board",
                    subtitle=common_subtitle_event,
                    interpretation="This board gives the panel a broader public-support context than the strict March 6 board alone.",
                    notes="Board assembled from publication-grade single figures only.",
                    note_lines=self._mindoro_event_note_lines(
                        self._mindoro_event_score_line("deterministic", "Deterministic OpenDrift"),
                        self._mindoro_event_score_line("ensemble", "Consolidated ensemble trajectory"),
                        self._mindoro_event_score_line("ensemble", "Close-up overlay"),
                    ),
                    panels=[
                        {"panel_title": "Observed corridor", "source_spec_id": "mindoro_event_observation_zoom"},
                        {"panel_title": "Deterministic OpenDrift", "source_spec_id": "mindoro_event_deterministic_zoom"},
                        {"panel_title": "Consolidated ensemble trajectory", "source_spec_id": "mindoro_event_ensemble_zoom"},
                        {"panel_title": "Close-up overlay", "source_spec_id": "mindoro_event_overlay_close"},
                    ],
                    recommended_for_main_defense=False,
                ),
                self._board_spec(
                    spec_id="mindoro_comparison_board",
                    figure_family_code="C",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3a_benchmark",
                    date_token="2023-03-04_to_2023-03-06",
                    model_names="opendrift_vs_pygnome",
                    run_type="comparison_board",
                    figure_slug="opendrift_vs_pygnome_board",
                    figure_title="Mindoro OpenDrift vs PyGNOME publication board",
                    subtitle="Mindoro | 4-6 March 2023 | observation, official products, and PyGNOME comparator",
                    interpretation="This board is ready for panel use because it makes the OpenDrift-versus-PyGNOME comparison explicit without treating PyGNOME as truth.",
                    notes="Board assembled from publication-grade single figures only.",
                    note_lines=self._mindoro_comparison_note_lines(
                        self._mindoro_event_score_line("deterministic", "OpenDrift deterministic"),
                        self._mindoro_event_score_line("ensemble", "OpenDrift consolidated ensemble trajectory"),
                        self._mindoro_event_score_line("pygnome", "PyGNOME comparator"),
                    ),
                    panels=[
                        {"panel_title": "Observed corridor", "source_spec_id": "mindoro_event_observation_zoom"},
                        {"panel_title": "OpenDrift deterministic", "source_spec_id": "mindoro_comparison_od_zoom"},
                        {"panel_title": "OpenDrift consolidated ensemble trajectory", "source_spec_id": "mindoro_comparison_ensemble_zoom"},
                        {"panel_title": "PyGNOME comparator", "source_spec_id": "mindoro_comparison_pygnome_zoom"},
                    ],
                    recommended_for_main_defense=True,
                ),
                self._board_spec(
                    spec_id="mindoro_trajectory_board",
                    figure_family_code="D",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase2_official",
                    date_token="2023-03-03_to_2023-03-06",
                    model_names="opendrift_vs_pygnome",
                    run_type="trajectory_board",
                    figure_slug="trajectory_board",
                    figure_title="Mindoro trajectory publication board",
                    subtitle="Mindoro | 3-6 March 2023 | deterministic, ensemble, corridor, and PyGNOME trajectory views",
                    interpretation="This is one of the clearest early-story boards because it explains transport path, spread, and comparator behavior before any score tables appear.",
                    notes="Board assembled from publication-grade trajectory figures only.",
                    note_lines=[
                        "Use this board before the validation metrics if the panel wants an intuitive transport explanation first.",
                        "The board moves from single-path transport to ensemble spread to corridor geometry, then ends with the PyGNOME comparator path.",
                    ],
                    panels=[
                        {"panel_title": "Deterministic path", "source_spec_id": "mindoro_track_deterministic"},
                        {"panel_title": "Sampled ensemble", "source_spec_id": "mindoro_track_ensemble"},
                        {"panel_title": "Corridor and hull", "source_spec_id": "mindoro_track_corridor"},
                        {"panel_title": "PyGNOME comparator path", "source_spec_id": "mindoro_track_pygnome"},
                    ],
                    recommended_for_main_defense=True,
                ),
            ]
        )
        return singles, boards

    def _phase4_publication_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        summary_csv = "output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_summary.csv"
        arrival_csv = "output/phase4/CASE_MINDORO_RETRO_2023/phase4_shoreline_arrival.csv"
        segments_csv = "output/phase4/CASE_MINDORO_RETRO_2023/phase4_shoreline_segments.csv"
        lines = self._phase4_lines()
        deferred_lines = self._phase4_deferred_note_lines()
        singles = [
            self._chart_spec(
                spec_id="phase4_oil_budget_summary",
                figure_family_code="E",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil",
                run_type="single_summary",
                figure_slug="oil_budget_summary",
                figure_title="Mindoro Phase 4 oil-budget comparison",
                subtitle="Mindoro | 3-6 March 2023 | current shoreline-aware OpenOil replay",
                interpretation="This summary figure is the fastest way to show how oil type changes the final weathering and shoreline burden story.",
                notes="Built from the stored Phase 4 oil-budget summary CSV only.",
                note_lines=lines,
                renderer="oil_budget_summary",
                csv_path=summary_csv,
                recommended_for_main_defense=True,
            ),
            self._chart_spec(
                spec_id="phase4_oil_budget_lighter",
                figure_family_code="E",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil",
                run_type="single_scenario",
                figure_slug="oil_budget_timeseries",
                figure_title="Mindoro Phase 4 light-oil budget through time",
                subtitle="Mindoro | 3-6 March 2023 | lighter oil scenario",
                interpretation="This paper-ready figure shows the rapid evaporation-dominated behavior of the light-oil scenario.",
                notes="Built from the stored lighter-oil timeseries CSV only.",
                note_lines=lines,
                renderer="oil_budget_timeseries",
                csv_path="output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_timeseries_lighter_oil.csv",
                scenario_id="lighter_oil",
            ),
            self._chart_spec(
                spec_id="phase4_oil_budget_base",
                figure_family_code="E",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil",
                run_type="single_scenario",
                figure_slug="oil_budget_timeseries",
                figure_title="Mindoro Phase 4 medium-heavy proxy budget through time",
                subtitle="Mindoro | 3-6 March 2023 | fixed base medium-heavy proxy",
                interpretation="This paper-ready figure shows the medium-heavy proxy scenario that is still flagged for mass-balance follow-up.",
                notes="Built from the stored medium-heavy timeseries CSV only.",
                note_lines=lines,
                renderer="oil_budget_timeseries",
                csv_path="output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_timeseries_fixed_base_medium_heavy_proxy.csv",
                scenario_id="fixed_base_medium_heavy_proxy",
            ),
            self._chart_spec(
                spec_id="phase4_oil_budget_heavier",
                figure_family_code="E",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil",
                run_type="single_scenario",
                figure_slug="oil_budget_timeseries",
                figure_title="Mindoro Phase 4 heavier-oil budget through time",
                subtitle="Mindoro | 3-6 March 2023 | heavier oil scenario",
                interpretation="This paper-ready figure shows the slow-weathering, high-stranding behavior of the heavy-oil scenario.",
                notes="Built from the stored heavier-oil timeseries CSV only.",
                note_lines=lines,
                renderer="oil_budget_timeseries",
                csv_path="output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_timeseries_heavier_oil.csv",
                scenario_id="heavier_oil",
            ),
            self._chart_spec(
                spec_id="phase4_shoreline_arrival",
                figure_family_code="E",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil",
                run_type="single_summary",
                figure_slug="shoreline_arrival",
                figure_title="Mindoro Phase 4 shoreline-arrival timing",
                subtitle="Mindoro | 3-6 March 2023 | shoreline-aware replay summary",
                interpretation="This figure gives the panel a clear timing summary of when each oil scenario first reaches shoreline segments.",
                notes="Built from the stored shoreline-arrival CSV only.",
                note_lines=lines,
                renderer="shoreline_arrival",
                csv_path=arrival_csv,
                recommended_for_main_defense=True,
            ),
            {
                **self._track_spec(
                    spec_id="phase4_shoreline_segments",
                    figure_family_code="E",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase4",
                    date_token="2023-03-03_to_2023-03-06",
                    model_names="openoil",
                    run_type="single_map",
                    view_type="close",
                    variant="paper",
                    figure_slug="shoreline_segment_impacts",
                    figure_title="Mindoro Phase 4 shoreline-segment impact map",
                    subtitle="Mindoro | 3-6 March 2023 | shoreline segments colored by dominant oil scenario",
                    interpretation="This figure translates the Phase 4 shoreline table into a readable shoreline-impact map for non-technical readers.",
                    notes="Built from the stored Phase 4 shoreline segment CSV and shoreline geometry only.",
                    note_lines=lines,
                    legend_keys=["oil_lighter", "oil_base", "oil_heavier", "source_point"],
                    renderer="shoreline_segment",
                    track_paths=[],
                ),
                "csv_path": segments_csv,
                "scenario_id": "all_scenarios",
            },
            self._text_spec(
                spec_id="phase4_crossmodel_deferred_note",
                figure_family_code="F",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4_crossmodel_comparability_audit",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil_only",
                run_type="single_note",
                figure_slug="crossmodel_comparison_deferred",
                figure_title="Mindoro Phase 4 cross-model comparison is deferred",
                subtitle="Mindoro | Phase 4 | publication package shows OpenDrift-only fate and shoreline results",
                interpretation="This figure explains why the publication package includes OpenDrift-only Phase 4 figures and does not include a fake OpenDrift-versus-PyGNOME Phase 4 comparison.",
                notes="Built from the stored Phase 4 cross-model comparability audit only.",
                note_lines=deferred_lines,
                source_paths=[
                    str(PHASE4_CROSSMODEL_MATRIX),
                    str(PHASE4_CROSSMODEL_VERDICT),
                    str(PHASE4_CROSSMODEL_BLOCKERS),
                ],
            ),
        ]
        boards = [
            self._board_spec(
                spec_id="phase4_oil_budget_board",
                figure_family_code="E",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil",
                run_type="comparison_board",
                figure_slug="oil_budget_board",
                figure_title="Mindoro Phase 4 oil-budget publication board",
                subtitle="Mindoro | 3-6 March 2023 | oil-type fate comparison",
                interpretation="This board is ready for panel use because it turns the Phase 4 oil-type tables into an immediately readable weathering story.",
                notes="Board assembled from publication-grade Phase 4 oil-budget figures only.",
                note_lines=lines,
                panels=[
                    {"panel_title": "All-scenario summary", "source_spec_id": "phase4_oil_budget_summary"},
                    {"panel_title": "Light oil", "source_spec_id": "phase4_oil_budget_lighter"},
                    {"panel_title": "Base medium-heavy proxy", "source_spec_id": "phase4_oil_budget_base"},
                    {"panel_title": "Heavier oil", "source_spec_id": "phase4_oil_budget_heavier"},
                ],
                scenario_id="all_scenarios",
                recommended_for_main_defense=True,
            ),
            self._board_spec(
                spec_id="phase4_shoreline_board",
                figure_family_code="E",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase4",
                date_token="2023-03-03_to_2023-03-06",
                model_names="openoil",
                run_type="comparison_board",
                figure_slug="shoreline_impact_board",
                figure_title="Mindoro Phase 4 shoreline-impact publication board",
                subtitle="Mindoro | 3-6 March 2023 | shoreline arrival and segment burden",
                interpretation="This board is ready for panel use because it makes the shoreline-impact story legible without asking the reader to parse raw CSVs.",
                notes="Board assembled from publication-grade shoreline figures only.",
                note_lines=lines,
                panels=[
                    {"panel_title": "Arrival timing by scenario", "source_spec_id": "phase4_shoreline_arrival"},
                    {"panel_title": "Segment-level impact map", "source_spec_id": "phase4_shoreline_segments"},
                    {"panel_title": "Interpretation note", "text": "Heavier oils strand the most mass, but all three scenarios reach shoreline segments quickly in the current shoreline-aware replay. These results are scientifically reportable now, while still inheriting the upstream Phase 1 and Phase 2 provisionality."},
                ],
                scenario_id="all_scenarios",
                recommended_for_main_defense=True,
            ),
        ]
        return singles, boards

    def _dwh_publication_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        obs_21 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_setup/obs_mask_2010-05-21.tif"
        obs_22 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_setup/obs_mask_2010-05-22.tif"
        obs_23 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_setup/obs_mask_2010-05-23.tif"
        det_21 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-21_datecomposite.tif"
        det_22 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-22_datecomposite.tif"
        det_23 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-23_datecomposite.tif"
        event_obs = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/obs_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        event_det = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        event_p50 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p50_2010-05-21_2010-05-23_eventcorridor.tif"
        event_p90 = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p90_2010-05-21_2010-05-23_eventcorridor.tif"
        event_pygnome = "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/products/pygnome_eventcorridor_union_2010-05-21_to_2010-05-23.tif"
        det_track = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/tracks/opendrift_control_dwh_phase3c.nc"
        pygnome_track = "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/tracks/pygnome_dwh_phase3c.nc"
        note_lines_dwh_21 = self._dwh_deterministic_note_lines(self._dwh_deterministic_score_line("2010-05-21"))
        note_lines_dwh_22 = self._dwh_deterministic_note_lines(self._dwh_deterministic_score_line("2010-05-22"))
        note_lines_dwh_23 = self._dwh_deterministic_note_lines(self._dwh_deterministic_score_line("2010-05-23"))
        note_lines_dwh_event_observation = self._dwh_model_note_lines()
        note_lines_dwh_event_det = self._dwh_deterministic_note_lines(self._dwh_event_score_line("deterministic"))
        note_lines_dwh_event_p50 = self._dwh_deterministic_note_lines(self._dwh_event_score_line("p50"))
        note_lines_dwh_event_p90 = self._dwh_deterministic_note_lines(self._dwh_event_score_line("p90"))
        note_lines_dwh_event_pygnome = self._dwh_model_note_lines(self._dwh_event_score_line("pygnome"))
        note_lines_dwh_tracks = self._dwh_trajectory_note_lines()
        singles = [
            self._spatial_spec(
                spec_id="dwh_2010_05_21_overlay",
                figure_family_code="G",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-21",
                model_names="opendrift",
                run_type="single_overlay",
                view_type="zoom",
                variant="paper",
                figure_slug="forecast_vs_observation",
                figure_title="DWH 21 May 2010 forecast vs observation",
                subtitle="Deepwater Horizon | 21 May 2010 | deterministic OpenDrift vs public observation mask",
                interpretation="This daily figure shows the first DWH date-composite comparison with both the forecast and observed footprint clearly visible.",
                notes="Built from stored DWH observation and deterministic forecast rasters only.",
                note_lines=note_lines_dwh_21,
                legend_keys=["observed_mask", "deterministic_opendrift", "source_point"],
                raster_layers=[
                    {"path": obs_21, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5},
                    {"path": det_21, "legend_key": "deterministic_opendrift", "alpha": 0.28, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
            ),
            self._spatial_spec(
                spec_id="dwh_2010_05_22_overlay",
                figure_family_code="G",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-22",
                model_names="opendrift",
                run_type="single_overlay",
                view_type="zoom",
                variant="paper",
                figure_slug="forecast_vs_observation",
                figure_title="DWH 22 May 2010 forecast vs observation",
                subtitle="Deepwater Horizon | 22 May 2010 | deterministic OpenDrift vs public observation mask",
                interpretation="This daily figure shows the second DWH date-composite comparison with the same publication grammar.",
                notes="Built from stored DWH observation and deterministic forecast rasters only.",
                note_lines=note_lines_dwh_22,
                legend_keys=["observed_mask", "deterministic_opendrift", "source_point"],
                raster_layers=[
                    {"path": obs_22, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5},
                    {"path": det_22, "legend_key": "deterministic_opendrift", "alpha": 0.28, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
            ),
            self._spatial_spec(
                spec_id="dwh_2010_05_23_overlay",
                figure_family_code="G",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-23",
                model_names="opendrift",
                run_type="single_overlay",
                view_type="zoom",
                variant="paper",
                figure_slug="forecast_vs_observation",
                figure_title="DWH 23 May 2010 forecast vs observation",
                subtitle="Deepwater Horizon | 23 May 2010 | deterministic OpenDrift vs public observation mask",
                interpretation="This daily figure shows the third DWH date-composite comparison with the same publication grammar.",
                notes="Built from stored DWH observation and deterministic forecast rasters only.",
                note_lines=note_lines_dwh_23,
                legend_keys=["observed_mask", "deterministic_opendrift", "source_point"],
                raster_layers=[
                    {"path": obs_23, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5},
                    {"path": det_23, "legend_key": "deterministic_opendrift", "alpha": 0.28, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
            ),
            self._spatial_spec(
                spec_id="dwh_event_observation",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-21_to_2010-05-23",
                model_names="observation",
                run_type="single_observation",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_observation",
                figure_title="DWH event-corridor observation",
                subtitle="Deepwater Horizon | 21-23 May 2010 | public observation corridor",
                interpretation="This figure isolates the observed DWH event corridor that serves as truth for the separate external transfer-validation case before any model comparison is shown.",
                notes="Built from the stored DWH event-corridor observation raster only.",
                note_lines=note_lines_dwh_event_observation,
                legend_keys=["observed_mask"],
                raster_layers=[{"path": event_obs, "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5}],
                show_source=False,
                status_key_override="dwh_observation_truth_context",
            ),
            self._spatial_spec(
                spec_id="dwh_event_deterministic",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="single_model",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_deterministic",
                figure_title="DWH event corridor: OpenDrift deterministic",
                subtitle="Deepwater Horizon | 21-23 May 2010 | deterministic event-corridor comparison",
                interpretation="This figure shows the readiness-gated HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes deterministic event corridor against the observed DWH truth corridor.",
                notes="Built from stored DWH event-corridor rasters only.",
                note_lines=note_lines_dwh_event_det,
                legend_keys=["observed_mask", "deterministic_opendrift", "source_point"],
                raster_layers=[
                    {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                    {"path": event_det, "legend_key": "deterministic_opendrift", "alpha": 0.26, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
                status_key_override="dwh_deterministic_transfer",
            ),
            self._spatial_spec(
                spec_id="dwh_event_p50",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="single_model",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_p50",
                figure_title="DWH event corridor: OpenDrift ensemble p50",
                subtitle="Deepwater Horizon | 21-23 May 2010 | ensemble p50 event-corridor comparison",
                interpretation="This figure shows the DWH ensemble p50 event corridor against the observed DWH truth corridor on the same readiness-gated forcing stack.",
                notes="Built from stored DWH event-corridor rasters only.",
                note_lines=note_lines_dwh_event_p50,
                legend_keys=["observed_mask", "ensemble_p50", "source_point"],
                raster_layers=[
                    {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                    {"path": event_p50, "legend_key": "ensemble_p50", "alpha": 0.26, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
                status_key_override="dwh_ensemble_transfer",
            ),
            self._spatial_spec(
                spec_id="dwh_event_p90",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="single_model",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_p90",
                figure_title="DWH event corridor: OpenDrift ensemble p90",
                subtitle="Deepwater Horizon | 21-23 May 2010 | ensemble p90 event-corridor comparison",
                interpretation="This figure shows the wider DWH ensemble p90 event corridor against the observed DWH truth corridor on the same readiness-gated forcing stack.",
                notes="Built from stored DWH event-corridor rasters only.",
                note_lines=note_lines_dwh_event_p90,
                legend_keys=["observed_mask", "ensemble_p90", "source_point"],
                raster_layers=[
                    {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                    {"path": event_p90, "legend_key": "ensemble_p90", "alpha": 0.22, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
                status_key_override="dwh_ensemble_transfer",
            ),
            self._spatial_spec(
                spec_id="dwh_event_pygnome",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="pygnome",
                run_type="single_model",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_pygnome",
                figure_title="DWH event corridor: PyGNOME comparator",
                subtitle="Deepwater Horizon | 21-23 May 2010 | PyGNOME comparator event-corridor comparison",
                interpretation="This figure shows the PyGNOME comparator corridor against the observed DWH truth corridor while keeping PyGNOME in a comparator-only role.",
                notes="Built from stored DWH comparator rasters only.",
                note_lines=note_lines_dwh_event_pygnome,
                legend_keys=["observed_mask", "pygnome", "source_point"],
                raster_layers=[
                    {"path": event_obs, "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                    {"path": event_pygnome, "legend_key": "pygnome", "alpha": 0.24, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
                status_key_override="dwh_crossmodel_comparator",
            ),
            self._track_spec(
                spec_id="dwh_track_deterministic",
                figure_family_code="J",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-20_to_2010-05-23",
                model_names="opendrift",
                run_type="single_trajectory",
                view_type="zoom",
                variant="paper",
                figure_slug="deterministic_trajectory",
                figure_title="DWH deterministic transport path",
                subtitle="Deepwater Horizon | 20-23 May 2010 | deterministic OpenDrift trajectory",
                interpretation="This figure gives the panel an intuitive picture of the deterministic transport path behind the separate DWH external transfer-validation case.",
                notes="Built from the stored DWH deterministic track NetCDF.",
                note_lines=note_lines_dwh_tracks,
                legend_keys=["deterministic_opendrift", "source_point"],
                renderer="track",
                track_path=det_track,
                model_kind="opendrift",
            ),
            self._track_spec(
                spec_id="dwh_track_ensemble",
                figure_family_code="J",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token="2010-05-20_to_2010-05-23",
                model_names="opendrift",
                run_type="single_trajectory",
                view_type="zoom",
                variant="paper",
                figure_slug="ensemble_sampled_trajectory",
                figure_title="DWH sampled ensemble trajectories",
                subtitle="Deepwater Horizon | 20-23 May 2010 | sampled member centroids and mean path",
                interpretation="This figure shows how the DWH ensemble spreads around the main transport pathway while staying readable on the same readiness-gated forcing stack.",
                notes="Built from stored DWH ensemble track NetCDFs only.",
                note_lines=note_lines_dwh_tracks,
                legend_keys=["ensemble_member_path", "centroid_path", "source_point"],
                renderer="ensemble_track",
                track_paths=self._dwh_member_paths(),
            ),
            self._track_spec(
                spec_id="dwh_track_pygnome",
                figure_family_code="J",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-20_to_2010-05-23",
                model_names="pygnome",
                run_type="single_trajectory",
                view_type="zoom",
                variant="paper",
                figure_slug="pygnome_trajectory",
                figure_title="DWH PyGNOME comparator trajectory",
                subtitle="Deepwater Horizon | 20-23 May 2010 | PyGNOME comparator trajectory",
                interpretation="This figure gives the panel a like-for-like PyGNOME trajectory picture while keeping PyGNOME as comparator-only against the observed DWH truth masks.",
                notes="Built from the stored DWH PyGNOME track NetCDF.",
                note_lines=note_lines_dwh_tracks,
                legend_keys=["pygnome", "source_point"],
                renderer="track",
                track_path=pygnome_track,
                model_kind="pygnome",
            ),
        ]
        boards = [
            self._board_spec(
                spec_id="dwh_deterministic_board",
                figure_family_code="G",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="daily_deterministic_board",
                figure_title="DWH deterministic forecast-vs-observation board",
                subtitle="Deepwater Horizon | 21-23 May 2010 | daily deterministic comparisons",
                interpretation="This board is ready for panel use because it shows the separate DWH external transfer-validation story on observed daily truth masks with the same readable grammar across all three dates.",
                notes="Board assembled from publication-grade single figures only.",
                note_lines=self._dwh_deterministic_note_lines(
                    self._dwh_deterministic_score_line("2010-05-21", "21 May"),
                    self._dwh_deterministic_score_line("2010-05-22", "22 May"),
                    self._dwh_deterministic_score_line("2010-05-23", "23 May"),
                ),
                panels=[
                    {"panel_title": "21 May", "source_spec_id": "dwh_2010_05_21_overlay"},
                    {"panel_title": "22 May", "source_spec_id": "dwh_2010_05_22_overlay"},
                    {"panel_title": "23 May", "source_spec_id": "dwh_2010_05_23_overlay"},
                ],
                recommended_for_main_defense=True,
            ),
            self._board_spec(
                spec_id="dwh_det_vs_ensemble_board",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="deterministic_vs_ensemble_board",
                figure_title="DWH deterministic versus ensemble publication board",
                subtitle="Deepwater Horizon | 21-23 May 2010 | deterministic, p50, and p90 side by side",
                interpretation="This board is ready for panel use because it compares deterministic, p50, and p90 against the observed DWH truth corridor on the same readiness-gated forcing stack.",
                notes="Board assembled from publication-grade single figures only.",
                note_lines=self._dwh_deterministic_note_lines(
                    self._dwh_event_score_line("deterministic", "Deterministic"),
                    self._dwh_event_score_line("p50", "Ensemble p50"),
                    self._dwh_event_score_line("p90", "Ensemble p90"),
                ),
                panels=[
                    {"panel_title": "Deterministic", "source_spec_id": "dwh_event_deterministic"},
                    {"panel_title": "Ensemble p50", "source_spec_id": "dwh_event_p50"},
                    {"panel_title": "Ensemble p90", "source_spec_id": "dwh_event_p90"},
                ],
                recommended_for_main_defense=True,
            ),
            self._board_spec(
                spec_id="dwh_model_comparison_board",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="opendrift_vs_pygnome_board",
                figure_title="DWH OpenDrift versus PyGNOME publication board",
                subtitle="Deepwater Horizon | 21-23 May 2010 | observation, official products, and PyGNOME comparator",
                interpretation="This board is ready for panel use because it keeps observed DWH masks as truth and makes the OpenDrift-versus-PyGNOME comparator framing explicit in the separate external case.",
                notes="Board assembled from publication-grade single figures only.",
                note_lines=self._dwh_model_note_lines(
                    self._dwh_event_score_line("deterministic", "OpenDrift deterministic"),
                    self._dwh_event_score_line("p50", "OpenDrift ensemble p50"),
                    self._dwh_event_score_line("pygnome", "PyGNOME comparator"),
                ),
                panels=[
                    {"panel_title": "Observed corridor", "source_spec_id": "dwh_event_observation"},
                    {"panel_title": "OpenDrift deterministic", "source_spec_id": "dwh_event_deterministic"},
                    {"panel_title": "OpenDrift ensemble p50", "source_spec_id": "dwh_event_p50"},
                    {"panel_title": "PyGNOME comparator", "source_spec_id": "dwh_event_pygnome"},
                ],
                recommended_for_main_defense=True,
            ),
            self._board_spec(
                spec_id="dwh_trajectory_board",
                figure_family_code="J",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-20_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="trajectory_board",
                figure_slug="trajectory_board",
                figure_title="DWH trajectory publication board",
                subtitle="Deepwater Horizon | 20-23 May 2010 | deterministic, ensemble, and PyGNOME trajectories",
                interpretation="This board is appendix/support material for the separate DWH external transfer-validation case and explains transport path before score-based truth comparisons.",
                notes="Board assembled from publication-grade trajectory figures only.",
                note_lines=note_lines_dwh_tracks,
                panels=[
                    {"panel_title": "Deterministic path", "source_spec_id": "dwh_track_deterministic"},
                    {"panel_title": "Sampled ensemble", "source_spec_id": "dwh_track_ensemble"},
                    {"panel_title": "PyGNOME comparator path", "source_spec_id": "dwh_track_pygnome"},
                ],
                recommended_for_main_defense=False,
            ),
        ]
        return singles, boards

    def _prototype_support_publication_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        registry_path = self.prototype_similarity_registry_path
        prototype_dir = self.prototype_similarity_dir
        if self.prototype_similarity_figure_registry.empty:
            if prototype_dir.exists() and not registry_path.exists():
                self._record_missing(registry_path, "Prototype similarity package exists but the figure registry is missing.")
            return [], []

        is_preferred_2021 = prototype_dir.name == PREFERRED_PROTOTYPE_SIMILARITY_DIR.name
        subtitle = (
            "Prototype 2021 | accepted-segment deterministic transport comparator | support-only publication copy"
            if is_preferred_2021
            else "Prototype 2016 legacy | deterministic transport comparator | support-only publication copy"
        )
        notes = (
            "Support-only publication copy of the preferred accepted-segment debug OpenDrift-versus-PyGNOME comparator figure."
            if is_preferred_2021
            else "Support-only publication copy of the legacy prototype deterministic OpenDrift-versus-PyGNOME comparator figure."
        )

        specs: list[dict[str, Any]] = []
        registry = self.prototype_similarity_figure_registry.copy()
        required_columns = {"figure_id", "case_id", "relative_path", "view_type", "model_name", "date_token"}
        if not required_columns.issubset(set(registry.columns)):
            self._record_missing(registry_path, "Prototype similarity figure registry is missing required columns for publication family K.")
            return [], []

        registry = registry.sort_values(["case_id", "view_type", "hour", "model_name", "figure_id"]).reset_index(drop=True)
        for _, row in registry.iterrows():
            relative_path = str(row.get("relative_path") or "").strip()
            if not relative_path:
                continue
            source_path = self._resolve(relative_path)
            if not source_path.exists():
                self._record_missing(source_path, "Prototype support figure listed in the similarity registry is missing.")
                continue

            view_type = str(row.get("view_type") or "single").strip() or "single"
            is_board = view_type == "board"
            model_name = str(row.get("model_name") or "opendrift_vs_pygnome").strip() or "opendrift_vs_pygnome"
            hour_text = str(row.get("hour") or "").strip()
            default_title = source_path.stem.replace("_", " ")
            title = str(row.get("figure_title") or default_title)
            interpretation = str(row.get("short_plain_language_interpretation") or "").strip()
            if not interpretation:
                if is_board:
                    interpretation = (
                        f"Legacy prototype support board for {row['case_id']} showing the paired 24 h, 48 h, and 72 h "
                        "deterministic OpenDrift and PyGNOME footprints on the same benchmark grid."
                    )
                else:
                    interpretation = (
                        f"Legacy prototype support figure for {row['case_id']} showing the "
                        f"{model_name.replace('_', ' ')} deterministic footprint at {hour_text or 'paired'} h."
                    )
            if "PyGNOME" not in interpretation:
                interpretation += " PyGNOME remains comparator-only, not truth."
            if "final Chapter 3 evidence" not in interpretation:
                interpretation += " This is not final Chapter 3 evidence."

            source_paths: list[str] = [relative_path]
            raw_source_paths = str(row.get("source_paths") or "").strip()
            if raw_source_paths:
                source_paths.extend(part.strip() for part in raw_source_paths.split("|") if part.strip())

            specs.append(
                self._image_spec(
                    spec_id=f"prototype_support_{_safe_token(row.get('figure_id') or source_path.stem)}",
                    figure_family_code="K",
                    case_id=str(row["case_id"]),
                    phase_or_track="prototype_pygnome_similarity_summary",
                    date_token=str(row.get("date_token") or ("24_48_72h" if is_board else "legacy_support")),
                    model_names=model_name,
                    run_type="comparison_board" if is_board else "single_forecast",
                    view_type=view_type,
                    variant="slide" if is_board else "paper",
                    figure_slug=_safe_token(row.get("figure_id") or source_path.stem),
                    figure_title=title,
                    subtitle=subtitle,
                    interpretation=interpretation,
                notes=notes,
                source_image_path=relative_path,
                source_paths=source_paths,
                legacy_debug_only=not is_preferred_2021,
                recommended_for_main_defense=False,
                recommended_for_paper=not is_board,
            )
            )
        return specs, []

    def _build_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        single_specs: list[dict[str, Any]] = []
        board_specs: list[dict[str, Any]] = []
        for builder in (
            self._mindoro_publication_specs,
            self._phase4_publication_specs,
            self._dwh_publication_specs,
            self._prototype_support_publication_specs,
        ):
            singles, boards = builder()
            single_specs.extend(singles)
            board_specs.extend(boards)
        return single_specs, board_specs

    def _write_registry(self, path: Path) -> list[dict[str, Any]]:
        rows = [record.as_row() for record in sorted(self.figure_records, key=lambda item: (item.figure_family_code, item.figure_id))]
        _write_csv(
            path,
            rows,
            columns=[
                "figure_id",
                "figure_family_code",
                "figure_family_label",
                "case_id",
                "phase_or_track",
                "date_token",
                "model_names",
                "run_type",
                "scenario_id",
                "view_type",
                "variant",
                "relative_path",
                "file_path",
                "pixel_width",
                "pixel_height",
                "short_plain_language_interpretation",
                "recommended_for_main_defense",
                "recommended_for_paper",
                "source_paths",
                "notes",
                "status_key",
                "status_label",
                "status_role",
                "status_reportability",
                "status_official_status",
                "status_frozen_status",
                "status_provenance",
                "status_panel_text",
                "status_dashboard_summary",
            ],
        )
        return rows

    def _build_captions_markdown(self) -> str:
        lines = [
            "# Publication Figure Captions",
            "",
            "These captions are written for defense-panel use and are aligned with the publication-grade figure package.",
            "",
        ]
        for family_code, family_label in FIGURE_FAMILIES.items():
            lines.append(f"## {family_code}. {family_label}")
            lines.append("")
            family_records = [record for record in self.figure_records if record.figure_family_code == family_code]
            if not family_records:
                lines.append("- No figure generated for this family.")
                lines.append("")
                continue
            for record in sorted(family_records, key=lambda item: item.figure_id):
                status_label = record.status_label or record.figure_family_label
                provenance = f" Provenance: {record.status_provenance}" if record.status_provenance else ""
                lines.append(
                    f"- `{record.figure_id}` [{status_label}]: {record.short_plain_language_interpretation}{provenance}"
                )
            lines.append("")
        return "\n".join(lines)

    def _build_talking_points_markdown(self) -> str:
        recommended = [record for record in self.figure_records if record.recommended_for_main_defense]
        paper_ready = [record for record in self.figure_records if record.recommended_for_paper and record.variant == "paper"]
        supporting_honesty = [record for record in self.figure_records if record.figure_family_code == "F"]
        prototype_support = [record for record in self.figure_records if record.figure_family_code == "K"]
        lines = [
            "# Publication Figure Talking Points",
            "",
            "## Start Here",
            "",
        ]
        for record in sorted(recommended, key=lambda item: (item.figure_family_code, item.figure_id)):
            lines.append(
                f"- `{record.figure_id}` [{record.status_label or record.figure_family_label}]: {record.status_panel_text or record.short_plain_language_interpretation}"
            )
        lines.extend(
            [
                "",
                "## Paper-Ready Singles",
                "",
            ]
        )
        for record in sorted(paper_ready, key=lambda item: (item.figure_family_code, item.figure_id)):
            lines.append(f"- `{record.figure_id}`: suitable for single-image paper or appendix use.")
        if supporting_honesty:
            lines.extend(
                [
                    "",
                    "## Supporting Honesty Figures",
                    "",
                ]
            )
            for record in sorted(supporting_honesty, key=lambda item: item.figure_id):
                lines.append(
                    f"- `{record.figure_id}` [{record.status_label or record.figure_family_label}]: {record.status_panel_text or 'Use this figure when the panel asks why Phase 4 OpenDrift-versus-PyGNOME comparison is not shown.'}"
                )
        if prototype_support:
            lines.extend(
                [
                    "",
                    "## Prototype Support Figures",
                    "",
                ]
            )
            for record in sorted(prototype_support, key=lambda item: item.figure_id):
                lines.append(
                    f"- `{record.figure_id}` [{record.status_label or record.figure_family_label}]: {record.status_panel_text or 'Legacy prototype comparator only; deterministic OpenDrift control versus deterministic PyGNOME, with PyGNOME shown as a comparator rather than truth.'}"
                )
        if self.missing_optional_artifacts:
            lines.extend(["", "## Missing Optional Inputs", ""])
            for item in self.missing_optional_artifacts:
                lines.append(f"- `{item['relative_path']}`: {item['notes']}")
        return "\n".join(lines)

    def _build_manifest(self, generated_at_utc: str) -> dict[str, Any]:
        rows = [record.as_row() for record in self.figure_records]
        family_counts = {code: len([record for record in self.figure_records if record.figure_family_code == code]) for code in FIGURE_FAMILIES}
        recommended = [
            record.figure_id
            for record in sorted(self.figure_records, key=lambda item: (item.figure_family_code, item.figure_id))
            if record.recommended_for_main_defense
        ]
        paper_ready = [
            record.figure_id
            for record in sorted(self.figure_records, key=lambda item: (item.figure_family_code, item.figure_id))
            if record.recommended_for_paper and record.variant == "paper"
        ]
        deferred_note_figure_ids = [record.figure_id for record in self.figure_records if record.figure_family_code == "F"]
        return {
            "phase": PHASE,
            "generated_at_utc": generated_at_utc,
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "publication_package_built_from_existing_outputs_only": True,
            "expensive_scientific_reruns_triggered": False,
            "phase4_crossmodel_comparison_status": "deferred",
            "phase4_deferred_comparison_note_figure_produced": bool(deferred_note_figure_ids),
            "phase4_deferred_comparison_note_figure_ids": deferred_note_figure_ids,
            "style_config_path": _relative_to_repo(self.repo_root, self.repo_root / STYLE_CONFIG_PATH),
            "map_label_paths": {
                "mindoro": _relative_to_repo(self.repo_root, self.repo_root / MINDORO_LABELS_PATH),
                "dwh": _relative_to_repo(self.repo_root, self.repo_root / DWH_LABELS_PATH),
            },
            "visual_semantics": {
                "observed_mask": self._legend_labels().get("observed_mask", ""),
                "deterministic_opendrift": self._legend_labels().get("deterministic_opendrift", ""),
                "ensemble_consolidated": self._legend_labels().get("ensemble_consolidated", ""),
                "ensemble_p50": self._legend_labels().get("ensemble_p50", ""),
                "ensemble_p90": self._legend_labels().get("ensemble_p90", ""),
                "pygnome": self._legend_labels().get("pygnome", ""),
                "source_point": self._legend_labels().get("source_point", ""),
                "initialization_polygon": self._legend_labels().get("initialization_polygon", ""),
                "corridor_hull": self._legend_labels().get("corridor_hull", ""),
            },
            "figure_families_generated": family_counts,
            "side_by_side_comparison_boards_produced": any(record.view_type == "board" for record in self.figure_records),
            "single_image_paper_figures_produced": any(record.variant == "paper" for record in self.figure_records),
            "recommended_main_defense_figures": recommended,
            "recommended_paper_figures": paper_ready,
            "upstream_status_context": {
                "phase1_scientifically_frozen": bool(self._phase_status_flag("phase1", "phase1_regional_baseline", "scientifically_frozen", False)),
                "phase2_scientifically_usable": bool(self._phase_status_flag("phase2", "phase2_machine_readable_forecast", "scientifically_reportable", True)),
                "phase2_scientifically_frozen": bool(self._phase_status_flag("phase2", "phase2_machine_readable_forecast", "scientifically_frozen", False)),
                "phase4_scientifically_reportable": bool(self._phase_status_flag("phase4", "mindoro_phase4", "scientifically_reportable", True)),
            },
            "missing_optional_artifacts": self.missing_optional_artifacts,
            "figures": rows,
        }

    def run(self) -> dict[str, Any]:
        generated_at_utc = pd.Timestamp.now(tz="UTC").isoformat()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        single_specs, board_specs = self._build_specs()
        saved_by_spec_id: dict[str, PublicationFigureRecord] = {}
        for spec in single_specs:
            saved_by_spec_id[str(spec["spec_id"])] = self._save_single_figure(spec)
        for spec in board_specs:
            saved_by_spec_id[str(spec["spec_id"])] = self._save_board_figure(spec, saved_by_spec_id)

        registry_csv = self.output_dir / "publication_figure_registry.csv"
        manifest_json = self.output_dir / "publication_figure_manifest.json"
        captions_md = self.output_dir / "publication_figure_captions.md"
        talking_points_md = self.output_dir / "publication_figure_talking_points.md"

        rows = self._write_registry(registry_csv)
        _write_text(captions_md, self._build_captions_markdown())
        _write_text(talking_points_md, self._build_talking_points_markdown())
        _write_json(manifest_json, self._build_manifest(generated_at_utc))

        return {
            "output_dir": str(self.output_dir),
            "registry_csv": str(registry_csv),
            "manifest_json": str(manifest_json),
            "captions_md": str(captions_md),
            "talking_points_md": str(talking_points_md),
            "figure_count": len(self.figure_records),
            "figure_families_generated": sorted({record.figure_family_code for record in self.figure_records}),
            "side_by_side_comparison_boards_produced": any(record.view_type == "board" for record in self.figure_records),
            "single_image_paper_figures_produced": any(record.variant == "paper" for record in self.figure_records),
            "phase4_deferred_comparison_note_figure_produced": any(record.figure_family_code == "F" for record in self.figure_records),
            "recommended_main_defense_figures": [record.figure_id for record in self.figure_records if record.recommended_for_main_defense],
            "recommended_paper_figures": [record.figure_id for record in self.figure_records if record.recommended_for_paper and record.variant == "paper"],
            "missing_optional_artifacts": self.missing_optional_artifacts,
            "figure_rows": rows,
        }


def run_figure_package_publication(repo_root: str | Path = ".", output_dir: str | Path | None = None) -> dict[str, Any]:
    return FigurePackagePublicationService(repo_root=repo_root, output_dir=output_dir).run()
