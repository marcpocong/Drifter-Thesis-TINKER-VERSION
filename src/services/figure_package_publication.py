"""Publication-grade figure package built from existing outputs only."""

from __future__ import annotations

import json
import shutil
import textwrap
import os
import hashlib
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
from matplotlib.patches import Patch, Polygon, Rectangle
from pyproj import Transformer
from rasterio.plot import show
from rasterio.warp import transform_bounds
from shapely.geometry import MultiPoint, box as shapely_box

from src.core.artifact_status import artifact_status_columns, artifact_status_columns_for_key
from src.core.publication_figure_governance import publication_figure_governance_columns
from src.core.study_box_catalog import (
    ARCHIVE_ONLY_STUDY_BOX_NUMBERS,
    THESIS_FACING_STUDY_BOX_NUMBERS,
    parse_study_box_numbers,
    study_box_catalog_rows,
    study_box_figure_metadata,
)
from src.services.mindoro_primary_validation_metadata import (
    MINDORO_BASE_CASE_CONFIG_PATH,
    MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH,
    MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
    MINDORO_OBSERVATION_INDEPENDENCE_NOTE,
)

matplotlib.use("Agg")

PHASE = "figure_package_publication"
OUTPUT_DIR = Path("output") / "figure_package_publication"
LOCAL_FONT_DIR = Path("output") / "_local_fonts"
STYLE_CONFIG_PATH = Path("config") / "publication_figure_style.yaml"
MINDORO_LABELS_PATH = Path("config") / "publication_map_labels_mindoro.csv"
DWH_LABELS_PATH = Path("config") / "publication_map_labels_dwh.csv"
PHASE1_BASELINE_SELECTION_PATH = Path("config") / "phase1_baseline_selection.yaml"
DOMAIN_GLOSSARY_PATH = Path("docs") / "DOMAIN_GLOSSARY.md"
PROTOTYPE_2016_PROVENANCE_METADATA_PATH = (
    Path("output") / "2016 Legacy Runs FINAL Figures" / "manifests" / "prototype_2016_provenance_metadata.json"
)
PROTOTYPE_2016_FINAL_DIR = Path("output") / "2016 Legacy Runs FINAL Figures"
STUDY_BOX_LAND_CONTEXT_PATH = Path("data_processed") / "reference" / "study_box_land_context.geojson"

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
    "F": "Mindoro Phase 4 no-matched-PyGNOME note package",
    "F1": "DWH observation truth-context publication package",
    "G": "DWH deterministic publication package",
    "H": "DWH ensemble extension publication package",
    "I": "DWH OpenDrift vs PyGNOME publication package",
    "J": "DWH trajectory publication package",
    "K": "Prototype accepted-segment OpenDrift vs PyGNOME support package",
    "L": "Thesis study-box reference package",
    "M": "Legacy 2016 home-overview support package",
}


@dataclass
class PublicationFigureRecord:
    figure_id: str
    display_title: str
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
    surface_key: str
    surface_label: str
    surface_description: str
    surface_home_visible: bool
    surface_panel_visible: bool
    surface_archive_visible: bool
    surface_advanced_visible: bool
    surface_recommended_visible: bool
    thesis_surface: bool
    archive_only: bool
    legacy_support: bool
    comparator_support: bool
    display_order: int
    page_target: str
    study_box_id: str
    study_box_numbers: str
    study_box_label: str
    recommended_scope: str

    def as_row(self) -> dict[str, Any]:
        return {
            "figure_id": self.figure_id,
            "display_title": self.display_title,
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
            "surface_key": self.surface_key,
            "surface_label": self.surface_label,
            "surface_description": self.surface_description,
            "surface_home_visible": self.surface_home_visible,
            "surface_panel_visible": self.surface_panel_visible,
            "surface_archive_visible": self.surface_archive_visible,
            "surface_advanced_visible": self.surface_advanced_visible,
            "surface_recommended_visible": self.surface_recommended_visible,
            "thesis_surface": self.thesis_surface,
            "archive_only": self.archive_only,
            "legacy_support": self.legacy_support,
            "comparator_support": self.comparator_support,
            "display_order": self.display_order,
            "page_target": self.page_target,
            "study_box_id": self.study_box_id,
            "study_box_numbers": self.study_box_numbers,
            "study_box_label": self.study_box_label,
            "recommended_scope": self.recommended_scope,
        }


@dataclass
class PublicationFontAudit:
    requested_font_family: str
    actual_font_family: str
    actual_font_path: str
    exact_requested_font_used: bool
    fallback_used: bool
    fallback_candidates: list[str]

    def as_row(self) -> dict[str, Any]:
        return {
            "requested_font_family": self.requested_font_family,
            "actual_font_family": self.actual_font_family,
            "actual_font_path": self.actual_font_path,
            "exact_requested_font_used": self.exact_requested_font_used,
            "fallback_used": self.fallback_used,
            "fallback_candidates": " | ".join(self.fallback_candidates),
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
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except Exception:
        return Path(path).as_posix()


def _path_for_io(path: str | Path) -> str:
    path_obj = Path(path)
    path_text = str(path_obj)
    if os.name != "nt":
        return path_text
    absolute = str(path_obj.resolve())
    if absolute.startswith("\\\\?\\"):
        return absolute
    if absolute.startswith("\\\\"):
        return "\\\\?\\UNC\\" + absolute.lstrip("\\")
    return "\\\\?\\" + absolute


def _windows_safe_publication_filename(spec: dict[str, Any], original_filename: str) -> str:
    digest = hashlib.sha1(original_filename.encode("utf-8")).hexdigest()[:10]
    case_token = _safe_token(str(spec.get("case_id") or "case"))
    model_token = _safe_token(str(spec.get("model_names") or "model"))
    slug_token = _safe_token(str(spec.get("figure_slug") or Path(original_filename).stem)) or "figure"
    shortened = f"{case_token}__{model_token}__{slug_token}__{digest}.png"
    if len(shortened) <= 150:
        return shortened
    return f"{slug_token[:112].rstrip('_')}__{digest}.png"


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


def _normalized_font_name(value: str) -> str:
    return "".join(char for char in str(value or "").strip().lower() if char.isalnum())


def resolve_publication_typography(style: dict[str, Any], repo_root: str | Path) -> PublicationFontAudit:
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
    windows_font_dir = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts"
    if _normalized_font_name(font_family) == "arial" and windows_font_dir.exists():
        for pattern in ("arial*.ttf", "arial*.otf", "ARIAL*.TTF", "ARIAL*.OTF"):
            candidate_paths.extend(sorted(windows_font_dir.glob(pattern)))
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
    requested_norm = _normalized_font_name(font_family)
    resolved_family = font_family
    resolved_path = ""
    exact_requested_font_used = False
    for candidate_family, allow_fallback in [(font_family, False), *[(fallback, False) for fallback in fallbacks], ("sans-serif", True)]:
        try:
            candidate_path = matplotlib.font_manager.findfont(candidate_family, fallback_to_default=allow_fallback)
        except Exception:
            continue
        resolved_path = str(candidate_path)
        try:
            resolved_family = matplotlib.font_manager.FontProperties(fname=candidate_path).get_name() or candidate_family
        except Exception:
            resolved_family = candidate_family
        exact_requested_font_used = _normalized_font_name(resolved_family) == requested_norm or (
            requested_norm == "arial" and Path(candidate_path).name.lower().startswith("arial")
        )
        if candidate_family == font_family or resolved_path:
            break
    if not resolved_path:
        resolved_path = str(matplotlib.font_manager.findfont("sans-serif"))
        try:
            resolved_family = matplotlib.font_manager.FontProperties(fname=resolved_path).get_name() or "sans-serif"
        except Exception:
            resolved_family = "sans-serif"
    sans_serif = [resolved_family]
    for candidate in [font_family, *fallbacks]:
        if candidate and candidate not in sans_serif:
            sans_serif.append(candidate)
    matplotlib.rcParams["font.family"] = [resolved_family]
    matplotlib.rcParams["font.sans-serif"] = sans_serif
    return PublicationFontAudit(
        requested_font_family=font_family,
        actual_font_family=resolved_family,
        actual_font_path=resolved_path,
        exact_requested_font_used=exact_requested_font_used,
        fallback_used=not exact_requested_font_used,
        fallback_candidates=fallbacks,
    )


def apply_publication_typography(style: dict[str, Any], repo_root: str | Path) -> str:
    return resolve_publication_typography(style, repo_root).actual_font_family


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
        self.font_audit = self._configure_typography()
        self.mindoro_labels = _read_csv(self.repo_root / MINDORO_LABELS_PATH)
        self.dwh_labels = _read_csv(self.repo_root / DWH_LABELS_PATH)
        self.mindoro_base_case_config = _read_yaml(self.repo_root / MINDORO_BASE_CASE_CONFIG_PATH)
        self.phase1_baseline_selection = _read_yaml(self.repo_root / PHASE1_BASELINE_SELECTION_PATH)
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
        self.prototype_2016_provenance_metadata = _read_json(self.repo_root / PROTOTYPE_2016_PROVENANCE_METADATA_PATH)
        self.dwh_run_manifest = _read_json(self.repo_root / DWH_RUN_MANIFEST)
        self.dwh_summary = _read_csv(self.repo_root / DWH_SUMMARY)
        self.dwh_all_results = _read_csv(self.repo_root / DWH_ALL_RESULTS)
        self.prototype_similarity_dir, self.prototype_similarity_registry_path = self._resolve_prototype_support_dir()
        self.prototype_similarity_figure_registry = _read_csv(self.prototype_similarity_registry_path)
        self.final_phase_status = _read_csv(self.repo_root / FINAL_PHASE_STATUS_CSV)
        self.figure_records: list[PublicationFigureRecord] = []
        self.missing_optional_artifacts: list[dict[str, str]] = []
        self.board_layout_audit_rows: list[dict[str, Any]] = []
        self._raster_cache: dict[Path, dict[str, Any]] = {}
        self._vector_cache: dict[tuple[Path, str], gpd.GeoDataFrame] = {}
        self._spec_lookup: dict[str, dict[str, Any]] = {}

    def _resolve_prototype_support_dir(self) -> tuple[Path, Path]:
        preferred_dir = self.repo_root / PREFERRED_PROTOTYPE_SIMILARITY_DIR
        preferred_registry = preferred_dir / "prototype_pygnome_figure_registry.csv"
        if preferred_registry.exists():
            return preferred_dir, preferred_registry
        legacy_dir = self.repo_root / LEGACY_PROTOTYPE_SIMILARITY_DIR
        legacy_registry = legacy_dir / "prototype_pygnome_figure_registry.csv"
        return legacy_dir, legacy_registry

    def _configure_typography(self) -> PublicationFontAudit:
        return resolve_publication_typography(self.style, self.repo_root)

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

    def _legend_label_overrides(self, spec: dict[str, Any] | None = None) -> dict[str, str]:
        raw = {}
        if spec is not None:
            raw = spec.get("legend_label_overrides") or {}
        overrides: dict[str, str] = {}
        for key, value in dict(raw).items():
            normalized_key = str(key).strip()
            normalized_value = str(value).strip()
            if normalized_key and normalized_value:
                overrides[normalized_key] = normalized_value
        return overrides

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

    def _legend_handles(
        self,
        legend_keys: list[str],
        *,
        label_overrides: dict[str, str] | None = None,
    ) -> list[Any]:
        palette = self._palette()
        labels = dict(self._legend_labels())
        if label_overrides:
            labels.update({str(key): str(value) for key, value in label_overrides.items()})
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

    def _add_legend(
        self,
        ax: plt.Axes,
        legend_keys: list[str],
        *,
        compact: bool = False,
        label_overrides: dict[str, str] | None = None,
    ) -> Any:
        ax.axis("off")
        handles = self._legend_handles(legend_keys, label_overrides=label_overrides)
        legend = ax.legend(
            handles=handles,
            loc="upper left",
            frameon=True,
            fontsize=float((self.style.get("typography") or {}).get("note_size") or 8),
            title="Legend",
            ncol=2 if compact and len(handles) > 4 else 1,
        )
        legend.get_frame().set_facecolor((1, 1, 1, 0.98))
        legend.get_frame().set_edgecolor((self.style.get("layout") or {}).get("legend_edgecolor") or "#94a3b8")
        if legend.get_title():
            legend.get_title().set_fontsize(float((self.style.get("typography") or {}).get("legend_title_size") or 9))
        return legend

    def _axis_wrap_width(
        self,
        ax: plt.Axes,
        *,
        fontsize: float,
        min_chars: int = 18,
        max_chars: int = 56,
    ) -> int:
        width_px = ax.get_position().width * ax.figure.get_figwidth() * ax.figure.dpi
        approx = int(width_px / max(fontsize * 0.82, 1.0))
        return max(min_chars, min(max_chars, approx))

    def _wrap_card_lines(
        self,
        lines: list[str],
        *,
        width: int,
        bullet_lines: bool = False,
    ) -> str:
        wrapped_lines: list[str] = []
        for line in lines:
            raw = str(line).strip()
            if not raw:
                continue
            if bullet_lines:
                wrapped_lines.append(textwrap.fill(f"- {raw}", width=width, subsequent_indent="  "))
            else:
                wrapped_lines.append(textwrap.fill(raw, width=width))
        return "\n".join(wrapped_lines)

    @staticmethod
    def _artist_vertical_gap_px(upper_artist: Any, lower_artist: Any) -> float:
        renderer = upper_artist.figure.canvas.get_renderer()
        upper_bbox = upper_artist.get_window_extent(renderer=renderer)
        lower_bbox = lower_artist.get_window_extent(renderer=renderer)
        return float(upper_bbox.y0 - lower_bbox.y1)

    def _enforce_artist_gap(
        self,
        ax: plt.Axes,
        upper_artist: Any,
        lower_artist: Any,
        *,
        minimum_gap_px: float,
        min_lower_y: float = 0.02,
    ) -> None:
        if float(minimum_gap_px) <= 0.0:
            return
        fig = ax.figure
        fig.canvas.draw()
        current_gap_px = self._artist_vertical_gap_px(upper_artist, lower_artist)
        if current_gap_px >= float(minimum_gap_px):
            return
        shift_px = float(minimum_gap_px) - current_gap_px
        axis_height_px = max(float(ax.bbox.height), 1.0)
        x_value, y_value = lower_artist.get_position()
        lower_artist.set_position((float(x_value), max(float(min_lower_y), float(y_value) - (shift_px / axis_height_px))))
        fig.canvas.draw()

    def _add_note_box(
        self,
        ax: plt.Axes,
        title: str,
        lines: list[str],
        *,
        wrap_width: int | None = None,
        bullet_lines: bool = False,
        title_y: float = 1.0,
        body_y: float = 0.92,
        box_pad: float = 0.36,
        minimum_title_gap_px: float = 10.0,
    ) -> tuple[Any, Any]:
        ax.axis("off")
        if wrap_width is None:
            wrap_width = self._axis_wrap_width(
                ax,
                fontsize=float((self.style.get("typography") or {}).get("note_size") or 8),
            )
        wrapped = self._wrap_card_lines(lines, width=wrap_width, bullet_lines=bullet_lines)
        title_artist = ax.text(
            0.0,
            title_y,
            title,
            ha="left",
            va="top",
            fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11),
            fontweight="bold",
            color="#0f172a",
            transform=ax.transAxes,
        )
        body_artist = ax.text(
            0.0,
            body_y,
            wrapped,
            ha="left",
            va="top",
            fontsize=float((self.style.get("typography") or {}).get("note_size") or 8),
            color="#334155",
            transform=ax.transAxes,
            bbox={"boxstyle": f"round,pad={box_pad:.2f}", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
        )
        self._enforce_artist_gap(
            ax,
            title_artist,
            body_artist,
            minimum_gap_px=float(minimum_title_gap_px),
        )
        return title_artist, body_artist

    def _artist_within_bbox(self, artist: Any, bbox: Any, *, pad_px: float = 2.0) -> bool:
        renderer = artist.figure.canvas.get_renderer()
        artist_bbox = artist.get_window_extent(renderer=renderer)
        return (
            artist_bbox.x0 >= (bbox.x0 - pad_px)
            and artist_bbox.y0 >= (bbox.y0 - pad_px)
            and artist_bbox.x1 <= (bbox.x1 + pad_px)
            and artist_bbox.y1 <= (bbox.y1 + pad_px)
        )

    def _board_panel_legend_keys(self, panel_spec: dict[str, Any]) -> list[str]:
        keys = [str(item) for item in panel_spec.get("legend_keys", []) if str(item).strip()]
        legend_key = str(panel_spec.get("legend_key") or "").strip()
        if legend_key and legend_key not in keys:
            keys.append(legend_key)
        return keys

    def _board_legend_label_overrides(self, board_spec: dict[str, Any]) -> dict[str, str]:
        overrides = dict(self._legend_label_overrides(board_spec))
        for panel in list(board_spec.get("panels", [])):
            source_spec_id = str(panel.get("source_spec_id") or "").strip()
            source_spec = self._spec_lookup.get(source_spec_id)
            if source_spec is not None and str(source_spec.get("renderer") or "") != "external_image":
                overrides.update(self._legend_label_overrides(source_spec))
            else:
                overrides.update(self._legend_label_overrides(panel))
        return overrides

    def _board_text_blocks(self, spec: dict[str, Any]) -> dict[str, Any]:
        guide_bullets = [str(item).strip() for item in spec.get("guide_bullets", []) if str(item).strip()]
        note_lines = [str(item).strip() for item in spec.get("note_lines", []) if str(item).strip()]
        has_explicit_board_text = bool(guide_bullets) or bool(str(spec.get("caveat_line") or "").strip()) or bool(
            str(spec.get("provenance_line") or "").strip()
        )
        if not guide_bullets:
            guide_bullets = note_lines[:4]
        caveat_line = str(spec.get("caveat_line") or "").strip()
        provenance_line = str(spec.get("provenance_line") or "").strip()
        if not caveat_line:
            caveat_line = next(
                (
                    line
                    for line in note_lines
                    if any(token in line.lower() for token in ("caveat", "day-specific", "truth", "comparator-only"))
                ),
                "",
            )
        if not provenance_line:
            provenance_line = next(
                (
                    line
                    for line in note_lines
                    if any(token in line.lower() for token in ("provenance", "recipe", "forcing stack", "stored"))
                ),
                "",
            )
        extra_lines = [] if has_explicit_board_text else [line for line in note_lines if line not in guide_bullets and line not in {caveat_line, provenance_line}]
        return {
            "guide_heading": str(spec.get("guide_heading") or "How to read this board"),
            "guide_bullets": guide_bullets[:5],
            "caveat_line": caveat_line,
            "provenance_line": provenance_line,
            "extra_lines": extra_lines,
        }

    def _board_layout_settings(self, spec: dict[str, Any], panel_count: int) -> dict[str, Any]:
        layout_mode = "bottom_strip" if panel_count <= 3 else "sidecar"
        defaults: dict[str, Any] = {
            "layout_mode": layout_mode,
            "outer_left": 0.04,
            "outer_right": 0.98,
            "outer_top": 0.89,
            "outer_bottom": 0.06,
            "outer_hspace": 0.12,
            "outer_wspace": 0.06,
            "panel_grid_wspace": 0.10,
            "panel_grid_hspace": 0.12 if layout_mode == "bottom_strip" else 0.14,
            "info_grid_wspace": 0.12,
            "locator_stack_hspace": 0.10,
            "side_grid_hspace": 0.12,
            "guide_title_y": 0.98,
            "guide_body_y": 0.84 if layout_mode == "bottom_strip" else 0.82,
            "guide_box_pad": 0.34,
            "guide_minimum_title_gap_px": 18.0,
            "guide_wrap_max_chars": 52,
            "note_title_y": 0.98,
            "note_body_y": 0.84 if layout_mode == "bottom_strip" else 0.82,
            "note_box_pad": 0.34,
            "note_minimum_title_gap_px": 18.0,
            "note_wrap_max_chars": 50,
            "title_y": 0.965,
            "subtitle_y": 0.935,
            "subtitle_wrap_width": 116,
        }
        overrides = spec.get("board_layout_overrides") or {}
        for key, value in overrides.items():
            defaults[str(key)] = value
        return defaults

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

    def _add_locator(
        self,
        ax: plt.Axes,
        case_id: str,
        crop_bounds: tuple[float, float, float, float] | None,
        target_crs: str,
        *,
        compact: bool = False,
    ) -> None:
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
        if compact:
            ax.set_xlabel("")
            ax.set_ylabel("")
            ax.tick_params(labelsize=5)
        else:
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

    def _coerce_wgs84_bounds(self, value: Any) -> tuple[float, float, float, float] | None:
        if not isinstance(value, (list, tuple)) or len(value) != 4:
            return None
        try:
            min_lon, max_lon, min_lat, max_lat = [float(item) for item in value]
        except (TypeError, ValueError):
            return None
        return (min_lon, max_lon, min_lat, max_lat)

    def _format_wgs84_bounds(self, bounds: tuple[float, float, float, float] | None) -> str:
        if bounds is None:
            return "Unavailable"
        return "[" + ", ".join(format(value, ".6f").rstrip("0").rstrip(".") for value in bounds) + "]"

    def _thesis_study_box_entries(self) -> list[dict[str, Any]]:
        focused_box = self._coerce_wgs84_bounds(
            (self.phase1_baseline_selection.get("chapter3_finalization_audit") or {}).get("phase1_validation_box")
            or self.phase1_baseline_selection.get("phase1_validation_box")
        ) or (118.751, 124.305, 10.620, 16.026)
        mindoro_case_domain = self._coerce_wgs84_bounds(self.mindoro_base_case_config.get("mindoro_case_domain")) or (
            115.0,
            122.0,
            6.0,
            14.5,
        )
        scoring_bounds = self._coerce_wgs84_bounds((self.mindoro_forecast_manifest.get("grid") or {}).get("display_bounds_wgs84")) or (
            120.90964677179262,
            122.0621541786303,
            12.249384840763462,
            13.783655303175253,
        )
        first_code_box = self._coerce_wgs84_bounds(
            self.prototype_2016_provenance_metadata.get("prototype_2016_initial_capture_box")
        ) or (108.6465, 121.3655, 6.1865, 20.3515)
        focused_meta = study_box_figure_metadata("focused_phase1_validation_box")
        case_domain_meta = study_box_figure_metadata("mindoro_case_domain")
        scoring_meta = study_box_figure_metadata("scoring_grid_display_bounds")
        prototype_meta = study_box_figure_metadata("prototype_first_code_search_box")
        return [
            {
                "study_box_id": "focused_phase1_validation_box",
                "tag": focused_meta["study_box_numbers"],
                "label": focused_meta["box_label"],
                "short_label": "Focused provenance box",
                "bounds": focused_box,
                "color": "#d97706",
                "fill_alpha": 0.10,
                "line_width": 2.2,
                "text_anchor": (focused_box[0] + 0.06, focused_box[3] - 0.10),
            },
            {
                "study_box_id": "mindoro_case_domain",
                "tag": case_domain_meta["study_box_numbers"],
                "label": case_domain_meta["box_label"],
                "short_label": "Mindoro case domain",
                "bounds": mindoro_case_domain,
                "color": "#165ba8",
                "fill_alpha": 0.07,
                "line_width": 2.0,
                "text_anchor": (mindoro_case_domain[0] + 0.10, mindoro_case_domain[2] + 0.16),
            },
            {
                "study_box_id": "scoring_grid_display_bounds",
                "tag": scoring_meta["study_box_numbers"],
                "label": scoring_meta["box_label"],
                "short_label": "Scoring-grid bounds",
                "bounds": scoring_bounds,
                "color": "#b42318",
                "fill_alpha": 0.16,
                "line_width": 2.4,
                "text_anchor": (scoring_bounds[1] + 0.12, scoring_bounds[3] + 0.30),
                "arrow_to_center": True,
            },
            {
                "study_box_id": "prototype_first_code_search_box",
                "tag": prototype_meta["study_box_numbers"],
                "label": prototype_meta["box_label"],
                "short_label": "Prototype origin box",
                "bounds": first_code_box,
                "color": "#7c3aed",
                "fill_alpha": 0.05,
                "line_width": 1.9,
                "text_anchor": (first_code_box[0] + 0.20, first_code_box[3] - 0.30),
            },
        ]

    def _study_box_geography_features(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "Palawan",
                "label": "Palawan",
                "label_pos": (118.35, 9.85),
                "coordinates": [
                    (117.10, 11.85),
                    (117.35, 11.10),
                    (117.55, 10.25),
                    (117.78, 9.35),
                    (118.05, 8.55),
                    (118.32, 7.80),
                    (118.62, 7.25),
                    (118.92, 7.38),
                    (118.78, 8.05),
                    (118.54, 8.95),
                    (118.28, 9.85),
                    (118.02, 10.70),
                    (117.78, 11.38),
                    (117.45, 11.98),
                ],
            },
            {
                "name": "Mindoro",
                "label": "Mindoro",
                "label_pos": (121.05, 12.92),
                "coordinates": [
                    (120.30, 13.52),
                    (120.55, 13.15),
                    (120.82, 12.82),
                    (121.05, 12.40),
                    (121.35, 12.18),
                    (121.52, 12.42),
                    (121.46, 12.85),
                    (121.35, 13.18),
                    (121.12, 13.45),
                    (120.78, 13.62),
                    (120.48, 13.65),
                ],
            },
            {
                "name": "Busuanga",
                "label": "Calamian",
                "label_pos": (120.02, 12.02),
                "coordinates": [
                    (119.55, 12.20),
                    (119.78, 11.95),
                    (120.05, 11.82),
                    (120.30, 11.96),
                    (120.12, 12.25),
                    (119.82, 12.33),
                ],
            },
            {
                "name": "Luzon",
                "label": "Luzon",
                "label_pos": (120.75, 16.55),
                "coordinates": [
                    (119.35, 13.28),
                    (119.72, 13.88),
                    (120.18, 14.52),
                    (120.65, 15.12),
                    (121.18, 15.88),
                    (121.85, 16.72),
                    (122.28, 17.58),
                    (122.34, 18.22),
                    (121.85, 18.78),
                    (121.02, 18.52),
                    (120.20, 17.72),
                    (119.68, 16.88),
                    (119.34, 15.92),
                    (119.18, 14.92),
                    (119.12, 14.12),
                ],
            },
            {
                "name": "Panay",
                "label": "Panay",
                "label_pos": (122.22, 11.20),
                "coordinates": [
                    (121.92, 11.55),
                    (122.18, 11.18),
                    (122.45, 10.78),
                    (122.62, 10.92),
                    (122.56, 11.32),
                    (122.28, 11.58),
                ],
            },
            {
                "name": "Negros",
                "label": "Negros",
                "label_pos": (123.10, 10.52),
                "coordinates": [
                    (122.86, 10.82),
                    (123.06, 10.34),
                    (123.24, 9.78),
                    (123.44, 9.98),
                    (123.36, 10.48),
                    (123.12, 10.88),
                ],
            },
            {
                "name": "North Borneo",
                "label": "North Borneo",
                "label_pos": (117.05, 6.55),
                "coordinates": [
                    (115.78, 5.70),
                    (116.42, 6.02),
                    (117.12, 6.30),
                    (117.88, 6.55),
                    (118.55, 6.92),
                    (119.05, 7.28),
                    (118.52, 7.62),
                    (117.72, 7.48),
                    (116.92, 7.24),
                    (116.18, 6.92),
                    (115.72, 6.38),
                ],
            },
        ]

    def _load_study_box_land_context(self) -> gpd.GeoDataFrame | None:
        path = self._resolve(STUDY_BOX_LAND_CONTEXT_PATH)
        key = (path.resolve(), "EPSG:4326")
        if key in self._vector_cache:
            return self._vector_cache[key]
        if not path.exists():
            return None
        gdf = gpd.read_file(path)
        if gdf.empty:
            return None
        if gdf.crs and str(gdf.crs) != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
        self._vector_cache[key] = gdf
        return gdf

    def _study_box_land_context_subset(
        self,
        *,
        xlim: tuple[float, float],
        ylim: tuple[float, float],
    ) -> gpd.GeoDataFrame | None:
        land = self._load_study_box_land_context()
        if land is None or land.empty:
            return None
        window = shapely_box(xlim[0] - 0.25, ylim[0] - 0.25, xlim[1] + 0.25, ylim[1] + 0.25)
        subset = land.loc[land.geometry.intersects(window)].copy()
        if subset.empty:
            return None
        subset["geometry"] = subset.geometry.intersection(window)
        subset = subset.loc[subset.geometry.notna() & ~subset.geometry.is_empty].copy()
        return subset if not subset.empty else None

    def _draw_study_box_geography_context(
        self,
        ax: plt.Axes,
        *,
        xlim: tuple[float, float],
        ylim: tuple[float, float],
    ) -> None:
        ax.set_facecolor("#d8edf7")
        ax.axvspan(xlim[0], xlim[1], color="#ebf5fb", alpha=0.68, zorder=0)
        ax.axhspan(ylim[0], ylim[1], color="#eff7fc", alpha=0.50, zorder=0)

        land_context = self._study_box_land_context_subset(xlim=xlim, ylim=ylim)
        if land_context is not None and not land_context.empty:
            land_context.plot(ax=ax, color="#f3ebdd", edgecolor="none", linewidth=0.0, zorder=1)
            land_context.boundary.plot(ax=ax, color="#b6a89b", linewidth=1.15, alpha=0.35, zorder=1)
            land_context.boundary.plot(ax=ax, color="#77695f", linewidth=0.55, alpha=0.95, zorder=2)
        else:
            for feature in self._study_box_geography_features():
                coords = feature["coordinates"]
                lons = [point[0] for point in coords]
                lats = [point[1] for point in coords]
                if max(lons) < xlim[0] or min(lons) > xlim[1] or max(lats) < ylim[0] or min(lats) > ylim[1]:
                    continue
                patch = Polygon(
                    coords,
                    closed=True,
                    facecolor="#f6efe4",
                    edgecolor="#7c6f64",
                    linewidth=0.9,
                    alpha=0.96,
                    zorder=1,
                )
                ax.add_patch(patch)

        for feature in self._study_box_geography_features():
            label_lon, label_lat = feature["label_pos"]
            if xlim[0] <= label_lon <= xlim[1] and ylim[0] <= label_lat <= ylim[1]:
                ax.text(
                    label_lon,
                    label_lat,
                    str(feature["label"]),
                    fontsize=7.4,
                    color="#334155",
                    ha="center",
                    va="center",
                    zorder=3,
                    bbox={
                        "boxstyle": "round,pad=0.18",
                        "facecolor": (1, 1, 1, 0.78),
                        "edgecolor": "#cbd5e1",
                        "linewidth": 0.45,
                    },
                )

        ocean_labels = [
            ("South China Sea", (max(xlim[0] + 0.85, 112.8), min(ylim[1] - 0.85, 13.0))),
            ("Sulu Sea", (min(xlim[1] - 0.70, 120.55), max(ylim[0] + 0.65, 10.25))),
        ]
        for label, (lon, lat) in ocean_labels:
            if xlim[0] <= lon <= xlim[1] and ylim[0] <= lat <= ylim[1]:
                ax.text(
                    lon,
                    lat,
                    label,
                    fontsize=7.2,
                    color="#0f3d63",
                    fontstyle="italic",
                    ha="center",
                    va="center",
                    alpha=0.72,
                    zorder=1,
                )

    def _render_study_boxes_panel(self, ax: plt.Axes, spec: dict[str, Any]) -> dict[str, Any]:
        boxes = [item for item in spec.get("study_boxes", []) if self._coerce_wgs84_bounds(item.get("bounds"))]
        if not boxes:
            ax.text(0.5, 0.5, "Study-box metadata unavailable", ha="center", va="center", fontsize=12)
            ax.axis("off")
            return {"source_paths": [], "crop_bounds": None, "target_crs": None}

        min_lon = min(float(item["bounds"][0]) for item in boxes)
        max_lon = max(float(item["bounds"][1]) for item in boxes)
        min_lat = min(float(item["bounds"][2]) for item in boxes)
        max_lat = max(float(item["bounds"][3]) for item in boxes)
        lon_span = max(max_lon - min_lon, 1.0)
        lat_span = max(max_lat - min_lat, 1.0)
        pad_fraction = float(spec.get("box_padding_fraction") or 0.08)
        pad_lon = max(float(spec.get("minimum_pad_lon") or 0.9), lon_span * pad_fraction)
        pad_lat = max(float(spec.get("minimum_pad_lat") or 0.7), lat_span * pad_fraction)
        xlim = (min_lon - pad_lon, max_lon + pad_lon)
        ylim = (min_lat - pad_lat, max_lat + pad_lat)

        self._draw_study_box_geography_context(ax, xlim=xlim, ylim=ylim)
        ax.text(
            xlim[0] + 0.35,
            ylim[0] + 0.35,
            "West-coast Philippines study context",
            fontsize=8,
            color="#475569",
            ha="left",
            va="bottom",
            bbox={"boxstyle": "round,pad=0.22", "facecolor": (1, 1, 1, 0.86), "edgecolor": "#cbd5e1", "linewidth": 0.4},
            zorder=1,
        )

        for item in boxes:
            bounds = tuple(float(value) for value in item["bounds"])
            width = bounds[1] - bounds[0]
            height = bounds[3] - bounds[2]
            rect = Rectangle(
                (bounds[0], bounds[2]),
                width,
                height,
                linewidth=float(item.get("line_width", 2.0)),
                edgecolor=str(item.get("color") or "#0f172a"),
                facecolor=matplotlib.colors.to_rgba(str(item.get("color") or "#0f172a"), alpha=float(item.get("fill_alpha", 0.08))),
                zorder=3,
            )
            ax.add_patch(rect)
            label_box = {
                "boxstyle": "round,pad=0.18",
                "facecolor": "#ffffff",
                "edgecolor": str(item.get("color") or "#0f172a"),
                "linewidth": 1.15,
            }
            label_text = f"{item['tag']}. {item['short_label']}"
            if item.get("arrow_to_center"):
                center = (bounds[0] + width / 2.0, bounds[2] + height / 2.0)
                ax.annotate(
                    label_text,
                    xy=center,
                    xytext=item["text_anchor"],
                    textcoords="data",
                    fontsize=8,
                    fontweight="bold",
                    color=str(item.get("color") or "#0f172a"),
                    ha="left",
                    va="bottom",
                    bbox=label_box,
                    arrowprops={"arrowstyle": "->", "color": str(item.get("color") or "#0f172a"), "linewidth": 1.1},
                    zorder=5,
                )
            else:
                ax.text(
                    float(item["text_anchor"][0]),
                    float(item["text_anchor"][1]),
                    label_text,
                    fontsize=8,
                    fontweight="bold",
                    color=str(item.get("color") or "#0f172a"),
                    ha="left",
                    va="bottom",
                    bbox=label_box,
                    zorder=5,
                )
            ax.text(
                bounds[0] + (width * 0.50),
                bounds[2] + (height * 0.50),
                str(item["tag"]),
                fontsize=12,
                fontweight="bold",
                color=str(item.get("color") or "#0f172a"),
                ha="center",
                va="center",
                alpha=0.90,
                zorder=4,
            )

        ax.set_xlim(*xlim)
        ax.set_ylim(*ylim)
        ax.set_title(str(spec["panel_title"]), fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), loc="left")
        ax.set_xlabel("Longitude (degrees east)")
        ax.set_ylabel("Latitude (degrees north)")
        ax.grid(True, linestyle="--", linewidth=0.35, alpha=0.40, color=(self.style.get("layout") or {}).get("grid_color") or "#cbd5e1")
        ax.set_aspect(self._geographic_aspect((ylim[0] + ylim[1]) / 2.0), adjustable="box")
        for spine in ax.spines.values():
            spine.set_color("#94a3b8")
            spine.set_linewidth(0.8)

        source_paths = [str(item) for item in spec.get("source_paths", []) if str(item).strip()]
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
        if renderer == "study_boxes":
            return self._render_study_boxes_panel(ax, spec)
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
        direct_filename = str(spec.get("direct_output_filename") or "").strip()
        if direct_filename:
            return self.output_dir / direct_filename
        filename = build_publication_figure_filename(
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
        path = self.output_dir / filename
        if os.name == "nt" and len(str(path.resolve())) >= 240:
            path = self.output_dir / _windows_safe_publication_filename(spec, filename)
        return path

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
        record_context = {
            "case_id": str(spec["case_id"]),
            "phase_or_track": str(spec["phase_or_track"]),
            "run_type": str(spec["run_type"]),
            "figure_slug": str(spec.get("figure_slug") or ""),
            "figure_id": path.stem,
            "relative_path": relative_path,
            "notes": str(spec.get("notes") or ""),
            "short_plain_language_interpretation": str(spec["short_plain_language_interpretation"]),
            "legacy_debug_only": bool(spec.get("legacy_debug_only")),
        }
        status_override = str(spec.get("status_key_override") or "").strip()
        if status_override:
            status = artifact_status_columns_for_key(status_override, record_context)
        else:
            status = artifact_status_columns(record_context)
        governance = publication_figure_governance_columns({**record_context, **status, **spec})
        record = PublicationFigureRecord(
            figure_id=path.stem,
            display_title=str(spec.get("figure_title") or ""),
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
            surface_key=str(status["surface_key"]),
            surface_label=str(status["surface_label"]),
            surface_description=str(status["surface_description"]),
            surface_home_visible=bool(status["surface_home_visible"]),
            surface_panel_visible=bool(status["surface_panel_visible"]),
            surface_archive_visible=bool(status["surface_archive_visible"]),
            surface_advanced_visible=bool(status["surface_advanced_visible"]),
            surface_recommended_visible=bool(status["surface_recommended_visible"]),
            thesis_surface=bool(governance["thesis_surface"]),
            archive_only=bool(governance["archive_only"]),
            legacy_support=bool(governance["legacy_support"]),
            comparator_support=bool(governance["comparator_support"]),
            display_order=int(governance["display_order"]),
            page_target=str(governance["page_target"]),
            study_box_id=str(governance["study_box_id"]),
            study_box_numbers=str(governance["study_box_numbers"]),
            study_box_label=str(governance["study_box_label"]),
            recommended_scope=str(governance["recommended_scope"]),
        )
        self.figure_records.append(record)
        return record

    def _image_pixel_size(self, path: Path) -> tuple[int, int]:
        image = plt.imread(_path_for_io(path))
        return int(image.shape[1]), int(image.shape[0])

    def _save_external_image_figure(self, spec: dict[str, Any]) -> PublicationFigureRecord:
        source_path = self._resolve(str(spec["source_image_path"]))
        if not source_path.exists():
            raise FileNotFoundError(f"External image source missing for publication figure: {source_path}")
        output_path = self._figure_path(spec)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(_path_for_io(source_path), _path_for_io(output_path))
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
        is_study_box = str(spec["renderer"]) == "study_boxes"
        minimal_map_layout = bool(spec.get("minimal_map_layout")) and is_spatial
        fig = plt.figure(figsize=self._single_size(), dpi=self._dpi(), facecolor=(self.style.get("layout") or {}).get("figure_facecolor") or "#ffffff")
        if minimal_map_layout:
            grid = fig.add_gridspec(
                2,
                2,
                width_ratios=[4.35, 1.05],
                height_ratios=[1.0, 0.72],
                left=0.045,
                right=0.985,
                top=0.90,
                bottom=0.07,
                wspace=0.14,
                hspace=0.18,
            )
            main_ax = fig.add_subplot(grid[:, 0])
            locator_ax = fig.add_subplot(grid[0, 1])
            legend_ax = fig.add_subplot(grid[1, 1])
            panel_title = spec.get("figure_title")
            if "map_panel_title" in spec:
                panel_title = spec.get("map_panel_title") or ""
            render_info = self._render_panel(main_ax, dict(spec, panel_title=panel_title))
            self._add_locator(
                locator_ax,
                str(spec["case_id"]),
                render_info.get("crop_bounds"),
                str(render_info.get("target_crs") or self._case_context(str(spec["case_id"]))["projected_crs"]),
                compact=True,
            )
            self._add_legend(
                legend_ax,
                [str(item) for item in spec.get("legend_keys", [])],
                label_overrides=self._legend_label_overrides(spec),
            )
        elif is_spatial:
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
            self._add_legend(
                legend_ax,
                [str(item) for item in spec.get("legend_keys", [])],
                label_overrides=self._legend_label_overrides(spec),
            )
            self._add_note_box(
                note_ax,
                str(spec.get("note_box_title") or "How to read this figure"),
                [str(item) for item in spec.get("note_lines", [])],
                title_y=0.98,
                body_y=0.82,
                box_pad=0.34,
                minimum_title_gap_px=18.0,
            )
        else:
            width_ratios = [3.2, 1.45] if is_study_box else [3.3, 1.4]
            height_ratios = [0.24, 0.76] if is_study_box else [1.0, 1.0]
            top = 0.905 if is_study_box else 0.90
            hspace = 0.10 if is_study_box else 0.24
            grid = fig.add_gridspec(
                2,
                2,
                width_ratios=width_ratios,
                height_ratios=height_ratios,
                left=0.05,
                right=0.98,
                top=top,
                bottom=0.07,
                wspace=0.18,
                hspace=hspace,
            )
            main_ax = fig.add_subplot(grid[:, 0])
            info_ax = fig.add_subplot(grid[0, 1])
            note_ax = fig.add_subplot(grid[1, 1])
            panel_title = "" if is_study_box else spec.get("figure_title")
            if "map_panel_title" in spec:
                panel_title = spec.get("map_panel_title") or ""
            render_info = self._render_panel(main_ax, dict(spec, panel_title=panel_title))
            subtitle_box_lines = [
                str(item)
                for item in (
                    spec.get("subtitle_box_lines")
                    or [
                        str(spec.get("subtitle") or ""),
                        f"Case: {spec['case_id']}",
                        f"Track: {spec['phase_or_track']}",
                    ]
                )
                if str(item).strip()
            ]
            self._add_note_box(
                info_ax,
                str(spec.get("subtitle_box_title") or "Context"),
                subtitle_box_lines,
                title_y=0.98,
                body_y=0.79 if is_study_box else 0.82,
                box_pad=0.38 if is_study_box else 0.34,
                minimum_title_gap_px=22.0 if is_study_box else 18.0,
            )
            self._add_note_box(
                note_ax,
                str(spec.get("note_box_title") or "How to read this figure"),
                [str(item) for item in spec.get("note_lines", [])],
                title_y=0.98,
                body_y=0.79 if is_study_box else 0.82,
                box_pad=0.38 if is_study_box else 0.34,
                minimum_title_gap_px=22.0 if is_study_box else 18.0,
            )
        subtitle_text = str(spec["subtitle"])
        if is_study_box:
            subtitle_text = textwrap.fill(subtitle_text, width=116)
        fig.suptitle(str(spec["figure_title"]), x=0.05, y=0.965, ha="left", fontsize=float((self.style.get("typography") or {}).get("title_size") or 19), fontweight="bold")
        fig.text(0.05, 0.932, subtitle_text, ha="left", va="top", fontsize=float((self.style.get("typography") or {}).get("subtitle_size") or 10), color="#475569")
        pixel_width, pixel_height = self._figure_pixel_size(fig)
        fig.savefig(_path_for_io(output_path), dpi=self._dpi())
        plt.close(fig)
        return self._register_figure(
            spec,
            output_path,
            [str(item) for item in render_info.get("source_paths", [])],
            pixel_width=pixel_width,
            pixel_height=pixel_height,
        )

    def _board_grid_shape(self, panel_count: int) -> tuple[int, int]:
        if panel_count <= 3:
            return 1, panel_count
        if panel_count == 4:
            return 2, 2
        return int(np.ceil(panel_count / 3.0)), min(3, panel_count)

    def _render_board_panel(
        self,
        ax: plt.Axes,
        panel: dict[str, Any],
        saved_figures: dict[str, PublicationFigureRecord],
    ) -> tuple[dict[str, Any], list[str], bool]:
        source_spec_id = str(panel.get("source_spec_id") or "").strip()
        source_spec = self._spec_lookup.get(source_spec_id)
        panel_title = str(panel.get("panel_title") or (source_spec or {}).get("figure_title") or "").strip()
        if source_spec is not None and str(source_spec.get("renderer") or "") != "external_image":
            render_info = self._render_panel(ax, dict(source_spec, panel_title=panel_title))
            return render_info, self._board_panel_legend_keys(source_spec), True
        ax.axis("off")
        if source_spec_id:
            source_record = saved_figures.get(source_spec_id)
            if source_record is None:
                ax.text(0.5, 0.5, f"Missing figure\n{source_spec_id}", ha="center", va="center", fontsize=11)
                return {"source_paths": [], "crop_bounds": None, "target_crs": None}, [], False
            image = plt.imread(_path_for_io(source_record.file_path))
            ax.imshow(image)
            ax.set_title(panel_title, loc="left", fontsize=float((self.style.get("typography") or {}).get("panel_title_size") or 11), fontweight="bold", pad=10)
            return {"source_paths": [source_record.relative_path], "crop_bounds": None, "target_crs": None}, [], False
        self._add_note_box(
            ax,
            panel_title or "Context",
            [str(panel.get("text") or "")],
            wrap_width=46,
        )
        return {"source_paths": [], "crop_bounds": None, "target_crs": None}, [], False

    def _append_board_layout_audit(
        self,
        spec: dict[str, Any],
        *,
        output_path: Path,
        layout_mode: str,
        rows: int,
        cols: int,
        panel_count: int,
        title_within_bounds: bool,
        subtitle_within_bounds: bool,
        guide_within_bounds: bool,
        filenames_stayed_same: bool,
    ) -> None:
        issue_types = [str(item) for item in spec.get("board_issue_types", []) if str(item).strip()]
        if not issue_types:
            issue_types = ["mixed issue"]
        self.board_layout_audit_rows.append(
            {
                "board_file": _relative_to_repo(self.repo_root, output_path),
                "board_family": FIGURE_FAMILIES[str(spec["figure_family_code"])],
                "panel_count": int(panel_count),
                "grid_structure": f"{rows}x{cols}",
                "layout_mode": layout_mode,
                "issue_types_found": " | ".join(issue_types),
                "layout_fix_applied": (
                    "Direct panel rerender from stored figure specs, standardized gutters/margins, one shared board guide, "
                    "and a separate locator/legend/provenance region."
                ),
                "requested_font_family": self.font_audit.requested_font_family,
                "actual_font_resolved": self.font_audit.actual_font_family,
                "exact_arial_used": bool(self.font_audit.exact_requested_font_used),
                "fallback_needed": bool(self.font_audit.fallback_used),
                "text_shortened_or_wrapped": bool(spec.get("guide_bullets")) or bool(spec.get("caveat_line")) or bool(spec.get("provenance_line")),
                "filenames_stayed_same": bool(filenames_stayed_same),
                "title_within_bounds": bool(title_within_bounds),
                "subtitle_within_bounds": bool(subtitle_within_bounds),
                "guide_within_bounds": bool(guide_within_bounds),
            }
        )

    def _save_board_figure(self, spec: dict[str, Any], saved_figures: dict[str, PublicationFigureRecord]) -> PublicationFigureRecord:
        output_path = self._figure_path(spec)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        panels = list(spec.get("panels", []))
        panel_count = max(1, len(panels))
        rows, cols = self._board_grid_shape(panel_count)
        fig = plt.figure(figsize=self._board_size(), dpi=self._dpi(), facecolor=(self.style.get("layout") or {}).get("figure_facecolor") or "#ffffff")
        source_paths: list[str] = []
        legend_keys: list[str] = []
        crop_candidates: list[tuple[float, float, float, float] | None] = []
        target_crs = ""
        layout = self._board_layout_settings(spec, panel_count)
        layout_mode = str(layout["layout_mode"])
        compact_board_layout = bool(spec.get("compact_board_layout"))
        if layout_mode == "bottom_strip" and compact_board_layout:
            outer_grid = fig.add_gridspec(
                2,
                1,
                height_ratios=[1.0, 0.22],
                left=float(layout["outer_left"]),
                right=float(layout["outer_right"]),
                top=float(layout["outer_top"]),
                bottom=float(layout["outer_bottom"]),
                hspace=float(layout["outer_hspace"]),
            )
            panel_grid = outer_grid[0, 0].subgridspec(
                rows,
                cols,
                wspace=float(layout["panel_grid_wspace"]),
                hspace=float(layout["panel_grid_hspace"]),
            )
            info_grid = outer_grid[1, 0].subgridspec(1, 3, width_ratios=[0.82, 1.12, 2.06], wspace=float(layout["info_grid_wspace"]))
            locator_ax = fig.add_subplot(info_grid[0, 0])
            legend_ax = fig.add_subplot(info_grid[0, 1])
            guide_ax = fig.add_subplot(info_grid[0, 2])
            note_ax = None
        elif layout_mode == "bottom_strip":
            outer_grid = fig.add_gridspec(
                2,
                1,
                height_ratios=[1.0, 0.32],
                left=float(layout["outer_left"]),
                right=float(layout["outer_right"]),
                top=float(layout["outer_top"]),
                bottom=float(layout["outer_bottom"]),
                hspace=float(layout["outer_hspace"]),
            )
            panel_grid = outer_grid[0, 0].subgridspec(
                rows,
                cols,
                wspace=float(layout["panel_grid_wspace"]),
                hspace=float(layout["panel_grid_hspace"]),
            )
            info_grid = outer_grid[1, 0].subgridspec(1, 3, width_ratios=[0.95, 1.55, 1.15], wspace=float(layout["info_grid_wspace"]))
            locator_stack = info_grid[0, 0].subgridspec(2, 1, height_ratios=[1.0, 0.88], hspace=float(layout["locator_stack_hspace"]))
            locator_ax = fig.add_subplot(locator_stack[0, 0])
            legend_ax = fig.add_subplot(locator_stack[1, 0])
            guide_ax = fig.add_subplot(info_grid[0, 1])
            note_ax = fig.add_subplot(info_grid[0, 2])
        elif compact_board_layout:
            outer_grid = fig.add_gridspec(
                1,
                2,
                width_ratios=[4.9, 1.55],
                left=float(layout["outer_left"]),
                right=float(layout["outer_right"]),
                top=float(layout["outer_top"]),
                bottom=float(layout["outer_bottom"]),
                wspace=float(layout["outer_wspace"]),
            )
            panel_grid = outer_grid[0, 0].subgridspec(
                rows,
                cols,
                wspace=float(layout["panel_grid_wspace"]),
                hspace=float(layout["panel_grid_hspace"]),
            )
            side_grid = outer_grid[0, 1].subgridspec(3, 1, height_ratios=[0.94, 0.78, 1.18], hspace=float(layout["side_grid_hspace"]))
            locator_ax = fig.add_subplot(side_grid[0, 0])
            legend_ax = fig.add_subplot(side_grid[1, 0])
            guide_ax = fig.add_subplot(side_grid[2, 0])
            note_ax = None
        else:
            outer_grid = fig.add_gridspec(
                1,
                2,
                width_ratios=[4.65, 1.85],
                left=float(layout["outer_left"]),
                right=float(layout["outer_right"]),
                top=float(layout["outer_top"]),
                bottom=float(layout["outer_bottom"]),
                wspace=float(layout["outer_wspace"]),
            )
            panel_grid = outer_grid[0, 0].subgridspec(
                rows,
                cols,
                wspace=float(layout["panel_grid_wspace"]),
                hspace=float(layout["panel_grid_hspace"]),
            )
            side_grid = outer_grid[0, 1].subgridspec(4, 1, height_ratios=[0.96, 0.82, 1.34, 1.18], hspace=float(layout["side_grid_hspace"]))
            locator_ax = fig.add_subplot(side_grid[0, 0])
            legend_ax = fig.add_subplot(side_grid[1, 0])
            guide_ax = fig.add_subplot(side_grid[2, 0])
            note_ax = fig.add_subplot(side_grid[3, 0])

        for idx, panel in enumerate(panels):
            row_idx = idx // cols
            col_idx = idx % cols
            ax = fig.add_subplot(panel_grid[row_idx, col_idx])
            render_info, panel_legend_keys, rendered_directly = self._render_board_panel(ax, panel, saved_figures)
            source_paths.extend(str(item) for item in render_info.get("source_paths", []) if str(item).strip())
            legend_keys.extend(panel_legend_keys)
            crop_candidates.append(render_info.get("crop_bounds"))
            target_crs = target_crs or str(render_info.get("target_crs") or "")
            if rendered_directly and rows > 1 and row_idx < rows - 1:
                ax.set_xlabel("")
                ax.tick_params(labelbottom=False)
            if rendered_directly and cols > 1 and col_idx > 0:
                ax.set_ylabel("")
                ax.tick_params(labelleft=False)

        text_blocks = self._board_text_blocks(spec)
        locator_bounds = self._union_bounds(crop_candidates)
        if target_crs:
            self._add_locator(locator_ax, str(spec["case_id"]), locator_bounds, target_crs, compact=True)
        else:
            locator_ax.axis("off")
        legend_artist = None
        legend_label_overrides = self._board_legend_label_overrides(spec)
        legend_keys = list(dict.fromkeys(key for key in legend_keys if key))
        if legend_keys:
            legend_artist = self._add_legend(
                legend_ax,
                legend_keys,
                compact=True,
                label_overrides=legend_label_overrides,
            )
        else:
            legend_ax.axis("off")
        _, guide_body_artist = self._add_note_box(
            guide_ax,
            str(text_blocks["guide_heading"]),
            list(text_blocks["guide_bullets"]),
            wrap_width=self._axis_wrap_width(
                guide_ax,
                fontsize=float((self.style.get("typography") or {}).get("note_size") or 8),
                max_chars=int(layout["guide_wrap_max_chars"]),
            ),
            bullet_lines=bool(spec.get("guide_bullet_lines", True)),
            title_y=float(layout["guide_title_y"]),
            body_y=float(layout["guide_body_y"]),
            box_pad=float(layout["guide_box_pad"]),
            minimum_title_gap_px=float(layout["guide_minimum_title_gap_px"]),
        )
        note_lines = [
            line
            for line in [
                str(text_blocks.get("caveat_line") or "").strip(),
                str(text_blocks.get("provenance_line") or "").strip(),
                *[str(item).strip() for item in text_blocks.get("extra_lines", [])[:2]],
            ]
            if line
        ]
        if note_ax is None:
            note_body_artist = None
        elif note_lines:
            _, note_body_artist = self._add_note_box(
                note_ax,
                "Caveat and Provenance",
                note_lines,
                wrap_width=self._axis_wrap_width(
                    note_ax,
                    fontsize=float((self.style.get("typography") or {}).get("note_size") or 8),
                    max_chars=int(layout["note_wrap_max_chars"]),
                ),
                title_y=float(layout["note_title_y"]),
                body_y=float(layout["note_body_y"]),
                box_pad=float(layout["note_box_pad"]),
                minimum_title_gap_px=float(layout["note_minimum_title_gap_px"]),
            )
        else:
            note_ax.axis("off")
            note_body_artist = None
        title_artist = fig.suptitle(
            str(spec["figure_title"]),
            x=0.04,
            y=float(layout["title_y"]),
            ha="left",
            fontsize=float((self.style.get("typography") or {}).get("title_size") or 19),
            fontweight="bold",
        )
        subtitle_text = textwrap.fill(
            str(spec["subtitle"]),
            width=int(layout["subtitle_wrap_width"]),
            break_long_words=False,
            break_on_hyphens=False,
        )
        subtitle_artist = fig.text(
            0.04,
            float(layout["subtitle_y"]),
            subtitle_text,
            ha="left",
            va="top",
            fontsize=float((self.style.get("typography") or {}).get("subtitle_size") or 10),
            color="#475569",
        )
        fig.canvas.draw()
        title_within_bounds = self._artist_within_bbox(title_artist, fig.bbox)
        subtitle_within_bounds = self._artist_within_bbox(subtitle_artist, fig.bbox)
        guide_within_bounds = self._artist_within_bbox(guide_body_artist, guide_ax.bbox)
        if note_body_artist is not None:
            guide_within_bounds = guide_within_bounds and self._artist_within_bbox(note_body_artist, note_ax.bbox)
        pixel_width, pixel_height = self._figure_pixel_size(fig)
        fig.savefig(_path_for_io(output_path), dpi=self._dpi())
        plt.close(fig)
        self._append_board_layout_audit(
            spec,
            output_path=output_path,
            layout_mode=layout_mode,
            rows=rows,
            cols=cols,
            panel_count=panel_count,
            title_within_bounds=title_within_bounds,
            subtitle_within_bounds=subtitle_within_bounds,
            guide_within_bounds=guide_within_bounds,
            filenames_stayed_same=True,
        )
        return self._register_figure(
            spec,
            output_path,
            list(dict.fromkeys(source_paths)),
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
        is_current_mindoro_march13_14 = (
            case_id == "CASE_MINDORO_RETRO_2023"
            and phase_or_track in {"phase3b_reinit_primary", "phase3a_reinit_crossmodel"}
        )
        if is_current_mindoro_march13_14:
            legend_keys = [key for key in legend_keys if key != "source_point"]
            show_source = False
            include_source_in_crop = False
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
            "minimal_map_layout": is_current_mindoro_march13_14,
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

    def _study_box_spec(
        self,
        *,
        spec_id: str,
        figure_slug: str,
        figure_title: str,
        subtitle: str,
        interpretation: str,
        notes: str,
        note_lines: list[str],
        subtitle_box_title: str,
        subtitle_box_lines: list[str],
        study_boxes: list[dict[str, Any]],
        source_paths: list[str],
        run_type: str = "single_reference_map",
        model_names: str = "study_boxes",
        box_padding_fraction: float = 0.08,
        minimum_pad_lon: float = 0.9,
        minimum_pad_lat: float = 0.7,
        recommended_for_main_defense: bool = False,
        recommended_for_paper: bool = True,
    ) -> dict[str, Any]:
        return {
            "spec_id": spec_id,
            "renderer": "study_boxes",
            "figure_family_code": "L",
            "case_id": "THESIS_STUDY_CONTEXT",
            "phase_or_track": "phase1_study_context",
            "date_token": "shared_thesis_context",
            "model_names": model_names,
            "run_type": run_type,
            "scenario_id": "",
            "view_type": "single",
            "variant": "paper",
            "figure_slug": figure_slug,
            "figure_title": figure_title,
            "subtitle": subtitle,
            "legend_keys": [],
            "note_lines": note_lines,
            "note_box_title": "How to read this figure",
            "subtitle_box_title": subtitle_box_title,
            "subtitle_box_lines": subtitle_box_lines,
            "short_plain_language_interpretation": interpretation,
            "recommended_for_main_defense": recommended_for_main_defense,
            "recommended_for_paper": recommended_for_paper,
            "notes": notes,
            "source_paths": source_paths,
            "study_boxes": study_boxes,
            "status_key_override": "thesis_study_box_reference",
            "box_padding_fraction": box_padding_fraction,
            "minimum_pad_lon": minimum_pad_lon,
            "minimum_pad_lat": minimum_pad_lat,
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
        guide_heading: str = "How to read this board",
        guide_bullets: list[str] | None = None,
        caveat_line: str = "",
        provenance_line: str = "",
        board_issue_types: list[str] | None = None,
        board_layout_overrides: dict[str, Any] | None = None,
        compact_board_layout: bool = False,
        guide_bullet_lines: bool = True,
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
            "guide_heading": guide_heading,
            "guide_bullets": list(guide_bullets or []),
            "caveat_line": caveat_line,
            "provenance_line": provenance_line,
            "board_issue_types": list(board_issue_types or []),
            "board_layout_overrides": dict(board_layout_overrides or {}),
            "compact_board_layout": bool(compact_board_layout),
            "guide_bullet_lines": bool(guide_bullet_lines),
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

    def _format_fss_summary(self, row: pd.Series | dict[str, Any] | None, label: str) -> str:
        if row is None:
            return f"{label}: stored mean FSS unavailable."
        mean_value = self._row_mean_fss(row)
        if mean_value is None:
            return f"{label}: stored mean FSS unavailable."
        return f"{label}: stored mean FSS {self._format_score_value(mean_value)}."

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

    def _dwh_model_row(self, model_key: str, date_token: str) -> pd.Series | None:
        summary = self.dwh_all_results
        if summary.empty:
            return None
        normalized_date = self._normalize_date_token(date_token)
        pair_role = "event_corridor" if "/" in normalized_date else "per_date"
        filters = pd.Series(True, index=summary.index)
        if "pair_role" in summary.columns:
            filters &= summary["pair_role"].astype(str) == pair_role
        if "pairing_date_utc" in summary.columns:
            filters &= summary["pairing_date_utc"].map(self._normalize_date_token) == normalized_date
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
        if "pair_role" in summary.columns:
            fallback = summary.loc[summary["pair_role"].astype(str) == pair_role]
            if not fallback.empty:
                return fallback.iloc[0]
        return summary.iloc[0]

    def _dwh_event_model_row(self, model_key: str) -> pd.Series | None:
        return self._dwh_model_row(model_key, "2010-05-21_to_2010-05-23")

    def _mindoro_primary_context_lines(self) -> list[str]:
        row = self._mindoro_primary_row()
        if row is None:
            return ["The March 13 -> March 14 reinit summary CSV was not available, so this figure should be read visually."]
        official_recipe = str(self.phase1_baseline_selection.get("selected_recipe", "") or "").strip()
        historical_winner = str(
            self.phase1_baseline_selection.get("historical_four_recipe_winner", "")
            or official_recipe
        ).strip()
        lines = [
            f"{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE} is now carried by the March 13 -> March 14 promoted Mindoro validation pair, seeded from the March 13 NOAA polygon and scored against the March 14 NOAA target.",
            MINDORO_OBSERVATION_INDEPENDENCE_NOTE,
            (
                f"The separate focused 2016-2023 Mindoro drifter rerun found {historical_winner} as the historical four-recipe winner, and official B1 now uses {official_recipe} from that focused lane."
                if official_recipe
                else "The separate focused 2016-2023 Mindoro drifter provenance artifact was unavailable."
            ),
            f"The promoted OpenDrift R1 previous reinit p50 row reaches FSS beyond zero at 3/5/10 km with {int(row.get('forecast_nonzero_cells', 0))} forecast cells against {int(row.get('obs_nonzero_cells', 0))} observed cells.",
        ]
        threshold_line = self._mindoro_reinit_threshold_equivalence_line()
        if threshold_line:
            lines.append(threshold_line)
        return lines

    def _mindoro_strict_context_lines(self) -> list[str]:
        row = self._mindoro_strict_row()
        if row is None:
            return ["The strict March 6 summary CSV was not available, so this figure should be read visually."]
        return [
            f"March 6 remains the legacy sparse-reference honesty figure: the observed footprint is only {int(row.get('obs_nonzero_cells', 0))} non-zero cells.",
            f"The official p50 product is intentionally sparse here, with {int(row.get('forecast_nonzero_cells', 0))} forecast cells in this strict slice.",
            "March 6 stays in the package as methods-honesty context rather than the promoted primary validation row.",
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
        lines = [
            "This cross-model lane reuses the completed March 13 -> March 14 reinit outputs and adds one deterministic PyGNOME surrogate seeded from the same March 13 NOAA polygon.",
            f"{top['model_name']} currently ranks first in the promoted local cross-model bundle under the current case definition.",
            "PyGNOME remains comparator-only here; March 13 and March 14 are independent NOAA-published day-specific observation products.",
        ]
        threshold_line = self._mindoro_reinit_threshold_equivalence_line()
        if threshold_line:
            lines.append(threshold_line)
        return lines

    def _mindoro_comparison_context_lines(self) -> list[str]:
        lines = [
            "This comparison keeps the promoted OpenDrift reinit and PyGNOME comparator views side by side without treating PyGNOME as truth.",
            "In this lane, the score lines come from the March 14 NOAA target while the seed geometry comes from the March 13 NOAA polygon.",
            MINDORO_OBSERVATION_INDEPENDENCE_NOTE,
        ]
        threshold_line = self._mindoro_reinit_threshold_equivalence_line()
        if threshold_line:
            lines.append(threshold_line)
        return lines

    def _dwh_observation_context_lines(self) -> list[str]:
        return [
            "These figures isolate the public observation-derived DWH daily masks and the event-corridor union before any model panel is introduced.",
            "Read every DWH truth panel as date-composite context only: no exact sub-daily acquisition time is implied.",
        ]

    def _dwh_deterministic_context_lines(self) -> list[str]:
        return [
            "DWH is a separate external transfer-validation/support case; Mindoro remains the main Philippine thesis case.",
            "The DWH forcing rule is readiness-gated rather than Phase 1 drifter-selected baseline logic, and the current stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
        ]

    def _dwh_ensemble_context_lines(self) -> list[str]:
        return [
            "The ensemble lane reuses the same frozen DWH truth masks and forcing stack as the deterministic baseline.",
            "The official public observation-derived DWH date-composite masks remain the scoring reference for every C2 panel.",
            "Within C2, mask_p50 is the preferred probabilistic extension while mask_p90 remains support/comparison only.",
        ]

    def _dwh_model_context_lines(self) -> list[str]:
        return [
            "The official public observation-derived DWH date-composite masks remain the scoring reference throughout these figures; PyGNOME is comparator-only.",
            "Each score line belongs to the matching model panel only; the observed corridor panel is included for visual reference.",
        ]

    def _dwh_trajectory_context_lines(self) -> list[str]:
        return [
            "These trajectory views are appendix/support material for the separate DWH transfer-validation case rather than the main thesis case.",
            "Scored footprint figures handle validation overlap; these trajectory views focus on transport shape.",
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

    def _dwh_model_score_line(self, model_key: str, date_token: str, label: str = "") -> str:
        return self._format_fss_line(self._dwh_model_row(model_key, date_token), label)

    def _dwh_event_score_line(self, model_key: str, label: str = "") -> str:
        return self._dwh_model_score_line(model_key, "2010-05-21_to_2010-05-23", label)

    def _mindoro_primary_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_primary_context_lines(), list(score_lines))

    def _mindoro_strict_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_strict_context_lines(), list(score_lines))

    def _mindoro_crossmodel_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_crossmodel_context_lines(), list(score_lines))

    def _mindoro_comparison_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._mindoro_comparison_context_lines(), list(score_lines))

    def _mindoro_recipe_provenance_line(self) -> str:
        recipe = str(self.phase1_baseline_selection.get("selected_recipe", "") or "").strip()
        if not recipe:
            return "Provenance: focused Mindoro drifter recipe confirmation was unavailable."
        return f"Provenance: focused 2016-2023 Mindoro drifter rerun promoted `{recipe}` into the active official B1 lane."

    def _mindoro_primary_branch_probability_path(self, branch_id: str) -> Path | None:
        row = self._mindoro_primary_branch_row(branch_id)
        if row is None:
            return None
        candidates = [
            row.get("probability_path"),
            row.get("march14_probability_path"),
            row.get("probability_path_survival"),
        ]
        for value in candidates:
            text = str(value or "").strip()
            if text and text.lower() not in {"nan", "none", "null"}:
                return self._resolve(text)
        return None

    def _mindoro_reinit_threshold_equivalence_line(self, branch_id: str = "R1_previous") -> str:
        probability_path = self._mindoro_primary_branch_probability_path(branch_id)
        if probability_path is None:
            return ""
        info = self._load_raster_mask(probability_path)
        if info is None:
            return ""
        probability = np.asarray(info["array"], dtype=float)
        finite = np.isfinite(probability)
        if not finite.any():
            return ""
        nonzero = finite & (probability > 0)
        p50_mask = finite & (probability >= 0.5)
        p90_mask = finite & (probability >= 0.9)
        p50_cells = int(np.count_nonzero(p50_mask))
        p90_cells = int(np.count_nonzero(p90_mask))
        if np.array_equal(p50_mask, p90_mask):
            if p50_cells == 0:
                return "Derived March 14 p90 is also empty in this stored reinit output."
            min_nonzero = self._format_score_value(float(np.min(probability[nonzero])))
            max_nonzero = self._format_score_value(float(np.max(probability[nonzero])))
            return (
                "Derived March 14 p90 equals p50 for the promoted R1 previous reinit branch: "
                f"all {p50_cells} nonzero cells are already {min_nonzero}-{max_nonzero} probability."
            )
        if p50_cells > 0:
            return (
                "Derived March 14 p90 is smaller than p50 for the promoted R1 previous reinit branch: "
                f"{p90_cells} of {p50_cells} p50 cells remain at >=0.900 probability."
            )
        return ""

    @staticmethod
    def _mindoro_reinit_equivalent_legend_label() -> str:
        return "OpenDrift p50 footprint (= p90 here)"

    def _mindoro_primary_board_layout_fields(self) -> dict[str, Any]:
        threshold_line = self._mindoro_reinit_threshold_equivalence_line()
        guide_bullets = [
            self._format_fss_summary(self._mindoro_primary_branch_row("R1_previous"), "Promoted R1 previous reinit p50"),
        ]
        if threshold_line:
            guide_bullets.append(threshold_line)
        return {
            "compact_board_layout": True,
            "guide_heading": "Scorecard",
            "guide_bullets": guide_bullets,
            "guide_bullet_lines": False,
            "caveat_line": "",
            "provenance_line": "",
            "board_issue_types": [
                "uneven panel spacing",
                "overlapping text",
                "awkward note placement",
                "weak title hierarchy",
            ],
            "board_layout_overrides": {
                "outer_top": 0.89,
                "outer_bottom": 0.05,
                "outer_hspace": 0.08,
                "guide_wrap_max_chars": 88,
                "guide_body_y": 0.76,
            },
        }

    def _mindoro_crossmodel_board_layout_fields(self) -> dict[str, Any]:
        threshold_line = self._mindoro_reinit_threshold_equivalence_line()
        guide_bullets = [
            self._format_fss_summary(self._mindoro_crossmodel_row("r1"), "OpenDrift R1 previous reinit p50"),
            self._format_fss_summary(self._mindoro_crossmodel_row("pygnome"), "PyGNOME comparator"),
        ]
        if threshold_line:
            guide_bullets.append(threshold_line)
        return {
            "compact_board_layout": True,
            "guide_heading": "Scorecard",
            "guide_bullets": guide_bullets,
            "guide_bullet_lines": False,
            "caveat_line": "",
            "provenance_line": "",
            "board_issue_types": [
                "uneven panel spacing",
                "overlapping text",
                "awkward note placement",
                "legend clutter",
            ],
            "board_layout_overrides": {
                "outer_top": 0.89,
                "outer_bottom": 0.05,
                "outer_hspace": 0.08,
                "guide_wrap_max_chars": 88,
                "guide_body_y": 0.76,
            },
        }

    def _mindoro_observed_masks_crossmodel_board_layout_fields(self) -> dict[str, Any]:
        threshold_line = self._mindoro_reinit_threshold_equivalence_line()
        guide_bullets = [
            self._format_fss_summary(self._mindoro_crossmodel_row("r1"), "OpenDrift R1 previous reinit p50"),
            self._format_fss_summary(self._mindoro_crossmodel_row("pygnome"), "PyGNOME comparator"),
        ]
        if threshold_line:
            guide_bullets.append(threshold_line)
        return {
            "compact_board_layout": True,
            "guide_heading": "Scorecard",
            "guide_bullets": guide_bullets,
            "guide_bullet_lines": False,
            "caveat_line": "",
            "provenance_line": "",
            "board_issue_types": [
                "uneven panel spacing",
                "overlapping text",
                "awkward note placement",
                "legend clutter",
            ],
            "board_layout_overrides": {
                "outer_top": 0.82,
                "outer_bottom": 0.05,
                "outer_wspace": 0.045,
                "panel_grid_hspace": 0.24,
                "guide_wrap_max_chars": 56,
                "guide_body_y": 0.76,
                "subtitle_wrap_width": 108,
            },
        }

    def _dwh_model_board_layout_fields(self) -> dict[str, Any]:
        return {
            "guide_bullets": [
                "Read clockwise: observed corridor, deterministic OpenDrift, ensemble p50, then the PyGNOME comparator.",
                "Observed DWH masks remain truth; each model score belongs only to its matching panel.",
                self._format_fss_summary(self._dwh_event_model_row("deterministic"), "OpenDrift deterministic"),
                self._format_fss_summary(self._dwh_event_model_row("p50"), "OpenDrift ensemble p50"),
                self._format_fss_summary(self._dwh_event_model_row("pygnome"), "PyGNOME comparator"),
            ],
            "caveat_line": "Caveat: DWH observation masks are honest date-composite truth; no exact sub-daily acquisition time is implied.",
            "provenance_line": "Provenance: HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes in this external transfer-validation lane.",
            "board_issue_types": [
                "uneven panel spacing",
                "awkward reading-guide placement",
                "legend clutter",
                "weak title hierarchy",
            ],
        }

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
        subtitle = "Mindoro | 13-14 March 2023 | primary validation with independent NOAA-published day-specific observations"
        seed_mask = self._mindoro_reinit_seed_mask_path()
        target_mask = self._mindoro_reinit_target_mask_path()
        r1_row = self._mindoro_primary_branch_row("R1_previous")
        r0_row = self._mindoro_primary_branch_row("R0")
        r1_mask = str(r1_row.get("forecast_path") or r1_row.get("march14_forecast_path") or "") if r1_row is not None else ""
        r0_mask = str(r0_row.get("forecast_path") or r0_row.get("march14_forecast_path") or "") if r0_row is not None else ""
        reinit_p50_label = self._mindoro_reinit_equivalent_legend_label()
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
                    "the primary March 13 -> March 14 validation starts from a "
                    "publication-grade observation panel rather than a QA screenshot."
                ),
                notes=(
                    "Built from the stored March 13 seed mask raster and shoreline context only; "
                    "no scientific rerun was triggered, and the later "
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
                spec_id="mindoro_primary_target_mask",
                figure_family_code="A",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3b_reinit_primary",
                date_token="2023-03-14",
                model_names="observation",
                run_type="single_target_observation",
                view_type="single",
                variant="paper",
                figure_slug="march14_target_mask_on_grid",
                figure_title="Mindoro March 14 target mask on grid",
                map_panel_title="",
                subtitle=subtitle,
                interpretation=(
                    "This figure isolates the March 14 NOAA target mask so the comparator lane can show the scoring truth "
                    "without folding the seed geometry into every observation panel."
                ),
                notes=(
                    "Built from the stored March 14 observation mask only; this is the scoring truth surface for the "
                    "promoted March 13 -> March 14 validation pair."
                ),
                note_lines=self._mindoro_primary_note_lines(
                    "Dark slate shows the March 14 observation target only; March 13 is kept separate as the reinitialization seed context."
                ),
                legend_keys=["observed_mask"],
                raster_layers=[
                    {
                        "path": target_mask,
                        "legend_key": "observed_mask",
                        "alpha": 0.34,
                        "linewidth": 1.3,
                        "zorder": 5,
                    }
                ],
                show_source=False,
                include_source_in_crop=False,
            ),
            {
                **self._spatial_spec(
                    spec_id="mindoro_noaa_mar13_worldview3_support",
                    figure_family_code="A",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3b_reinit_primary",
                    date_token="2023-03-13",
                    model_names="observation",
                    run_type="support_noaa_analysis_map",
                    view_type="single",
                    variant="paper",
                    figure_slug="noaa_mar13_worldview3",
                    figure_title="Figure 4.4A. NOAA-published March 13 WorldView-3 analysis map",
                    map_panel_title="",
                    subtitle="Mindoro | 13 March 2023 | independent NOAA-published day-specific observation product",
                    interpretation="Figure 4.4A. NOAA-published March 13 WorldView-3 analysis map.",
                    notes="Support figure rendered from the stored March 13 NOAA/NESDIS public observation mask and shoreline context.",
                    note_lines=[
                        "March 13 is treated as a NOAA-published day-specific public seed observation.",
                        "Blue shows the observed spill extent used to seed the March 13 -> March 14 B1 validation.",
                    ],
                    legend_keys=["deterministic_opendrift"],
                    raster_layers=[
                        {
                            "path": seed_mask,
                            "legend_key": "deterministic_opendrift",
                            "alpha": 0.32,
                            "linewidth": 1.5,
                            "zorder": 5,
                        }
                    ],
                    show_source=False,
                    include_source_in_crop=False,
                ),
                "direct_output_filename": "figure_4_4A_noaa_mar13_worldview3.png",
                "legend_label_overrides": {"deterministic_opendrift": "March 13 observed spill extent"},
            },
            {
                **self._spatial_spec(
                    spec_id="mindoro_noaa_mar14_worldview3_support",
                    figure_family_code="A",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3b_reinit_primary",
                    date_token="2023-03-14",
                    model_names="observation",
                    run_type="support_noaa_analysis_map",
                    view_type="single",
                    variant="paper",
                    figure_slug="noaa_mar14_worldview3",
                    figure_title="Figure 4.4B. NOAA-published March 14 WorldView-3 analysis map",
                    map_panel_title="",
                    subtitle="Mindoro | 14 March 2023 | independent NOAA-published day-specific observation product",
                    interpretation="Figure 4.4B. NOAA-published March 14 WorldView-3 analysis map.",
                    notes="Support figure rendered from the stored March 14 NOAA/NESDIS public observation mask and shoreline context.",
                    note_lines=[
                        "March 14 is treated as a NOAA-published day-specific public target observation.",
                        "Dark gray shows the observed spill extent used as the B1 scoring target.",
                    ],
                    legend_keys=["observed_mask"],
                    raster_layers=[
                        {
                            "path": target_mask,
                            "legend_key": "observed_mask",
                            "alpha": 0.36,
                            "linewidth": 1.5,
                            "zorder": 5,
                        }
                    ],
                    show_source=False,
                    include_source_in_crop=False,
                ),
                "direct_output_filename": "figure_4_4B_noaa_mar14_worldview3.png",
                "legend_label_overrides": {"observed_mask": "March 14 observed spill extent"},
            },
            {
                **self._spatial_spec(
                    spec_id="mindoro_arcgis_mar13_mar14_observed_overlay",
                    figure_family_code="A",
                    case_id="CASE_MINDORO_RETRO_2023",
                    phase_or_track="phase3b_reinit_primary",
                    date_token="2023-03-13_to_2023-03-14",
                    model_names="observation",
                    run_type="support_arcgis_observed_overlay",
                    view_type="single",
                    variant="paper",
                    figure_slug="arcgis_mar13_mar14_observed_overlay",
                    figure_title="Figure 4.4C. ArcGIS overlay of March 13 and March 14 observed oil-spill extents",
                    map_panel_title="",
                    subtitle="Mindoro | 13-14 March 2023 | ArcGIS public-observation overlay",
                    interpretation="Figure 4.4C. ArcGIS overlay of March 13 and March 14 observed oil-spill extents.",
                    notes="Support figure rendered from the stored March 13 and March 14 NOAA/NESDIS public observation masks.",
                    note_lines=[
                        "March 13 is shown in blue and March 14 is shown in dark gray.",
                        "The two layers are independent NOAA-published day-specific observation products.",
                    ],
                    legend_keys=["deterministic_opendrift", "observed_mask"],
                    raster_layers=[
                        {
                            "path": seed_mask,
                            "legend_key": "deterministic_opendrift",
                            "alpha": 0.28,
                            "linewidth": 1.5,
                            "zorder": 5,
                        },
                        {
                            "path": target_mask,
                            "legend_key": "observed_mask",
                            "alpha": 0.34,
                            "linewidth": 1.5,
                            "zorder": 6,
                        },
                    ],
                    show_source=False,
                    include_source_in_crop=False,
                ),
                "direct_output_filename": "figure_4_4C_arcgis_mar13_mar14_observed_overlay.png",
                "legend_label_overrides": {
                    "deterministic_opendrift": "March 13 observed spill extent",
                    "observed_mask": "March 14 observed spill extent",
                },
            },
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
                    "explicit before any model overlay is shown for the primary validation, "
                    "using the stored on-grid rasters rather than the earlier QA composite."
                ),
                notes=(
                    "Built from the stored March 13 seed mask and March 14 observation mask only, "
                    "with the observation-independence note and the separate focused Phase 1 provenance note carried into the note box."
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
            {
                **self._spatial_spec(
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
                    "the primary March 13 -> March 14 validation, rebuilt from stored "
                    "rasters with the same publication grammar used by the DWH boards."
                ),
                notes=(
                    "Built from the stored March 14 observation mask, the stored March 13 seed mask "
                    "outline, and the stored OpenDrift R1 previous p50 raster only; the later "
                    "2016-2023 Mindoro-focused drifter rerun now supplies the active B1 recipe provenance through the promoted focused winner."
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
                "legend_label_overrides": {"ensemble_p50": reinit_p50_label},
            },
            {
                **self._spatial_spec(
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
                figure_title="Mindoro March 13 -> March 14 R0 archived baseline",
                map_panel_title="",
                subtitle=subtitle,
                interpretation=(
                    "This repo-preserved archive figure keeps the stored March 13 -> March 14 R0 "
                    "archived baseline available for provenance, audit, and reproducibility only. "
                    "It is not thesis-facing and is excluded from main-paper reporting."
                ),
                notes=(
                    "Built from the stored March 14 observation mask, the stored March 13 seed mask "
                    "outline, and the stored OpenDrift R0 p50 raster only. This figure remains "
                    "archive-only and is not part of the thesis-facing Mindoro validation row."
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
                "legend_label_overrides": {"ensemble_p50": reinit_p50_label},
            },
        ]

    def _mindoro_crossmodel_publication_specs(self) -> list[dict[str, Any]]:
        subtitle = "Mindoro | 13-14 March 2023 | cross-model comparator on the March 14 NOAA target | March 13 public seed and March 14 public target observations"
        seed_mask = self._mindoro_reinit_seed_mask_path()
        target_mask = self._mindoro_reinit_target_mask_path()
        r1_row = self._mindoro_crossmodel_row("r1")
        r0_row = self._mindoro_crossmodel_row("r0")
        pygnome_row = self._mindoro_crossmodel_row("pygnome")
        r1_mask = str(r1_row.get("forecast_path") or "") if r1_row is not None else ""
        r0_mask = str(r0_row.get("forecast_path") or "") if r0_row is not None else ""
        pygnome_mask = str(pygnome_row.get("forecast_path") or "") if pygnome_row is not None else ""
        reinit_p50_label = self._mindoro_reinit_equivalent_legend_label()
        return [
            {
                **self._spatial_spec(
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
                "legend_label_overrides": {"ensemble_p50": reinit_p50_label},
            },
            {
                **self._spatial_spec(
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
                figure_title="Mindoro March 13 -> March 14 R0 archived comparator support",
                map_panel_title="",
                subtitle=subtitle,
                interpretation=(
                    "This repo-preserved archive figure keeps the March 13 -> March 14 R0 branch "
                    "available for comparator provenance only. It is not thesis-facing and is not "
                    "part of the promoted same-case comparator story."
                ),
                notes=(
                    "Built from the stored March 14 observation mask, March 13 seed mask outline, "
                    "and the stored OpenDrift R0 p50 raster only. This figure remains archive-only "
                    "and is excluded from the thesis-facing comparator lane."
                ),
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
                "legend_label_overrides": {"ensemble_p50": reinit_p50_label},
            },
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
            {
                **self._spatial_spec(
                spec_id="mindoro_observed_masks_ensemble_pygnome_overlay",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3a_reinit_crossmodel",
                date_token="2023-03-13_to_2023-03-14",
                model_names="opendrift_vs_pygnome",
                run_type="single_comparison_overlay",
                view_type="single",
                variant="paper",
                figure_slug="mindoro_observed_masks_ensemble_pygnome_overlay",
                figure_title="Mindoro March 13-14 observed masks, ensemble forecast, and PyGNOME overlay",
                map_panel_title="",
                subtitle="Mindoro | 13-14 March 2023 | March 13 and March 14 observed extents with the promoted ensemble forecast and PyGNOME comparator",
                interpretation=(
                    "This single overlay keeps the March 13 observed extent, the March 14 observed target, the "
                    "promoted ensemble forecast, and the PyGNOME comparator visible together on one map."
                ),
                notes=(
                    "Built from the stored March 13 seed mask, March 14 target mask, promoted OpenDrift ensemble raster, "
                    "and stored PyGNOME comparator raster only."
                ),
                note_lines=self._mindoro_crossmodel_note_lines(
                    self._mindoro_crossmodel_score_line("r1", "Ensemble forecast"),
                    self._mindoro_crossmodel_score_line("pygnome", "PyGNOME forecast"),
                ),
                legend_keys=["observed_mask", "initialization_polygon", "ensemble_p50", "pygnome", "source_point"],
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
                    {
                        "path": pygnome_mask,
                        "legend_key": "pygnome",
                        "alpha": 0.24,
                        "linewidth": 1.4,
                        "zorder": 8,
                    },
                ],
                show_source=True,
                include_source_in_crop=False,
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                ),
                "legend_label_overrides": {
                    "observed_mask": "March 14 observed spill extent",
                    "initialization_polygon": "March 13 observed spill extent",
                    "ensemble_p50": "Ensemble forecast",
                    "pygnome": "PyGNOME forecast",
                },
            },
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
        primary_subtitle = "Mindoro | 13-14 March 2023 | primary validation with independent NOAA-published day-specific observations"
        crossmodel_subtitle = (
            "Mindoro | 13-14 March 2023 | promoted cross-model comparator on the March 14 NOAA target | independent NOAA-published day-specific observations"
        )
        observed_masks_crossmodel_subtitle = (
            "Mindoro | 13-14 March 2023 | observed seed/target masks plus OpenDrift ensemble and PyGNOME comparator | independent NOAA-published day-specific observations"
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
                    "This is now the main Mindoro presentation board for observation-based spatial validation "
                    "using public Mindoro spill extents because it centers the promoted March 13 -> March 14 validation "
                    "pair, the thesis-facing R1 row, and the independent NOAA-published day-specific observation products without rewriting the stored run provenance."
                ),
                notes="Board assembled from publication-grade March 13 -> March 14 singles rebuilt from stored rasters and vectors only.",
                note_lines=self._mindoro_primary_note_lines(
                    self._mindoro_primary_branch_score_line("R1_previous", "OpenDrift R1 previous reinit p50"),
                ),
                **self._mindoro_primary_board_layout_fields(),
                panels=[
                    {"panel_title": "March 13 seed vs March 14 target", "source_spec_id": "mindoro_primary_seed_vs_target"},
                    {"panel_title": "Promoted R1 previous reinit overlay", "source_spec_id": "mindoro_primary_r1_overlay"},
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
                interpretation="This board answers the cross-model question on the promoted March 14 target while keeping PyGNOME comparator-only and preserving March 13 as the public seed observation.",
                notes="Board assembled from publication-grade March 13 -> March 14 singles rebuilt from stored rasters only; PyGNOME remains comparator-only.",
                note_lines=self._mindoro_comparison_note_lines(
                    self._mindoro_crossmodel_score_line("r1", "OpenDrift R1 previous reinit p50"),
                    self._mindoro_crossmodel_score_line("pygnome", "PyGNOME comparator"),
                ),
                **self._mindoro_crossmodel_board_layout_fields(),
                panels=[
                    {"panel_title": "March 14 observation reference context", "source_spec_id": "mindoro_primary_seed_vs_target"},
                    {"panel_title": "OpenDrift R1 previous reinit p50", "source_spec_id": "mindoro_crossmodel_r1_overlay"},
                    {"panel_title": "PyGNOME deterministic comparator", "source_spec_id": "mindoro_crossmodel_pygnome_overlay"},
                ],
                recommended_for_main_defense=True,
            ),
            self._board_spec(
                spec_id="mindoro_observed_masks_ensemble_pygnome_board",
                figure_family_code="B",
                case_id="CASE_MINDORO_RETRO_2023",
                phase_or_track="phase3a_reinit_crossmodel",
                date_token="2023-03-13_to_2023-03-14",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="mindoro_observed_masks_ensemble_pygnome_board",
                figure_title="Mindoro March 13 -> March 14 observed masks and comparator board",
                subtitle=observed_masks_crossmodel_subtitle,
                interpretation=(
                    "This board keeps the March 13 seed mask, the March 14 target mask, the promoted OpenDrift ensemble "
                    "forecast, and the PyGNOME comparator visible together without treating PyGNOME as truth."
                ),
                notes=(
                    "Board assembled from publication-grade observation, OpenDrift, and PyGNOME singles rebuilt from stored "
                    "rasters only; March 14 remains the scoring truth and PyGNOME remains comparator-only."
                ),
                note_lines=self._mindoro_comparison_note_lines(
                    self._mindoro_crossmodel_score_line("r1", "OpenDrift R1 previous reinit p50"),
                    self._mindoro_crossmodel_score_line("pygnome", "PyGNOME comparator"),
                ),
                **self._mindoro_observed_masks_crossmodel_board_layout_fields(),
                panels=[
                    {"panel_title": "March 13 seed mask on grid", "source_spec_id": "mindoro_primary_seed_mask"},
                    {"panel_title": "March 14 target mask on grid", "source_spec_id": "mindoro_primary_target_mask"},
                    {"panel_title": "OpenDrift R1 previous reinit p50", "source_spec_id": "mindoro_crossmodel_r1_overlay"},
                    {"panel_title": "PyGNOME deterministic comparator", "source_spec_id": "mindoro_crossmodel_pygnome_overlay"},
                ],
                recommended_for_main_defense=False,
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

    def _dwh_observation_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._dwh_observation_context_lines(), list(score_lines))

    def _dwh_ensemble_note_lines(self, *score_lines: str) -> list[str]:
        return self._compose_note_lines(self._dwh_ensemble_context_lines(), list(score_lines))

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
        not_packaged = int(
            (
                matrix.get("classification", pd.Series(dtype=str)).astype(str)
                == "no_matched_phase4_pygnome_package_yet"
            ).sum()
        ) if not matrix.empty else 0
        blocker_text = ""
        blockers_path = self.repo_root / PHASE4_CROSSMODEL_BLOCKERS
        if blockers_path.exists():
            blocker_lines = [line.strip("- ").strip() for line in blockers_path.read_text(encoding="utf-8").splitlines() if line.strip().startswith("-")]
            if blocker_lines:
                blocker_text = blocker_lines[0]
        if not blocker_text:
            blocker_text = "The current Mindoro PyGNOME benchmark is transport-only with weathering disabled, so it cannot support Phase 4 fate or shoreline comparison."
        quantity_text = (
            f"All {not_packaged} audited Phase 4 quantities still lack a matched PyGNOME package in the current repo outputs."
            if not_packaged
            else "No matched Phase 4 PyGNOME comparison is packaged yet."
        )
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
        selected_recipe = str(self.phase1_baseline_selection.get("selected_recipe", "") or "").strip() or "cmems_gfs"
        deterministic_track = f"output/CASE_MINDORO_RETRO_2023/forecast/deterministic_control_{selected_recipe}.nc"
        if not (self.repo_root / deterministic_track).exists():
            fallback_track = next(
                iter(
                    sorted(
                        (self.repo_root / "output" / "CASE_MINDORO_RETRO_2023" / "forecast").glob("deterministic_control_*.nc")
                    )
                ),
                None,
            )
            if fallback_track is not None:
                deterministic_track = str(fallback_track.relative_to(self.repo_root)).replace("\\", "/")
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
                    interpretation="This close-up overlay keeps the observed corridor, the consolidated ensemble corridor, and the PyGNOME comparator in one compact comparison view.",
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
                        "This board foregrounds transport path and spread before the score-based validation figures.",
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
                figure_title="No matched Mindoro Phase 4 PyGNOME comparison is packaged yet",
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

    def _study_box_publication_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        study_boxes = self._thesis_study_box_entries()
        featured_tags = set(THESIS_FACING_STUDY_BOX_NUMBERS)
        featured_study_boxes = [item for item in study_boxes if str(item.get("tag")) in featured_tags]
        source_paths = [
            PHASE1_BASELINE_SELECTION_PATH.as_posix(),
            MINDORO_BASE_CASE_CONFIG_PATH.as_posix(),
            STUDY_BOX_LAND_CONTEXT_PATH.as_posix(),
            MINDORO_FORECAST_MANIFEST.as_posix(),
            PROTOTYPE_2016_PROVENANCE_METADATA_PATH.as_posix(),
            DOMAIN_GLOSSARY_PATH.as_posix(),
        ]
        subtitle_box_lines = [
            f"{item['tag']}. {item['label']}: {self._format_wgs84_bounds(item['bounds'])}"
            for item in featured_study_boxes
        ]
        note_lines = [
            "All boxes are WGS84 longitude/latitude references copied from stored repo metadata only.",
            "Study Boxes 2 and 4 are the thesis-facing defaults: the broader Mindoro case-domain overview box and the prototype-origin search box.",
            "Study Boxes 1 and 3 remain preserved as archive-only references and are not deleted from the package.",
            "The prototype_2016 first-code search box remains historical-origin support metadata only; the stored case-local prototype extents remain the operative scientific/display extents for the 2016 figures.",
        ]
        singles = [
            self._study_box_spec(
                spec_id="thesis_study_boxes_reference",
                figure_slug="thesis_study_boxes_reference",
                figure_title="Study Boxes 2 and 4 - Thesis study boxes reference figure",
                subtitle=(
                    "Panel-friendly thesis-facing WGS84 box reference centered on Study Box 2, the Mindoro "
                    "case-domain overview box, and Study Box 4, the prototype_2016 historical-origin search box"
                ),
                interpretation=(
                    "This panel-friendly thesis default foregrounds only Study Box 2, the broader Mindoro "
                    "case-domain overview box, and Study Box 4, the prototype_2016 historical-origin search box, "
                    "while keeping Study Boxes 1 and 3 available as archived secondary references."
                ),
                notes=(
                    "Built from stored Phase 1 baseline/config metadata, a local clipped land-context geography file, "
                    "the stored Mindoro forecast manifest display bounds, and the curated prototype_2016 provenance "
                    "metadata only; no scientific rerun or live extent rewriting was triggered."
                ),
                note_lines=note_lines,
                subtitle_box_title="Included boxes",
                subtitle_box_lines=subtitle_box_lines,
                study_boxes=featured_study_boxes,
                source_paths=source_paths,
            ),
            self._study_box_spec(
                spec_id="thesis_study_boxes_reference_archive_full_context",
                figure_slug="thesis_study_boxes_reference_archive_full_context",
                figure_title="Study Boxes 1-4 - Archived full study-box reference figure",
                subtitle=(
                    "Archive copy of the earlier all-box WGS84 overview showing Study Boxes 1, 2, 3, and 4 "
                    "together for traceability"
                ),
                interpretation=(
                    "This archive preserves the earlier all-box study-area overview so the previous Study Boxes 1, 2, "
                    "3, and 4 rendering remains available after the thesis-default overview switched to the cleaner "
                    "Study Boxes 2 and 4 presentation."
                ),
                notes=(
                    "Archive copy of the earlier full-context study-box overview preserved for traceability. The "
                    "thesis-default overview now foregrounds only Study Box 2 and Study Box 4."
                ),
                note_lines=[
                    "This archived overview keeps the earlier full Study Boxes 1, 2, 3, and 4 set available for traceability.",
                    "The current thesis-default panel now foregrounds only Study Boxes 2 and 4 for a cleaner panel story.",
                    "No study-box outputs were deleted; Study Boxes 1 and 3 remain available separately as archive/support references.",
                ],
                subtitle_box_title="Archived box set",
                subtitle_box_lines=[
                    f"{item['tag']}. {item['label']}: {self._format_wgs84_bounds(item['bounds'])}"
                    for item in study_boxes
                ],
                study_boxes=study_boxes,
                source_paths=source_paths,
                run_type="archived_reference_map",
                model_names="study_boxes_archive",
                recommended_for_paper=False,
            ),
        ]

        detail_specs = {
            "1": {
                "spec_id": "focused_phase1_box_geography_reference",
                "figure_slug": "focused_phase1_box_geography_reference",
                "figure_title": "Study Box 1 - Focused Mindoro Phase 1 box geography reference",
                "subtitle": (
                    "Archived Study Box 1, the focused Mindoro Phase 1 provenance box, shown on a west-coast "
                    "Philippines geographic reference backdrop"
                ),
                "interpretation": (
                    "This archived panel isolates Study Box 1, the focused Mindoro Phase 1 provenance box, while "
                    "still showing the surrounding west-coast Philippine geography."
                ),
                "notes": (
                    "Built from the stored focused Phase 1 baseline-selection box only, with a presentation-only "
                    "geographic backdrop for orientation. Study Box 1 is archive-only in the thesis-facing package."
                ),
                "note_lines": [
                    "Study Box 1 is the focused Phase 1 provenance box for the B1 recipe-selection story.",
                    "It is preserved here as an archive-only reference rather than a thesis-facing overview image.",
                    "Built from stored repo metadata only; no scientific rerun or live extent rewriting was triggered.",
                ],
                "subtitle_box_lines": [
                    "Selected box: Study Box 1 - Focused Mindoro Phase 1 validation box",
                    f"Bounds (WGS84): {self._format_wgs84_bounds(study_boxes[0]['bounds'])}",
                    "Role: archive-only provenance reference for the B1 recipe story",
                ],
                "recommended_for_paper": False,
            },
            "2": {
                "spec_id": "mindoro_case_domain_geography_reference",
                "figure_slug": "mindoro_case_domain_geography_reference",
                "figure_title": "Study Box 2 - Mindoro case-domain geography reference",
                "subtitle": (
                    "Study Box 2, the stored `mindoro_case_domain` fallback transport and overview extent, shown with geographic context"
                ),
                "interpretation": (
                    "This panel keeps Study Box 2 visible as the broader `mindoro_case_domain` transport and overview "
                    "extent without confusing it with archive-only Study Box 1."
                ),
                "notes": (
                    "Built from the stored `mindoro_case_domain` config only, with a presentation-only geographic "
                    "backdrop for orientation. Study Box 2 is thesis-facing."
                ),
                "note_lines": [
                    "Study Box 2 is `mindoro_case_domain`, the broader fallback transport and overview extent.",
                    "It is one of the two boxes foregrounded in the thesis-default panel-friendly overview.",
                    "Built from stored repo metadata only; no scientific rerun or live extent rewriting was triggered.",
                ],
                "subtitle_box_lines": [
                    "Selected box: Study Box 2 - Mindoro case-domain overview box (`mindoro_case_domain`)",
                    f"Bounds (WGS84): {self._format_wgs84_bounds(study_boxes[1]['bounds'])}",
                    "Role: broader Mindoro transport and overview context, not the active thesis provenance box",
                ],
                "recommended_for_paper": True,
            },
            "3": {
                "spec_id": "scoring_grid_bounds_geography_reference",
                "figure_slug": "scoring_grid_bounds_geography_reference",
                "figure_title": "Study Box 3 - Mindoro scoring-grid bounds geography reference",
                "subtitle": (
                    "Archived Study Box 3, the stored Mindoro scoring-grid display bounds, shown with geographic "
                    "context for the validation area"
                ),
                "interpretation": (
                    "This archived panel isolates Study Box 3, the narrower scoring-grid display bounds, so the "
                    "scoreable Mindoro display extent stays visible without being mistaken for Study Box 2."
                ),
                "notes": (
                    "Built from the stored Mindoro forecast-manifest display bounds only, with a presentation-only "
                    "geographic backdrop for orientation. Study Box 3 is archive-only in the thesis-facing package."
                ),
                "note_lines": [
                    "Study Box 3 is the stored scoring-grid display bounds used for the Mindoro scoreable display extent.",
                    "It is narrower than Study Box 2 and remains an archive-only reference rather than a thesis-facing overview image.",
                    "Built from stored repo metadata only; no scientific rerun or live extent rewriting was triggered.",
                ],
                "subtitle_box_lines": [
                    "Selected box: Study Box 3 - Mindoro scoring-grid display bounds",
                    f"Bounds (WGS84): {self._format_wgs84_bounds(study_boxes[2]['bounds'])}",
                    "Role: narrower scoreable display extent derived from the stored forecast manifest",
                ],
                "recommended_for_paper": False,
            },
            "4": {
                "spec_id": "prototype_first_code_search_box_geography_reference",
                "figure_slug": "prototype_first_code_search_box_geography_reference",
                "figure_title": "Study Box 4 - prototype_2016 first-code search-box geography reference",
                "subtitle": (
                    "Study Box 4, the historical-origin `prototype_2016` first-code search box, shown with the "
                    "broader west-coast Philippines geography"
                ),
                "interpretation": (
                    "This panel isolates Study Box 4, the historical-origin `prototype_2016` first-code search box, "
                    "so the early west-coast Philippines focus stays visible without implying that it is an active "
                    "Mindoro validation box today."
                ),
                "notes": (
                    "Built from the curated `prototype_2016` provenance metadata only, with a presentation-only "
                    "geographic backdrop for orientation. Study Box 4 is thesis-facing as historical-origin support."
                ),
                "note_lines": [
                    "Study Box 4 is historical-origin support metadata from the very first prototype code, not an active Mindoro validation box.",
                    "It is one of the two boxes foregrounded in the thesis-default panel-friendly overview.",
                    "The stored case-local prototype extents remain the operative scientific/display extents for the 2016 figures.",
                    "Built from stored repo metadata only; no scientific rerun or live extent rewriting was triggered.",
                ],
                "subtitle_box_lines": [
                    "Selected box: Study Box 4 - `prototype_2016` first-code search box",
                    f"Bounds (WGS84): {self._format_wgs84_bounds(study_boxes[3]['bounds'])}",
                    "Role: historical-origin west-coast Philippines search box used by the earliest prototype code",
                ],
                "recommended_for_paper": True,
            },
        }

        for item in study_boxes:
            detail_spec = detail_specs.get(str(item["tag"]))
            if detail_spec is None:
                continue
            singles.append(
                self._study_box_spec(
                    spec_id=str(detail_spec["spec_id"]),
                    figure_slug=str(detail_spec["figure_slug"]),
                    figure_title=str(detail_spec["figure_title"]),
                    subtitle=str(detail_spec["subtitle"]),
                    interpretation=str(detail_spec["interpretation"]),
                    notes=str(detail_spec["notes"]),
                    note_lines=[str(line) for line in detail_spec["note_lines"]],
                    subtitle_box_title="Selected box",
                    subtitle_box_lines=[str(line) for line in detail_spec["subtitle_box_lines"]],
                    study_boxes=[item],
                    source_paths=source_paths,
                    run_type="single_box_reference_map",
                    model_names="study_box_geography",
                    box_padding_fraction=0.22,
                    minimum_pad_lon=1.05,
                    minimum_pad_lat=0.85,
                    recommended_for_paper=bool(detail_spec.get("recommended_for_paper", True)),
                )
            )
        return singles, []

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
                **self._dwh_model_board_layout_fields(),
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

    def _dwh_publication_specs_v2(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        daily_cases = [
            {
                "date_token": "2010-05-21",
                "period_title": "24 h | 2010-05-21",
                "period_caption": "24 h (2010-05-21)",
                "panel_label": "24 h | 2010-05-21",
                "obs_phase_or_track": "phase3c_external_case_setup",
                "obs": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_setup/obs_mask_2010-05-21.tif",
                "det": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-21_datecomposite.tif",
                "p50": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p50_2010-05-21_datecomposite.tif",
                "p90": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p90_2010-05-21_datecomposite.tif",
                "pygnome": "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/products/pygnome_footprint_mask_2010-05-21_datecomposite.tif",
            },
            {
                "date_token": "2010-05-22",
                "period_title": "48 h | 2010-05-22",
                "period_caption": "48 h (2010-05-22)",
                "panel_label": "48 h | 2010-05-22",
                "obs_phase_or_track": "phase3c_external_case_setup",
                "obs": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_setup/obs_mask_2010-05-22.tif",
                "det": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-22_datecomposite.tif",
                "p50": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p50_2010-05-22_datecomposite.tif",
                "p90": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p90_2010-05-22_datecomposite.tif",
                "pygnome": "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/products/pygnome_footprint_mask_2010-05-22_datecomposite.tif",
            },
            {
                "date_token": "2010-05-23",
                "period_title": "72 h | 2010-05-23",
                "period_caption": "72 h (2010-05-23)",
                "panel_label": "72 h | 2010-05-23",
                "obs_phase_or_track": "phase3c_external_case_setup",
                "obs": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_setup/obs_mask_2010-05-23.tif",
                "det": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-23_datecomposite.tif",
                "p50": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p50_2010-05-23_datecomposite.tif",
                "p90": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p90_2010-05-23_datecomposite.tif",
                "pygnome": "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/products/pygnome_footprint_mask_2010-05-23_datecomposite.tif",
            },
        ]
        event_case = {
            "date_token": "2010-05-21_to_2010-05-23",
            "period_title": "event corridor | 2010-05-21_to_2010-05-23",
            "period_caption": "event corridor (2010-05-21_to_2010-05-23)",
            "panel_label": "event corridor | 2010-05-21_to_2010-05-23",
            "obs_phase_or_track": "phase3c_external_case_run",
            "obs": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/obs_mask_2010-05-21_2010-05-23_eventcorridor.tif",
            "det": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/products/control_footprint_mask_2010-05-21_2010-05-23_eventcorridor.tif",
            "p50": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p50_2010-05-21_2010-05-23_eventcorridor.tif",
            "p90": "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/products/mask_p90_2010-05-21_2010-05-23_eventcorridor.tif",
            "pygnome": "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/products/pygnome_eventcorridor_union_2010-05-21_to_2010-05-23.tif",
        }
        det_track = "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/tracks/opendrift_control_dwh_phase3c.nc"
        pygnome_track = "output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/tracks/pygnome_dwh_phase3c.nc"
        note_lines_dwh_tracks = self._dwh_trajectory_note_lines()

        def _dwh_caveat_line() -> str:
            return "Caveat: DWH observation masks are used honestly as date-composite truth; no exact sub-daily acquisition time is implied."

        def _dwh_provenance_line() -> str:
            return "Provenance: all DWH panels here are rebuilt from stored rasters only under the frozen HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes stack."

        def _official_scoring_reference_line() -> str:
            return "Official public observation-derived DWH date-composite masks remain the scoring reference for every panel."

        def _daily_mean_panel_title(panel_label: str, row: pd.Series | None, model_label: str = "") -> str:
            mean_text = self._format_score_value(self._row_mean_fss(row))
            label = f"{model_label} " if model_label else ""
            return f"{panel_label} | {label}mean FSS {mean_text}"

        def _dual_threshold_panel_title(panel_label: str, date_token: str) -> str:
            p50_mean = self._format_score_value(self._row_mean_fss(self._dwh_model_row("p50", date_token)))
            p90_mean = self._format_score_value(self._row_mean_fss(self._dwh_model_row("p90", date_token)))
            return f"{panel_label} | p50 mean FSS {p50_mean} | p90 mean FSS {p90_mean}"

        def _overview_board_note_lines(summary_line: str, *score_lines: str) -> list[str]:
            return self._compose_note_lines(
                [
                    _official_scoring_reference_line(),
                    summary_line,
                ],
                list(score_lines),
            )

        def _observation_spec(entry: dict[str, str], safe_date: str) -> dict[str, Any]:
            is_event = entry["date_token"] == event_case["date_token"]
            return self._spatial_spec(
                spec_id=f"dwh_{safe_date}_observation_truth_context",
                figure_family_code="F1",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track=entry["obs_phase_or_track"],
                date_token=entry["date_token"],
                model_names="observation",
                run_type="single_truth_context",
                view_type="zoom",
                variant="paper",
                figure_slug="eventcorridor_observation_truth_context" if is_event else "observation_truth_context",
                figure_title=f"DWH observation truth context | {entry['period_title']}",
                subtitle=f"Deepwater Horizon | observation-derived date-composite truth mask | {entry['period_caption']}",
                interpretation="This figure shows the observation-derived DWH truth mask before any model panel is introduced." if not is_event else "This figure shows the DWH event-corridor union that remains the observed truth context for the separate external transfer-validation lane.",
                notes="Built from the stored DWH observation-derived mask only.",
                note_lines=self._dwh_observation_note_lines(),
                legend_keys=["observed_mask"],
                raster_layers=[{"path": entry["obs"], "legend_key": "observed_mask", "alpha": 0.42, "zorder": 5}],
                show_source=False,
                recommended_for_paper=True,
                status_key_override="dwh_observation_truth_context",
            )

        def _overlay_spec(
            *,
            entry: dict[str, str],
            safe_date: str,
            figure_family_code: str,
            phase_or_track: str,
            model_names: str,
            run_type: str,
            figure_slug: str,
            figure_title: str,
            subtitle: str,
            interpretation: str,
            notes: str,
            note_lines: list[str],
            legend_key: str,
            raster_path: str,
            alpha: float,
            status_key_override: str,
        ) -> dict[str, Any]:
            return self._spatial_spec(
                spec_id=f"dwh_{safe_date}_{figure_slug}",
                figure_family_code=figure_family_code,
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track=phase_or_track,
                date_token=entry["date_token"],
                model_names=model_names,
                run_type=run_type,
                view_type="zoom",
                variant="paper",
                figure_slug=figure_slug,
                figure_title=figure_title,
                subtitle=subtitle,
                interpretation=interpretation,
                notes=notes,
                note_lines=note_lines,
                legend_keys=["observed_mask", legend_key, "source_point"],
                raster_layers=[
                    {"path": entry["obs"], "legend_key": "observed_mask", "alpha": 0.40, "zorder": 5},
                    {"path": raster_path, "legend_key": legend_key, "alpha": alpha, "zorder": 6},
                ],
                show_source=True,
                recommended_for_paper=True,
                status_key_override=status_key_override,
            )

        def _dual_threshold_overlay_spec(entry: dict[str, str], safe_date: str) -> dict[str, Any]:
            is_event = entry["date_token"] == event_case["date_token"]
            return self._spatial_spec(
                spec_id=f"dwh_{safe_date}_mask_p50_mask_p90_dual_threshold_overlay",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token=entry["date_token"],
                model_names="opendrift_mask_p50_mask_p90",
                run_type="single_dual_threshold_overlay",
                view_type="zoom",
                variant="paper",
                figure_slug="mask_p50_mask_p90_dual_threshold_overlay",
                figure_title=f"DWH mask_p50 and mask_p90 dual-threshold overlay | {entry['period_title']}",
                subtitle=(
                    "Deepwater Horizon | exact dual-threshold mask_p50 and mask_p90 overlay scored against official public "
                    f"observation-derived DWH date-composite masks | {entry['period_caption']}"
                ),
                interpretation=(
                    "This figure keeps the exact mask_p50 and mask_p90 thresholds on one map against the observed DWH mask; "
                    "it does not invent a combined score."
                    if not is_event
                    else "This figure keeps the exact mask_p50 and mask_p90 thresholds on one event-corridor map against "
                    "the observed DWH event corridor; it does not invent a combined score."
                ),
                notes=(
                    "Built from stored DWH observation, mask_p50, and mask_p90 rasters only; both thresholds remain scored "
                    "against the official public observation-derived DWH date-composite masks."
                ),
                note_lines=_overview_board_note_lines(
                    "This exact dual-threshold overlay keeps `mask_p50` and `mask_p90` together without inventing a combined score.",
                    self._dwh_model_score_line("p50", entry["date_token"], "mask_p50"),
                    self._dwh_model_score_line("p90", entry["date_token"], "mask_p90"),
                ),
                legend_keys=["observed_mask", "ensemble_p50", "ensemble_p90", "source_point"],
                raster_layers=[
                    {"path": entry["obs"], "legend_key": "observed_mask", "alpha": 0.38, "zorder": 5},
                    {"path": entry["p90"], "legend_key": "ensemble_p90", "alpha": 0.18, "zorder": 6},
                    {"path": entry["p50"], "legend_key": "ensemble_p50", "alpha": 0.28, "zorder": 7},
                ],
                show_source=True,
                recommended_for_paper=False,
                status_key_override="dwh_ensemble_transfer",
            )

        singles: list[dict[str, Any]] = []
        for entry in [*daily_cases, event_case]:
            safe_date = _safe_token(entry["date_token"])
            is_event = entry["date_token"] == event_case["date_token"]
            singles.append(_observation_spec(entry, safe_date))
            singles.append(
                _overlay_spec(
                    entry=entry,
                    safe_date=safe_date,
                    figure_family_code="G",
                    phase_or_track="phase3c_external_case_run",
                    model_names="opendrift_deterministic",
                    run_type="single_deterministic_overlay",
                    figure_slug="deterministic_footprint_overlay",
                    figure_title=f"DWH deterministic footprint overlay | {entry['period_title']}",
                    subtitle=f"Deepwater Horizon | deterministic footprint vs observed date-composite mask | {entry['period_caption']}",
                    interpretation="This figure keeps the deterministic OpenDrift footprint as the clean baseline transfer-validation result against the observed DWH mask." if not is_event else "This figure keeps the deterministic OpenDrift event-corridor union as the clean baseline transfer-validation result against the observed DWH event corridor.",
                    notes="Built from stored DWH observation and deterministic footprint rasters only.",
                    note_lines=self._dwh_deterministic_note_lines(
                        self._dwh_deterministic_score_line(entry["date_token"], "deterministic footprint")
                    ),
                    legend_key="deterministic_opendrift",
                    raster_path=entry["det"],
                    alpha=0.26,
                    status_key_override="dwh_deterministic_transfer",
                )
            )
            singles.append(
                _overlay_spec(
                    entry=entry,
                    safe_date=safe_date,
                    figure_family_code="H",
                    phase_or_track="phase3c_external_case_ensemble_comparison",
                    model_names="opendrift_mask_p50",
                    run_type="single_mask_p50_overlay",
                    figure_slug="mask_p50_overlay",
                    figure_title=f"DWH mask_p50 overlay | {entry['period_title']}",
                    subtitle=f"Deepwater Horizon | thresholded mask_p50 vs observed date-composite mask | {entry['period_caption']}",
                    interpretation="This figure shows the thresholded mask_p50 product against the observed DWH mask; mask_p50 is the preferred probabilistic extension." if not is_event else "This figure shows the thresholded mask_p50 event-corridor union against the observed DWH event corridor; mask_p50 is the preferred probabilistic extension.",
                    notes="Built from stored DWH observation and thresholded mask_p50 rasters only.",
                    note_lines=self._dwh_ensemble_note_lines(
                        self._dwh_model_score_line("p50", entry["date_token"], "mask_p50")
                    ),
                    legend_key="ensemble_p50",
                    raster_path=entry["p50"],
                    alpha=0.26,
                    status_key_override="dwh_ensemble_transfer",
                )
            )
            singles.append(
                _overlay_spec(
                    entry=entry,
                    safe_date=safe_date,
                    figure_family_code="H",
                    phase_or_track="phase3c_external_case_ensemble_comparison",
                    model_names="opendrift_mask_p90",
                    run_type="single_mask_p90_overlay",
                    figure_slug="mask_p90_overlay",
                    figure_title=f"DWH mask_p90 overlay | {entry['period_title']}",
                    subtitle=f"Deepwater Horizon | thresholded mask_p90 vs observed date-composite mask | {entry['period_caption']}",
                    interpretation="This figure shows the wider thresholded mask_p90 product against the observed DWH mask; mask_p90 remains support/comparison only." if not is_event else "This figure shows the wider thresholded mask_p90 event-corridor union against the observed DWH event corridor; mask_p90 remains support/comparison only.",
                    notes="Built from stored DWH observation and thresholded mask_p90 rasters only.",
                    note_lines=self._dwh_ensemble_note_lines(
                        self._dwh_model_score_line("p90", entry["date_token"], "mask_p90")
                    ),
                    legend_key="ensemble_p90",
                    raster_path=entry["p90"],
                    alpha=0.22,
                    status_key_override="dwh_ensemble_transfer",
                )
            )
            singles.append(_dual_threshold_overlay_spec(entry, safe_date))
            singles.append(
                _overlay_spec(
                    entry=entry,
                    safe_date=safe_date,
                    figure_family_code="I",
                    phase_or_track="phase3c_dwh_pygnome_comparator",
                    model_names="pygnome",
                    run_type="single_pygnome_overlay",
                    figure_slug="pygnome_footprint_overlay",
                    figure_title=f"DWH PyGNOME footprint overlay | {entry['period_title']}",
                    subtitle=f"Deepwater Horizon | PyGNOME footprint vs observed date-composite mask | {entry['period_caption']}",
                    interpretation="This figure keeps the PyGNOME footprint visible as a comparator-only overlay against the observed DWH mask." if not is_event else "This figure keeps the PyGNOME event-corridor union visible as a comparator-only overlay against the observed DWH event corridor.",
                    notes="Built from stored DWH observation and PyGNOME comparator rasters only.",
                    note_lines=self._dwh_model_note_lines(
                        self._dwh_model_score_line("pygnome", entry["date_token"], "PyGNOME footprint")
                    ),
                    legend_key="pygnome",
                    raster_path=entry["pygnome"],
                    alpha=0.24,
                    status_key_override="dwh_crossmodel_comparator",
                )
            )
        singles.extend(
            [
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
        )
        boards: list[dict[str, Any]] = [
            self._board_spec(
                spec_id="dwh_deterministic_board",
                figure_family_code="G",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_run",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="daily_deterministic_footprint_overview_board",
                figure_title="DWH deterministic footprint overview board | 24 h / 48 h / 72 h",
                subtitle="Deepwater Horizon | daily deterministic footprint overlays | 24 h (2010-05-21), 48 h (2010-05-22), 72 h (2010-05-23)",
                interpretation="This board keeps the C1 deterministic baseline legible across all three daily date-composite truth masks before the richer C2 and C3 support boards are introduced.",
                notes="Board assembled from publication-grade single figures only.",
                note_lines=[
                    self._dwh_deterministic_score_line("2010-05-21", "24 h"),
                    self._dwh_deterministic_score_line("2010-05-22", "48 h"),
                    self._dwh_deterministic_score_line("2010-05-23", "72 h"),
                ],
                guide_bullets=[
                    "Read left to right: deterministic footprint overlay at 24 h, 48 h, and 72 h.",
                    "This board keeps C1 separate from the ensemble and PyGNOME support lanes.",
                    self._format_fss_summary(self._dwh_deterministic_row("2010-05-21"), "24 h deterministic footprint"),
                    self._format_fss_summary(self._dwh_deterministic_row("2010-05-22"), "48 h deterministic footprint"),
                    self._format_fss_summary(self._dwh_deterministic_row("2010-05-23"), "72 h deterministic footprint"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {"panel_title": "24 h | 2010-05-21", "source_spec_id": "dwh_2010_05_21_deterministic_footprint_overlay"},
                    {"panel_title": "48 h | 2010-05-22", "source_spec_id": "dwh_2010_05_22_deterministic_footprint_overlay"},
                    {"panel_title": "72 h | 2010-05-23", "source_spec_id": "dwh_2010_05_23_deterministic_footprint_overlay"},
                ],
                recommended_for_main_defense=True,
                status_key_override="dwh_deterministic_transfer",
            ),
            self._board_spec(
                spec_id="dwh_24h_48h_72h_mask_p50_footprint_overview_board",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="24h_48h_72h_mask_p50_footprint_overview_board",
                figure_title="DWH mask_p50 footprint overview board | 24 h / 48 h / 72 h",
                subtitle=(
                    "Deepwater Horizon | daily mask_p50 footprint overview scored against official public "
                    "observation-derived DWH date-composite masks | 24 h (2010-05-21), 48 h (2010-05-22), 72 h (2010-05-23)"
                ),
                interpretation="This board keeps the preferred `mask_p50` extension legible across the three daily DWH date-composite masks while keeping the official observation-derived masks as the scoring reference.",
                notes="Board assembled from publication-grade single figures only; all displayed scores reuse stored daily DWH rows against the official public observation-derived DWH date-composite masks.",
                note_lines=_overview_board_note_lines(
                    "This daily overview isolates the preferred `mask_p50` footprint extension without adding deterministic or PyGNOME rows.",
                    self._dwh_model_score_line("p50", "2010-05-21", "24 h mask_p50"),
                    self._dwh_model_score_line("p50", "2010-05-22", "48 h mask_p50"),
                    self._dwh_model_score_line("p50", "2010-05-23", "72 h mask_p50"),
                ),
                guide_bullets=[
                    "Read left to right: `mask_p50` footprint overlay at 24 h, 48 h, and 72 h.",
                    _official_scoring_reference_line(),
                    self._format_fss_summary(self._dwh_model_row("p50", "2010-05-21"), "24 h mask_p50"),
                    self._format_fss_summary(self._dwh_model_row("p50", "2010-05-22"), "48 h mask_p50"),
                    self._format_fss_summary(self._dwh_model_row("p50", "2010-05-23"), "72 h mask_p50"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("p50", "2010-05-21"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_21_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("p50", "2010-05-22"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_22_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("p50", "2010-05-23"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_23_mask_p50_overlay",
                    },
                ],
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                status_key_override="dwh_ensemble_transfer",
            ),
            self._board_spec(
                spec_id="dwh_24h_48h_72h_mask_p90_footprint_overview_board",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="24h_48h_72h_mask_p90_footprint_overview_board",
                figure_title="DWH mask_p90 footprint overview board | 24 h / 48 h / 72 h",
                subtitle=(
                    "Deepwater Horizon | daily mask_p90 footprint overview scored against official public "
                    "observation-derived DWH date-composite masks | 24 h (2010-05-21), 48 h (2010-05-22), 72 h (2010-05-23)"
                ),
                interpretation="This board keeps the support-only `mask_p90` extension legible across the three daily DWH date-composite masks while keeping the official observation-derived masks as the scoring reference.",
                notes="Board assembled from publication-grade single figures only; all displayed scores reuse stored daily DWH rows against the official public observation-derived DWH date-composite masks.",
                note_lines=_overview_board_note_lines(
                    "This daily overview isolates the support/comparison-only `mask_p90` footprint without adding deterministic or PyGNOME rows.",
                    self._dwh_model_score_line("p90", "2010-05-21", "24 h mask_p90"),
                    self._dwh_model_score_line("p90", "2010-05-22", "48 h mask_p90"),
                    self._dwh_model_score_line("p90", "2010-05-23", "72 h mask_p90"),
                ),
                guide_bullets=[
                    "Read left to right: `mask_p90` footprint overlay at 24 h, 48 h, and 72 h.",
                    _official_scoring_reference_line(),
                    self._format_fss_summary(self._dwh_model_row("p90", "2010-05-21"), "24 h mask_p90"),
                    self._format_fss_summary(self._dwh_model_row("p90", "2010-05-22"), "48 h mask_p90"),
                    self._format_fss_summary(self._dwh_model_row("p90", "2010-05-23"), "72 h mask_p90"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("p90", "2010-05-21"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_21_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("p90", "2010-05-22"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_22_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("p90", "2010-05-23"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_23_mask_p90_overlay",
                    },
                ],
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                status_key_override="dwh_ensemble_transfer",
            ),
            self._board_spec(
                spec_id="dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board",
                figure_family_code="H",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_external_case_ensemble_comparison",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift",
                run_type="comparison_board",
                figure_slug="24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board",
                figure_title="DWH mask_p50 and mask_p90 dual-threshold overview board | 24 h / 48 h / 72 h",
                subtitle=(
                    "Deepwater Horizon | daily exact dual-threshold `mask_p50` and `mask_p90` overview scored against "
                    "official public observation-derived DWH date-composite masks | 24 h (2010-05-21), 48 h (2010-05-22), "
                    "72 h (2010-05-23)"
                ),
                interpretation="This board keeps the exact `mask_p50` and `mask_p90` thresholds together across the three daily DWH masks while making clear that the official observation-derived masks remain the scoring reference and no combined score is implied.",
                notes="Board assembled from publication-grade single figures only; all displayed scores reuse stored daily DWH rows against the official public observation-derived DWH date-composite masks, and no combined dual-threshold FSS is invented.",
                note_lines=_overview_board_note_lines(
                    "This exact dual-threshold daily overview keeps `mask_p50` and `mask_p90` together without inventing a combined score.",
                    self._dwh_model_score_line("p50", "2010-05-21", "24 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-21", "24 h mask_p90"),
                    self._dwh_model_score_line("p50", "2010-05-22", "48 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-22", "48 h mask_p90"),
                    self._dwh_model_score_line("p50", "2010-05-23", "72 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-23", "72 h mask_p90"),
                ),
                guide_bullets=[
                    "Read left to right: the exact dual-threshold `mask_p50` and `mask_p90` overlay at 24 h, 48 h, and 72 h.",
                    _official_scoring_reference_line(),
                    "No combined dual-threshold FSS is reported; each date keeps the stored `mask_p50` and `mask_p90` rows separate.",
                    "24 h means: mask_p50 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p50", "2010-05-21")))
                    + "; mask_p90 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p90", "2010-05-21")))
                    + ".",
                    "48 h means: mask_p50 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p50", "2010-05-22")))
                    + "; mask_p90 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p90", "2010-05-22")))
                    + ".",
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {
                        "panel_title": _dual_threshold_panel_title("24 h | 2010-05-21", "2010-05-21"),
                        "source_spec_id": "dwh_2010_05_21_mask_p50_mask_p90_dual_threshold_overlay",
                    },
                    {
                        "panel_title": _dual_threshold_panel_title("48 h | 2010-05-22", "2010-05-22"),
                        "source_spec_id": "dwh_2010_05_22_mask_p50_mask_p90_dual_threshold_overlay",
                    },
                    {
                        "panel_title": _dual_threshold_panel_title("72 h | 2010-05-23", "2010-05-23"),
                        "source_spec_id": "dwh_2010_05_23_mask_p50_mask_p90_dual_threshold_overlay",
                    },
                ],
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                status_key_override="dwh_ensemble_transfer",
            ),
            self._board_spec(
                spec_id="dwh_24h_48h_72h_mask_p50_vs_pygnome_overview_board",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="24h_48h_72h_mask_p50_vs_pygnome_overview_board",
                figure_title="DWH mask_p50 vs PyGNOME overview board | 24 h / 48 h / 72 h",
                subtitle=(
                    "Deepwater Horizon | top-row `mask_p50`, bottom-row PyGNOME, all scored against official public "
                    "observation-derived DWH date-composite masks | 24 h (2010-05-21), 48 h (2010-05-22), 72 h (2010-05-23)"
                ),
                interpretation="This board keeps the preferred `mask_p50` extension on the top row and the PyGNOME comparator on the bottom row while preserving the official DWH observation-derived masks as the scoring reference.",
                notes="Board assembled from publication-grade single figures only; all displayed scores reuse stored daily DWH rows against the official public observation-derived DWH date-composite masks, and PyGNOME remains comparator-only.",
                note_lines=_overview_board_note_lines(
                    "Top row = `mask_p50`; bottom row = PyGNOME comparator-only. The official public observation-derived DWH masks remain the scoring reference for all displayed FSS values.",
                    self._dwh_model_score_line("p50", "2010-05-21", "24 h mask_p50"),
                    self._dwh_model_score_line("pygnome", "2010-05-21", "24 h PyGNOME"),
                    self._dwh_model_score_line("p50", "2010-05-22", "48 h mask_p50"),
                    self._dwh_model_score_line("pygnome", "2010-05-22", "48 h PyGNOME"),
                    self._dwh_model_score_line("p50", "2010-05-23", "72 h mask_p50"),
                    self._dwh_model_score_line("pygnome", "2010-05-23", "72 h PyGNOME"),
                ),
                guide_bullets=[
                    "Read top row first: `mask_p50` at 24 h, 48 h, and 72 h. Then read the bottom row: PyGNOME at the same three dates.",
                    _official_scoring_reference_line(),
                    "PyGNOME remains comparator-only and never replaces the DWH observed masks as truth.",
                    self._format_fss_summary(self._dwh_model_row("p50", "2010-05-21"), "24 h mask_p50"),
                    self._format_fss_summary(self._dwh_model_row("pygnome", "2010-05-21"), "24 h PyGNOME"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("p50", "2010-05-21"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_21_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("p50", "2010-05-22"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_22_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("p50", "2010-05-23"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_23_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("pygnome", "2010-05-21"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_21_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("pygnome", "2010-05-22"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_22_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("pygnome", "2010-05-23"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_23_pygnome_footprint_overlay",
                    },
                ],
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                status_key_override="dwh_crossmodel_comparator",
            ),
            self._board_spec(
                spec_id="dwh_24h_48h_72h_mask_p90_vs_pygnome_overview_board",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="24h_48h_72h_mask_p90_vs_pygnome_overview_board",
                figure_title="DWH mask_p90 vs PyGNOME overview board | 24 h / 48 h / 72 h",
                subtitle=(
                    "Deepwater Horizon | top-row `mask_p90`, bottom-row PyGNOME, all scored against official public "
                    "observation-derived DWH date-composite masks | 24 h (2010-05-21), 48 h (2010-05-22), 72 h (2010-05-23)"
                ),
                interpretation="This board keeps the support-only `mask_p90` extension on the top row and the PyGNOME comparator on the bottom row while preserving the official DWH observation-derived masks as the scoring reference.",
                notes="Board assembled from publication-grade single figures only; all displayed scores reuse stored daily DWH rows against the official public observation-derived DWH date-composite masks, and PyGNOME remains comparator-only.",
                note_lines=_overview_board_note_lines(
                    "Top row = `mask_p90`; bottom row = PyGNOME comparator-only. The official public observation-derived DWH masks remain the scoring reference for all displayed FSS values.",
                    self._dwh_model_score_line("p90", "2010-05-21", "24 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-21", "24 h PyGNOME"),
                    self._dwh_model_score_line("p90", "2010-05-22", "48 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-22", "48 h PyGNOME"),
                    self._dwh_model_score_line("p90", "2010-05-23", "72 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-23", "72 h PyGNOME"),
                ),
                guide_bullets=[
                    "Read top row first: `mask_p90` at 24 h, 48 h, and 72 h. Then read the bottom row: PyGNOME at the same three dates.",
                    _official_scoring_reference_line(),
                    "PyGNOME remains comparator-only and never replaces the DWH observed masks as truth.",
                    self._format_fss_summary(self._dwh_model_row("p90", "2010-05-21"), "24 h mask_p90"),
                    self._format_fss_summary(self._dwh_model_row("pygnome", "2010-05-21"), "24 h PyGNOME"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("p90", "2010-05-21"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_21_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("p90", "2010-05-22"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_22_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("p90", "2010-05-23"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_23_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("pygnome", "2010-05-21"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_21_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("pygnome", "2010-05-22"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_22_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("pygnome", "2010-05-23"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_23_pygnome_footprint_overlay",
                    },
                ],
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                status_key_override="dwh_crossmodel_comparator",
            ),
            self._board_spec(
                spec_id="dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_vs_pygnome_overview_board",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="24h_48h_72h_mask_p50_mask_p90_dual_threshold_vs_pygnome_overview_board",
                figure_title="DWH mask_p50 and mask_p90 dual-threshold vs PyGNOME overview board | 24 h / 48 h / 72 h",
                subtitle=(
                    "Deepwater Horizon | top-row exact dual-threshold `mask_p50` and `mask_p90`, bottom-row PyGNOME, all scored "
                    "against official public observation-derived DWH date-composite masks | 24 h (2010-05-21), "
                    "48 h (2010-05-22), 72 h (2010-05-23)"
                ),
                interpretation="This board keeps the exact dual-threshold OpenDrift view on the top row and the PyGNOME comparator on the bottom row while preserving the official DWH observation-derived masks as the scoring reference and avoiding any invented combined score.",
                notes="Board assembled from publication-grade single figures only; all displayed scores reuse stored daily DWH rows against the official public observation-derived DWH date-composite masks, PyGNOME remains comparator-only, and no combined dual-threshold FSS is invented.",
                note_lines=_overview_board_note_lines(
                    "Top row = exact dual-threshold `mask_p50` and `mask_p90`; bottom row = PyGNOME comparator-only. No combined dual-threshold FSS is invented.",
                    self._dwh_model_score_line("p50", "2010-05-21", "24 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-21", "24 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-21", "24 h PyGNOME"),
                    self._dwh_model_score_line("p50", "2010-05-22", "48 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-22", "48 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-22", "48 h PyGNOME"),
                    self._dwh_model_score_line("p50", "2010-05-23", "72 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-23", "72 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-23", "72 h PyGNOME"),
                ),
                guide_bullets=[
                    "Read top row first: exact dual-threshold `mask_p50` and `mask_p90` at 24 h, 48 h, and 72 h. Then read the bottom row: PyGNOME at the same three dates.",
                    _official_scoring_reference_line(),
                    "No combined dual-threshold FSS is reported; each date keeps the stored `mask_p50` and `mask_p90` rows separate on the top row.",
                    self._format_fss_summary(self._dwh_model_row("pygnome", "2010-05-21"), "24 h PyGNOME"),
                    self._format_fss_summary(self._dwh_model_row("pygnome", "2010-05-22"), "48 h PyGNOME"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {
                        "panel_title": _dual_threshold_panel_title("24 h | 2010-05-21", "2010-05-21"),
                        "source_spec_id": "dwh_2010_05_21_mask_p50_mask_p90_dual_threshold_overlay",
                    },
                    {
                        "panel_title": _dual_threshold_panel_title("48 h | 2010-05-22", "2010-05-22"),
                        "source_spec_id": "dwh_2010_05_22_mask_p50_mask_p90_dual_threshold_overlay",
                    },
                    {
                        "panel_title": _dual_threshold_panel_title("72 h | 2010-05-23", "2010-05-23"),
                        "source_spec_id": "dwh_2010_05_23_mask_p50_mask_p90_dual_threshold_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("pygnome", "2010-05-21"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_21_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("pygnome", "2010-05-22"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_22_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("pygnome", "2010-05-23"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_23_pygnome_footprint_overlay",
                    },
                ],
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                status_key_override="dwh_crossmodel_comparator",
            ),
            self._board_spec(
                spec_id="dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board",
                figure_title="DWH mask_p50 vs mask_p90 vs PyGNOME three-row overview board | 24 h / 48 h / 72 h",
                subtitle=(
                    "Deepwater Horizon | top-row `mask_p50`, middle-row `mask_p90`, bottom-row PyGNOME, all scored "
                    "against official public observation-derived DWH date-composite masks | 24 h (2010-05-21), "
                    "48 h (2010-05-22), 72 h (2010-05-23)"
                ),
                interpretation=(
                    "This board keeps `mask_p50`, `mask_p90`, and PyGNOME on separate daily rows so the preferred "
                    "probabilistic extension, the support-only threshold, and the comparator-only model can be read "
                    "against the same official DWH observation-derived scoring reference."
                ),
                notes=(
                    "Board assembled from publication-grade single figures only; all displayed scores reuse stored "
                    "daily DWH rows against the official public observation-derived DWH date-composite masks, with "
                    "`mask_p50` preferred, `mask_p90` support-only, and PyGNOME comparator-only."
                ),
                note_lines=_overview_board_note_lines(
                    "Top row = `mask_p50`; middle row = `mask_p90`; bottom row = PyGNOME comparator-only. The official public observation-derived DWH masks remain the scoring reference for all displayed FSS values.",
                    self._dwh_model_score_line("p50", "2010-05-21", "24 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-21", "24 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-21", "24 h PyGNOME"),
                    self._dwh_model_score_line("p50", "2010-05-22", "48 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-22", "48 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-22", "48 h PyGNOME"),
                    self._dwh_model_score_line("p50", "2010-05-23", "72 h mask_p50"),
                    self._dwh_model_score_line("p90", "2010-05-23", "72 h mask_p90"),
                    self._dwh_model_score_line("pygnome", "2010-05-23", "72 h PyGNOME"),
                ),
                guide_bullets=[
                    "Read rows from top to bottom: `mask_p50`, then `mask_p90`, then PyGNOME, while columns stay fixed at 24 h, 48 h, and 72 h.",
                    _official_scoring_reference_line(),
                    "`mask_p50` remains the preferred probabilistic extension, `mask_p90` remains support/comparison only, and PyGNOME remains comparator-only.",
                    "24 h means: mask_p50 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p50", "2010-05-21")))
                    + "; mask_p90 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p90", "2010-05-21")))
                    + "; PyGNOME "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("pygnome", "2010-05-21")))
                    + ".",
                    "48 h means: mask_p50 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p50", "2010-05-22")))
                    + "; mask_p90 "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("p90", "2010-05-22")))
                    + "; PyGNOME "
                    + self._format_score_value(self._row_mean_fss(self._dwh_model_row("pygnome", "2010-05-22")))
                    + ".",
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("p50", "2010-05-21"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_21_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("p50", "2010-05-22"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_22_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("p50", "2010-05-23"),
                            "mask_p50",
                        ),
                        "source_spec_id": "dwh_2010_05_23_mask_p50_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("p90", "2010-05-21"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_21_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("p90", "2010-05-22"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_22_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("p90", "2010-05-23"),
                            "mask_p90",
                        ),
                        "source_spec_id": "dwh_2010_05_23_mask_p90_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "24 h | 2010-05-21",
                            self._dwh_model_row("pygnome", "2010-05-21"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_21_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "48 h | 2010-05-22",
                            self._dwh_model_row("pygnome", "2010-05-22"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_22_pygnome_footprint_overlay",
                    },
                    {
                        "panel_title": _daily_mean_panel_title(
                            "72 h | 2010-05-23",
                            self._dwh_model_row("pygnome", "2010-05-23"),
                            "PyGNOME",
                        ),
                        "source_spec_id": "dwh_2010_05_23_pygnome_footprint_overlay",
                    },
                ],
                recommended_for_main_defense=False,
                recommended_for_paper=True,
                status_key_override="dwh_crossmodel_comparator",
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
        for entry in [*daily_cases, event_case]:
            safe_date = _safe_token(entry["date_token"])
            is_event = entry["date_token"] == event_case["date_token"]
            boards.append(
                self._board_spec(
                    spec_id=f"dwh_{safe_date}_observed_det_p50_p90_board",
                    figure_family_code="H",
                    case_id="CASE_DWH_RETRO_2010_72H",
                    phase_or_track="phase3c_external_case_ensemble_comparison",
                    date_token=entry["date_token"],
                    model_names="opendrift",
                    run_type="comparison_board",
                    figure_slug="observed_deterministic_mask_p50_mask_p90_board",
                    figure_title=f"DWH observed, deterministic, mask_p50, and mask_p90 | {entry['period_title']}",
                    subtitle=f"Deepwater Horizon | deterministic baseline plus C2 threshold masks | {entry['period_caption']}",
                    interpretation="This board keeps the observed truth mask, deterministic baseline, mask_p50 extension, and mask_p90 support mask on one page without relabeling any product semantics.",
                    notes="Board assembled from publication-grade single figures only.",
                    note_lines=[
                        self._dwh_deterministic_score_line(entry["date_token"], "deterministic footprint"),
                        self._dwh_model_score_line("p50", entry["date_token"], "mask_p50"),
                        self._dwh_model_score_line("p90", entry["date_token"], "mask_p90"),
                    ],
                    guide_bullets=[
                        "Read left to right: observed truth context, deterministic footprint overlay, mask_p50 overlay, then mask_p90 overlay.",
                        "Deterministic remains the clean baseline; mask_p50 is the preferred probabilistic extension; mask_p90 stays support/comparison only.",
                        self._format_fss_summary(self._dwh_deterministic_row(entry["date_token"]), "deterministic footprint"),
                        self._format_fss_summary(self._dwh_model_row("p50", entry["date_token"]), "mask_p50"),
                        self._format_fss_summary(self._dwh_model_row("p90", entry["date_token"]), "mask_p90"),
                    ],
                    caveat_line=_dwh_caveat_line(),
                    provenance_line=_dwh_provenance_line(),
                    panels=[
                        {"panel_title": entry["panel_label"], "source_spec_id": f"dwh_{safe_date}_observation_truth_context"},
                        {"panel_title": "deterministic footprint", "source_spec_id": f"dwh_{safe_date}_deterministic_footprint_overlay"},
                        {"panel_title": "mask_p50", "source_spec_id": f"dwh_{safe_date}_mask_p50_overlay"},
                        {"panel_title": "mask_p90", "source_spec_id": f"dwh_{safe_date}_mask_p90_overlay"},
                    ],
                    recommended_for_main_defense=is_event,
                    status_key_override="dwh_ensemble_transfer",
                )
            )
            boards.append(
                self._board_spec(
                    spec_id=f"dwh_{safe_date}_observed_det_p50_pygnome_board",
                    figure_family_code="I",
                    case_id="CASE_DWH_RETRO_2010_72H",
                    phase_or_track="phase3c_dwh_pygnome_comparator",
                    date_token=entry["date_token"],
                    model_names="opendrift_vs_pygnome",
                    run_type="comparison_board",
                    figure_slug="observed_deterministic_mask_p50_pygnome_board",
                    figure_title=f"DWH observed, deterministic, mask_p50, and PyGNOME | {entry['period_title']}",
                    subtitle=f"Deepwater Horizon | truth context plus OpenDrift and PyGNOME comparator views | {entry['period_caption']}",
                    interpretation="This board keeps the observed DWH mask as truth while showing the deterministic baseline, the preferred mask_p50 extension, and the PyGNOME comparator on the same frozen case definition.",
                    notes="Board assembled from publication-grade single figures only.",
                    note_lines=[
                        self._dwh_deterministic_score_line(entry["date_token"], "deterministic footprint"),
                        self._dwh_model_score_line("p50", entry["date_token"], "mask_p50"),
                        self._dwh_model_score_line("pygnome", entry["date_token"], "PyGNOME footprint"),
                    ],
                    guide_bullets=[
                        "Read clockwise: observed truth context, deterministic footprint overlay, mask_p50 overlay, then the PyGNOME comparator overlay.",
                        "Observed DWH masks stay truth; mask_p50 is the preferred probabilistic extension; PyGNOME remains comparator-only.",
                        self._format_fss_summary(self._dwh_deterministic_row(entry["date_token"]), "deterministic footprint"),
                        self._format_fss_summary(self._dwh_model_row("p50", entry["date_token"]), "mask_p50"),
                        self._format_fss_summary(self._dwh_model_row("pygnome", entry["date_token"]), "PyGNOME footprint"),
                    ],
                    caveat_line=_dwh_caveat_line(),
                    provenance_line=_dwh_provenance_line(),
                    panels=[
                        {"panel_title": entry["panel_label"], "source_spec_id": f"dwh_{safe_date}_observation_truth_context"},
                        {"panel_title": "deterministic footprint", "source_spec_id": f"dwh_{safe_date}_deterministic_footprint_overlay"},
                        {"panel_title": "mask_p50", "source_spec_id": f"dwh_{safe_date}_mask_p50_overlay"},
                        {"panel_title": "PyGNOME footprint", "source_spec_id": f"dwh_{safe_date}_pygnome_footprint_overlay"},
                    ],
                    recommended_for_main_defense=is_event,
                    status_key_override="dwh_crossmodel_comparator",
                )
            )
        boards.append(
            self._board_spec(
                spec_id="dwh_event_deterministic_vs_pygnome_board",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="eventcorridor_deterministic_vs_pygnome_board",
                figure_title="DWH deterministic footprint versus PyGNOME | event corridor | 2010-05-21_to_2010-05-23",
                subtitle="Deepwater Horizon | event-corridor deterministic baseline versus PyGNOME comparator | event corridor (2010-05-21_to_2010-05-23)",
                interpretation="This support board isolates the deterministic baseline and the PyGNOME comparator against the same observed DWH event-corridor truth mask.",
                notes="Board assembled from publication-grade single figures only.",
                note_lines=[
                    self._dwh_event_score_line("deterministic", "deterministic footprint"),
                    self._dwh_event_score_line("pygnome", "PyGNOME footprint"),
                ],
                guide_bullets=[
                    "Read left to right: observed event-corridor truth context, deterministic footprint overlay, then PyGNOME footprint overlay.",
                    "PyGNOME remains comparator-only; the observed DWH mask stays truth.",
                    self._format_fss_summary(self._dwh_event_model_row("deterministic"), "event deterministic footprint"),
                    self._format_fss_summary(self._dwh_event_model_row("pygnome"), "event PyGNOME footprint"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {"panel_title": event_case["panel_label"], "source_spec_id": "dwh_2010_05_21_to_2010_05_23_observation_truth_context"},
                    {"panel_title": "deterministic footprint", "source_spec_id": "dwh_2010_05_21_to_2010_05_23_deterministic_footprint_overlay"},
                    {"panel_title": "PyGNOME footprint", "source_spec_id": "dwh_2010_05_21_to_2010_05_23_pygnome_footprint_overlay"},
                ],
                recommended_for_main_defense=False,
                status_key_override="dwh_crossmodel_comparator",
            )
        )
        boards.append(
            self._board_spec(
                spec_id="dwh_event_mask_p50_vs_pygnome_board",
                figure_family_code="I",
                case_id="CASE_DWH_RETRO_2010_72H",
                phase_or_track="phase3c_dwh_pygnome_comparator",
                date_token="2010-05-21_to_2010-05-23",
                model_names="opendrift_vs_pygnome",
                run_type="comparison_board",
                figure_slug="eventcorridor_mask_p50_vs_pygnome_board",
                figure_title="DWH mask_p50 versus PyGNOME | event corridor | 2010-05-21_to_2010-05-23",
                subtitle="Deepwater Horizon | event-corridor mask_p50 versus PyGNOME comparator | event corridor (2010-05-21_to_2010-05-23)",
                interpretation="This support board isolates the preferred mask_p50 extension and the PyGNOME comparator against the same observed DWH event-corridor truth mask.",
                notes="Board assembled from publication-grade single figures only.",
                note_lines=[
                    self._dwh_event_score_line("p50", "mask_p50"),
                    self._dwh_event_score_line("pygnome", "PyGNOME footprint"),
                ],
                guide_bullets=[
                    "Read left to right: observed event-corridor truth context, mask_p50 overlay, then PyGNOME footprint overlay.",
                    "This support board checks whether mask_p50 changes the OpenDrift-versus-PyGNOME comparison.",
                    self._format_fss_summary(self._dwh_event_model_row("p50"), "event mask_p50"),
                    self._format_fss_summary(self._dwh_event_model_row("pygnome"), "event PyGNOME footprint"),
                ],
                caveat_line=_dwh_caveat_line(),
                provenance_line=_dwh_provenance_line(),
                panels=[
                    {"panel_title": event_case["panel_label"], "source_spec_id": "dwh_2010_05_21_to_2010_05_23_observation_truth_context"},
                    {"panel_title": "mask_p50", "source_spec_id": "dwh_2010_05_21_to_2010_05_23_mask_p50_overlay"},
                    {"panel_title": "PyGNOME footprint", "source_spec_id": "dwh_2010_05_21_to_2010_05_23_pygnome_footprint_overlay"},
                ],
                recommended_for_main_defense=False,
                status_key_override="dwh_crossmodel_comparator",
            )
        )
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

    def _legacy_2016_home_overview_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        legacy_cases = [
            {"case_id": "CASE_2016-09-01", "label": "1 Sep 2016", "date_token": "2016-09-01"},
            {"case_id": "CASE_2016-09-06", "label": "6 Sep 2016", "date_token": "2016-09-06"},
            {"case_id": "CASE_2016-09-17", "label": "17 Sep 2016", "date_token": "2016-09-17"},
        ]
        source_root = self.repo_root / PROTOTYPE_2016_FINAL_DIR
        if not source_root.exists():
            self._record_missing(
                source_root,
                "Legacy 2016 final support root is missing, so the requested home-overview triptychs could not be built.",
            )
            return [], []

        view_specs = {
            "drifter_track": {
                "filename": "drifter_track_72h.png",
                "model_names": "observation",
                "figure_title": "Legacy 2016 observed drifter tracks across the three support cases",
                "subtitle": "Legacy 2016 | observed drifter-of-record reference tracks | case-local 72 h support view",
                "interpretation": (
                    "This triptych keeps the three observed 72 h drifter-of-record tracks visible together so the "
                    "legacy September 2016 support cases can be scanned from one board."
                ),
                "notes": (
                    "Board assembled from the stored case-local legacy 72 h drifter-track figures only. "
                    "This remains support-only context."
                ),
                "guide_bullets": [
                    "Read left to right as 1 Sep, 6 Sep, and 17 Sep 2016.",
                    "These are the observed drifter-of-record reference paths only, with no forecast envelope added yet.",
                    "Use this board to compare path shape and extent before reading the forecast overlays below.",
                ],
                "note_lines": [
                    "These three panels preserve the observed 72 h drifter-of-record tracks for the legacy September 2016 support cases.",
                    "Legacy support only; these figures do not replace the final regional Phase 1 evidence.",
                ],
                "figure_slug": "legacy_2016_drifter_track_triptych_board",
                "spec_suffix": "drifter_track",
            },
            "drifter_vs_ensemble": {
                "filename": "drifter_vs_ensemble_72h.png",
                "model_names": "observation_vs_opendrift",
                "figure_title": "Legacy 2016 drifter vs ensemble p50/p90 across the three support cases",
                "subtitle": "Legacy 2016 | observed drifter track against the stored 72 h OpenDrift p50/p90 occupancy footprints",
                "interpretation": (
                    "This board compares each observed drifter path with the stored 72 h ensemble p50 and p90 "
                    "footprint envelopes for the three legacy support cases."
                ),
                "notes": (
                    "Board assembled from the stored case-local legacy drifter-versus-ensemble 72 h panels only. "
                    "The source images already preserve the exact stored p50/p90 footprint rendering."
                ),
                "guide_bullets": [
                    "Each panel overlays the observed drifter track on the stored 72 h p50 and p90 occupancy footprints.",
                    "p50 is the tighter preferred legacy ensemble footprint; p90 is the broader support envelope.",
                    "Use this board to compare how closely the observed track sits inside the stored forecast envelope for each case.",
                ],
                "note_lines": [
                    "The source panels already preserve the exact stored 72 h p50 and p90 member-occupancy footprint geometry.",
                    "Legacy support only; these figures remain descriptive rather than thesis-facing validation claims.",
                ],
                "figure_slug": "legacy_2016_drifter_vs_mask_p50_mask_p90_triptych_board",
                "spec_suffix": "drifter_vs_ensemble",
            },
            "pygnome_vs_ensemble": {
                "filename": "pygnome_vs_ensemble_consolidated_72h.png",
                "model_names": "opendrift_vs_pygnome",
                "figure_title": "Legacy 2016 ensemble p50/p90 vs PyGNOME across the three support cases",
                "subtitle": "Legacy 2016 | consolidated 72 h OpenDrift p50/p90 versus deterministic PyGNOME comparator with stored FSS callouts",
                "interpretation": (
                    "This board keeps the stored consolidated PyGNOME-versus-ensemble legacy comparison panels together "
                    "so the three September 2016 support cases can be compared on one page."
                ),
                "notes": (
                    "Board assembled from the stored consolidated PyGNOME-versus-ensemble legacy panels only. "
                    "PyGNOME remains comparator-only where shown."
                ),
                "guide_bullets": [
                    "Each source panel keeps the stored p50, p90, and deterministic PyGNOME footprints visible for one legacy case.",
                    "The corresponding FSS summary remains embedded inside each source panel.",
                    "Read this board together with the drifter-reference boards above because the stored consolidated PyGNOME source panels do not redraw the observed drifter line.",
                ],
                "note_lines": [
                    "PyGNOME remains comparator-only; it is never shown as truth in the legacy 2016 lane.",
                    "The stored consolidated PyGNOME panels keep the corresponding FSS summary visible for each case.",
                    "Observed drifter tracks remain visible in the companion legacy boards above because these stored comparator panels focus on the forecast overlays and score callouts.",
                ],
                "figure_slug": "legacy_2016_mask_p50_mask_p90_vs_pygnome_triptych_board",
                "spec_suffix": "pygnome_vs_ensemble",
            },
        }

        singles: list[dict[str, Any]] = []
        boards: list[dict[str, Any]] = []
        board_panels: dict[str, list[dict[str, Any]]] = {key: [] for key in view_specs}

        for case in legacy_cases:
            case_root = source_root / case["case_id"]
            for view_key, view in view_specs.items():
                source_path = case_root / str(view["filename"])
                if not source_path.exists():
                    self._record_missing(
                        source_path,
                        f"Legacy 2016 home-overview source image missing for {case['case_id']} ({view_key}).",
                    )
                    continue
                spec_id = f"legacy_2016_{_safe_token(case['case_id'])}_{str(view['spec_suffix'])}"
                singles.append(
                    self._image_spec(
                        spec_id=spec_id,
                        figure_family_code="M",
                        case_id=case["case_id"],
                        phase_or_track="prototype_2016_home_overview",
                        date_token=case["date_token"],
                        model_names=str(view["model_names"]),
                        run_type="single_support_context",
                        view_type="single",
                        variant="paper",
                        figure_slug=f"{str(view['spec_suffix'])}_{_safe_token(case['case_id'])}",
                        figure_title=f"{case['label']} legacy support panel",
                        subtitle=f"Legacy 2016 | {case['label']} | home-overview support source panel",
                        interpretation=f"Stored legacy support source panel for {case['label']}.",
                        notes=(
                            f"Copied from the curated legacy 2016 final package root for {case['case_id']} and reused "
                            "as a home-overview source panel."
                        ),
                        source_image_path=_relative_to_repo(self.repo_root, source_path),
                        source_paths=[_relative_to_repo(self.repo_root, source_path)],
                        recommended_for_main_defense=False,
                        recommended_for_paper=False,
                        legacy_debug_only=True,
                    )
                )
                board_panels[view_key].append({"panel_title": case["label"], "source_spec_id": spec_id})

        board_case_id = "CASE_LEGACY_2016"
        for view_key, view in view_specs.items():
            if len(board_panels[view_key]) != len(legacy_cases):
                continue
            boards.append(
                self._board_spec(
                    spec_id=str(view["figure_slug"]),
                    figure_family_code="M",
                    case_id=board_case_id,
                    phase_or_track="prototype_2016_home_overview",
                    date_token="2016-09-01_to_2016-09-17",
                    model_names=str(view["model_names"]),
                    run_type="comparison_board",
                    figure_slug=str(view["figure_slug"]),
                    figure_title=str(view["figure_title"]),
                    subtitle=str(view["subtitle"]),
                    interpretation=str(view["interpretation"]),
                    notes=str(view["notes"]),
                    note_lines=[str(item) for item in view["note_lines"]],
                    panels=board_panels[view_key],
                    guide_bullets=[str(item) for item in view["guide_bullets"]],
                    recommended_for_main_defense=False,
                    recommended_for_paper=False,
                    status_key_override="prototype_2016_support",
                )
            )

        return singles, boards

    def _build_specs(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        single_specs: list[dict[str, Any]] = []
        board_specs: list[dict[str, Any]] = []
        for builder in (
            self._mindoro_publication_specs,
            self._phase4_publication_specs,
            self._study_box_publication_specs,
            self._dwh_publication_specs_v2,
            self._prototype_support_publication_specs,
            self._legacy_2016_home_overview_specs,
        ):
            singles, boards = builder()
            single_specs.extend(singles)
            board_specs.extend(boards)
        return single_specs, board_specs

    def _write_registry(self, path: Path) -> list[dict[str, Any]]:
        rows = [
            record.as_row()
            for record in sorted(
                self.figure_records,
                key=lambda item: (item.display_order, item.figure_family_code, item.figure_id),
            )
        ]
        _write_csv(
            path,
            rows,
            columns=[
                "figure_id",
                "display_title",
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
                "surface_key",
                "surface_label",
                "surface_description",
                "surface_home_visible",
                "surface_panel_visible",
                "surface_archive_visible",
                "surface_advanced_visible",
                "surface_recommended_visible",
                "thesis_surface",
                "archive_only",
                "legacy_support",
                "comparator_support",
                "display_order",
                "page_target",
                "study_box_id",
                "study_box_numbers",
                "study_box_label",
                "recommended_scope",
            ],
        )
        return rows

    def _write_font_audit(self, path: Path) -> list[dict[str, Any]]:
        rows = [self.font_audit.as_row()]
        _write_csv(
            path,
            rows,
            columns=[
                "requested_font_family",
                "actual_font_family",
                "actual_font_path",
                "exact_requested_font_used",
                "fallback_used",
                "fallback_candidates",
            ],
        )
        return rows

    def _write_board_layout_audit(self, path: Path) -> list[dict[str, Any]]:
        rows = sorted(self.board_layout_audit_rows, key=lambda item: item["board_file"])
        _write_csv(
            path,
            rows,
            columns=[
                "board_file",
                "board_family",
                "panel_count",
                "grid_structure",
                "layout_mode",
                "issue_types_found",
                "layout_fix_applied",
                "requested_font_family",
                "actual_font_resolved",
                "exact_arial_used",
                "fallback_needed",
                "text_shortened_or_wrapped",
                "filenames_stayed_same",
                "title_within_bounds",
                "subtitle_within_bounds",
                "guide_within_bounds",
            ],
        )
        return rows

    def _record_display_label(self, record: PublicationFigureRecord) -> str:
        return record.display_title or record.study_box_label or record.status_label or record.figure_family_label

    def _validate_study_box_surface_rules(self) -> None:
        thesis_numbers = set(THESIS_FACING_STUDY_BOX_NUMBERS)
        archive_numbers = set(ARCHIVE_ONLY_STUDY_BOX_NUMBERS)
        for record in self.figure_records:
            study_box_numbers = set(parse_study_box_numbers(record.study_box_numbers))
            if not study_box_numbers:
                continue
            if record.thesis_surface and not study_box_numbers.issubset(thesis_numbers):
                raise ValueError(
                    f"Thesis-facing study-box figure {record.figure_id} includes archive-only study box numbers: {record.study_box_numbers}"
                )
            if (
                record.run_type == "single_box_reference_map"
                and study_box_numbers.intersection(archive_numbers)
                and not record.archive_only
            ):
                raise ValueError(
                    f"Archive-only study-box detail figure {record.figure_id} is not marked archive_only."
                )

    def _study_box_figure_index(self) -> dict[str, list[str]]:
        index: dict[str, list[str]] = {}
        for record in sorted(self.figure_records, key=lambda item: (item.display_order, item.figure_id)):
            if not record.study_box_numbers:
                continue
            index.setdefault(record.study_box_numbers, []).append(record.figure_id)
        return index

    def _build_inventory_markdown(self) -> str:
        ordered_records = sorted(self.figure_records, key=lambda item: (item.display_order, item.figure_id))
        lines = [
            "# Publication Figure Inventory",
            "",
            "This inventory is the curated thesis-surface index for `output/figure_package_publication/`.",
            "",
            "Classification keys:",
            "",
            "- `thesis_surface=true`: visible on the curated thesis-facing surface.",
            "- `archive_only=true`: preserved for provenance/archive only.",
            "- `legacy_support=true`: prototype or legacy support material.",
            "- `comparator_support=true`: comparator-only support material.",
            "- `display_order`: manifest-driven thesis presentation order.",
            "- `page_target`: thesis page or archive view that should expose the figure.",
            "- `study_box_id`: stable study-box identifier when the figure is a study-context map.",
            "- `study_box_numbers`: study box number or number set shown by the figure (for example `2`, `4`, or `2,4`).",
            "- `recommended_scope`: whether the figure belongs in main text, page support, appendix support, legacy support, or archive only.",
            "",
        ]
        for page_target in sorted({record.page_target for record in ordered_records}):
            page_records = [record for record in ordered_records if record.page_target == page_target]
            if not page_records:
                continue
            lines.append(f"## {page_target}")
            lines.append("")
            for record in page_records:
                tags = [
                    f"scope={record.recommended_scope}",
                    f"display_order={record.display_order}",
                    f"thesis_surface={str(record.thesis_surface).lower()}",
                    f"archive_only={str(record.archive_only).lower()}",
                    f"legacy_support={str(record.legacy_support).lower()}",
                    f"comparator_support={str(record.comparator_support).lower()}",
                ]
                if record.study_box_id:
                    tags.append(f"study_box_id={record.study_box_id}")
                if record.study_box_numbers:
                    tags.append(f"study_box_numbers={record.study_box_numbers}")
                lines.append(
                    f"- `{record.figure_id}` [{self._record_display_label(record)}]: "
                    + ", ".join(tags)
                )
            lines.append("")
        return "\n".join(lines)

    def _manifest_inventory(self) -> dict[str, Any]:
        ordered_records = sorted(self.figure_records, key=lambda item: (item.display_order, item.figure_id))
        page_targets = sorted({record.page_target for record in ordered_records})
        return {
            "figure_count": len(ordered_records),
            "thesis_surface_count": sum(1 for record in ordered_records if record.thesis_surface),
            "archive_only_count": sum(1 for record in ordered_records if record.archive_only),
            "legacy_support_count": sum(1 for record in ordered_records if record.legacy_support),
            "comparator_support_count": sum(1 for record in ordered_records if record.comparator_support),
            "page_target_counts": {
                page_target: sum(1 for record in ordered_records if record.page_target == page_target)
                for page_target in page_targets
            },
            "recommended_scope_counts": {
                scope: sum(1 for record in ordered_records if record.recommended_scope == scope)
                for scope in sorted({record.recommended_scope for record in ordered_records})
            },
            "thesis_surface_figure_ids": [record.figure_id for record in ordered_records if record.thesis_surface],
            "archive_figure_ids": [record.figure_id for record in ordered_records if record.archive_only],
            "page_target_index": {
                page_target: [record.figure_id for record in ordered_records if record.page_target == page_target]
                for page_target in page_targets
            },
            "study_box_figure_index": self._study_box_figure_index(),
        }

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
                status_label = self._record_display_label(record)
                provenance = f" Provenance: {record.status_provenance}" if record.status_provenance else ""
                lines.append(
                    f"- `{record.figure_id}` [{status_label}]: {record.short_plain_language_interpretation}{provenance}"
                )
            lines.append("")
        return "\n".join(lines)

    def _build_talking_points_markdown(self) -> str:
        recommended = [
            record
            for record in self.figure_records
            if record.recommended_for_main_defense and record.thesis_surface
        ]
        paper_ready = [
            record
            for record in self.figure_records
            if record.recommended_for_paper and record.variant == "paper" and record.thesis_surface
        ]
        supporting_honesty = [
            record for record in self.figure_records if record.figure_family_code == "F" and record.thesis_surface
        ]
        legacy_support = [
            record
            for record in self.figure_records
            if record.legacy_support and record.thesis_surface
        ]
        archive_rows = [record for record in self.figure_records if record.archive_only]
        lines = [
            "# Publication Figure Talking Points",
            "",
            "## Start Here",
            "",
        ]
        for record in sorted(recommended, key=lambda item: (item.display_order, item.figure_id)):
            lines.append(
                f"- `{record.figure_id}` [{self._record_display_label(record)}]: {record.status_panel_text or record.short_plain_language_interpretation}"
            )
        lines.extend(
            [
                "",
                "## Paper-Ready Singles",
                "",
            ]
        )
        for record in sorted(paper_ready, key=lambda item: (item.display_order, item.figure_id)):
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
                    f"- `{record.figure_id}` [{self._record_display_label(record)}]: {record.status_panel_text or 'This figure explains why Phase 4 OpenDrift-versus-PyGNOME comparison is not shown.'}"
                )
        if legacy_support:
            lines.extend(
                [
                    "",
                    "## Selected Legacy Support",
                    "",
                ]
            )
            for record in sorted(legacy_support, key=lambda item: (item.display_order, item.figure_id)):
                lines.append(
                    f"- `{record.figure_id}` [{self._record_display_label(record)}]: {record.status_panel_text or 'Legacy support retained as context only.'}"
                )
        if archive_rows:
            lines.extend(
                [
                    "",
                    "## Archive Index",
                    "",
                ]
            )
            for record in sorted(archive_rows, key=lambda item: (item.display_order, item.figure_id)):
                lines.append(
                    f"- `{record.figure_id}` [{self._record_display_label(record)}]: archive/provenance only."
                )
        if self.missing_optional_artifacts:
            lines.extend(["", "## Missing Optional Inputs", ""])
            for item in self.missing_optional_artifacts:
                lines.append(f"- `{item['relative_path']}`: {item['notes']}")
        return "\n".join(lines)

    def _build_manifest(self, generated_at_utc: str) -> dict[str, Any]:
        rows = [record.as_row() for record in self.figure_records]
        family_counts = {code: len([record for record in self.figure_records if record.figure_family_code == code]) for code in FIGURE_FAMILIES}
        ordered_records = sorted(self.figure_records, key=lambda item: (item.display_order, item.figure_id))
        recommended = [
            record.figure_id
            for record in ordered_records
            if record.recommended_for_main_defense and record.surface_recommended_visible and record.thesis_surface
        ]
        paper_ready = [
            record.figure_id
            for record in ordered_records
            if record.recommended_for_paper and record.variant == "paper" and record.thesis_surface
        ]
        deferred_note_figure_ids = [record.figure_id for record in self.figure_records if record.figure_family_code == "F"]
        return {
            "phase": PHASE,
            "generated_at_utc": generated_at_utc,
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "publication_package_built_from_existing_outputs_only": True,
            "expensive_scientific_reruns_triggered": False,
            "phase4_crossmodel_comparison_status": "no_matched_pygnome_package_yet",
            "phase4_deferred_comparison_note_figure_produced": bool(deferred_note_figure_ids),
            "phase4_deferred_comparison_note_figure_ids": deferred_note_figure_ids,
            "style_config_path": _relative_to_repo(self.repo_root, self.repo_root / STYLE_CONFIG_PATH),
            "inventory_markdown_path": _relative_to_repo(
                self.repo_root, self.output_dir / "publication_figure_inventory.md"
            ),
            "font_audit": self.font_audit.as_row(),
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
            "inventory": self._manifest_inventory(),
            "study_box_presentation_rule": {
                "thesis_surface_numbers": list(THESIS_FACING_STUDY_BOX_NUMBERS),
                "archive_only_numbers": list(ARCHIVE_ONLY_STUDY_BOX_NUMBERS),
                "summary": "Study Boxes 2 and 4 may appear on thesis-facing surfaces. Study Boxes 1 and 3 are archive/advanced/support only.",
            },
            "study_box_catalog": study_box_catalog_rows(),
            "upstream_status_context": {
                "phase1_scientifically_frozen": bool(self._phase_status_flag("phase1", "phase1_regional_baseline", "scientifically_frozen", False)),
                "phase2_scientifically_usable": bool(self._phase_status_flag("phase2", "phase2_machine_readable_forecast", "scientifically_reportable", True)),
                "phase2_scientifically_frozen": bool(self._phase_status_flag("phase2", "phase2_machine_readable_forecast", "scientifically_frozen", False)),
                "phase4_scientifically_reportable": bool(self._phase_status_flag("phase4", "mindoro_phase4", "scientifically_reportable", True)),
            },
            "missing_optional_artifacts": self.missing_optional_artifacts,
            "board_layout_audit_row_count": len(self.board_layout_audit_rows),
            "figures": rows,
        }

    def run(self) -> dict[str, Any]:
        generated_at_utc = pd.Timestamp.now(tz="UTC").isoformat()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        single_specs, board_specs = self._build_specs()
        self._spec_lookup = {str(spec["spec_id"]): spec for spec in [*single_specs, *board_specs]}
        saved_by_spec_id: dict[str, PublicationFigureRecord] = {}
        for spec in single_specs:
            saved_by_spec_id[str(spec["spec_id"])] = self._save_single_figure(spec)
        for spec in board_specs:
            saved_by_spec_id[str(spec["spec_id"])] = self._save_board_figure(spec, saved_by_spec_id)
        self._validate_study_box_surface_rules()

        registry_csv = self.output_dir / "publication_figure_registry.csv"
        manifest_json = self.output_dir / "publication_figure_manifest.json"
        captions_md = self.output_dir / "publication_figure_captions.md"
        talking_points_md = self.output_dir / "publication_figure_talking_points.md"
        inventory_md = self.output_dir / "publication_figure_inventory.md"
        font_audit_csv = self.output_dir / "font_audit.csv"
        board_layout_audit_csv = self.output_dir / "board_layout_audit.csv"

        rows = self._write_registry(registry_csv)
        self._write_font_audit(font_audit_csv)
        self._write_board_layout_audit(board_layout_audit_csv)
        _write_text(captions_md, self._build_captions_markdown())
        _write_text(talking_points_md, self._build_talking_points_markdown())
        _write_text(inventory_md, self._build_inventory_markdown())
        _write_json(manifest_json, self._build_manifest(generated_at_utc))

        return {
            "output_dir": str(self.output_dir),
            "registry_csv": str(registry_csv),
            "manifest_json": str(manifest_json),
            "captions_md": str(captions_md),
            "talking_points_md": str(talking_points_md),
            "inventory_md": str(inventory_md),
            "font_audit_csv": str(font_audit_csv),
            "board_layout_audit_csv": str(board_layout_audit_csv),
            "figure_count": len(self.figure_records),
            "figure_families_generated": sorted({record.figure_family_code for record in self.figure_records}),
            "side_by_side_comparison_boards_produced": any(record.view_type == "board" for record in self.figure_records),
            "single_image_paper_figures_produced": any(record.variant == "paper" for record in self.figure_records),
            "phase4_deferred_comparison_note_figure_produced": any(record.figure_family_code == "F" for record in self.figure_records),
            "recommended_main_defense_figures": [
                record.figure_id
                for record in self.figure_records
                if record.recommended_for_main_defense and record.surface_recommended_visible and record.thesis_surface
            ],
            "recommended_paper_figures": [
                record.figure_id
                for record in self.figure_records
                if record.recommended_for_paper and record.variant == "paper" and record.thesis_surface
            ],
            "missing_optional_artifacts": self.missing_optional_artifacts,
            "figure_rows": rows,
        }


def run_figure_package_publication(repo_root: str | Path = ".", output_dir: str | Path | None = None) -> dict[str, Any]:
    return FigurePackagePublicationService(repo_root=repo_root, output_dir=output_dir).run()

