"""Appendix-only March 6 recovery sensitivity matrix for Mindoro."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, rasterize_observation_layer, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.io import get_case_output_dir

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import rasterio
except ImportError:  # pragma: no cover
    rasterio = None


MARCH6_RECOVERY_DIR_NAME = "march6_recovery_sensitivity"
STRICT_VALIDATION_DATE = "2023-03-06"
SUPPORT_BUFFER_KM = [0, 1, 2, 5, 10, 15, 20, 25]
BRANCH_PRECEDENCE = {"R1_previous": 1, "A2_24H": 2, "A2_48H": 3, "A2_72H": 4}
LOCKED_OUTPUT_FILES = [
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_pairing_manifest.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_fss_by_date_window.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_summary.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/public_obs_appendix/appendix_eventcorridor_pairing_manifest.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/public_obs_appendix/appendix_eventcorridor_fss_by_window.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/public_obs_appendix/appendix_eventcorridor_diagnostics.csv"),
]


@dataclass(frozen=True)
class ThresholdSpec:
    label: str
    threshold: float
    any_presence: bool = False

    @property
    def sort_value(self) -> float:
        return 0.0 if self.any_presence else float(self.threshold)

    @property
    def comparison_rule(self) -> str:
        if self.any_presence:
            return "probability > 0.0"
        return f"probability >= {float(self.threshold):.2f}"


@dataclass(frozen=True)
class RecoveryBranch:
    branch_id: str
    branch_label: str
    precedence: int
    source_summary_path: Path
    forecast_path: Path
    probability_path: Path
    notes: str


THRESHOLD_LADDER = [
    ThresholdSpec(label="p50", threshold=0.50),
    ThresholdSpec(label="p40", threshold=0.40),
    ThresholdSpec(label="p30", threshold=0.30),
    ThresholdSpec(label="p20", threshold=0.20),
    ThresholdSpec(label="p10", threshold=0.10),
    ThresholdSpec(label="p05", threshold=0.05),
    ThresholdSpec(label="any_presence", threshold=0.0, any_presence=True),
]


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required CSV not found: {path}")
    return pd.read_csv(path)


def _read_raster(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required for march6_recovery_sensitivity.")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _buffer_label(buffer_km: int) -> str:
    return f"{int(buffer_km):02d}km"


def _mean_fss(row: pd.Series | dict) -> float:
    values = []
    for window in OFFICIAL_PHASE3B_WINDOWS_KM:
        try:
            value = float(row.get(f"fss_{window}km", np.nan))
        except (TypeError, ValueError):
            value = np.nan
        if np.isfinite(value):
            values.append(value)
    return float(np.mean(values)) if values else 0.0


def build_threshold_mask(probability: np.ndarray, spec: ThresholdSpec, sea_mask: np.ndarray | None) -> np.ndarray:
    clean = np.nan_to_num(np.asarray(probability, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    if spec.any_presence:
        mask = (clean > 0.0).astype(np.float32)
    else:
        mask = (clean >= float(spec.threshold)).astype(np.float32)
    mask = apply_ocean_mask(mask, sea_mask=sea_mask, fill_value=0.0)
    return mask.astype(np.float32)


def build_support_geometry_gdf(source_gdf, *, target_crs: str, buffer_km: int):
    if gpd is None:
        raise ImportError("geopandas is required for march6_recovery_sensitivity.")
    if source_gdf is None or source_gdf.empty:
        raise RuntimeError("March 6 support construction requires a non-empty source geometry.")
    if source_gdf.crs is None:
        raise RuntimeError("March 6 support construction requires a source geometry CRS.")

    working = source_gdf.to_crs(target_crs) if str(source_gdf.crs) != str(target_crs) else source_gdf.copy()
    valid = working.dropna(subset=["geometry"]).copy()
    valid = valid[~valid.geometry.is_empty]
    if valid.empty:
        raise RuntimeError("March 6 support construction requires at least one valid geometry.")

    dissolved = valid.geometry.union_all() if hasattr(valid.geometry, "union_all") else valid.geometry.unary_union
    if int(buffer_km) > 0:
        dissolved = dissolved.buffer(float(buffer_km) * 1000.0)
    repaired = dissolved.buffer(0)
    if repaired.is_empty:
        raise RuntimeError(f"March 6 support geometry became empty after {buffer_km} km buffering.")
    return gpd.GeoDataFrame(geometry=[repaired], crs=working.crs)


def build_support_mask(source_gdf, *, grid: GridBuilder, sea_mask: np.ndarray | None, buffer_km: int):
    support_gdf = build_support_geometry_gdf(source_gdf, target_crs=grid.crs, buffer_km=buffer_km)
    mask = rasterize_observation_layer(support_gdf, grid)
    mask = apply_ocean_mask(mask, sea_mask=sea_mask, fill_value=0.0)
    if int(np.count_nonzero(mask > 0)) == 0:
        raise RuntimeError(f"March 6 support mask buffered to {buffer_km} km produced zero ocean cells.")
    return support_gdf, mask.astype(np.float32)


def build_matrix_index(branch_ids: list[str]) -> pd.DataFrame:
    rows = []
    for branch_id in branch_ids:
        for spec in THRESHOLD_LADDER:
            for buffer_km in SUPPORT_BUFFER_KM:
                rows.append({"branch_id": branch_id, "threshold_label": spec.label, "buffer_km": int(buffer_km)})
    return pd.DataFrame(rows)


def rank_recovery_rows(summary_df: pd.DataFrame) -> pd.DataFrame:
    working = summary_df.copy()
    working["mean_fss"] = working.apply(_mean_fss, axis=1)
    working["threshold_sort_value"] = pd.to_numeric(working["threshold_value"], errors="coerce").fillna(0.0)
    working["branch_precedence"] = working["branch_id"].map(BRANCH_PRECEDENCE).fillna(999).astype(int)
    working = working.sort_values(
        ["mean_fss", "fss_1km", "buffer_km", "threshold_sort_value", "branch_precedence"],
        ascending=[False, False, True, False, True],
    ).reset_index(drop=True)
    working["recovery_rank"] = np.arange(1, len(working) + 1)
    working["is_best_overall_candidate"] = False
    working["is_best_r1_candidate"] = False
    if not working.empty:
        working.loc[0, "is_best_overall_candidate"] = True
    r1 = working[working["branch_id"] == "R1_previous"]
    if not r1.empty:
        working.loc[r1.index[0], "is_best_r1_candidate"] = True
    return working


class March6RecoverySensitivityService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("march6_recovery_sensitivity is only supported for official Mindoro workflows.")
        if gpd is None:
            raise ImportError("geopandas is required for march6_recovery_sensitivity.")
        if rasterio is None:
            raise ImportError("rasterio is required for march6_recovery_sensitivity.")
        if plt is None:
            raise ImportError("matplotlib is required for march6_recovery_sensitivity.")

        self.case_output = get_case_output_dir(self.case.run_name)
        self.output_dir = self.case_output / MARCH6_RECOVERY_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.products_dir = self.output_dir / "products"
        self.observations_dir = self.output_dir / "observations"
        self.precheck_dir = self.output_dir / "precheck"
        self.qa_dir = self.output_dir / "qa"
        self.overlays_dir = self.output_dir / "overlays"
        for path in (self.products_dir, self.observations_dir, self.precheck_dir, self.qa_dir, self.overlays_dir):
            path.mkdir(parents=True, exist_ok=True)

        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.march6_vector_path = self.case.validation_layer.processed_vector_path(self.case.run_name)
        self.locked_hashes_before = self._snapshot_locked_outputs()

    def _snapshot_locked_outputs(self) -> dict[str, str]:
        hashes: dict[str, str] = {}
        for path in LOCKED_OUTPUT_FILES:
            if path.exists():
                hashes[str(path)] = _sha256(path)
        return hashes

    def _verify_locked_outputs_unchanged(self) -> None:
        current = self._snapshot_locked_outputs()
        if current != self.locked_hashes_before:
            raise RuntimeError("march6_recovery_sensitivity modified locked official outputs.")

    def _load_branches(self) -> list[RecoveryBranch]:
        official_summary_path = self.case_output / "official_rerun_r1" / "official_rerun_r1_summary.csv"
        official_summary = _read_csv(official_summary_path)
        r1_row = official_summary[
            (official_summary["scenario_id"].astype(str) == "R1")
            & (official_summary["pair_role"].astype(str) == "strict_march6")
        ]
        if r1_row.empty:
            raise RuntimeError("official_rerun_r1 summary is missing the R1 strict March 6 row.")
        r1 = r1_row.iloc[0]

        source_summary_path = self.case_output / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_summary.csv"
        source_history_summary = _read_csv(source_summary_path)

        branches = [
            RecoveryBranch(
                branch_id="R1_previous",
                branch_label="R1 previous wet-location retention",
                precedence=BRANCH_PRECEDENCE["R1_previous"],
                source_summary_path=official_summary_path,
                forecast_path=Path(str(r1["forecast_path"])),
                probability_path=Path(str(r1["probability_path"])),
                notes="Best official-like retention branch already selected by official_rerun_r1.",
            )
        ]
        for scenario_id in ["A2_24H", "A2_48H", "A2_72H"]:
            row = source_history_summary[
                (source_history_summary["scenario_id"].astype(str) == scenario_id)
                & (source_history_summary["pair_role"].astype(str) == "strict_march6")
            ]
            if row.empty:
                raise RuntimeError(f"source_history_reconstruction_r1 summary is missing {scenario_id} strict March 6.")
            record = row.iloc[0]
            branches.append(
                RecoveryBranch(
                    branch_id=scenario_id,
                    branch_label=scenario_id.replace("_", " "),
                    precedence=BRANCH_PRECEDENCE[scenario_id],
                    source_summary_path=source_summary_path,
                    forecast_path=Path(str(record["forecast_path"])),
                    probability_path=Path(str(record["probability_path"])),
                    notes=f"Source-history reconstruction candidate {scenario_id}.",
                )
            )
        for branch in branches:
            if not branch.probability_path.exists():
                raise FileNotFoundError(f"March 6 probability product missing for {branch.branch_id}: {branch.probability_path}")
            if not branch.forecast_path.exists():
                raise FileNotFoundError(f"March 6 p50 product missing for {branch.branch_id}: {branch.forecast_path}")
        return branches

    def _materialize_support_ladder(self) -> dict[int, dict[str, Path]]:
        if not self.march6_vector_path.exists():
            raise FileNotFoundError(f"March 6 processed vector not found: {self.march6_vector_path}")
        source_gdf = gpd.read_file(self.march6_vector_path)
        outputs: dict[int, dict[str, Path]] = {}
        for buffer_km in SUPPORT_BUFFER_KM:
            label = _buffer_label(buffer_km)
            vector_path = self.observations_dir / f"march6_support_{label}_vector.gpkg"
            mask_path = self.observations_dir / f"march6_support_{label}_obs_mask.tif"
            support_gdf, mask = build_support_mask(source_gdf, grid=self.grid, sea_mask=self.sea_mask, buffer_km=buffer_km)
            if vector_path.exists():
                vector_path.unlink()
            support_gdf[["geometry"]].to_file(vector_path, driver="GPKG")
            save_raster(self.grid, mask.astype(np.float32), mask_path)
            outputs[int(buffer_km)] = {"vector": vector_path, "mask": mask_path}
        return outputs

    def _materialize_threshold_products(self, branches: list[RecoveryBranch]) -> dict[str, dict[str, Path]]:
        outputs: dict[str, dict[str, Path]] = {}
        for branch in branches:
            probability = _read_raster(branch.probability_path)
            branch_dir = self.products_dir / branch.branch_id
            branch_dir.mkdir(parents=True, exist_ok=True)
            label_map: dict[str, Path] = {}
            for spec in THRESHOLD_LADDER:
                out_path = branch_dir / f"mask_{spec.label}_{STRICT_VALIDATION_DATE}_datecomposite.tif"
                mask = build_threshold_mask(probability, spec, self.sea_mask)
                save_raster(self.grid, mask.astype(np.float32), out_path)
                label_map[spec.label] = out_path
            outputs[branch.branch_id] = label_map
        return outputs

    def _score_matrix(
        self,
        branches: list[RecoveryBranch],
        threshold_products: dict[str, dict[str, Path]],
        support_products: dict[int, dict[str, Path]],
    ) -> pd.DataFrame:
        rows = []
        expected = build_matrix_index([branch.branch_id for branch in branches])
        for branch in branches:
            for spec in THRESHOLD_LADDER:
                forecast_path = threshold_products[branch.branch_id][spec.label]
                for buffer_km in SUPPORT_BUFFER_KM:
                    support_path = support_products[int(buffer_km)]["mask"]
                    pair_id = f"{branch.branch_id}_{spec.label}_support_{_buffer_label(buffer_km)}"
                    precheck = precheck_same_grid(forecast_path, support_path, report_base_path=self.precheck_dir / pair_id)
                    if not precheck.passed:
                        raise RuntimeError(
                            f"March 6 recovery same-grid precheck failed for {pair_id}: {precheck.json_report_path}"
                        )
                    forecast_mask = self.helper._load_binary_score_mask(forecast_path)
                    obs_mask = self.helper._load_binary_score_mask(support_path)
                    diagnostics = self.helper._compute_mask_diagnostics(forecast_mask, obs_mask)
                    row = {
                        "pair_id": pair_id,
                        "branch_id": branch.branch_id,
                        "branch_label": branch.branch_label,
                        "branch_precedence": int(branch.precedence),
                        "threshold_label": spec.label,
                        "threshold_value": float(spec.sort_value),
                        "threshold_rule": spec.comparison_rule,
                        "buffer_km": int(buffer_km),
                        "buffer_m": int(buffer_km) * 1000,
                        "obs_date": STRICT_VALIDATION_DATE,
                        "forecast_path": str(forecast_path),
                        "observation_path": str(support_path),
                        "observation_vector_path": str(support_products[int(buffer_km)]["vector"]),
                        "source_probability_path": str(branch.probability_path),
                        "source_summary_path": str(branch.source_summary_path),
                        "appendix_only": True,
                        "precheck_csv": str(precheck.csv_report_path),
                        "precheck_json": str(precheck.json_report_path),
                        **diagnostics,
                    }
                    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                        window_cells = self.helper._window_km_to_cells(int(window_km))
                        row[f"fss_{window_km}km"] = float(
                            np.clip(
                                calculate_fss(
                                    forecast_mask,
                                    obs_mask,
                                    window=window_cells,
                                    valid_mask=self.valid_mask,
                                ),
                                0.0,
                                1.0,
                            )
                        )
                    rows.append(row)
        matrix_df = pd.DataFrame(rows)
        if len(matrix_df) != len(expected):
            raise RuntimeError(
                f"March 6 recovery matrix is incomplete: expected {len(expected)} rows but scored {len(matrix_df)}."
            )
        return matrix_df

    def _write_heatmap(self, ranked: pd.DataFrame, *, value_col: str, title: str, out_path: Path) -> Path | None:
        if plt is None:
            return None
        branch_order = [branch_id for branch_id, _ in sorted(BRANCH_PRECEDENCE.items(), key=lambda item: item[1])]
        threshold_order = [spec.label for spec in THRESHOLD_LADDER]
        fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True, sharey=True)
        axes_array = axes.flatten()
        for ax, branch_id in zip(axes_array, branch_order):
            subset = ranked[ranked["branch_id"] == branch_id].copy()
            subset["buffer_km"] = pd.to_numeric(subset["buffer_km"], errors="coerce")
            pivot = subset.pivot(index="buffer_km", columns="threshold_label", values=value_col).reindex(
                index=SUPPORT_BUFFER_KM,
                columns=threshold_order,
            )
            values = pivot.to_numpy(dtype=float)
            image = ax.imshow(values, aspect="auto", origin="lower")
            ax.set_title(branch_id)
            ax.set_xticks(np.arange(len(threshold_order)))
            ax.set_xticklabels(threshold_order, rotation=45, ha="right")
            ax.set_yticks(np.arange(len(SUPPORT_BUFFER_KM)))
            ax.set_yticklabels([str(value) for value in SUPPORT_BUFFER_KM])
            ax.set_xlabel("Threshold")
            ax.set_ylabel("Buffer km")
            fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
        fig.suptitle(title)
        fig.tight_layout()
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    @staticmethod
    def _render_overlay(ax, forecast: np.ndarray, obs: np.ndarray, title: str) -> None:
        canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
        canvas[obs > 0] = [0.1, 0.35, 0.95]
        canvas[forecast > 0] = [0.95, 0.35, 0.1]
        canvas[(forecast > 0) & (obs > 0)] = [0.1, 0.65, 0.25]
        ax.imshow(canvas, origin="upper")
        ax.set_title(title)
        ax.set_axis_off()

    def _write_overlay(self, row: pd.Series, out_path: Path, title: str) -> Path | None:
        if plt is None:
            return None
        fig, ax = plt.subplots(figsize=(6, 6))
        forecast = self.helper._load_binary_score_mask(Path(str(row["forecast_path"])))
        obs = self.helper._load_binary_score_mask(Path(str(row["observation_path"])))
        self._render_overlay(ax, forecast, obs, title)
        fig.tight_layout()
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    def _compute_next_buffer_suggestions(self, ranked: pd.DataFrame) -> list[dict[str, Any]]:
        suggestions = []
        if (pd.to_numeric(ranked["iou"], errors="coerce").fillna(0.0) > 0.0).any():
            return suggestions
        for branch_id, precedence in sorted(BRANCH_PRECEDENCE.items(), key=lambda item: item[1]):
            capped = ranked[(ranked["branch_id"] == branch_id) & (ranked["buffer_km"].astype(int) == max(SUPPORT_BUFFER_KM))].copy()
            if capped.empty:
                continue
            capped["nearest_distance_to_obs_m"] = pd.to_numeric(capped["nearest_distance_to_obs_m"], errors="coerce")
            capped = capped.sort_values(["nearest_distance_to_obs_m", "threshold_value"], ascending=[True, False])
            best = capped.iloc[0]
            nearest = best["nearest_distance_to_obs_m"]
            if not np.isfinite(nearest):
                continue
            next_buffer_km = int(max(SUPPORT_BUFFER_KM) + math.ceil(float(nearest) / 1000.0))
            suggestions.append(
                {
                    "branch_id": branch_id,
                    "threshold_label": str(best["threshold_label"]),
                    "nearest_distance_to_obs_m_at_25km": float(nearest),
                    "next_buffer_km_to_consider": next_buffer_km,
                }
            )
        return suggestions

    def _write_report(
        self,
        ranked: pd.DataFrame,
        best_r1: pd.Series,
        best_overall: pd.Series,
        next_buffers: list[dict[str, Any]],
        excluded_notes: list[str],
    ) -> Path:
        report_path = self.output_dir / "march6_recovery_report.md"
        zero_buffer_r1 = ranked[(ranked["branch_id"] == "R1_previous") & (ranked["buffer_km"].astype(int) == 0)].copy()
        zero_buffer_r1 = zero_buffer_r1.sort_values(["threshold_value"], ascending=False)
        any_overlap = bool((pd.to_numeric(ranked["iou"], errors="coerce").fillna(0.0) > 0.0).any())

        lines = [
            "# March 6 Recovery Sensitivity",
            "",
            "This is an appendix-only recovery matrix. It does not replace or relabel the frozen strict March 6 official result.",
            "",
            "## Scope",
            "",
            "- Candidate branches: `R1_previous`, `A2_24H`, `A2_48H`, `A2_72H`",
            "- Threshold ladder: `p50`, `p40`, `p30`, `p20`, `p10`, `p05`, `any_presence`",
            f"- Observation-support ladder (km): {', '.join(str(value) for value in SUPPORT_BUFFER_KM)}",
            "",
            "## Repo Truth",
            "",
        ]
        if zero_buffer_r1.empty:
            lines.append("- R1 zero-buffer threshold ladder could not be summarized.")
        else:
            overlaps = pd.to_numeric(zero_buffer_r1["iou"], errors="coerce").fillna(0.0)
            if not (overlaps > 0.0).any():
                lines.append("- Current repo truth remains that `R1` `p10` through `any_presence` still do not overlap March 6 at `0 km` support.")
            else:
                lines.append("- At least one zero-buffer `R1` threshold row achieved non-zero March 6 overlap.")
            for _, row in zero_buffer_r1.iterrows():
                lines.append(
                    f"- `R1 {row['threshold_label']}` @ `0 km`: "
                    f"FSS(1/3/5/10)={float(row['fss_1km']):.4f}, {float(row['fss_3km']):.4f}, "
                    f"{float(row['fss_5km']):.4f}, {float(row['fss_10km']):.4f}; "
                    f"IoU={float(row['iou']):.4f}; nearest={float(row['nearest_distance_to_obs_m']):.1f} m"
                )

        lines.extend(
            [
                "",
                "## Winners",
                "",
                (
                    f"- Best `R1` candidate: `{best_r1['threshold_label']}` with `{int(best_r1['buffer_km'])} km` support; "
                    f"mean FSS={float(best_r1['mean_fss']):.6f}; IoU={float(best_r1['iou']):.4f}; "
                    f"nearest distance={float(best_r1['nearest_distance_to_obs_m']):.1f} m."
                ),
                (
                    f"- Best overall candidate: `{best_overall['branch_id']}` + `{best_overall['threshold_label']}` + "
                    f"`{int(best_overall['buffer_km'])} km`; mean FSS={float(best_overall['mean_fss']):.6f}; "
                    f"IoU={float(best_overall['iou']):.4f}; nearest distance={float(best_overall['nearest_distance_to_obs_m']):.1f} m."
                ),
                f"- Any non-zero strict March 6 overlap within the `<=25 km` cap: `{'yes' if any_overlap else 'no'}`",
                "",
                "## Excluded Branches",
                "",
            ]
        )
        for note in excluded_notes:
            lines.append(f"- {note}")

        if not any_overlap:
            lines.extend(["", "## Next Buffer Evidence", ""])
            if not next_buffers:
                lines.append("- No next-buffer suggestions were available.")
            for item in next_buffers:
                lines.append(
                    f"- `{item['branch_id']}` with `{item['threshold_label']}` still misses after `25 km`; "
                    f"nearest residual distance is `{float(item['nearest_distance_to_obs_m_at_25km']):.1f} m`, "
                    f"so the next buffer to consider would be about `{int(item['next_buffer_km_to_consider'])} km`."
                )

        _write_text(report_path, "\n".join(lines) + "\n")
        return report_path

    def run(self) -> dict[str, Any]:
        branches = self._load_branches()
        support_products = self._materialize_support_ladder()
        threshold_products = self._materialize_threshold_products(branches)
        matrix_df = self._score_matrix(branches, threshold_products, support_products)
        ranked_df = rank_recovery_rows(matrix_df)
        matrix_path = self.output_dir / "march6_recovery_matrix.csv"
        summary_path = self.output_dir / "march6_recovery_summary.csv"
        _write_csv(matrix_path, matrix_df)
        _write_csv(summary_path, ranked_df)

        best_overall = ranked_df.iloc[0]
        best_r1 = ranked_df[ranked_df["branch_id"] == "R1_previous"].iloc[0]
        best_by_branch = {
            branch_id: ranked_df[ranked_df["branch_id"] == branch_id].iloc[0]
            for branch_id in BRANCH_PRECEDENCE
            if not ranked_df[ranked_df["branch_id"] == branch_id].empty
        }
        next_buffers = self._compute_next_buffer_suggestions(ranked_df)
        excluded_notes = [
            "A1 source-point same-start initialization is excluded because current saved strict March 6 rows collapse to zero forecast cells.",
            "HYCOM recipe branches are excluded because current saved strict March 6 rows remain zero-overlap while also moving farther away than the selected candidate set.",
            "R3 no-land-interaction remains excluded because it is diagnostic-only and not a thesis-final candidate.",
        ]

        qa_mean = self._write_heatmap(ranked_df, value_col="mean_fss", title="March 6 recovery mean FSS", out_path=self.qa_dir / "qa_march6_recovery_mean_fss.png")
        qa_nearest = self._write_heatmap(ranked_df, value_col="nearest_distance_to_obs_m", title="March 6 recovery nearest distance to obs (m)", out_path=self.qa_dir / "qa_march6_recovery_nearest_distance.png")

        overlay_paths = {
            "strict_r1_p50": self._write_overlay(
                ranked_df[
                    (ranked_df["branch_id"] == "R1_previous")
                    & (ranked_df["threshold_label"] == "p50")
                    & (ranked_df["buffer_km"].astype(int) == 0)
                ].iloc[0],
                self.overlays_dir / "qa_r1_p50_strict_overlay.png",
                "R1 p50 vs strict March 6",
            ),
            "best_r1_candidate": self._write_overlay(best_r1, self.overlays_dir / "qa_best_r1_candidate_overlay.png", "Best R1 March 6 recovery candidate"),
            "best_overall_candidate": self._write_overlay(best_overall, self.overlays_dir / "qa_best_overall_candidate_overlay.png", "Best overall March 6 recovery candidate"),
        }
        for branch_id in ["A2_24H", "A2_48H", "A2_72H"]:
            if branch_id in best_by_branch:
                overlay_paths[f"best_{branch_id.lower()}"] = self._write_overlay(
                    best_by_branch[branch_id],
                    self.overlays_dir / f"qa_best_{branch_id.lower()}_overlay.png",
                    f"Best {branch_id} March 6 recovery candidate",
                )

        report_path = self._write_report(ranked_df, best_r1, best_overall, next_buffers, excluded_notes)
        manifest = {
            "phase": "march6_recovery_sensitivity",
            "appendix_only": True,
            "run_name": self.case.run_name,
            "strict_official_main_unchanged": True,
            "buffer_cap_km": max(SUPPORT_BUFFER_KM),
            "threshold_ladder": [asdict(spec) | {"comparison_rule": spec.comparison_rule} for spec in THRESHOLD_LADDER],
            "support_buffer_km": list(SUPPORT_BUFFER_KM),
            "candidate_branches": [asdict(branch) for branch in branches],
            "excluded_branch_notes": excluded_notes,
            "non_zero_overlap_found": bool((pd.to_numeric(ranked_df["iou"], errors="coerce").fillna(0.0) > 0.0).any()),
            "best_r1_candidate": best_r1.to_dict(),
            "best_overall_candidate": best_overall.to_dict(),
            "next_buffer_suggestions_if_needed": next_buffers,
            "guardrails": {
                "phase3b_unchanged": True,
                "public_obs_appendix_eventcorridor_unchanged": True,
                "final_validation_package_unchanged": True,
                "phase5_docs_unchanged": True,
            },
            "artifacts": {
                "matrix_csv": str(matrix_path),
                "summary_csv": str(summary_path),
                "report_md": str(report_path),
                "qa_mean_fss": str(qa_mean) if qa_mean else "",
                "qa_nearest_distance": str(qa_nearest) if qa_nearest else "",
                "overlay_pngs": {key: str(value) for key, value in overlay_paths.items() if value},
            },
        }
        manifest_path = self.output_dir / "march6_recovery_manifest.json"
        _write_json(manifest_path, manifest)

        self._verify_locked_outputs_unchanged()
        return {
            "output_dir": self.output_dir,
            "matrix_csv": matrix_path,
            "summary_csv": summary_path,
            "report_md": report_path,
            "run_manifest_json": manifest_path,
            "best_r1_candidate": best_r1.to_dict(),
            "best_overall_candidate": best_overall.to_dict(),
        }


def run_march6_recovery_sensitivity() -> dict[str, Any]:
    return March6RecoverySensitivityService().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_march6_recovery_sensitivity(), indent=2, default=_json_default))
