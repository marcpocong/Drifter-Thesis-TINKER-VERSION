"""Mindoro-local transport-recipe experiment staged from historical Phase 1 outputs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from src.core.base import BaseService
from src.core.case_context import get_case_context
from src.services.recipe_sensitivity_r1_multibranch import (
    RecipeSensitivityR1MultibranchService,
    select_promotable_opendrift_recipe,
)
from src.utils.io import get_case_output_dir

MINDORO_LOCAL_RECIPE_EXPERIMENT_DIR_NAME = "mindoro_local_recipe_experiment"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


def _relative(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _sha256(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mean_fss(row: pd.Series | dict[str, Any]) -> float:
    values = []
    for window in (1, 3, 5, 10):
        try:
            value = float(row.get(f"fss_{window}km", np.nan))
        except (TypeError, ValueError):
            value = np.nan
        if np.isfinite(value):
            values.append(value)
    return float(np.mean(values)) if values else 0.0


def _rank_event_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    ranked = rows.copy()
    if "mean_fss" not in ranked.columns:
        ranked["mean_fss"] = ranked.apply(_mean_fss, axis=1)
    for column in ("iou", "dice", "forecast_nonzero_cells", "nearest_distance_to_obs_m"):
        ranked[column] = pd.to_numeric(ranked.get(column, np.nan), errors="coerce")
    return ranked.sort_values(
        ["mean_fss", "iou", "dice", "forecast_nonzero_cells", "nearest_distance_to_obs_m"],
        ascending=[False, False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)


def _path_or_placeholder(repo_root: Path, value: Any) -> str:
    if not value:
        return ""
    return _relative(repo_root, Path(value))


class MindoroLocalRecipeExperimentService(BaseService):
    def __init__(
        self,
        *,
        repo_root: str | Path | None = None,
        baseline_path: str | Path = "config/phase1_baseline_selection.yaml",
        phase1_output_dir: str | Path = "output/phase1_production_rerun",
        output_dir_name: str = MINDORO_LOCAL_RECIPE_EXPERIMENT_DIR_NAME,
        distance_threshold_km: float = 250.0,
        comparison_output_slug: str | None = None,
        comparison_service_factory=RecipeSensitivityR1MultibranchService,
    ):
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[2])
        self.case = get_case_context()
        if not self.case.is_official or str(self.case.run_name) != "CASE_MINDORO_RETRO_2023":
            raise RuntimeError(
                "mindoro_local_recipe_experiment is only supported for the official Mindoro retrospective workflow."
            )

        self.baseline_path = self.repo_root / Path(baseline_path)
        self.phase1_output_dir = self.repo_root / Path(phase1_output_dir)
        self.case_output_dir = self.repo_root / get_case_output_dir(self.case.run_name)
        self.output_dir_name = str(output_dir_name)
        self.output_dir = self.case_output_dir / self.output_dir_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.distance_threshold_km = float(distance_threshold_km)
        self.comparison_output_slug = comparison_output_slug or (
            f"{self.output_dir_name}/event_recipe_comparison"
        )
        self.comparison_service_factory = comparison_service_factory
        self.paths = {
            "subset_registry": self.output_dir / "mindoro_local_accepted_subset_registry.csv",
            "recipe_summary": self.output_dir / "mindoro_local_recipe_summary.csv",
            "recipe_ranking": self.output_dir / "mindoro_local_recipe_ranking.csv",
            "candidate_baseline": self.output_dir / "mindoro_local_candidate_baseline.yaml",
            "report": self.output_dir / "mindoro_local_recipe_experiment_report.md",
            "manifest": self.output_dir / "mindoro_local_recipe_experiment_manifest.json",
        }

    @staticmethod
    def load_source_point(source_point_path: str | Path) -> tuple[float, float]:
        path = Path(source_point_path)
        if not path.exists():
            raise FileNotFoundError(f"Mindoro source-point metadata not found: {path}")
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle) or {}
        features = payload.get("features") or []
        if not features:
            raise RuntimeError(f"Mindoro source-point metadata has no features: {path}")
        geometry = dict(features[0].get("geometry") or {})
        coordinates = geometry.get("coordinates") or []
        if len(coordinates) < 2:
            raise RuntimeError(f"Mindoro source-point metadata is missing point coordinates: {path}")
        return float(coordinates[0]), float(coordinates[1])

    @staticmethod
    def _haversine_km(lat1: pd.Series, lon1: pd.Series, lat2: float, lon2: float) -> pd.Series:
        lat1_rad = np.radians(pd.to_numeric(lat1, errors="coerce").astype(float))
        lon1_rad = np.radians(pd.to_numeric(lon1, errors="coerce").astype(float))
        lat2_rad = np.radians(float(lat2))
        lon2_rad = np.radians(float(lon2))
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
        return 6371.0088 * 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))

    @classmethod
    def build_local_subset_registry(
        cls,
        accepted_df: pd.DataFrame,
        *,
        source_lon: float,
        source_lat: float,
        threshold_km: float,
    ) -> pd.DataFrame:
        if accepted_df.empty:
            raise RuntimeError("The Phase 1 accepted-segment registry is empty.")

        working = accepted_df.copy()
        if "segment_status" in working.columns:
            working = working[working["segment_status"].astype(str) == "accepted"].copy()
        if working.empty:
            raise RuntimeError("No accepted segments are available for the Mindoro-local subset.")

        start_distance = cls._haversine_km(working["start_lat"], working["start_lon"], source_lat, source_lon)
        end_distance = cls._haversine_km(working["end_lat"], working["end_lon"], source_lat, source_lon)
        working["mindoro_source_lon"] = float(source_lon)
        working["mindoro_source_lat"] = float(source_lat)
        working["distance_start_km"] = start_distance.astype(float)
        working["distance_end_km"] = end_distance.astype(float)
        working["nearest_endpoint_distance_km"] = np.minimum(start_distance, end_distance).astype(float)
        working["nearest_endpoint_label"] = np.where(
            working["distance_start_km"] <= working["distance_end_km"],
            "start",
            "end",
        )

        subset = working[working["nearest_endpoint_distance_km"] <= float(threshold_km)].copy()
        subset = subset.sort_values(
            ["nearest_endpoint_distance_km", "start_time_utc", "segment_id"],
            ascending=[True, True, True],
        ).reset_index(drop=True)
        if subset.empty:
            raise RuntimeError(
                f"No accepted Phase 1 segments fall within {float(threshold_km):.1f} km of the Mindoro source point."
            )
        return subset

    @staticmethod
    def build_local_recipe_tables(
        segment_metrics_df: pd.DataFrame,
        local_segment_ids: list[str] | pd.Series,
        *,
        recipe_rank_pool: str = "mindoro_local_accepted_subset_250km",
    ) -> tuple[pd.DataFrame, pd.DataFrame, str]:
        if segment_metrics_df.empty:
            raise RuntimeError("The Phase 1 segment-metrics table is empty.")

        local_ids = {str(value) for value in pd.Series(local_segment_ids).astype(str).tolist()}
        working = segment_metrics_df[segment_metrics_df["segment_id"].astype(str).isin(local_ids)].copy()
        if working.empty:
            raise RuntimeError("No Phase 1 metrics rows matched the Mindoro-local accepted subset.")

        summary_rows: list[dict[str, Any]] = []
        for recipe, group in working.groupby("recipe", sort=True):
            valid_group = group[group["validity_flag"].astype(str) == "valid"].copy()
            if valid_group.empty:
                raise RuntimeError(f"Recipe {recipe} has zero valid Phase 1 rows in the Mindoro-local subset.")
            summary_rows.append(
                {
                    "recipe": str(recipe),
                    "recipe_rank_pool": recipe_rank_pool,
                    "segment_count": int(len(group)),
                    "valid_segment_count": int(len(valid_group)),
                    "invalid_segment_count": int(len(group) - len(valid_group)),
                    "mean_ncs_score": float(valid_group["ncs_score"].mean()),
                    "median_ncs_score": float(valid_group["ncs_score"].median()),
                    "std_ncs_score": float(valid_group["ncs_score"].std(ddof=0)),
                    "min_ncs_score": float(valid_group["ncs_score"].min()),
                    "max_ncs_score": float(valid_group["ncs_score"].max()),
                    "is_gfs_recipe": bool(str(recipe).endswith("_gfs")),
                }
            )

        recipe_summary_df = pd.DataFrame(summary_rows).sort_values("recipe").reset_index(drop=True)
        recipe_ranking_df = recipe_summary_df.sort_values(
            ["mean_ncs_score", "median_ncs_score", "invalid_segment_count", "recipe"],
            ascending=[True, True, True, True],
        ).reset_index(drop=True)
        recipe_ranking_df.insert(0, "rank", np.arange(1, len(recipe_ranking_df) + 1))
        winning_recipe = str(recipe_ranking_df.iloc[0]["recipe"])
        return recipe_summary_df, recipe_ranking_df, winning_recipe

    def _load_phase1_accepted_registry(self) -> pd.DataFrame:
        path = self.phase1_output_dir / "phase1_accepted_segment_registry.csv"
        if not path.exists():
            raise FileNotFoundError(f"Phase 1 accepted-segment registry not found: {path}")
        return pd.read_csv(path)

    def _load_phase1_segment_metrics(self) -> pd.DataFrame:
        path = self.phase1_output_dir / "phase1_segment_metrics.csv"
        if not path.exists():
            raise FileNotFoundError(f"Phase 1 segment-metrics table not found: {path}")
        return pd.read_csv(path)

    def _load_canonical_baseline(self) -> dict[str, Any]:
        if not self.baseline_path.exists():
            raise FileNotFoundError(f"Canonical baseline selection not found: {self.baseline_path}")
        with open(self.baseline_path, "r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        selected_recipe = str(payload.get("selected_recipe") or "").strip()
        if not selected_recipe:
            raise RuntimeError(f"{self.baseline_path} is missing selected_recipe.")
        return payload

    def _source_point_path(self) -> Path:
        return self.repo_root / "data" / "arcgis" / self.case.run_name / "source_point_metadata.geojson"

    def _build_candidate_baseline_payload(
        self,
        *,
        winning_recipe: str,
        subset_df: pd.DataFrame,
        source_lon: float,
        source_lat: float,
    ) -> dict[str, Any]:
        return {
            "baseline_id": "mindoro_local_recipe_candidate_250km_v1",
            "description": (
                "Staged Mindoro-local forcing-recipe candidate built from accepted historical Phase 1 "
                "segments within 250 km of the Mindoro source point."
            ),
            "selected_recipe": winning_recipe,
            "source_kind": "staged_mindoro_local_candidate",
            "status_flag": "provisional",
            "valid": False,
            "provisional": True,
            "rerun_required": False,
            "promotion_required": True,
            "selection_basis": (
                "Historical-local accepted-subset ranking using the nearest accepted Phase 1 segment endpoint "
                f"within {self.distance_threshold_km:.0f} km of the Mindoro source point."
            ),
            "workflow_scope": ["mindoro_retro_2023"],
            "historical_validation_artifacts": [
                _relative(self.repo_root, self.paths["subset_registry"]),
                _relative(self.repo_root, self.paths["recipe_summary"]),
                _relative(self.repo_root, self.paths["recipe_ranking"]),
            ],
            "notes": [
                "This artifact is staged only and does not overwrite config/phase1_baseline_selection.yaml.",
                "Trial Mindoro reruns may set BASELINE_SELECTION_PATH to this candidate artifact explicitly.",
            ],
            "experiment_context": {
                "mindoro_source_point_lon": float(source_lon),
                "mindoro_source_point_lat": float(source_lat),
                "distance_threshold_km": float(self.distance_threshold_km),
                "accepted_subset_segment_count": int(len(subset_df)),
                "accepted_subset_drifter_count": int(subset_df["drifter_id"].astype(str).nunique()),
                "accepted_subset_date_windows_utc": subset_df[["segment_id", "start_time_utc", "end_time_utc"]].to_dict(
                    orient="records"
                ),
            },
        }

    def _select_best_event_row(self, summary_df: pd.DataFrame, recipe_id: str) -> dict[str, Any]:
        candidates = summary_df[
            (summary_df["pair_role"].astype(str) == "eventcorridor_march4_6")
            & (summary_df["model_family"].astype(str) == "OpenDrift")
            & (summary_df["branch_id"].astype(str) == "B")
            & (summary_df["recipe_id"].astype(str) == str(recipe_id))
        ].copy()
        if candidates.empty:
            return {}
        winner = _rank_event_rows(candidates).iloc[0].to_dict()
        winner["mean_fss"] = _mean_fss(winner)
        return winner

    def _compare_candidate_vs_frozen(
        self,
        summary_df: pd.DataFrame,
        *,
        frozen_recipe: str,
        candidate_recipe: str,
    ) -> dict[str, Any]:
        frozen_row = self._select_best_event_row(summary_df, frozen_recipe)
        candidate_row = self._select_best_event_row(summary_df, candidate_recipe)
        if not frozen_row or not candidate_row:
            return {
                "candidate_recipe": candidate_recipe,
                "frozen_recipe": frozen_recipe,
                "candidate_beats_frozen": False,
                "comparison_basis": "Missing OpenDrift branch-B event-corridor rows for one or both recipes.",
                "frozen_event_row": frozen_row,
                "candidate_event_row": candidate_row,
            }
        if frozen_recipe == candidate_recipe:
            return {
                "candidate_recipe": candidate_recipe,
                "frozen_recipe": frozen_recipe,
                "candidate_beats_frozen": False,
                "comparison_basis": "The local candidate matches the frozen baseline recipe, so there is no incremental improvement to test.",
                "frozen_event_row": frozen_row,
                "candidate_event_row": candidate_row,
            }

        duel = _rank_event_rows(pd.DataFrame([frozen_row, candidate_row]))
        winner_recipe = str(duel.iloc[0]["recipe_id"])
        return {
            "candidate_recipe": candidate_recipe,
            "frozen_recipe": frozen_recipe,
            "candidate_beats_frozen": winner_recipe == candidate_recipe,
            "comparison_basis": (
                "Compared the best OpenDrift branch-B March 4-6 event-corridor row for each recipe using "
                "mean FSS, then IOU, dice, forecast footprint size, and nearest observation distance."
            ),
            "frozen_event_row": frozen_row,
            "candidate_event_row": candidate_row,
        }

    def _build_report(
        self,
        *,
        source_lon: float,
        source_lat: float,
        subset_df: pd.DataFrame,
        local_ranking_df: pd.DataFrame,
        frozen_baseline: dict[str, Any],
        comparison_results: dict[str, Any],
        event_best: dict[str, Any],
        candidate_vs_frozen: dict[str, Any],
    ) -> None:
        lines = [
            "# Mindoro Local Recipe Experiment",
            "",
            "## Historical-local subset",
            "",
            f"- Mindoro source point: `{source_lon:.6f}, {source_lat:.6f}`",
            f"- Accepted subset threshold: `{self.distance_threshold_km:.0f} km`",
            f"- Accepted subset segments: `{len(subset_df)}`",
            f"- Accepted subset drifters: `{subset_df['drifter_id'].astype(str).nunique()}`",
            f"- Date window range: `{subset_df['start_time_utc'].min()}` to `{subset_df['end_time_utc'].max()}`",
            "",
            "## Historical-local ranking",
            "",
            "```csv",
            local_ranking_df.to_csv(index=False).strip(),
            "```",
            "",
            "## Event-scale comparison",
            "",
            f"- Frozen historical baseline recipe: `{frozen_baseline.get('selected_recipe', '')}`",
            f"- Mindoro-local candidate recipe: `{local_ranking_df.iloc[0]['recipe']}`",
            f"- Best event-scale OpenDrift branch-B recipe: `{event_best.get('recipe_id', '') or 'none'}`",
            f"- Best event-scale track: `{event_best.get('track_id', '') or 'none'}`",
            f"- GFS preparation requested: `{comparison_results.get('forcing_preparation', {}).get('gfs_requested', False)}`",
            f"- GFS preparation status: `{comparison_results.get('forcing_preparation', {}).get('gfs_status', 'unknown')}`",
            f"- GFS recipes included: `{', '.join(comparison_results.get('gfs_recipes_included', [])) or 'none'}`",
            f"- GFS recipes skipped: `{', '.join(comparison_results.get('gfs_recipes_skipped', [])) or 'none'}`",
            "",
            "## Decision",
            "",
            f"- Candidate beats frozen baseline on the Mindoro event corridor: `{candidate_vs_frozen['candidate_beats_frozen']}`",
            f"- Comparison basis: {candidate_vs_frozen['comparison_basis']}",
            "",
            "## Outputs",
            "",
            f"- Subset registry: `{_relative(self.repo_root, self.paths['subset_registry'])}`",
            f"- Local recipe summary: `{_relative(self.repo_root, self.paths['recipe_summary'])}`",
            f"- Local recipe ranking: `{_relative(self.repo_root, self.paths['recipe_ranking'])}`",
            f"- Candidate baseline: `{_relative(self.repo_root, self.paths['candidate_baseline'])}`",
            f"- Event comparison summary: `{_path_or_placeholder(self.repo_root, comparison_results.get('summary_csv'))}`",
            f"- Event comparison ranking: `{_path_or_placeholder(self.repo_root, comparison_results.get('ranking_csv'))}`",
            f"- Event comparison report: `{_path_or_placeholder(self.repo_root, comparison_results.get('report_md'))}`",
        ]
        self.paths["report"].write_text("\n".join(lines), encoding="utf-8")

    def _build_manifest(
        self,
        *,
        source_lon: float,
        source_lat: float,
        subset_df: pd.DataFrame,
        local_summary_df: pd.DataFrame,
        local_ranking_df: pd.DataFrame,
        frozen_baseline: dict[str, Any],
        candidate_payload: dict[str, Any],
        comparison_results: dict[str, Any],
        event_best: dict[str, Any],
        candidate_vs_frozen: dict[str, Any],
        baseline_hash_before: str,
        baseline_hash_after: str,
        baseline_mutation_detected: bool,
        baseline_restored_to_pre_run_state: bool,
    ) -> dict[str, Any]:
        return {
            "phase": MINDORO_LOCAL_RECIPE_EXPERIMENT_DIR_NAME,
            "run_name": self.case.run_name,
            "source_point": {
                "path": _relative(self.repo_root, self._source_point_path()),
                "lon": float(source_lon),
                "lat": float(source_lat),
            },
            "historical_subset": {
                "threshold_km": float(self.distance_threshold_km),
                "accepted_segment_count": int(len(subset_df)),
                "accepted_drifter_count": int(subset_df["drifter_id"].astype(str).nunique()),
                "segment_ids": subset_df["segment_id"].astype(str).tolist(),
            },
            "frozen_baseline": {
                "path": _relative(self.repo_root, self.baseline_path),
                "selected_recipe": str(frozen_baseline.get("selected_recipe") or ""),
                "source_kind": str(frozen_baseline.get("source_kind") or ""),
            },
            "local_candidate": {
                "path": _relative(self.repo_root, self.paths["candidate_baseline"]),
                "selected_recipe": str(candidate_payload["selected_recipe"]),
                "promotion_required": bool(candidate_payload.get("promotion_required", True)),
            },
            "historical_local_summary": local_summary_df.to_dict(orient="records"),
            "historical_local_ranking": local_ranking_df.to_dict(orient="records"),
            "event_comparison": {
                "output_dir": _relative(self.repo_root, Path(comparison_results["output_dir"])),
                "summary_csv": _relative(self.repo_root, Path(comparison_results["summary_csv"])),
                "ranking_csv": _relative(self.repo_root, Path(comparison_results["ranking_csv"])),
                "report_md": _relative(self.repo_root, Path(comparison_results["report_md"])),
                "run_manifest": _relative(self.repo_root, Path(comparison_results["run_manifest"])),
                "forcing_preparation": comparison_results.get("forcing_preparation", {}),
                "gfs_recipes_included": comparison_results.get("gfs_recipes_included", []),
                "gfs_recipes_skipped": comparison_results.get("gfs_recipes_skipped", []),
                "best_event_scale_recipe": event_best,
                "candidate_vs_frozen": candidate_vs_frozen,
            },
            "canonical_baseline_integrity": {
                "path": _relative(self.repo_root, self.baseline_path),
                "sha256_before": baseline_hash_before,
                "sha256_after": baseline_hash_after,
                "mutation_detected_and_reverted": bool(baseline_mutation_detected),
                "restored_to_pre_run_state": bool(baseline_restored_to_pre_run_state),
                "unchanged": baseline_hash_before == baseline_hash_after,
            },
            "artifacts": {key: _relative(self.repo_root, value) for key, value in self.paths.items()},
        }

    def run(self) -> dict[str, Any]:
        baseline_bytes_before = self.baseline_path.read_bytes()
        baseline_hash_before = _sha256(self.baseline_path)
        frozen_baseline = self._load_canonical_baseline()
        accepted_df = self._load_phase1_accepted_registry()
        segment_metrics_df = self._load_phase1_segment_metrics()
        source_lon, source_lat = self.load_source_point(self._source_point_path())

        subset_df = self.build_local_subset_registry(
            accepted_df,
            source_lon=source_lon,
            source_lat=source_lat,
            threshold_km=self.distance_threshold_km,
        )
        subset_df.to_csv(self.paths["subset_registry"], index=False)

        local_summary_df, local_ranking_df, local_winner = self.build_local_recipe_tables(
            segment_metrics_df,
            subset_df["segment_id"],
            recipe_rank_pool=f"mindoro_local_accepted_subset_{int(round(self.distance_threshold_km))}km",
        )
        local_summary_df.to_csv(self.paths["recipe_summary"], index=False)
        local_ranking_df.to_csv(self.paths["recipe_ranking"], index=False)

        candidate_payload = self._build_candidate_baseline_payload(
            winning_recipe=local_winner,
            subset_df=subset_df,
            source_lon=source_lon,
            source_lat=source_lat,
        )
        _write_yaml(self.paths["candidate_baseline"], candidate_payload)

        comparison_service = self.comparison_service_factory(
            output_slug=self.comparison_output_slug,
            prepare_missing_gfs=True,
            gfs_prepare_strict=True,
        )
        comparison_results = comparison_service.run()
        comparison_results["gfs_recipes_included"] = [
            row["recipe_id"]
            for row in comparison_results.get("recipes", [])
            if str(row.get("recipe_id", "")).endswith("_gfs") and row.get("available")
        ]
        comparison_results["gfs_recipes_skipped"] = [
            row["recipe_id"]
            for row in comparison_results.get("recipes", [])
            if str(row.get("recipe_id", "")).endswith("_gfs") and not row.get("available")
        ]

        summary_df = comparison_results["summary"]
        event_best = select_promotable_opendrift_recipe(summary_df)
        candidate_vs_frozen = self._compare_candidate_vs_frozen(
            summary_df,
            frozen_recipe=str(frozen_baseline["selected_recipe"]),
            candidate_recipe=local_winner,
        )

        self._build_report(
            source_lon=source_lon,
            source_lat=source_lat,
            subset_df=subset_df,
            local_ranking_df=local_ranking_df,
            frozen_baseline=frozen_baseline,
            comparison_results=comparison_results,
            event_best=event_best,
            candidate_vs_frozen=candidate_vs_frozen,
        )

        baseline_hash_after = _sha256(self.baseline_path)
        baseline_mutation_detected = baseline_hash_before != baseline_hash_after
        baseline_restored_to_pre_run_state = False
        if baseline_mutation_detected:
            self.baseline_path.write_bytes(baseline_bytes_before)
            baseline_hash_after = _sha256(self.baseline_path)
            baseline_restored_to_pre_run_state = baseline_hash_before == baseline_hash_after
            if not baseline_restored_to_pre_run_state:
                raise RuntimeError(
                    "config/phase1_baseline_selection.yaml changed during the Mindoro-local experiment "
                    "and the pre-run bytes could not be restored."
                )

        manifest_payload = self._build_manifest(
            source_lon=source_lon,
            source_lat=source_lat,
            subset_df=subset_df,
            local_summary_df=local_summary_df,
            local_ranking_df=local_ranking_df,
            frozen_baseline=frozen_baseline,
            candidate_payload=candidate_payload,
            comparison_results=comparison_results,
            event_best=event_best,
            candidate_vs_frozen=candidate_vs_frozen,
            baseline_hash_before=baseline_hash_before,
            baseline_hash_after=baseline_hash_after,
            baseline_mutation_detected=baseline_mutation_detected,
            baseline_restored_to_pre_run_state=baseline_restored_to_pre_run_state,
        )
        _write_json(self.paths["manifest"], manifest_payload)

        return {
            "output_dir": str(self.output_dir),
            "subset_registry_csv": str(self.paths["subset_registry"]),
            "recipe_summary_csv": str(self.paths["recipe_summary"]),
            "recipe_ranking_csv": str(self.paths["recipe_ranking"]),
            "candidate_baseline_path": str(self.paths["candidate_baseline"]),
            "report_md": str(self.paths["report"]),
            "manifest_json": str(self.paths["manifest"]),
            "local_candidate_recipe": local_winner,
            "frozen_baseline_recipe": str(frozen_baseline["selected_recipe"]),
            "event_best_recipe": str(event_best.get("recipe_id") or ""),
            "candidate_beats_frozen": bool(candidate_vs_frozen["candidate_beats_frozen"]),
            "gfs_recipes_included": comparison_results.get("gfs_recipes_included", []),
            "gfs_recipes_skipped": comparison_results.get("gfs_recipes_skipped", []),
            "comparison_output_dir": str(comparison_results["output_dir"]),
        }


def run_mindoro_local_recipe_experiment() -> dict[str, Any]:
    return MindoroLocalRecipeExperimentService().run()
