"""Formal multi-date public-observation Phase 3B validation for Mindoro."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.io import get_case_output_dir

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None


MULTIDATE_DIR_NAME = "phase3b_multidate_public"
INIT_DATE = "2023-03-03"
VALIDATION_START_DATE = "2023-03-04"
VALIDATION_END_DATE = "2023-03-06"
SOURCE_TAXONOMY_OBS = "observation_derived_quantitative"
SOURCE_TAXONOMY_MODELED = "modeled_forecast_exclude_from_truth"
SOURCE_TAXONOMY_QUALITATIVE = "qualitative_context_only"


def load_phase3b_multidate_validation_dates(
    case_output_dir: Path,
    fallback_dates: list[str] | None = None,
) -> list[str]:
    manifest_path = case_output_dir / MULTIDATE_DIR_NAME / "phase3b_multidate_run_manifest.json"
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as handle:
            manifest = json.load(handle) or {}
        accepted_dates = [
            str(value).strip()
            for value in manifest.get("accepted_validation_dates_used_for_forecast_skill") or []
            if str(value).strip()
        ]
        if accepted_dates:
            return list(dict.fromkeys(accepted_dates))

    summary_path = case_output_dir / MULTIDATE_DIR_NAME / "phase3b_multidate_summary.csv"
    if summary_path.exists():
        summary = pd.read_csv(summary_path)
        if {"pair_role", "obs_date"}.issubset(summary.columns):
            accepted_dates = [
                str(value).strip()
                for value in summary.loc[summary["pair_role"].astype(str) == "per_date_union", "obs_date"].tolist()
                if str(value).strip()
            ]
            if accepted_dates:
                return list(dict.fromkeys(accepted_dates))

    defaults = fallback_dates or [VALIDATION_START_DATE, VALIDATION_END_DATE]
    return [str(value).strip() for value in defaults if str(value).strip()]


def format_phase3b_multidate_eventcorridor_label(validation_dates: list[str]) -> str:
    cleaned = [str(value).strip() for value in validation_dates if str(value).strip()]
    if not cleaned:
        return f"{VALIDATION_START_DATE}_to_{VALIDATION_END_DATE}"
    if len(cleaned) == 1:
        return cleaned[0]
    return f"{cleaned[0]}_to_{cleaned[-1]}"


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    return str(value)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _is_modeled_forecast_row(row: pd.Series | dict) -> bool:
    text = " ".join(
        str(row.get(key, ""))
        for key in ("source_name", "provider", "source_type", "notes", "rejection_reason")
    ).lower()
    modeled_patterns = [
        r"\btrajectory model\b",
        r"\btrajectory forecast\b",
        r"\bforecast\b",
        r"\bprediction\b",
        r"\bmodel[- ]generated\b",
        r"\bbulletin\b",
    ]
    return any(re.search(pattern, text) for pattern in modeled_patterns)


def classify_public_source(row: pd.Series | dict) -> tuple[str, str]:
    """Return source taxonomy and source-based reason."""
    obs_date = str(row.get("obs_date", "") or "").strip()
    has_date = bool(obs_date)
    machine = _as_bool(row.get("machine_readable"))
    public = _as_bool(row.get("public"))
    obs_derived = _as_bool(row.get("observation_derived"))
    reproducible = _as_bool(row.get("reproducibly_ingestible"))
    accepted_old = _as_bool(row.get("accept_for_appendix_quantitative"))
    geometry_type = str(row.get("geometry_type", "") or "").lower()
    mask_path = Path(str(row.get("appendix_obs_mask", "") or ""))
    rasterizable = bool(mask_path.exists()) or geometry_type in {"polygon", "multipolygon"}

    if _is_modeled_forecast_row(row):
        return SOURCE_TAXONOMY_MODELED, "modeled/forecast wording detected in source metadata"
    if machine and public and obs_derived and reproducible and has_date and rasterizable and accepted_old:
        return SOURCE_TAXONOMY_OBS, "public machine-readable dated observation-derived rasterizable layer"
    return SOURCE_TAXONOMY_QUALITATIVE, "does not satisfy all quantitative truth requirements"


def _hash_file(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _window_cells(helper: Phase3BScoringService, window_km: int) -> int:
    return helper._window_km_to_cells(window_km)


def _clamp_unit_interval(value: float, tolerance: float = 1e-12) -> float:
    if abs(float(value)) <= tolerance:
        return 0.0
    if abs(float(value) - 1.0) <= tolerance:
        return 1.0
    return float(np.clip(value, 0.0, 1.0))


class Phase3BMultidatePublicService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("phase3b_multidate_public is only supported for official Mindoro workflows.")
        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.appendix_dir = self.case_output_dir / "public_obs_appendix"
        self.output_dir = self.case_output_dir / MULTIDATE_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = (self.sea_mask > 0.5) if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.forecast_date_dir = self.output_dir / "forecast_datecomposites"
        self.obs_union_dir = self.output_dir / "date_union_obs_masks"
        self.precheck_dir = self.output_dir / "precheck"
        self.forecast_date_dir.mkdir(parents=True, exist_ok=True)
        self.obs_union_dir.mkdir(parents=True, exist_ok=True)
        self.precheck_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict:
        strict_hashes_before = self._strict_phase3b_hashes()
        inventory = self._load_inventory()
        taxonomy = self._build_taxonomy(inventory)
        accepted_skill = taxonomy[taxonomy["accepted_for_multidate_quantitative"]].copy()
        init_qa = taxonomy[taxonomy["role_in_phase3b"] == "initialization_consistency_qa"].copy()
        accepted_dates = sorted(accepted_skill["obs_date"].dropna().astype(str).unique().tolist())
        self._prune_stale_outputs(accepted_dates, include_context_date=not init_qa.empty)

        forecast_datecomposites = self._prepare_forecast_datecomposites(accepted_dates, include_context_date=not init_qa.empty)
        source_pairs = self._build_source_pairs(accepted_skill, forecast_datecomposites)
        union_pairs = self._build_date_union_pairs(accepted_skill, forecast_datecomposites)
        event_pair = self._build_eventcorridor_pair(accepted_skill, forecast_datecomposites)
        context_pair = self._build_initialization_inclusive_context_pair(taxonomy, forecast_datecomposites)
        pairing_manifest = pd.DataFrame(source_pairs + union_pairs + [event_pair] + ([context_pair] if context_pair else []))

        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pairing_manifest)
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df)
        event_summary = summary_df[summary_df["score_group"].isin(["eventcorridor", "initialization_inclusive_context_only"])].copy()
        init_qa_df = self._build_initialization_qa(init_qa, forecast_datecomposites)
        taxonomy_paths = self._write_taxonomy(taxonomy)
        qa_paths = self._write_qa(scored_pairings)
        memo_path = self._write_methodology_revision_memo(accepted_dates, taxonomy)
        guardrails_path = self._write_claim_guardrails()

        pairing_path = self.output_dir / "phase3b_multidate_pairing_manifest.csv"
        fss_path = self.output_dir / "phase3b_multidate_fss_by_date_window.csv"
        diagnostics_path = self.output_dir / "phase3b_multidate_diagnostics.csv"
        summary_path = self.output_dir / "phase3b_multidate_summary.csv"
        event_summary_path = self.output_dir / "phase3b_eventcorridor_summary.csv"
        init_qa_path = self.output_dir / "phase3b_initialization_consistency_qa.csv"
        run_manifest_path = self.output_dir / "phase3b_multidate_run_manifest.json"

        scored_pairings.to_csv(pairing_path, index=False)
        fss_df.to_csv(fss_path, index=False)
        diagnostics_df.to_csv(diagnostics_path, index=False)
        summary_df.to_csv(summary_path, index=False)
        event_summary.to_csv(event_summary_path, index=False)
        init_qa_df.to_csv(init_qa_path, index=False)

        strict_hashes_after = self._strict_phase3b_hashes()
        strict_files_unchanged = strict_hashes_before == strict_hashes_after
        run_manifest = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "phase3b_revision": {
                "phase3b1": "strict_single_date_stress_test",
                "phase3b2": "main_multi_date_public_observation_derived_validation",
                "march3_role": "initialization_consistency_only",
            },
            "accepted_validation_dates_used_for_forecast_skill": accepted_dates,
            "march3_excluded_from_forecast_skill_summary": True,
            "source_taxonomy": {
                "observation_derived_quantitative": "public, machine-readable, explicitly dated, observation-derived, reproducibly ingestible, rasterizable",
                "modeled_forecast_exclude_from_truth": "bulletins, trajectory forecasts, or model-generated predictions",
                "qualitative_context_only": "screenshots, app/webmap visualizations, timeline graphics, undownloadable visual products, contextual layers",
            },
            "strict_single_date_stress_test": {
                "existing_output_dir": str(self.case_output_dir / "phase3b"),
                "role": "strict_single_date_stress_test",
                "files_unchanged_by_this_run": strict_files_unchanged,
                "hashes_before": strict_hashes_before,
                "hashes_after": strict_hashes_after,
            },
            "artifacts": {
                "source_taxonomy_csv": str(taxonomy_paths["csv"]),
                "source_taxonomy_json": str(taxonomy_paths["json"]),
                "pairing_manifest": str(pairing_path),
                "fss_by_date_window": str(fss_path),
                "diagnostics": str(diagnostics_path),
                "summary": str(summary_path),
                "eventcorridor_summary": str(event_summary_path),
                "initialization_consistency_qa": str(init_qa_path),
                "methodology_revision_memo": str(memo_path),
                "claims_guardrails": str(guardrails_path),
                **{key: str(value) for key, value in qa_paths.items()},
            },
            "forecast_datecomposites": {date: str(path) for date, path in forecast_datecomposites.items()},
            "score_outputs": summary_df.to_dict(orient="records"),
        }
        _write_json(run_manifest_path, run_manifest)

        return {
            "output_dir": self.output_dir,
            "taxonomy_csv": taxonomy_paths["csv"],
            "taxonomy_json": taxonomy_paths["json"],
            "pairing_manifest": pairing_path,
            "fss_by_date_window": fss_path,
            "diagnostics": diagnostics_path,
            "summary": summary_path,
            "eventcorridor_summary": event_summary_path,
            "run_manifest": run_manifest_path,
            "methodology_revision_memo": memo_path,
            "claims_guardrails": guardrails_path,
            "accepted_validation_dates": accepted_dates,
            "march3_excluded": True,
            "strict_files_unchanged": strict_files_unchanged,
        }

    def _prune_stale_outputs(self, accepted_dates: list[str], include_context_date: bool) -> None:
        allowed_forecast_dates = set(accepted_dates)
        if include_context_date:
            allowed_forecast_dates.add(INIT_DATE)

        allowed_forecast_files = {f"mask_p50_{date}_datecomposite.tif" for date in allowed_forecast_dates}
        for path in self.forecast_date_dir.glob("mask_p50_*_datecomposite.tif"):
            if path.name not in allowed_forecast_files:
                path.unlink(missing_ok=True)

        allowed_union_files = {f"obs_union_{date}.tif" for date in accepted_dates}
        for path in self.obs_union_dir.glob("obs_union_*.tif"):
            if path.name not in allowed_union_files:
                path.unlink(missing_ok=True)

    def _load_inventory(self) -> pd.DataFrame:
        inventory_path = self.appendix_dir / "public_obs_inventory.csv"
        if not inventory_path.exists():
            raise FileNotFoundError(f"Public observation appendix inventory not found: {inventory_path}")
        inventory = pd.read_csv(inventory_path)
        registry_path = self.appendix_dir / "public_obs_acceptance_registry.csv"
        if registry_path.exists():
            registry = pd.read_csv(registry_path)
            join_key = "source_key" if "source_key" in registry.columns else None
            if join_key and "source_key" in inventory.columns:
                registry_cols = [col for col in registry.columns if col not in inventory.columns or col == join_key]
                inventory = inventory.merge(registry[registry_cols], on=join_key, how="left", suffixes=("", "_registry"))
        return inventory

    def _build_taxonomy(self, inventory: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict] = []
        for _, row in inventory.iterrows():
            taxonomy, reason = classify_public_source(row)
            obs_date = str(row.get("obs_date", "") or "")
            mask_path = Path(str(row.get("appendix_obs_mask", "") or ""))
            within = _as_bool(row.get("within_current_72h_horizon"))
            validation_window_date = VALIDATION_START_DATE <= obs_date <= VALIDATION_END_DATE
            is_init = obs_date == INIT_DATE
            accepted_quant = (
                taxonomy == SOURCE_TAXONOMY_OBS
                and within
                and validation_window_date
                and not is_init
                and mask_path.exists()
            )
            role = "forecast_skill_validation" if accepted_quant else ""
            rejection = str(row.get("rejection_reason", "") or "")
            if is_init and taxonomy == SOURCE_TAXONOMY_OBS and mask_path.exists():
                role = "initialization_consistency_qa"
                rejection = "March 3 is initialization date; excluded from forecast-validation skill."
            elif taxonomy == SOURCE_TAXONOMY_MODELED:
                role = "excluded_from_truth"
                rejection = "Modeled forecast / trajectory product excluded from quantitative truth."
            elif not accepted_quant and not role:
                role = "qualitative_or_out_of_scope"
                rejection = rejection or reason
            rows.append(
                {
                    **row.to_dict(),
                    "source_taxonomy": taxonomy,
                    "taxonomy_reason": reason,
                    "accepted_for_multidate_quantitative": bool(accepted_quant),
                    "counted_in_forecast_skill_summary": bool(accepted_quant),
                    "role_in_phase3b": role,
                    "multidate_rejection_reason": rejection,
                    "mask_exists": bool(mask_path.exists()),
                    "date_is_initialization": bool(is_init),
                    "date_is_forecast_validation_window": bool(validation_window_date and not is_init),
                }
            )
        taxonomy_df = pd.DataFrame(rows)
        return taxonomy_df.sort_values(
            by=["accepted_for_multidate_quantitative", "obs_date", "source_name"],
            ascending=[False, True, True],
        )

    def _write_taxonomy(self, taxonomy: pd.DataFrame) -> dict[str, Path]:
        csv_path = self.output_dir / "phase3b_public_source_taxonomy.csv"
        json_path = self.output_dir / "phase3b_public_source_taxonomy.json"
        taxonomy.to_csv(csv_path, index=False)
        _write_json(json_path, taxonomy.to_dict(orient="records"))
        return {"csv": csv_path, "json": json_path}

    def _prepare_forecast_datecomposites(self, accepted_dates: list[str], include_context_date: bool) -> dict[str, Path]:
        dates = set(accepted_dates)
        if include_context_date:
            dates.add(INIT_DATE)
        source_dir = self.appendix_dir / "forecast_datecomposites"
        outputs: dict[str, Path] = {}
        for date in sorted(dates):
            source = source_dir / f"mask_p50_{date}_datecomposite.tif"
            if not source.exists():
                raise FileNotFoundError(f"Required forecast date-composite is missing: {source}")
            dest = self.forecast_date_dir / source.name
            if not dest.exists() or _hash_file(dest) != _hash_file(source):
                shutil.copyfile(source, dest)
            outputs[date] = dest
        return outputs

    def _build_source_pairs(self, accepted: pd.DataFrame, forecast_datecomposites: dict[str, Path]) -> list[dict]:
        pairs: list[dict] = []
        for _, row in accepted.iterrows():
            date = str(row["obs_date"])
            obs_path = Path(str(row["appendix_obs_mask"]))
            pairs.append(
                self._pair_record(
                    score_group="per_source",
                    pair_role="per_source",
                    pair_id=f"phase3b_multidate_source_{row['source_key']}",
                    obs_date=date,
                    source_key=str(row["source_key"]),
                    source_name=str(row["source_name"]),
                    provider=str(row.get("provider", "")),
                    forecast_path=forecast_datecomposites[date],
                    observation_path=obs_path,
                    observation_product=obs_path.name,
                    source_semantics=f"per_source_{date}_public_observation_derived_vs_p50_datecomposite",
                )
            )
        return pairs

    def _build_date_union_pairs(self, accepted: pd.DataFrame, forecast_datecomposites: dict[str, Path]) -> list[dict]:
        pairs: list[dict] = []
        for date, group in accepted.groupby("obs_date"):
            date = str(date)
            union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            source_keys = []
            source_names = []
            for _, row in group.iterrows():
                source_keys.append(str(row["source_key"]))
                source_names.append(str(row["source_name"]))
                union = np.maximum(union, self.helper._load_binary_score_mask(Path(str(row["appendix_obs_mask"]))))
            if self.sea_mask is not None:
                union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
            union_path = self.obs_union_dir / f"obs_union_{date}.tif"
            save_raster(self.grid, union.astype(np.float32), union_path)
            pairs.append(
                self._pair_record(
                    score_group="per_date_union",
                    pair_role="per_date_union",
                    pair_id=f"phase3b_multidate_date_union_{date}",
                    obs_date=date,
                    source_key=";".join(source_keys),
                    source_name=f"accepted_public_obs_union_{date}",
                    provider=";".join(sorted(set(str(value) for value in group.get("provider", [])))),
                    forecast_path=forecast_datecomposites[date],
                    observation_path=union_path,
                    observation_product=union_path.name,
                    source_semantics=f"per_date_union_{date}_public_observation_derived_vs_p50_datecomposite",
                    source_count=int(len(group)),
                    union_source_names=";".join(source_names),
                )
            )
        return pairs

    def _build_eventcorridor_pair(self, accepted: pd.DataFrame, forecast_datecomposites: dict[str, Path]) -> dict:
        if accepted.empty:
            raise RuntimeError("No accepted multi-date public observation masks are available for event-corridor validation.")
        used_dates = sorted(accepted["obs_date"].astype(str).unique().tolist())
        obs_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        model_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for _, row in accepted.iterrows():
            obs_union = np.maximum(obs_union, self.helper._load_binary_score_mask(Path(str(row["appendix_obs_mask"]))))
        for date in used_dates:
            model_union = np.maximum(model_union, self.helper._load_binary_score_mask(forecast_datecomposites[date]))
        if self.sea_mask is not None:
            obs_union = apply_ocean_mask(obs_union, sea_mask=self.sea_mask, fill_value=0.0)
            model_union = apply_ocean_mask(model_union, sea_mask=self.sea_mask, fill_value=0.0)
        obs_path = self.output_dir / "eventcorridor_obs_union_2023-03-04_to_2023-03-06.tif"
        model_path = self.output_dir / "eventcorridor_model_union_2023-03-04_to_2023-03-06.tif"
        save_raster(self.grid, obs_union.astype(np.float32), obs_path)
        save_raster(self.grid, model_union.astype(np.float32), model_path)
        return self._pair_record(
            score_group="eventcorridor",
            pair_role="main_event_corridor",
            pair_id="phase3b_eventcorridor_2023-03-04_to_2023-03-06",
            obs_date="2023-03-04_to_2023-03-06",
            source_key=";".join(accepted["source_key"].astype(str).tolist()),
            source_name="accepted_public_obs_eventcorridor_2023-03-04_to_2023-03-06",
            provider=";".join(sorted(set(accepted["provider"].astype(str).tolist()))),
            forecast_path=model_path,
            observation_path=obs_path,
            observation_product=obs_path.name,
            source_semantics="eventcorridor_public_observation_derived_union_excluding_initialization_date",
            source_count=int(len(accepted)),
            union_source_names=";".join(accepted["source_name"].astype(str).tolist()),
            validation_dates_used=",".join(used_dates),
        )

    def _build_initialization_inclusive_context_pair(
        self,
        taxonomy: pd.DataFrame,
        forecast_datecomposites: dict[str, Path],
    ) -> dict | None:
        context = taxonomy[
            (
                taxonomy["source_taxonomy"].astype(str) == SOURCE_TAXONOMY_OBS
            )
            & (
                taxonomy["role_in_phase3b"].astype(str).isin(
                    ["forecast_skill_validation", "initialization_consistency_qa"]
                )
            )
            & taxonomy["mask_exists"].astype(bool)
        ].copy()
        if context.empty or INIT_DATE not in forecast_datecomposites:
            return None
        used_dates = sorted(context["obs_date"].astype(str).unique().tolist())
        obs_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        model_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for _, row in context.iterrows():
            obs_union = np.maximum(obs_union, self.helper._load_binary_score_mask(Path(str(row["appendix_obs_mask"]))))
        for date in used_dates:
            if date in forecast_datecomposites:
                model_union = np.maximum(model_union, self.helper._load_binary_score_mask(forecast_datecomposites[date]))
        if self.sea_mask is not None:
            obs_union = apply_ocean_mask(obs_union, sea_mask=self.sea_mask, fill_value=0.0)
            model_union = apply_ocean_mask(model_union, sea_mask=self.sea_mask, fill_value=0.0)
        obs_path = self.output_dir / "eventcorridor_obs_union_2023-03-03_to_2023-03-06_initialization_inclusive_context_only.tif"
        model_path = self.output_dir / "eventcorridor_model_union_2023-03-03_to_2023-03-06_initialization_inclusive_context_only.tif"
        save_raster(self.grid, obs_union.astype(np.float32), obs_path)
        save_raster(self.grid, model_union.astype(np.float32), model_path)
        return self._pair_record(
            score_group="initialization_inclusive_context_only",
            pair_role="initialization_inclusive_context_only",
            pair_id="phase3b_eventcorridor_2023-03-03_to_2023-03-06_context_only",
            obs_date="2023-03-03_to_2023-03-06",
            source_key=";".join(context["source_key"].astype(str).tolist()),
            source_name="initialization_inclusive_context_only_public_obs_union",
            provider=";".join(sorted(set(context["provider"].astype(str).tolist()))),
            forecast_path=model_path,
            observation_path=obs_path,
            observation_product=obs_path.name,
            source_semantics="initialization_inclusive_context_only_not_main_skill_metric",
            source_count=int(len(context)),
            union_source_names=";".join(context["source_name"].astype(str).tolist()),
            validation_dates_used=",".join(used_dates),
        )

    def _pair_record(
        self,
        *,
        score_group: str,
        pair_role: str,
        pair_id: str,
        obs_date: str,
        source_key: str,
        source_name: str,
        provider: str,
        forecast_path: Path,
        observation_path: Path,
        observation_product: str,
        source_semantics: str,
        source_count: int = 1,
        union_source_names: str = "",
        validation_dates_used: str = "",
    ) -> dict:
        return {
            "score_group": score_group,
            "pair_id": pair_id,
            "pair_role": pair_role,
            "obs_date": obs_date,
            "validation_dates_used": validation_dates_used or obs_date,
            "source_key": source_key,
            "source_name": source_name,
            "provider": provider,
            "source_count": source_count,
            "union_source_names": union_source_names,
            "forecast_product": forecast_path.name,
            "forecast_path": str(forecast_path),
            "observation_product": observation_product,
            "observation_path": str(observation_path),
            "metric": "FSS",
            "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
            "source_taxonomy": SOURCE_TAXONOMY_OBS,
            "source_semantics": source_semantics,
            "track_label": "main_multi_date_public_validation" if score_group != "initialization_inclusive_context_only" else "context_only",
            "strict_march6_single_date_role": "strict_single_date_stress_test" if obs_date == VALIDATION_END_DATE else "",
            "precheck_csv": "",
            "precheck_json": "",
        }

    def _score_pairings(self, pairings: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        scored_rows: list[dict] = []
        fss_rows: list[dict] = []
        diagnostics_rows: list[dict] = []
        for _, row in pairings.iterrows():
            forecast_path = Path(str(row["forecast_path"]))
            observation_path = Path(str(row["observation_path"]))
            precheck_base = self.precheck_dir / str(row["pair_id"])
            precheck = precheck_same_grid(forecast=forecast_path, target=observation_path, report_base_path=precheck_base)
            if not precheck.passed:
                raise RuntimeError(
                    f"Multi-date Phase 3B same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}"
                )

            forecast_mask = self.helper._load_binary_score_mask(forecast_path)
            obs_mask = self.helper._load_binary_score_mask(observation_path)
            diagnostics = self.helper._compute_mask_diagnostics(forecast_mask, obs_mask)

            scored = row.to_dict()
            scored["precheck_csv"] = str(precheck.csv_report_path)
            scored["precheck_json"] = str(precheck.json_report_path)
            scored_rows.append(scored)
            diagnostics_rows.append({**scored, **diagnostics})

            for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                fss = _clamp_unit_interval(
                    calculate_fss(
                        forecast_mask,
                        obs_mask,
                        window=_window_cells(self.helper, window_km),
                        valid_mask=self.valid_mask,
                    )
                )
                fss_rows.append(
                    {
                        "score_group": scored["score_group"],
                        "pair_id": scored["pair_id"],
                        "pair_role": scored["pair_role"],
                        "obs_date": scored["obs_date"],
                        "validation_dates_used": scored["validation_dates_used"],
                        "source_name": scored["source_name"],
                        "provider": scored["provider"],
                        "window_km": int(window_km),
                        "window_cells": int(_window_cells(self.helper, window_km)),
                        "fss": fss,
                        "forecast_path": scored["forecast_path"],
                        "observation_path": scored["observation_path"],
                        "source_semantics": scored["source_semantics"],
                        "track_label": scored["track_label"],
                        "precheck_csv": scored["precheck_csv"],
                        "precheck_json": scored["precheck_json"],
                    }
                )
        return pd.DataFrame(scored_rows), pd.DataFrame(fss_rows), pd.DataFrame(diagnostics_rows)

    @staticmethod
    def _summarize(pairings: pd.DataFrame, fss_df: pd.DataFrame, diagnostics_df: pd.DataFrame) -> pd.DataFrame:
        if pairings.empty:
            return pd.DataFrame()
        fss_pivot = (
            fss_df.pivot(index="pair_id", columns="window_km", values="fss")
            .rename(columns={window: f"fss_{window}km" for window in OFFICIAL_PHASE3B_WINDOWS_KM})
            .reset_index()
        )
        diag_cols = [
            "pair_id",
            "forecast_nonzero_cells",
            "obs_nonzero_cells",
            "area_ratio_forecast_to_obs",
            "centroid_distance_m",
            "iou",
            "dice",
            "nearest_distance_to_obs_m",
            "ocean_cell_count",
        ]
        return pairings.merge(diagnostics_df[diag_cols], on="pair_id", how="left").merge(fss_pivot, on="pair_id", how="left")

    def _build_initialization_qa(self, init_qa: pd.DataFrame, forecast_datecomposites: dict[str, Path]) -> pd.DataFrame:
        rows: list[dict] = []
        if init_qa.empty or INIT_DATE not in forecast_datecomposites:
            return pd.DataFrame(rows)
        forecast_path = forecast_datecomposites[INIT_DATE]
        for _, row in init_qa.iterrows():
            obs_path = Path(str(row["appendix_obs_mask"]))
            if not obs_path.exists():
                continue
            precheck = precheck_same_grid(
                forecast=forecast_path,
                target=obs_path,
                report_base_path=self.precheck_dir / f"initialization_consistency_{row['source_key']}",
            )
            if not precheck.passed:
                raise RuntimeError(f"Initialization consistency precheck failed: {precheck.json_report_path}")
            forecast_mask = self.helper._load_binary_score_mask(forecast_path)
            obs_mask = self.helper._load_binary_score_mask(obs_path)
            diagnostics = self.helper._compute_mask_diagnostics(forecast_mask, obs_mask)
            fss_values = {
                f"fss_{window}km": _clamp_unit_interval(
                    calculate_fss(
                        forecast_mask,
                        obs_mask,
                        window=_window_cells(self.helper, window),
                        valid_mask=self.valid_mask,
                    )
                )
                for window in OFFICIAL_PHASE3B_WINDOWS_KM
            }
            rows.append(
                {
                    "score_group": "initialization_consistency_qa",
                    "source_key": row["source_key"],
                    "source_name": row["source_name"],
                    "obs_date": INIT_DATE,
                    "forecast_path": str(forecast_path),
                    "observation_path": str(obs_path),
                    "counted_in_forecast_skill_summary": False,
                    "reason": "March 3 is the initialization date and is reported only as initialization consistency QA.",
                    "precheck_csv": str(precheck.csv_report_path),
                    "precheck_json": str(precheck.json_report_path),
                    **diagnostics,
                    **fss_values,
                }
            )
        return pd.DataFrame(rows)

    def _write_qa(self, pairings: pd.DataFrame) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        overlay_path = self.output_dir / "qa_phase3b_multidate_overlays.png"
        event_path = self.output_dir / "qa_phase3b_eventcorridor_overlay.png"
        per_date = pairings[pairings["score_group"] == "per_date_union"].sort_values("obs_date")
        if not per_date.empty:
            fig, axes = plt.subplots(1, len(per_date), figsize=(5 * len(per_date), 5))
            if len(per_date) == 1:
                axes = [axes]
            for ax, (_, row) in zip(axes, per_date.iterrows()):
                forecast = self.helper._load_binary_score_mask(Path(row["forecast_path"]))
                obs = self.helper._load_binary_score_mask(Path(row["observation_path"]))
                self._render_overlay(ax, forecast, obs, f"{row['obs_date']} per-date union")
            fig.tight_layout()
            fig.savefig(overlay_path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            outputs["qa_multidate_overlays"] = overlay_path

        event = pairings[pairings["score_group"] == "eventcorridor"]
        if not event.empty:
            row = event.iloc[0]
            forecast = self.helper._load_binary_score_mask(Path(row["forecast_path"]))
            obs = self.helper._load_binary_score_mask(Path(row["observation_path"]))
            fig, ax = plt.subplots(figsize=(7, 7))
            self._render_overlay(ax, forecast, obs, "Main event corridor 2023-03-04 to 2023-03-06")
            fig.tight_layout()
            fig.savefig(event_path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            outputs["qa_eventcorridor_overlay"] = event_path
        return outputs

    @staticmethod
    def _render_overlay(ax, forecast: np.ndarray, obs: np.ndarray, title: str) -> None:
        overlap = (forecast > 0) & (obs > 0)
        canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
        canvas[obs > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
        ax.imshow(canvas, origin="upper")
        ax.set_title(title)
        ax.set_axis_off()

    def _strict_phase3b_hashes(self) -> dict[str, str]:
        strict_files = [
            self.case_output_dir / "phase3b" / "phase3b_pairing_manifest.csv",
            self.case_output_dir / "phase3b" / "phase3b_fss_by_date_window.csv",
            self.case_output_dir / "phase3b" / "phase3b_summary.csv",
            self.case_output_dir / "phase3b" / "phase3b_diagnostics.csv",
            self.case_output_dir / "phase3b" / "phase3b_run_manifest.json",
        ]
        return {str(path): _hash_file(path) for path in strict_files if path.exists()}

    def _write_methodology_revision_memo(self, accepted_dates: list[str], taxonomy: pd.DataFrame) -> Path:
        rejected_modeled = taxonomy[taxonomy["source_taxonomy"] == SOURCE_TAXONOMY_MODELED]
        path = self.output_dir / "chapter3_phase3b_revision_memo.md"
        lines = [
            "# Chapter 3 Phase 3B Revision Memo",
            "",
            "This revision keeps the existing strict March 6 single-date result intact but no longer treats it as the only headline quantitative validation.",
            "",
            "## Proposed Structure",
            "",
            "- Phase 3B1 = strict single-date March 6 stress test using the frozen March 6 date-composite p50 vs the March 6 ArcGIS observed mask.",
            "- Phase 3B2 = main multi-date public observation-derived validation over accepted within-horizon dates.",
            "- March 3 = initialization consistency only, not counted as normal forecast-validation skill.",
            "",
            "## Source Rules",
            "",
            "- Possible oil slick observation layers are observation-derived satellite interpretations, not perfect ground truth.",
            "- Modeled forecast bulletins, trajectory forecasts, or model-generated spill predictions are excluded from truth.",
            "- Qualitative-only app, webmap, screenshot, and timeline sources remain qualitative only.",
            "",
            "## Dates Used",
            "",
            f"- Forecast-skill validation dates used: {', '.join(accepted_dates) if accepted_dates else 'none'}",
            "- March 3 was excluded from the validation summary and reported separately as initialization consistency QA.",
        ]
        if not rejected_modeled.empty:
            lines.append("- Modeled/forecast sources excluded from truth:")
            for _, row in rejected_modeled.iterrows():
                lines.append(f"- `{row.get('source_name', '')}` on `{row.get('obs_date', '')}`: {row.get('multidate_rejection_reason', '')}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_claim_guardrails(self) -> Path:
        path = self.output_dir / "phase3b_claims_guardrails.md"
        text = "\n".join(
            [
                "# Phase 3B Claims Guardrails",
                "",
                "- Strict single-date track: The March 6-only result is reported as a stringent stress test of exact-date overlap against a very small accepted validation mask.",
                "- Multi-date main quantitative track: The headline Phase 3B validation is the reproducible multi-date comparison against accepted dated, machine-readable, observation-derived public spill-extent layers within the forecast horizon.",
                "- Observation-proxy limitation: Possible-slick layers are satellite-derived interpreted spill-extent proxies and should not be described as perfect ground truth.",
            ]
        )
        path.write_text(text + "\n", encoding="utf-8")
        return path


def run_phase3b_multidate_public() -> dict:
    return Phase3BMultidatePublicService().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_phase3b_multidate_public()
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, default=str))
