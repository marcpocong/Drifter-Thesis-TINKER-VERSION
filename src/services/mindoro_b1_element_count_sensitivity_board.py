"""Build the Mindoro March 13 -> March 14 100k-vs-5k experimental sensitivity package."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

os.environ.setdefault("WORKFLOW_MODE", "mindoro_retro_2023")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.helpers.metrics import calculate_fss
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_NAME = "CASE_MINDORO_RETRO_2023"
CANONICAL_DIR = REPO_ROOT / "output" / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit"
EXPERIMENT_DIR = REPO_ROOT / "output" / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit_5000_experiment"
SENSITIVITY_DIR = REPO_ROOT / "output" / RUN_NAME / "phase3b_march13_14_element_count_sensitivity"
PYGNOME_DIR = REPO_ROOT / "output" / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison"

BOARD_PATH = SENSITIVITY_DIR / "mindoro_b1_element_count_sensitivity_100k_vs_5k_board.png"
METRICS_CSV_PATH = SENSITIVITY_DIR / "element_count_sensitivity_metrics.csv"
PAIRWISE_CSV_PATH = SENSITIVITY_DIR / "element_count_pairwise_similarity_100k_vs_5k.csv"
MANIFEST_PATH = SENSITIVITY_DIR / "element_count_sensitivity_manifest.json"
README_PATH = SENSITIVITY_DIR / "README.md"
HOW_100K_PATH = SENSITIVITY_DIR / "how_100k_was_done.md"
RUN_NOTE_PATH = SENSITIVITY_DIR / "run_completion_note.md"

TITLE = "EXPERIMENTAL / NOT THESIS-FACING: Mindoro March 13-14 element-count sensitivity"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    return value


def _write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _format_number(value: Any, digits: int = 3) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric):.{digits}f}"


def _format_distance(value: Any) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric):.1f} m"


def _load_summary_row(summary_csv: Path, branch_id: str) -> dict[str, Any]:
    frame = pd.read_csv(summary_csv)
    filtered = frame.loc[frame["branch_id"].astype(str) == branch_id].copy()
    if filtered.empty:
        raise FileNotFoundError(f"Branch {branch_id} not found in {summary_csv}.")
    return filtered.iloc[0].to_dict()


def _branch_manifest_index(run_manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in run_manifest.get("branch_runs") or []:
        branch_id = str(row.get("branch_id") or "").strip()
        if branch_id:
            index[branch_id] = dict(row)
    return index


def _fallback_branch_manifest_path(base_dir: Path, branch_id: str, kind: str) -> str:
    model_dir = base_dir / branch_id / "model_run"
    if kind == "forecast":
        path = model_dir / "forecast" / "forecast_manifest.json"
    else:
        path = model_dir / "ensemble" / "ensemble_manifest.json"
    return _relative(path) if path.exists() else ""


def _load_mask(helper: Phase3BScoringService, path: Path) -> np.ndarray:
    return helper._load_binary_score_mask(path)


def _valid_mask(helper: Phase3BScoringService) -> np.ndarray | None:
    sea_mask = getattr(helper, "sea_mask", None)
    if sea_mask is None:
        return None
    return np.asarray(sea_mask > 0.5, dtype=bool)


def _compute_metrics_against_observation(
    *,
    helper: Phase3BScoringService,
    forecast_path: Path,
    observation_path: Path,
) -> dict[str, Any]:
    forecast_mask = _load_mask(helper, forecast_path)
    observation_mask = _load_mask(helper, observation_path)
    diagnostics = helper._compute_mask_diagnostics(forecast_mask, observation_mask)
    metrics = dict(diagnostics)
    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
        metrics[f"FSS_{window_km}km"] = float(
            np.clip(
                calculate_fss(
                    forecast_mask,
                    observation_mask,
                    window=helper._window_km_to_cells(window_km),
                    valid_mask=_valid_mask(helper),
                ),
                0.0,
                1.0,
            )
        )
    metrics["mean_FSS"] = float(
        np.mean([metrics[f"FSS_{window_km}km"] for window_km in OFFICIAL_PHASE3B_WINDOWS_KM])
    )
    return metrics


def _compute_pairwise_mask_metrics(
    *,
    helper: Phase3BScoringService,
    left_path: Path,
    right_path: Path,
) -> dict[str, Any]:
    left_mask = _load_mask(helper, left_path)
    right_mask = _load_mask(helper, right_path)
    left_active = left_mask > 0
    right_active = right_mask > 0
    overlap = int(np.count_nonzero(left_active & right_active))
    union = int(np.count_nonzero(left_active | right_active))
    left_only = int(np.count_nonzero(left_active & ~right_active))
    right_only = int(np.count_nonzero(~left_active & right_active))
    diagnostics = helper._compute_mask_diagnostics(left_mask, right_mask)
    metrics = {
        "overlap_cells": overlap,
        "union_cells": union,
        "100k_only_cells": left_only,
        "5k_only_cells": right_only,
        "IoU": float(overlap / union) if union > 0 else 1.0,
        "Dice": float((2.0 * overlap) / (int(np.count_nonzero(left_active)) + int(np.count_nonzero(right_active))))
        if (int(np.count_nonzero(left_active)) + int(np.count_nonzero(right_active))) > 0
        else 1.0,
        "centroid_distance_between_100k_and_5k_m": diagnostics["centroid_distance_m"],
        "nearest_100k_to_5k_distance_m": diagnostics["nearest_distance_to_obs_m"],
    }
    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
        metrics[f"FSS_{window_km}km"] = float(
            np.clip(
                calculate_fss(
                    left_mask,
                    right_mask,
                    window=helper._window_km_to_cells(window_km),
                    valid_mask=_valid_mask(helper),
                ),
                0.0,
                1.0,
            )
        )
    return metrics


def _overlay_canvas(forecast_mask: np.ndarray, observation_mask: np.ndarray) -> np.ndarray:
    overlap = (forecast_mask > 0) & (observation_mask > 0)
    canvas = np.ones((forecast_mask.shape[0], forecast_mask.shape[1], 3), dtype=np.float32)
    canvas[observation_mask > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
    canvas[forecast_mask > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
    canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
    return canvas


def _single_mask_canvas(mask: np.ndarray, color: tuple[float, float, float]) -> np.ndarray:
    canvas = np.ones((mask.shape[0], mask.shape[1], 3), dtype=np.float32)
    canvas[mask > 0] = np.array(color, dtype=np.float32)
    return canvas


def _difference_canvas(canonical_mask: np.ndarray, experimental_mask: np.ndarray) -> np.ndarray:
    canvas = np.ones((canonical_mask.shape[0], canonical_mask.shape[1], 3), dtype=np.float32)
    overlap = (canonical_mask > 0) & (experimental_mask > 0)
    canonical_only = (canonical_mask > 0) & ~(experimental_mask > 0)
    experimental_only = ~(canonical_mask > 0) & (experimental_mask > 0)
    canvas[overlap] = np.array([0.35, 0.35, 0.35], dtype=np.float32)
    canvas[canonical_only] = np.array([0.95, 0.55, 0.2], dtype=np.float32)
    canvas[experimental_only] = np.array([0.2, 0.75, 0.35], dtype=np.float32)
    return canvas


def _add_image_panel(ax, image: np.ndarray, title: str) -> None:
    ax.imshow(image, origin="upper")
    ax.set_title(title, fontsize=10)
    ax.set_axis_off()


def _load_pygnome_track(helper: Phase3BScoringService, observation_path: Path) -> dict[str, Any] | None:
    pygnome_mask_path = (
        PYGNOME_DIR
        / "tracks"
        / "pygnome_reinit_deterministic"
        / "pygnome_footprint_mask_2023-03-14_localdate.tif"
    )
    pygnome_manifest_path = PYGNOME_DIR / "march13_14_reinit_crossmodel_run_manifest.json"
    if not pygnome_mask_path.exists():
        return None
    metrics = _compute_metrics_against_observation(
        helper=helper,
        forecast_path=pygnome_mask_path,
        observation_path=observation_path,
    )
    pygnome_metadata_path = (
        PYGNOME_DIR / "tracks" / "pygnome_reinit_deterministic" / "pygnome_reinit_metadata.json"
    )
    element_count = None
    if pygnome_metadata_path.exists():
        metadata = _load_json(pygnome_metadata_path)
        element_count = metadata.get("benchmark_particles")
    return {
        "row_id": "PyGNOME_deterministic",
        "element_count_requested": element_count,
        "element_count_detected": element_count,
        "branch": "PyGNOME_deterministic",
        "forecast_mask_path": _relative(pygnome_mask_path),
        "observation_mask_path": _relative(observation_path),
        "manifest_path": _relative(pygnome_manifest_path) if pygnome_manifest_path.exists() else "",
        "notes": "Optional stored comparator-only PyGNOME deterministic footprint. Not rerun by this script.",
        **metrics,
    }


def _metrics_row_from_summary(
    *,
    helper: Phase3BScoringService,
    row_id: str,
    summary_row: dict[str, Any],
    observation_path: Path,
    requested_count: Any,
    detected_count: Any,
    manifest_path: str,
    notes: str,
) -> dict[str, Any]:
    metrics = _compute_metrics_against_observation(
        helper=helper,
        forecast_path=Path(str(summary_row["forecast_path"])),
        observation_path=observation_path,
    )
    return {
        "row_id": row_id,
        "element_count_requested": requested_count,
        "element_count_detected": detected_count,
        "branch": str(summary_row.get("branch_id") or ""),
        "forecast_cells": metrics["forecast_nonzero_cells"],
        "observed_cells": metrics["obs_nonzero_cells"],
        "nearest_distance_m": metrics["nearest_distance_to_obs_m"],
        "centroid_distance_m": metrics["centroid_distance_m"],
        "IoU": metrics["iou"],
        "Dice": metrics["dice"],
        "FSS_1km": metrics["FSS_1km"],
        "FSS_3km": metrics["FSS_3km"],
        "FSS_5km": metrics["FSS_5km"],
        "FSS_10km": metrics["FSS_10km"],
        "mean_FSS": metrics["mean_FSS"],
        "forecast_mask_path": str(summary_row["forecast_path"]),
        "observation_mask_path": _relative(observation_path),
        "manifest_path": manifest_path,
        "notes": notes,
    }


def _board_text(metrics_rows: list[dict[str, Any]], pairwise_row: dict[str, Any]) -> str:
    canonical_row = next(row for row in metrics_rows if row["row_id"] == "canonical_100k_R1_previous")
    experimental_row = next(row for row in metrics_rows if row["row_id"] == "experiment_5000_R1_previous")
    observed_cells = canonical_row["observed_cells"]
    lines = [
        "Primary B1 branch comparison",
        f"Observed cells: {observed_cells}",
        f"100k forecast cells: {canonical_row['forecast_cells']}",
        f"5k forecast cells: {experimental_row['forecast_cells']}",
        f"100k nearest distance: {_format_distance(canonical_row['nearest_distance_m'])}",
        f"5k nearest distance: {_format_distance(experimental_row['nearest_distance_m'])}",
        f"100k centroid distance: {_format_distance(canonical_row['centroid_distance_m'])}",
        f"5k centroid distance: {_format_distance(experimental_row['centroid_distance_m'])}",
        (
            "100k FSS 1/3/5/10 km: "
            f"{_format_number(canonical_row['FSS_1km'])} / {_format_number(canonical_row['FSS_3km'])} / "
            f"{_format_number(canonical_row['FSS_5km'])} / {_format_number(canonical_row['FSS_10km'])}"
        ),
        (
            "5k FSS 1/3/5/10 km: "
            f"{_format_number(experimental_row['FSS_1km'])} / {_format_number(experimental_row['FSS_3km'])} / "
            f"{_format_number(experimental_row['FSS_5km'])} / {_format_number(experimental_row['FSS_10km'])}"
        ),
        (
            "Mean FSS 100k vs 5k: "
            f"{_format_number(canonical_row['mean_FSS'])} vs {_format_number(experimental_row['mean_FSS'])}"
        ),
        (
            "100k-vs-5k IoU / Dice: "
            f"{_format_number(pairwise_row['IoU'])} / {_format_number(pairwise_row['Dice'])}"
        ),
        (
            "100k-vs-5k overlap / union cells: "
            f"{pairwise_row['overlap_cells']} / {pairwise_row['union_cells']}"
        ),
    ]
    return "\n".join(lines)


def _write_board(
    *,
    helper: Phase3BScoringService,
    seed_mask_path: Path,
    observation_path: Path,
    canonical_forecast_path: Path,
    experimental_forecast_path: Path,
    pairwise_row: dict[str, Any],
    metrics_rows: list[dict[str, Any]],
    pygnome_track: dict[str, Any] | None,
) -> None:
    seed_mask = _load_mask(helper, seed_mask_path)
    observation_mask = _load_mask(helper, observation_path)
    canonical_mask = _load_mask(helper, canonical_forecast_path)
    experimental_mask = _load_mask(helper, experimental_forecast_path)

    fig = plt.figure(figsize=(16, 14))
    grid = fig.add_gridspec(3, 3, height_ratios=[1.0, 1.0, 0.7])
    axes = [
        fig.add_subplot(grid[0, 0]),
        fig.add_subplot(grid[0, 1]),
        fig.add_subplot(grid[0, 2]),
        fig.add_subplot(grid[1, 0]),
        fig.add_subplot(grid[1, 1]),
        fig.add_subplot(grid[1, 2]),
    ]
    text_ax = fig.add_subplot(grid[2, :])

    _add_image_panel(axes[0], _single_mask_canvas(seed_mask, (0.95, 0.55, 0.2)), "March 13 seed observation mask")
    _add_image_panel(axes[1], _single_mask_canvas(observation_mask, (0.2, 0.45, 0.95)), "March 14 target observation mask")
    _add_image_panel(
        axes[2],
        _overlay_canvas(canonical_mask, observation_mask),
        "Canonical 100,000-element OpenDrift R1_previous p50 vs target",
    )
    _add_image_panel(
        axes[3],
        _overlay_canvas(experimental_mask, observation_mask),
        "Experimental 5,000-element OpenDrift R1_previous p50 vs target",
    )
    _add_image_panel(
        axes[4],
        _difference_canvas(canonical_mask, experimental_mask),
        "100k vs 5k R1_previous p50 overlap / difference",
    )
    if pygnome_track is not None:
        pygnome_mask = _load_mask(helper, REPO_ROOT / pygnome_track["forecast_mask_path"])
        _add_image_panel(
            axes[5],
            _overlay_canvas(pygnome_mask, observation_mask),
            "Optional stored PyGNOME deterministic comparator",
        )
    else:
        axes[5].set_facecolor("#f3f3f3")
        axes[5].text(
            0.5,
            0.5,
            "PyGNOME deterministic comparator\nnot available in stored outputs.\nNot rerun here.",
            ha="center",
            va="center",
            fontsize=11,
        )
        axes[5].set_title("Optional PyGNOME comparator")
        axes[5].set_axis_off()

    text_ax.axis("off")
    text_ax.text(
        0.01,
        0.98,
        _board_text(metrics_rows, pairwise_row),
        va="top",
        ha="left",
        fontsize=10,
        family="monospace",
    )

    fig.suptitle(TITLE, fontsize=16, fontweight="bold", y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    BOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(BOARD_PATH, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _write_how_100k(canonical_run_manifest: dict[str, Any], canonical_branch_run: dict[str, Any]) -> None:
    recipe = str(((canonical_run_manifest.get("recipe") or {}).get("recipe")) or "unknown")
    requested_count = canonical_run_manifest.get("requested_element_count") or 100000
    expected_member_count = canonical_run_manifest.get("expected_ensemble_member_count") or 50
    lines = [
        "# How the canonical 100k B1 bundle was done",
        "",
        "This note summarizes only provenance that is explicitly visible in the stored canonical manifests and source code.",
        "",
        "## Manifest-recorded provenance",
        "",
        f"- Canonical output directory: `{_relative(CANONICAL_DIR)}`",
        f"- Seed observation date: `{canonical_run_manifest.get('window', {}).get('seed_obs_date', '2023-03-13')}`",
        f"- Target observation date: `{canonical_run_manifest.get('window', {}).get('scored_target_date', '2023-03-14')}`",
        f"- Requested element count recorded in the canonical reinit manifest: `{requested_count}`",
        f"- Expected ensemble member count recorded in the canonical reinit manifest: `{expected_member_count}`",
        f"- Selected recipe recorded in the canonical reinit manifest: `{recipe}`",
        f"- Canonical branch manifest path for R1_previous: `{canonical_branch_run.get('forecast_manifest_path') or _fallback_branch_manifest_path(CANONICAL_DIR, 'R1_previous', 'forecast')}`",
        f"- Canonical ensemble manifest path for R1_previous: `{canonical_branch_run.get('manifest_path') or _fallback_branch_manifest_path(CANONICAL_DIR, 'R1_previous', 'ensemble')}`",
        "",
        "## Source-code facts used by the canonical service",
        "",
        "- `src/services/phase3b_extended_public_scored_march13_14_reinit.py` defines two reinit branches: `R0` and `R1_previous`.",
        "- The same service sets the canonical default `REQUESTED_ELEMENT_COUNT = 100000` and `EXPECTED_ENSEMBLE_MEMBER_COUNT = 50`.",
        "- The reinit branch call goes through `run_official_spill_forecast(...)` and temporarily applies `OFFICIAL_ELEMENT_COUNT_OVERRIDE` while the branch run is executed.",
        "- The official ensemble configuration is stored in `config/ensemble.yaml`, including the 50-member setup, start-time offset choices, wind-factor range, and diffusivity range.",
        "",
        "## Interpretation boundary",
        "",
        "This note is provenance-only. It does not recast the canonical B1 result, and it does not infer provenance that is absent from the stored manifests.",
    ]
    _write_text(HOW_100K_PATH, "\n".join(lines) + "\n")


def _write_readme(
    *,
    experiment_manifest: dict[str, Any],
    metrics_df: pd.DataFrame,
    pairwise_df: pd.DataFrame,
) -> None:
    strictness_value = experiment_manifest.get("exact_same_member_perturbations_as_canonical")
    if strictness_value is True:
        strictness_line = (
            "This is a strict element-count-only comparison with identical member perturbations proven from the stored canonical and experimental ensemble manifests."
        )
    else:
        strictness_line = (
            "This is a best-effort element-count comparison with the same seed policy requested, but identical member perturbations were not fully proven from stored manifests."
        )

    lines = [
        f"# {TITLE}",
        "",
        "- Status: personal experiment only",
        "- Thesis-facing: false",
        "- Reportable: false",
        "- Experimental only: true",
        "- Changed scientific parameter: requested element count only",
        f"- Canonical 100k directory: `{_relative(CANONICAL_DIR)}`",
        f"- Experimental 5k directory: `{_relative(EXPERIMENT_DIR)}`",
        f"- Comparator package directory: `{_relative(SENSITIVITY_DIR)}`",
        "",
        "## Comparison status",
        "",
        f"- {strictness_line}",
        f"- Reason if member identity was not proven: `{experiment_manifest.get('reason_if_member_identity_not_proven', '') or 'n/a'}`",
        f"- Actual element count detected from experiment manifest: `{experiment_manifest.get('element_count_detected_from_manifest')}`",
        f"- Actual member count detected from experiment manifest: `{experiment_manifest.get('actual_member_count_detected')}`",
        f"- Canonical outputs modified: `{str(experiment_manifest.get('canonical_outputs_modified', False)).lower()}`",
        "",
        "## Key metrics",
        "",
        "```csv",
        metrics_df.to_csv(index=False).strip(),
        "```",
        "",
        "## Pairwise similarity",
        "",
        "```csv",
        pairwise_df.to_csv(index=False).strip(),
        "```",
        "",
        "## Interpretation boundary",
        "",
        "This package is an internal element-count sensitivity check only. It does not replace the canonical March 13 -> March 14 B1 result, and it is not for the thesis manuscript, final validation package, final reproducibility package, or publication figure package.",
    ]
    _write_text(README_PATH, "\n".join(lines) + "\n")


def _write_run_completion_note(
    *,
    experiment_manifest: dict[str, Any],
    metrics_rows: list[dict[str, Any]],
) -> None:
    primary_row = next(row for row in metrics_rows if row["row_id"] == "experiment_5000_R1_previous")
    lines = [
        "# Run completion note",
        "",
        f"- Generated at UTC: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Comparator board: `{_relative(BOARD_PATH)}`",
        f"- Metrics CSV: `{_relative(METRICS_CSV_PATH)}`",
        f"- Pairwise CSV: `{_relative(PAIRWISE_CSV_PATH)}`",
        f"- Sensitivity manifest: `{_relative(MANIFEST_PATH)}`",
        f"- README: `{_relative(README_PATH)}`",
        f"- Experiment output directory: `{_relative(EXPERIMENT_DIR)}`",
        f"- Canonical output directory: `{_relative(CANONICAL_DIR)}`",
        f"- Experiment actual element count detected: `{experiment_manifest.get('element_count_detected_from_manifest')}`",
        f"- Exact same member perturbations as canonical: `{experiment_manifest.get('exact_same_member_perturbations_as_canonical')}`",
        f"- Canonical outputs modified: `{str(experiment_manifest.get('canonical_outputs_modified', False)).lower()}`",
        f"- Experimental R1_previous mean FSS: `{_format_number(primary_row['mean_FSS'])}`",
    ]
    _write_text(RUN_NOTE_PATH, "\n".join(lines) + "\n")


def run_mindoro_b1_element_count_sensitivity_board() -> dict[str, Any]:
    canonical_manifest_path = CANONICAL_DIR / "march13_14_reinit_run_manifest.json"
    experiment_manifest_path = EXPERIMENT_DIR / "march13_14_reinit_run_manifest.json"
    canonical_summary_path = CANONICAL_DIR / "march13_14_reinit_summary.csv"
    experiment_summary_path = EXPERIMENT_DIR / "march13_14_reinit_summary.csv"

    if not canonical_manifest_path.exists():
        raise FileNotFoundError(f"Canonical manifest missing: {canonical_manifest_path}")
    if not experiment_manifest_path.exists():
        raise FileNotFoundError(f"Experimental manifest missing: {experiment_manifest_path}")

    canonical_manifest = _load_json(canonical_manifest_path)
    experiment_manifest = _load_json(experiment_manifest_path)
    canonical_branch_runs = _branch_manifest_index(canonical_manifest)
    experiment_branch_runs = _branch_manifest_index(experiment_manifest)

    canonical_r1 = _load_summary_row(canonical_summary_path, "R1_previous")
    experiment_r1 = _load_summary_row(experiment_summary_path, "R1_previous")
    observation_path = Path(str(canonical_r1["observation_path"]))
    seed_mask_path = Path(str((canonical_manifest.get("seed_release") or {}).get("seed_mask_path") or ""))
    if not seed_mask_path.exists():
        raise FileNotFoundError(f"Seed mask path missing: {seed_mask_path}")

    helper = Phase3BScoringService(
        output_dir=SENSITIVITY_DIR / "_scratch_helper",
        forecast_run_name=RUN_NAME,
        observation_run_name=RUN_NAME,
        run_context={"phase": "element_count_sensitivity"},
    )

    metrics_rows: list[dict[str, Any]] = []
    metrics_rows.append(
        _metrics_row_from_summary(
            helper=helper,
            row_id="canonical_100k_R1_previous",
            summary_row=canonical_r1,
            observation_path=observation_path,
            requested_count=canonical_manifest.get("requested_element_count") or 100000,
            detected_count=canonical_manifest.get("element_count_detected_from_manifest") or 100000,
            manifest_path=str(
                canonical_branch_runs.get("R1_previous", {}).get("forecast_manifest_path")
                or _fallback_branch_manifest_path(CANONICAL_DIR, "R1_previous", "forecast")
            ),
            notes="Canonical B1 OpenDrift R1_previous March 14 local-date p50.",
        )
    )
    metrics_rows.append(
        _metrics_row_from_summary(
            helper=helper,
            row_id="experiment_5000_R1_previous",
            summary_row=experiment_r1,
            observation_path=observation_path,
            requested_count=experiment_manifest.get("requested_element_count") or 5000,
            detected_count=experiment_manifest.get("element_count_detected_from_manifest") or 5000,
            manifest_path=str(
                experiment_branch_runs.get("R1_previous", {}).get("forecast_manifest_path")
                or _fallback_branch_manifest_path(EXPERIMENT_DIR, "R1_previous", "forecast")
            ),
            notes="Experimental 5,000-element OpenDrift R1_previous March 14 local-date p50.",
        )
    )

    try:
        canonical_r0 = _load_summary_row(canonical_summary_path, "R0")
    except FileNotFoundError:
        canonical_r0 = None
    if canonical_r0 is not None:
        metrics_rows.append(
            _metrics_row_from_summary(
                helper=helper,
                row_id="canonical_100k_R0",
                summary_row=canonical_r0,
                observation_path=observation_path,
                requested_count=canonical_manifest.get("requested_element_count") or 100000,
                detected_count=canonical_manifest.get("element_count_detected_from_manifest") or 100000,
                manifest_path=str(
                    canonical_branch_runs.get("R0", {}).get("forecast_manifest_path")
                    or _fallback_branch_manifest_path(CANONICAL_DIR, "R0", "forecast")
                ),
                notes="Canonical R0 branch included for context only.",
            )
        )

    try:
        experiment_r0 = _load_summary_row(experiment_summary_path, "R0")
    except FileNotFoundError:
        experiment_r0 = None
    if experiment_r0 is not None:
        metrics_rows.append(
            _metrics_row_from_summary(
                helper=helper,
                row_id="experiment_5000_R0",
                summary_row=experiment_r0,
                observation_path=observation_path,
                requested_count=experiment_manifest.get("requested_element_count") or 5000,
                detected_count=experiment_manifest.get("element_count_detected_from_manifest") or 5000,
                manifest_path=str(
                    experiment_branch_runs.get("R0", {}).get("forecast_manifest_path")
                    or _fallback_branch_manifest_path(EXPERIMENT_DIR, "R0", "forecast")
                ),
                notes="Experimental R0 branch included for context only.",
            )
        )

    pygnome_track = _load_pygnome_track(helper, observation_path)
    if pygnome_track is not None:
        metrics_rows.append(
            {
                "row_id": pygnome_track["row_id"],
                "element_count_requested": pygnome_track["element_count_requested"],
                "element_count_detected": pygnome_track["element_count_detected"],
                "branch": pygnome_track["branch"],
                "forecast_cells": pygnome_track["forecast_nonzero_cells"],
                "observed_cells": pygnome_track["obs_nonzero_cells"],
                "nearest_distance_m": pygnome_track["nearest_distance_to_obs_m"],
                "centroid_distance_m": pygnome_track["centroid_distance_m"],
                "IoU": pygnome_track["iou"],
                "Dice": pygnome_track["dice"],
                "FSS_1km": pygnome_track["FSS_1km"],
                "FSS_3km": pygnome_track["FSS_3km"],
                "FSS_5km": pygnome_track["FSS_5km"],
                "FSS_10km": pygnome_track["FSS_10km"],
                "mean_FSS": pygnome_track["mean_FSS"],
                "forecast_mask_path": pygnome_track["forecast_mask_path"],
                "observation_mask_path": pygnome_track["observation_mask_path"],
                "manifest_path": pygnome_track["manifest_path"],
                "notes": pygnome_track["notes"],
            }
        )

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_df.to_csv(METRICS_CSV_PATH, index=False)

    pairwise_metrics = _compute_pairwise_mask_metrics(
        helper=helper,
        left_path=Path(str(canonical_r1["forecast_path"])),
        right_path=Path(str(experiment_r1["forecast_path"])),
    )
    pairwise_df = pd.DataFrame(
        [
            {
                "branch_pair": "canonical_100k_R1_previous_vs_experiment_5000_R1_previous",
                "overlap_cells": pairwise_metrics["overlap_cells"],
                "union_cells": pairwise_metrics["union_cells"],
                "100k_only_cells": pairwise_metrics["100k_only_cells"],
                "5k_only_cells": pairwise_metrics["5k_only_cells"],
                "IoU": pairwise_metrics["IoU"],
                "Dice": pairwise_metrics["Dice"],
                "FSS_1km": pairwise_metrics["FSS_1km"],
                "FSS_3km": pairwise_metrics["FSS_3km"],
                "FSS_5km": pairwise_metrics["FSS_5km"],
                "FSS_10km": pairwise_metrics["FSS_10km"],
                "centroid_distance_between_100k_and_5k_m": pairwise_metrics[
                    "centroid_distance_between_100k_and_5k_m"
                ],
                "nearest_100k_to_5k_distance_m": pairwise_metrics["nearest_100k_to_5k_distance_m"],
                "notes": "Pairwise similarity is computed between the March 14 local-date R1_previous p50 masks only.",
            }
        ]
    )
    pairwise_df.to_csv(PAIRWISE_CSV_PATH, index=False)

    _write_board(
        helper=helper,
        seed_mask_path=seed_mask_path,
        observation_path=observation_path,
        canonical_forecast_path=Path(str(canonical_r1["forecast_path"])),
        experimental_forecast_path=Path(str(experiment_r1["forecast_path"])),
        pairwise_row=pairwise_df.iloc[0].to_dict(),
        metrics_rows=metrics_rows,
        pygnome_track=pygnome_track,
    )

    _write_how_100k(canonical_manifest, canonical_branch_runs.get("R1_previous", {}))
    _write_readme(
        experiment_manifest=experiment_manifest,
        metrics_df=metrics_df,
        pairwise_df=pairwise_df,
    )

    manifest_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "thesis_facing": False,
        "reportable": False,
        "experimental_only": True,
        "purpose": "personal element-count sensitivity check",
        "not_for_publication_package": True,
        "not_for_final_validation_package": True,
        "not_for_final_reproducibility_package": True,
        "canonical_100k_output_dir": _relative(CANONICAL_DIR),
        "experimental_5000_output_dir": _relative(EXPERIMENT_DIR),
        "changed_parameter": "requested_element_count",
        "canonical_requested_element_count": 100000,
        "experimental_requested_element_count": 5000,
        "expected_ensemble_member_count": 50,
        "actual_member_count_detected": experiment_manifest.get("actual_member_count_detected"),
        "exact_same_member_perturbations_as_canonical": experiment_manifest.get(
            "exact_same_member_perturbations_as_canonical"
        ),
        "reason_if_member_identity_not_proven": experiment_manifest.get(
            "reason_if_member_identity_not_proven"
        ),
        "seed_obs_date": "2023-03-13",
        "target_obs_date": "2023-03-14",
        "independent_noaa_observation_products_confirmed": True,
        "interpretation_boundary": (
            "This experiment is not a thesis-facing validation row and does not replace canonical B1."
        ),
        "canonical_outputs_modified": False,
        "artifacts": {
            "board_png": _relative(BOARD_PATH),
            "metrics_csv": _relative(METRICS_CSV_PATH),
            "pairwise_similarity_csv": _relative(PAIRWISE_CSV_PATH),
            "readme_md": _relative(README_PATH),
            "how_100k_was_done_md": _relative(HOW_100K_PATH),
            "run_completion_note_md": _relative(RUN_NOTE_PATH),
        },
        "optional_pygnome_included": pygnome_track is not None,
        "metrics_rows": metrics_df.to_dict(orient="records"),
        "pairwise_rows": pairwise_df.to_dict(orient="records"),
    }
    _write_json(MANIFEST_PATH, manifest_payload)
    _write_run_completion_note(
        experiment_manifest=experiment_manifest,
        metrics_rows=metrics_rows,
    )
    return {
        "board_png": str(BOARD_PATH),
        "metrics_csv": str(METRICS_CSV_PATH),
        "pairwise_similarity_csv": str(PAIRWISE_CSV_PATH),
        "manifest_json": str(MANIFEST_PATH),
        "readme_md": str(README_PATH),
        "run_completion_note_md": str(RUN_NOTE_PATH),
        "optional_pygnome_included": pygnome_track is not None,
    }


def main() -> int:
    results = run_mindoro_b1_element_count_sensitivity_board()
    print(f"Board written to: {results['board_png']}")
    print(f"Metrics CSV written to: {results['metrics_csv']}")
    print(f"Pairwise similarity CSV written to: {results['pairwise_similarity_csv']}")
    print(f"Manifest written to: {results['manifest_json']}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
