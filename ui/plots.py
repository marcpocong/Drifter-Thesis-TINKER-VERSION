"""Small matplotlib plots for the read-only dashboard."""

from __future__ import annotations

import matplotlib
import pandas as pd
from matplotlib import pyplot as plt

matplotlib.use("Agg")


def phase_status_overview_figure(phase_status: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    if phase_status.empty:
        ax.text(0.5, 0.5, "Phase-status registry not available.", ha="center", va="center")
        ax.axis("off")
        return fig
    grouped = (
        phase_status.assign(
            reportable_now=phase_status["reportable_now"].astype(bool),
            inherited_provisional=phase_status["inherited_provisional"].astype(bool),
        )
        .groupby("phase_id", dropna=False)[["reportable_now", "inherited_provisional"]]
        .sum()
        .reset_index()
    )
    x = range(len(grouped))
    ax.bar(x, grouped["reportable_now"], label="Reportable now", color="#165ba8")
    ax.bar(x, grouped["inherited_provisional"], label="Inherited-provisional", color="#f28c28", alpha=0.8)
    ax.set_xticks(list(x), grouped["phase_id"].astype(str).tolist(), rotation=0)
    ax.set_ylabel("Track count")
    ax.set_title("Current reportable and inherited-provisional tracks")
    ax.legend(frameon=False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    return fig


def phase4_budget_summary_figure(summary_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8.5, 4.0))
    if summary_df.empty:
        ax.text(0.5, 0.5, "Phase 4 oil-budget summary not available.", ha="center", va="center")
        ax.axis("off")
        return fig
    required_columns = {"oil_label", "final_evaporated_pct", "final_dispersed_pct", "final_beached_pct"}
    if not required_columns.issubset(summary_df.columns):
        ax.text(0.5, 0.5, "Phase 4 oil-budget summary is incomplete.", ha="center", va="center")
        ax.axis("off")
        return fig
    df = summary_df.copy()
    x = range(len(df))
    ax.bar(x, df["final_evaporated_pct"], label="Evaporated %", color="#f28c28")
    ax.bar(x, df["final_dispersed_pct"], bottom=df["final_evaporated_pct"], label="Dispersed %", color="#1f7a4d")
    ax.bar(
        x,
        df["final_beached_pct"],
        bottom=df["final_evaporated_pct"] + df["final_dispersed_pct"],
        label="Beached %",
        color="#8c564b",
    )
    ax.set_xticks(list(x), df["oil_label"].astype(str).tolist(), rotation=15, ha="right")
    ax.set_ylabel("Percent of initial mass")
    ax.set_title("Mindoro Phase 4 final oil-budget compartments")
    ax.legend(frameon=False)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    return fig


def comparability_status_figure(matrix_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(7.2, 3.8))
    if matrix_df.empty or "classification" not in matrix_df.columns:
        ax.text(0.5, 0.5, "Phase 4 cross-model matrix not available.", ha="center", va="center")
        ax.axis("off")
        return fig
    display_map = {
        "directly_comparable_now": "Comparable now",
        "comparable_with_small_adapter": "Small adapter needed",
        "no_matched_phase4_pygnome_package_yet": "No matched\nPyGNOME package yet",
    }
    counts = (
        matrix_df["classification"]
        .astype(str)
        .map(lambda value: display_map.get(value, value.replace("_", " ")))
        .value_counts()
        .sort_index()
    )
    ax.bar(counts.index.tolist(), counts.values.tolist(), color="#9b4dca")
    ax.set_ylabel("Quantity count")
    ax.set_title("Phase 4 cross-model comparability status")
    ax.tick_params(axis="x", rotation=0)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    return fig
