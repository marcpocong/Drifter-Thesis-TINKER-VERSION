"""Generate a Chapter 5-ready map for the focused Phase 1 accepted February-April subset."""

from __future__ import annotations

import argparse
import json
import textwrap
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Rectangle

from src.services.figure_package_publication import FigurePackagePublicationService

ACCEPTED_REGISTRY_PATH = (
    Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_accepted_segment_registry.csv"
)
RANKING_SUBSET_REGISTRY_PATH = (
    Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_ranking_subset_registry.csv"
)
CONFIG_PATH = Path("config") / "phase1_mindoro_focus_pre_spill_2016_2023.yaml"
RANKING_SUBSET_REPORT_PATH = (
    Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_ranking_subset_report.md"
)

DEFAULT_OUTPUT_DIR = Path("output") / "chapter5_generated"
DEFAULT_OUTPUT_STEM = "focused_phase1_accepted_february_april_segment_map"

MONTH_STYLES: dict[int, dict[str, str]] = {
    2: {"label": "February subset", "color": "#d97706"},
    3: {"label": "March subset", "color": "#0f766e"},
    4: {"label": "April subset", "color": "#7c3aed"},
}
MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})


def _month_number(frame: pd.DataFrame) -> pd.Series:
    month_from_key = pd.to_numeric(frame["month_key"].astype(str).str[4:6], errors="coerce")
    month_from_time = pd.to_datetime(frame["start_time_utc"], errors="coerce").dt.month
    return month_from_key.fillna(month_from_time).astype("Int64")


def _load_registries(repo_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    accepted = pd.read_csv(repo_root / ACCEPTED_REGISTRY_PATH)
    subset = pd.read_csv(repo_root / RANKING_SUBSET_REGISTRY_PATH)
    if accepted.empty:
        raise ValueError(f"Accepted segment registry is empty: {ACCEPTED_REGISTRY_PATH.as_posix()}")
    if subset.empty:
        raise ValueError(f"Ranking subset registry is empty: {RANKING_SUBSET_REGISTRY_PATH.as_posix()}")

    accepted = accepted.copy()
    subset = subset.copy()
    accepted["month_number"] = _month_number(accepted)
    subset["month_number"] = _month_number(subset)

    flagged_subset = accepted.loc[_bool_series(accepted["ranking_subset_included"])].copy()
    flagged_ids = set(flagged_subset["segment_id"].astype(str))
    subset_ids = set(subset["segment_id"].astype(str))
    if flagged_ids and flagged_ids != subset_ids:
        raise ValueError(
            "Accepted-registry ranking subset flags do not match the dedicated ranking subset registry."
        )
    if not subset_ids.issubset(set(accepted["segment_id"].astype(str))):
        raise ValueError("Ranking subset registry contains segment IDs missing from the accepted registry.")

    accepted = accepted.sort_values("start_time_utc", kind="stable").reset_index(drop=True)
    subset = subset.sort_values("start_time_utc", kind="stable").reset_index(drop=True)
    return accepted, subset


def _focused_box_entry(service: FigurePackagePublicationService) -> dict[str, Any]:
    for entry in service._thesis_study_box_entries():
        if str(entry.get("study_box_id")) == "focused_phase1_validation_box":
            return entry
    raise ValueError("Focused Study Box 1 metadata is unavailable.")


def _map_extent(bounds: tuple[float, float, float, float]) -> tuple[tuple[float, float], tuple[float, float]]:
    min_lon, max_lon, min_lat, max_lat = [float(value) for value in bounds]
    lon_span = max(max_lon - min_lon, 1.0)
    lat_span = max(max_lat - min_lat, 1.0)
    pad_lon = max(0.60, lon_span * 0.08)
    pad_lat = max(0.45, lat_span * 0.08)
    return (min_lon - pad_lon, max_lon + pad_lon), (min_lat - pad_lat, max_lat + pad_lat)


def _source_point(frame: pd.DataFrame) -> tuple[float, float] | None:
    if "distance_audit_source_lon" not in frame.columns or "distance_audit_source_lat" not in frame.columns:
        return None
    lon_values = pd.to_numeric(frame["distance_audit_source_lon"], errors="coerce").dropna()
    lat_values = pd.to_numeric(frame["distance_audit_source_lat"], errors="coerce").dropna()
    if lon_values.empty or lat_values.empty:
        return None
    return float(lon_values.iloc[0]), float(lat_values.iloc[0])


def _month_count_lines(subset: pd.DataFrame) -> list[str]:
    counts = (
        subset["month_number"]
        .dropna()
        .astype(int)
        .value_counts()
        .reindex([2, 3, 4], fill_value=0)
        .to_dict()
    )
    return [
        f"February subset: {counts.get(2, 0)}",
        f"March subset: {counts.get(3, 0)}",
        f"April subset: {counts.get(4, 0)}",
    ]


def _subset_month_summary(subset: pd.DataFrame) -> str:
    counts = (
        subset["month_number"]
        .dropna()
        .astype(int)
        .value_counts()
        .reindex([2, 3, 4], fill_value=0)
        .to_dict()
    )
    return ", ".join(
        f"{MONTH_NAMES[month]} {counts.get(month, 0)}" for month in (2, 3, 4)
    )


def _draw_segments(ax: plt.Axes, accepted: pd.DataFrame, subset: pd.DataFrame) -> None:
    for row in accepted.itertuples(index=False):
        ax.plot(
            [float(row.start_lon), float(row.end_lon)],
            [float(row.start_lat), float(row.end_lat)],
            color="#64748b",
            alpha=0.24,
            linewidth=1.05,
            solid_capstyle="round",
            zorder=4,
        )

    for month_number, style in MONTH_STYLES.items():
        month_subset = subset.loc[subset["month_number"] == month_number]
        if month_subset.empty:
            continue
        for row in month_subset.itertuples(index=False):
            ax.annotate(
                "",
                xy=(float(row.end_lon), float(row.end_lat)),
                xytext=(float(row.start_lon), float(row.start_lat)),
                arrowprops={
                    "arrowstyle": "-|>",
                    "color": style["color"],
                    "linewidth": 2.15,
                    "alpha": 0.94,
                    "shrinkA": 0.0,
                    "shrinkB": 0.0,
                    "mutation_scale": 10.5,
                },
                zorder=7,
            )
        ax.scatter(
            month_subset["start_lon"],
            month_subset["start_lat"],
            s=20,
            facecolor="#ffffff",
            edgecolor=style["color"],
            linewidth=0.9,
            zorder=8,
        )


def _draw_focus_box(ax: plt.Axes, bounds: tuple[float, float, float, float], color: str) -> None:
    min_lon, max_lon, min_lat, max_lat = [float(value) for value in bounds]
    rect = Rectangle(
        (min_lon, min_lat),
        max_lon - min_lon,
        max_lat - min_lat,
        linewidth=2.15,
        edgecolor=color,
        facecolor=matplotlib.colors.to_rgba(color, alpha=0.08),
        zorder=5,
    )
    ax.add_patch(rect)
    ax.text(
        min_lon + 0.08,
        max_lat - 0.10,
        "Study Box 1",
        ha="left",
        va="top",
        fontsize=8.2,
        fontweight="bold",
        color=color,
        bbox={
            "boxstyle": "round,pad=0.18",
            "facecolor": "#ffffff",
            "edgecolor": color,
            "linewidth": 1.0,
        },
        zorder=9,
    )


def _legend_handles(accepted_count: int, subset: pd.DataFrame, focus_color: str, source_color: str) -> list[Any]:
    counts = (
        subset["month_number"]
        .dropna()
        .astype(int)
        .value_counts()
        .reindex([2, 3, 4], fill_value=0)
        .to_dict()
    )
    handles: list[Any] = [
        Patch(
            facecolor=matplotlib.colors.to_rgba(focus_color, alpha=0.08),
            edgecolor=focus_color,
            linewidth=1.8,
            label="Study Box 1",
        ),
        Line2D(
            [0],
            [0],
            color="#64748b",
            linewidth=1.8,
            alpha=0.6,
            label=f"Accepted 72 h segments (n={accepted_count})",
        ),
    ]
    for month_number, style in MONTH_STYLES.items():
        handles.append(
            Line2D(
                [0],
                [0],
                color=style["color"],
                linewidth=2.2,
                marker="o",
                markerfacecolor="#ffffff",
                markeredgecolor=style["color"],
                markeredgewidth=0.9,
                markersize=5.5,
                label=f"{style['label']} (n={counts.get(month_number, 0)})",
            )
        )
    handles.append(
        Line2D(
            [0],
            [0],
            color="none",
            marker="*",
            markerfacecolor=source_color,
            markeredgecolor="#ffffff",
            markeredgewidth=0.8,
            markersize=12,
            label="Mindoro source point",
        )
    )
    return handles


def generate_focused_phase1_segment_map(
    *,
    repo_root: str | Path = ".",
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    output_stem: str = DEFAULT_OUTPUT_STEM,
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    output_dir = (repo_root / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    accepted, subset = _load_registries(repo_root)
    service = FigurePackagePublicationService(repo_root=repo_root)
    focused_box = _focused_box_entry(service)
    bounds = tuple(float(value) for value in focused_box["bounds"])
    xlim, ylim = _map_extent(bounds)

    output_path = output_dir / f"{output_stem}.png"
    metadata_path = output_dir / f"{output_stem}.json"

    fig = plt.figure(
        figsize=service._single_size(),
        dpi=service._dpi(),
        facecolor=(service.style.get("layout") or {}).get("figure_facecolor") or "#ffffff",
    )
    grid = fig.add_gridspec(
        2,
        2,
        width_ratios=[3.2, 1.45],
        height_ratios=[0.24, 0.76],
        left=0.05,
        right=0.98,
        top=0.855,
        bottom=0.07,
        wspace=0.18,
        hspace=0.12,
    )
    map_ax = fig.add_subplot(grid[:, 0])
    info_ax = fig.add_subplot(grid[0, 1])
    note_ax = fig.add_subplot(grid[1, 1])

    service._draw_study_box_geography_context(map_ax, xlim=xlim, ylim=ylim)
    focus_color = str(focused_box.get("color") or "#d97706")
    source_color = str((service._palette() or {}).get("source_point") or "#b42318")
    _draw_focus_box(map_ax, bounds, focus_color)
    _draw_segments(map_ax, accepted, subset)

    source_point = _source_point(accepted)
    if source_point is not None:
        source_lon, source_lat = source_point
        map_ax.scatter(
            [source_lon],
            [source_lat],
            marker="*",
            s=180,
            facecolor=source_color,
            edgecolor="#ffffff",
            linewidth=0.9,
            zorder=10,
        )
        map_ax.text(
            source_lon + 0.10,
            source_lat + 0.08,
            "Source point",
            fontsize=7.7,
            color=source_color,
            ha="left",
            va="bottom",
            zorder=10,
            bbox={
                "boxstyle": "round,pad=0.18",
                "facecolor": "#ffffff",
                "edgecolor": "#fecaca",
                "linewidth": 0.55,
            },
        )

    legend = map_ax.legend(
        handles=_legend_handles(len(accepted), subset, focus_color, source_color),
        loc="upper right",
        fontsize=7.9,
        frameon=True,
        facecolor="#ffffff",
        edgecolor="#cbd5e1",
        borderpad=0.55,
        labelspacing=0.45,
        handlelength=2.2,
    )
    legend.set_zorder(11)

    map_ax.set_xlim(*xlim)
    map_ax.set_ylim(*ylim)
    map_ax.set_title(
        "Accepted segments and ranked February-April subset",
        loc="left",
        fontsize=float((service.style.get("typography") or {}).get("panel_title_size") or 11),
        pad=12,
    )
    map_ax.set_xlabel("Longitude (degrees east)")
    map_ax.set_ylabel("Latitude (degrees north)")
    map_ax.grid(
        True,
        linestyle="--",
        linewidth=0.35,
        alpha=0.40,
        color=(service.style.get("layout") or {}).get("grid_color") or "#cbd5e1",
    )
    map_ax.set_aspect(service._geographic_aspect((ylim[0] + ylim[1]) / 2.0), adjustable="box")
    for spine in map_ax.spines.values():
        spine.set_color("#94a3b8")
        spine.set_linewidth(0.8)

    info_lines = [
        "Workflow: phase1_mindoro_focus_pre_spill_2016_2023",
        "Historical window: 2016-01-01 to 2023-03-02",
        f"Accepted segments in full strict registry: {len(accepted)}",
        f"Accepted segments included in ranking subset: {len(subset)}",
        f"Subset month mix: {_subset_month_summary(subset)}",
    ]
    service._add_note_box(
        info_ax,
        "Context",
        info_lines,
        title_y=0.98,
        body_y=0.74,
        box_pad=0.38,
        minimum_title_gap_px=26.0,
    )

    note_lines = [
        "Light gray vectors connect each accepted 72 h segment's stored start and end coordinates.",
        "Colored arrows highlight the accepted February-April subset used for the focused recipe ranking.",
        "The orange rectangle is Study Box 1, the focused Mindoro Phase 1 provenance box.",
        "The red star marks the stored Mindoro source-point reference carried in the registries.",
        *_month_count_lines(subset),
        "These vectors summarize segment displacement windows, not full drifter trajectories.",
    ]
    service._add_note_box(
        note_ax,
        "How to read this figure",
        note_lines,
        title_y=0.98,
        body_y=0.74,
        box_pad=0.38,
        minimum_title_gap_px=26.0,
    )

    subtitle = textwrap.fill(
        "Mindoro-focused drifter recipe-provenance workflow | 65 accepted 72 h segments with the 19-segment February-April ranking subset highlighted",
        width=116,
    )
    fig.suptitle(
        "Focused Phase 1 accepted February-April segment map",
        x=0.05,
        y=0.985,
        ha="left",
        fontsize=float((service.style.get("typography") or {}).get("title_size") or 19),
        fontweight="bold",
    )
    fig.text(
        0.05,
        0.948,
        subtitle,
        ha="left",
        va="top",
        fontsize=float((service.style.get("typography") or {}).get("subtitle_size") or 10),
        color="#475569",
        linespacing=1.16,
    )

    fig.savefig(output_path, dpi=service._dpi())
    pixel_width, pixel_height = service._figure_pixel_size(fig)
    plt.close(fig)

    metadata = {
        "title": "Focused Phase 1 accepted February-April segment map",
        "output_path": str(output_path),
        "pixel_width": pixel_width,
        "pixel_height": pixel_height,
        "accepted_registry_path": str((repo_root / ACCEPTED_REGISTRY_PATH).resolve()),
        "ranking_subset_registry_path": str((repo_root / RANKING_SUBSET_REGISTRY_PATH).resolve()),
        "config_path": str((repo_root / CONFIG_PATH).resolve()),
        "ranking_subset_report_path": str((repo_root / RANKING_SUBSET_REPORT_PATH).resolve()),
        "accepted_segment_count": int(len(accepted)),
        "ranking_subset_count": int(len(subset)),
        "subset_month_mix": {
            MONTH_NAMES.get(month_number, str(month_number)): int(count)
            for month_number, count in (
                subset["month_number"]
                .dropna()
                .astype(int)
                .value_counts()
                .reindex([2, 3, 4], fill_value=0)
                .items()
            )
        },
        "study_box_bounds_wgs84": [float(value) for value in bounds],
        "source_point_wgs84": list(source_point) if source_point is not None else None,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".", help="Repository root path.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for the rendered figure and metadata sidecar.",
    )
    parser.add_argument(
        "--output-stem",
        default=DEFAULT_OUTPUT_STEM,
        help="Base filename without extension.",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    metadata = generate_focused_phase1_segment_map(
        repo_root=args.repo_root,
        output_dir=args.output_dir,
        output_stem=args.output_stem,
    )
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
