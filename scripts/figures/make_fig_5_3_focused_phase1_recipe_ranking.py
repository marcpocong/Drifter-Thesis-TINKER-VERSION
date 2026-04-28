"""Generate Figure 5.3 for the focused Mindoro Phase 1 recipe ranking."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import FormatStrFormatter, MultipleLocator

REPO_ROOT = Path(__file__).resolve().parents[2]
FOCUSED_WORKFLOW = "phase1_mindoro_focus_pre_spill_2016_2023"
FOCUSED_SUBSET_LABEL = "mindoro_pre_spill_seasonal_subset_feb_apr"
FOCUSED_SUBSET_MONTHS = (2, 3, 4)
FOCUSED_SEGMENT_COUNT = 19
EXPECTED_RECIPES = ("cmems_gfs", "cmems_era5", "hycom_gfs", "hycom_era5")
EXPECTED_WINNER = "cmems_gfs"
OUTPUT_DIR = Path("output") / "chapter5_figures"
OUTPUT_STEM = "fig_5_3_focused_phase1_recipe_ranking"
OUTPUT_PNG = OUTPUT_DIR / f"{OUTPUT_STEM}.png"
OUTPUT_SVG = OUTPUT_DIR / f"{OUTPUT_STEM}.svg"
OUTPUT_SOURCE_CSV = OUTPUT_DIR / f"{OUTPUT_STEM}_source.csv"
OUTPUT_QA = OUTPUT_DIR / f"{OUTPUT_STEM}_QA.txt"

RECIPE_METADATA = {
    "cmems_gfs": {"current": "CMEMS", "wind": "GFS"},
    "cmems_era5": {"current": "CMEMS", "wind": "ERA5"},
    "hycom_gfs": {"current": "HYCOM", "wind": "GFS"},
    "hycom_era5": {"current": "HYCOM", "wind": "ERA5"},
}

FALLBACK_ROWS = [
    {
        "rank": 1,
        "recipe": "cmems_gfs",
        "current": "CMEMS",
        "wind": "GFS",
        "segments": 19,
        "mean_ncs": 4.5886,
        "median_ncs": 4.6305,
        "status": "Selected winner",
    },
    {
        "rank": 2,
        "recipe": "cmems_era5",
        "current": "CMEMS",
        "wind": "ERA5",
        "segments": 19,
        "mean_ncs": 4.6237,
        "median_ncs": 4.5916,
        "status": "Tested not selected",
    },
    {
        "rank": 3,
        "recipe": "hycom_gfs",
        "current": "HYCOM",
        "wind": "GFS",
        "segments": 19,
        "mean_ncs": 4.7027,
        "median_ncs": 4.9263,
        "status": "Tested not selected",
    },
    {
        "rank": 4,
        "recipe": "hycom_era5",
        "current": "HYCOM",
        "wind": "ERA5",
        "segments": 19,
        "mean_ncs": 4.7561,
        "median_ncs": 5.0106,
        "status": "Tested not selected",
    },
]


@dataclass
class SourceSelection:
    chart_table: pd.DataFrame
    source_csv_path: str | None
    source_kind: str
    used_fallback: bool
    lane: str
    subset_label: str
    subset_segment_count: int
    subset_note: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root. Defaults to the current script's repository.",
    )
    return parser.parse_args()


def _relative_to_repo(path: Path, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _month_from_key(series: pd.Series) -> pd.Series:
    month_key = series.astype(str).str.strip()
    month_values = pd.to_numeric(month_key.str[4:6], errors="coerce")
    return month_values.astype("Int64")


def _normalized_chart_table(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    working["recipe"] = working["recipe"].astype(str).str.strip()
    working = working.loc[working["recipe"].isin(EXPECTED_RECIPES)].copy()
    if set(working["recipe"]) != set(EXPECTED_RECIPES):
        raise ValueError("Focused chart table must contain all four focused Phase 1 recipes.")

    working["current"] = working["recipe"].map(lambda value: RECIPE_METADATA[value]["current"])
    working["wind"] = working["recipe"].map(lambda value: RECIPE_METADATA[value]["wind"])
    working["segments"] = pd.to_numeric(working["segments"], errors="raise").astype(int)
    working["mean_ncs"] = pd.to_numeric(working["mean_ncs"], errors="raise")
    working["median_ncs"] = pd.to_numeric(working["median_ncs"], errors="raise")
    working = working.sort_values(["mean_ncs", "recipe"], kind="stable").reset_index(drop=True)
    working["rank"] = range(1, len(working) + 1)
    winner = str(working.iloc[0]["recipe"])
    working["status"] = working["recipe"].map(
        lambda recipe: "Selected winner" if recipe == winner else "Tested not selected"
    )
    return working[["rank", "recipe", "current", "wind", "segments", "mean_ncs", "median_ncs", "status"]]


def _validate_subset_table(chart_table: pd.DataFrame) -> None:
    if set(chart_table["recipe"]) != set(EXPECTED_RECIPES):
        raise ValueError("The chart table does not include the required focused recipes.")
    if chart_table["segments"].nunique() != 1 or int(chart_table["segments"].iloc[0]) != FOCUSED_SEGMENT_COUNT:
        raise ValueError("The chart table is not restricted to the focused February-April subset with n = 19.")


def _from_recipe_ranking(frame: pd.DataFrame) -> pd.DataFrame | None:
    required = {"recipe", "segment_count", "mean_ncs_score", "median_ncs_score"}
    if not required.issubset(frame.columns):
        return None

    working = frame.copy()
    if "recipe_rank_pool" in working.columns:
        pools = set(working["recipe_rank_pool"].dropna().astype(str).str.strip())
        if pools and pools != {FOCUSED_SUBSET_LABEL}:
            return None

    working = working.rename(
        columns={
            "segment_count": "segments",
            "mean_ncs_score": "mean_ncs",
            "median_ncs_score": "median_ncs",
        }
    )
    try:
        normalized = _normalized_chart_table(working)
        _validate_subset_table(normalized)
        return normalized
    except ValueError:
        return None


def _from_recipe_summary(frame: pd.DataFrame) -> pd.DataFrame | None:
    return _from_recipe_ranking(frame)


def _from_segment_metrics(frame: pd.DataFrame) -> pd.DataFrame | None:
    required = {"segment_id", "recipe", "ncs_score"}
    if not required.issubset(frame.columns):
        return None

    working = frame.copy()
    if "validity_flag" in working.columns:
        working = working.loc[working["validity_flag"].astype(str).str.lower() == "valid"].copy()
    if "status_flag" in working.columns:
        working = working.loc[working["status_flag"].astype(str).str.lower() == "valid"].copy()
    if "hard_fail" in working.columns:
        hard_fail = working["hard_fail"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})
        working = working.loc[~hard_fail].copy()
    if "recipe_family" in working.columns:
        subset_rows = working.loc[working["recipe_family"].astype(str).str.strip() == FOCUSED_SUBSET_LABEL].copy()
        if not subset_rows.empty:
            working = subset_rows
    if "month_key" in working.columns:
        months = _month_from_key(working["month_key"])
        working = working.loc[months.isin(FOCUSED_SUBSET_MONTHS)].copy()

    if working.empty:
        return None

    grouped = (
        working.groupby("recipe", as_index=False)
        .agg(
            segments=("segment_id", "nunique"),
            mean_ncs=("ncs_score", "mean"),
            median_ncs=("ncs_score", "median"),
        )
        .copy()
    )
    try:
        normalized = _normalized_chart_table(grouped)
        _validate_subset_table(normalized)
        return normalized
    except ValueError:
        return None


def _from_loading_audit(frame: pd.DataFrame) -> pd.DataFrame | None:
    required = {"segment_id", "recipe", "ncs_score"}
    if not required.issubset(frame.columns):
        return None

    working = frame.copy()
    if "validity_flag" in working.columns:
        working = working.loc[working["validity_flag"].astype(str).str.lower() == "valid"].copy()
    if "status_flag" in working.columns:
        working = working.loc[working["status_flag"].astype(str).str.lower() == "valid"].copy()
    if "hard_fail" in working.columns:
        hard_fail = working["hard_fail"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})
        working = working.loc[~hard_fail].copy()
    if "month_key" in working.columns:
        months = _month_from_key(working["month_key"])
        working = working.loc[months.isin(FOCUSED_SUBSET_MONTHS)].copy()

    if working.empty:
        return None

    grouped = (
        working.groupby("recipe", as_index=False)
        .agg(
            segments=("segment_id", "nunique"),
            mean_ncs=("ncs_score", "mean"),
            median_ncs=("ncs_score", "median"),
        )
        .copy()
    )
    try:
        normalized = _normalized_chart_table(grouped)
        _validate_subset_table(normalized)
        return normalized
    except ValueError:
        return None


def _candidate_paths(repo_root: Path) -> list[Path]:
    focused_root = repo_root / "output" / FOCUSED_WORKFLOW
    explicit_candidates = [
        focused_root / "phase1_recipe_ranking.csv",
        focused_root / "focused_phase1_recipe_ranking.csv",
        focused_root / "phase1_recipe_summary.csv",
        focused_root / "phase1_segment_metrics.csv",
        focused_root / "phase1_loading_audit.csv",
    ]
    discovered: list[Path] = []
    seen: set[Path] = set()
    for candidate in explicit_candidates:
        if candidate.exists() and candidate not in seen:
            discovered.append(candidate)
            seen.add(candidate)

    search_roots = [
        repo_root / "output",
        repo_root / "output" / "final_reproducibility_package",
        repo_root / "docs",
        repo_root / "manifests",
    ]
    for root in search_roots:
        if not root.exists():
            continue
        for candidate in sorted(root.rglob("*.csv")):
            text = candidate.as_posix().lower()
            name = candidate.name.lower()
            if FOCUSED_WORKFLOW not in text:
                continue
            if not any(token in name for token in ("recipe", "ranking", "summary", "metrics", "audit")):
                continue
            if candidate not in seen:
                discovered.append(candidate)
                seen.add(candidate)
    return discovered


def _find_real_source(repo_root: Path) -> SourceSelection | None:
    loaders = (
        ("recipe ranking CSV", _from_recipe_ranking),
        ("recipe summary CSV", _from_recipe_summary),
        ("segment metrics CSV", _from_segment_metrics),
        ("loading audit CSV", _from_loading_audit),
    )
    for path in _candidate_paths(repo_root):
        frame = pd.read_csv(path)
        for source_kind, loader in loaders:
            chart_table = loader(frame)
            if chart_table is None:
                continue
            return SourceSelection(
                chart_table=chart_table,
                source_csv_path=_relative_to_repo(path, repo_root),
                source_kind=source_kind,
                used_fallback=False,
                lane=FOCUSED_WORKFLOW,
                subset_label=FOCUSED_SUBSET_LABEL,
                subset_segment_count=FOCUSED_SEGMENT_COUNT,
                subset_note="Focused February-April subset, n = 19 segments.",
            )
    return None


def _fallback_source() -> SourceSelection:
    chart_table = _normalized_chart_table(pd.DataFrame(FALLBACK_ROWS))
    return SourceSelection(
        chart_table=chart_table,
        source_csv_path=None,
        source_kind="Chapter 5 fallback table",
        used_fallback=True,
        lane=FOCUSED_WORKFLOW,
        subset_label=FOCUSED_SUBSET_LABEL,
        subset_segment_count=FOCUSED_SEGMENT_COUNT,
        subset_note="Focused February-April subset, n = 19 segments.",
    )


def _resolve_font_family(repo_root: Path) -> str:
    local_font_paths = [
        repo_root / "output" / "_local_fonts" / "arial.ttf",
        repo_root / "output" / "_local_fonts" / "arialbd.ttf",
        repo_root / "output" / "_local_fonts" / "ariali.ttf",
        repo_root / "output" / "_local_fonts" / "arialbi.ttf",
        Path("/host_fonts/arial.ttf"),
        Path("/host_fonts/arialbd.ttf"),
        Path("/host_fonts/ariali.ttf"),
        Path("/host_fonts/arialbi.ttf"),
    ]
    for font_path in local_font_paths:
        if font_path.exists():
            font_manager.fontManager.addfont(str(font_path))

    candidates = [
        "Arial",
        "Liberation Sans",
        "DejaVu Sans",
        "sans-serif",
    ]
    resolved = "sans-serif"
    for candidate in candidates:
        try:
            font_path = font_manager.findfont(candidate, fallback_to_default=False)
        except ValueError:
            continue
        if not font_path:
            continue
        font_name = font_manager.FontProperties(fname=font_path).get_name()
        if font_name:
            resolved = font_name
            break

    plt.rcParams["font.family"] = resolved
    plt.rcParams["font.sans-serif"] = [
        resolved,
        "Arial",
        "Liberation Sans",
        "DejaVu Sans",
        "sans-serif",
    ]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["svg.fonttype"] = "none"
    return resolved


def _recipe_label(recipe: str) -> str:
    meta = RECIPE_METADATA[recipe]
    return f"{recipe}\n({meta['current']} + {meta['wind']})"


def _plot_chart(chart_table: pd.DataFrame, output_png: Path, output_svg: Path, repo_root: Path) -> None:
    font_family = _resolve_font_family(repo_root)
    fig, ax = plt.subplots(figsize=(6.5, 3.9), facecolor="white")
    fig.subplots_adjust(left=0.34, right=0.98, bottom=0.22, top=0.88)

    plot_table = chart_table.reset_index(drop=True)
    y_positions = list(range(len(plot_table)))
    x_min = float(min(plot_table["mean_ncs"].min(), plot_table["median_ncs"].min()) - 0.12)
    x_max = float(max(plot_table["mean_ncs"].max(), plot_table["median_ncs"].max()) + 0.12)

    facecolors = ["#bdbdbd" if recipe == EXPECTED_WINNER else "#d9d9d9" for recipe in plot_table["recipe"]]
    edgecolors = ["#1f1f1f" if recipe == EXPECTED_WINNER else "#7a7a7a" for recipe in plot_table["recipe"]]
    linewidths = [1.4 if recipe == EXPECTED_WINNER else 0.9 for recipe in plot_table["recipe"]]
    hatches = ["////" if recipe == EXPECTED_WINNER else "" for recipe in plot_table["recipe"]]

    bars = ax.barh(
        y_positions,
        plot_table["mean_ncs"],
        color=facecolors,
        edgecolor=edgecolors,
        linewidth=linewidths,
        height=0.62,
        zorder=2,
    )
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)

    ax.scatter(
        plot_table["median_ncs"],
        y_positions,
        s=30,
        facecolors="white",
        edgecolors="#111111",
        linewidths=1.0,
        zorder=3,
    )

    labels = [_recipe_label(recipe) for recipe in plot_table["recipe"]]
    ax.set_yticks(y_positions, labels)
    ax.invert_yaxis()
    for text, recipe in zip(ax.get_yticklabels(), plot_table["recipe"]):
        text.set_fontfamily(font_family)
        if recipe == EXPECTED_WINNER:
            text.set_fontweight("bold")
    for text in ax.get_xticklabels():
        text.set_fontfamily(font_family)

    label_offset = 0.014
    for y_pos, mean_value in zip(y_positions, plot_table["mean_ncs"]):
        ax.text(
            float(mean_value) + label_offset,
            y_pos,
            f"{float(mean_value):.4f}",
            va="center",
            ha="left",
            fontsize=8.8,
            color="#111111",
            fontfamily=font_family,
        )

    ax.set_xlim(x_min, x_max)
    ax.set_xlabel("Mean raw NCS", fontsize=10, fontfamily=font_family)
    ax.set_ylabel("")
    ax.xaxis.set_major_locator(MultipleLocator(0.10))
    ax.xaxis.set_major_formatter(FormatStrFormatter("%.2f"))
    ax.grid(axis="x", color="#d4d4d4", linewidth=0.75)
    ax.set_axisbelow(True)
    ax.tick_params(axis="y", length=0, pad=8)
    ax.tick_params(axis="x", labelsize=8.7)
    for spine_name in ("top", "right"):
        ax.spines[spine_name].set_visible(False)
    ax.spines["left"].set_color("#b5b5b5")
    ax.spines["bottom"].set_color("#b5b5b5")
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_linewidth(0.8)

    ax.text(
        0.01,
        0.02,
        "Lower raw NCS = better transport agreement.",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=8.6,
        color="#444444",
        fontfamily=font_family,
    )

    fig.text(
        0.34,
        0.08,
        "Focused February-April subset, n = 19 segments.",
        ha="left",
        va="center",
        fontsize=8.8,
        color="#444444",
        fontfamily=font_family,
    )

    legend_handles = [
        Patch(facecolor="#d9d9d9", edgecolor="#7a7a7a", label="bar = mean raw NCS"),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="",
            markerfacecolor="white",
            markeredgecolor="#111111",
            markersize=5.5,
            label="marker = median raw NCS",
        ),
    ]
    fig.legend(
        handles=legend_handles,
        loc="upper right",
        bbox_to_anchor=(0.98, 0.98),
        frameon=False,
        fontsize=8.2,
        handlelength=1.1,
        borderaxespad=0.0,
        prop={"family": font_family, "size": 8.2},
    )

    fig.savefig(output_png, dpi=300, facecolor="white")
    fig.savefig(output_svg, dpi=300, facecolor="white")
    plt.close(fig)


def _validation_results(chart_table: pd.DataFrame, output_png: Path, output_svg: Path) -> dict[str, bool]:
    ascending = chart_table["mean_ncs"].is_monotonic_increasing
    four_recipes_present = set(chart_table["recipe"]) == set(EXPECTED_RECIPES)
    winner_is_lowest = str(chart_table.iloc[0]["recipe"]) == EXPECTED_WINNER
    png_exists = output_png.exists() and output_png.stat().st_size > 0
    svg_exists = output_svg.exists() and output_svg.stat().st_size > 0
    return {
        "all_four_recipes_present": four_recipes_present,
        "cmems_gfs_has_lowest_mean_raw_ncs": winner_is_lowest,
        "chart_sorted_ascending_by_mean_raw_ncs": bool(ascending),
        "png_exists_and_not_empty": png_exists,
        "svg_exists_and_not_empty": svg_exists,
    }


def _write_qa_note(
    repo_root: Path,
    selection: SourceSelection,
    chart_table: pd.DataFrame,
    validations: dict[str, bool],
    output_png: Path,
    output_svg: Path,
    output_source_csv: Path,
    output_qa: Path,
) -> None:
    winner = str(chart_table.iloc[0]["recipe"])
    output_lines = [
        "Figure 5.3 QA Note",
        "",
    ]
    if selection.used_fallback:
        output_lines.extend(
            [
                "WARNING: No qualifying stored focused Phase 1 source CSV was found.",
                "The figure was built from the Chapter 5 fallback table rather than directly from a stored CSV.",
            ]
        )
    else:
        output_lines.append(f"Source CSV path used: {selection.source_csv_path}")
        output_lines.append(f"Source kind: {selection.source_kind}")
    output_lines.extend(
        [
            f"Selected lane: {selection.lane}",
            f"Ranking subset: {selection.subset_label}",
            f"Subset note: {selection.subset_note}",
            "Recipes plotted: cmems_gfs, cmems_era5, hycom_gfs, hycom_era5",
            "Metric plotted: mean raw NCS (bars) with median raw NCS (markers)",
            "Interpretation: lower raw NCS is better transport agreement.",
            f"Selected winner: {winner}",
            "",
            "Validation:",
        ]
    )
    for label, passed in validations.items():
        status = "yes" if passed else "no"
        output_lines.append(f"- {label}: {status}")
    output_lines.extend(
        [
            "",
            "Output files:",
            f"- {_relative_to_repo(output_png, repo_root)}",
            f"- {_relative_to_repo(output_svg, repo_root)}",
            f"- {_relative_to_repo(output_source_csv, repo_root)}",
            f"- {_relative_to_repo(output_qa, repo_root)}",
        ]
    )
    qa_text = "\n".join(output_lines) + "\n"
    output_qa.write_text(qa_text, encoding="utf-8")
    if selection.used_fallback:
        return
    if not selection.source_csv_path or selection.source_csv_path not in qa_text:
        raise ValueError("QA note did not record the real source CSV path used.")


def generate_figure(repo_root: Path) -> SourceSelection:
    output_dir = repo_root / OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    selection = _find_real_source(repo_root)
    if selection is None:
        selection = _fallback_source()

    chart_table = selection.chart_table.copy()
    _validate_subset_table(chart_table)
    if str(chart_table.iloc[0]["recipe"]) != EXPECTED_WINNER:
        raise ValueError("Focused Phase 1 winner check failed: cmems_gfs is not the lowest-mean recipe.")

    source_csv_path = repo_root / OUTPUT_SOURCE_CSV
    chart_table.to_csv(source_csv_path, index=False)

    output_png = repo_root / OUTPUT_PNG
    output_svg = repo_root / OUTPUT_SVG
    _plot_chart(chart_table, output_png, output_svg, repo_root)

    validations = _validation_results(chart_table, output_png, output_svg)
    if not all(validations.values()):
        failed = [name for name, passed in validations.items() if not passed]
        raise ValueError(f"Figure 5.3 validation failed: {', '.join(failed)}")

    output_qa = repo_root / OUTPUT_QA
    _write_qa_note(
        repo_root,
        selection,
        chart_table,
        validations,
        output_png,
        output_svg,
        source_csv_path,
        output_qa,
    )
    return selection


def main() -> None:
    args = _parse_args()
    repo_root = Path(args.repo_root).resolve()
    selection = generate_figure(repo_root)
    source_label = selection.source_csv_path or "Chapter 5 fallback table"
    print(
        "Generated Figure 5.3 assets "
        f"from {source_label} in {OUTPUT_DIR.as_posix()}."
    )


if __name__ == "__main__":
    main()
