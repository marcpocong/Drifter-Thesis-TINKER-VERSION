"""Read-only display review export for prototype_2016 forecast figures."""

from __future__ import annotations

import json
import logging
import tempfile
import textwrap
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, to_rgba
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Polygon, Rectangle
import cartopy.crs as ccrs

from src.helpers.plotting import (
    add_prototype_2016_geoaxes,
    figure_relative_inset_rect,
    plot_legacy_drifter_track_ensemble_overlay,
    PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
    PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
)
from src.services.prototype_legacy_final_figures import (
    _relative_to_repo,
    _utc_now_iso,
    _write_csv,
    _write_json,
    PrototypeLegacyFinalFiguresService,
)
from src.services.prototype_pygnome_similarity_summary import (
    MODEL_STYLES,
    REQUIRED_HOURS,
)

matplotlib.use("Agg")

PHASE = "prototype_2016_display_review"
OUTPUT_DIR = Path("output") / "prototype_2016_display_review"
MANIFEST_FILENAME = "display_review_manifest.json"
AUDIT_FILENAME = "figure_input_audit.csv"
CAPTIONS_FILENAME = "display_review_captions.md"
TECHNICAL_MODE = "technical_exact"
DISPLAY_MODE = "publication_display_only"
RENDERING_MODES = (TECHNICAL_MODE, DISPLAY_MODE)
BOARD_FILENAME = "pygnome_vs_ensemble_board_24_48_72h.png"

logger = logging.getLogger(__name__)


def _merge_source_strings(repo_root: Path, source_paths: list[Path | str]) -> str:
    return " | ".join(
        dict.fromkeys(
            _relative_to_repo(repo_root, Path(path)) if not isinstance(path, Path) else _relative_to_repo(repo_root, path)
            for path in source_paths
        )
    )


class Prototype2016DisplayReviewService(PrototypeLegacyFinalFiguresService):
    def __init__(self, repo_root: str | Path = ".", output_dir: str | Path | None = None):
        super().__init__(repo_root=repo_root, output_dir=output_dir or OUTPUT_DIR)
        self.audit_rows: list[dict[str, Any]] = []
        self.caption_lines: list[str] = []
        self._active_rendering_mode = TECHNICAL_MODE

    def _single_size(self) -> tuple[float, float]:
        values = (self.style.get("layout") or {}).get("single_size_inches") or [13, 8]
        return float(values[0]), float(values[1])

    def _review_case_dir(self, case_id: str, rendering_mode: str) -> Path:
        return self.output_dir / case_id / rendering_mode

    def _record_figure(
        self,
        *,
        case_id: str,
        figure_id: str,
        output_path: Path,
        figure_type: str,
        source_paths: list[Path | str],
        notes: str,
        geometry_render_mode: str | None = None,
        density_render_mode: str | None = None,
        stored_geometry_status: str | None = None,
        extent_mode: str | None = None,
        plot_bounds_wgs84: tuple[float, float, float, float] | list[float] | None = None,
    ) -> None:
        self.figure_rows.append(
            {
                "case_id": case_id,
                "figure_id": figure_id,
                "figure_type": figure_type,
                "rendering_mode": self._active_rendering_mode,
                "file_name": output_path.name,
                "relative_path": _relative_to_repo(self.repo_root, output_path),
                "source_paths": _merge_source_strings(self.repo_root, source_paths),
                "notes": notes,
                "extent_mode": extent_mode or "",
                "plot_bounds_wgs84": (
                    ",".join(f"{float(value):.4f}" for value in plot_bounds_wgs84)
                    if plot_bounds_wgs84 is not None
                    else ""
                ),
                "geometry_render_mode": geometry_render_mode or "",
                "density_render_mode": density_render_mode or "",
                "stored_geometry_status": stored_geometry_status or "",
                "status": "generated",
                "placeholder_geometry_used": False,
                "science_rerun_performed": False,
                "pygnome_rerun_performed": False,
            }
        )

    def _record_missing(
        self,
        *,
        case_id: str,
        figure_id: str,
        figure_type: str,
        missing_sources: list[Path | str],
        category: str,
        notes: str,
    ) -> None:
        self.missing_rows.append(
            {
                "case_id": case_id,
                "figure_id": figure_id,
                "figure_type": figure_type,
                "rendering_mode": self._active_rendering_mode,
                "missing_cause": self._missing_cause(category=category, case_id=case_id),
                "missing_sources": _merge_source_strings(self.repo_root, missing_sources),
                "notes": notes,
                "science_rerun_performed": False,
                "pygnome_rerun_performed": False,
            }
        )

    def _record_review_audit(
        self,
        *,
        case_id: str,
        figure_id: str,
        figure_type: str,
        rendering_mode: str,
        source_paths: list[Path | str],
        geometry_source_types: list[str],
        extent_strategy: str,
        root_problem_classification: str,
        positive_cell_counts: dict[str, int],
        plot_bounds: tuple[float, float, float, float] | None,
        notes: str,
    ) -> None:
        self.audit_rows.append(
            {
                "case_id": case_id,
                "figure_id": figure_id,
                "figure_type": figure_type,
                "rendering_mode": rendering_mode,
                "source_paths": _merge_source_strings(self.repo_root, source_paths),
                "geometry_source_types": " | ".join(geometry_source_types),
                "extent_strategy": extent_strategy,
                "root_problem_classification": root_problem_classification,
                "positive_cell_counts": json.dumps(positive_cell_counts, sort_keys=True),
                "plot_bounds_wgs84": (
                    ",".join(f"{float(value):.4f}" for value in plot_bounds) if plot_bounds is not None else ""
                ),
                "placeholder_geometry_used": False,
                "science_rerun_performed": False,
                "pygnome_rerun_performed": False,
                "notes": notes,
            }
        )

    def _append_caption(self, *, figure_id: str, rendering_mode: str, text: str) -> None:
        self.caption_lines.append(f"- `{figure_id}` [{rendering_mode}]: {text}")

    def _continuous_positive_count(self, path: Path | None) -> int:
        if path is None or not Path(path).exists():
            return 0
        info = self.prototype_helper._load_raster_mask(Path(path))
        return int(np.count_nonzero(info["mask"]))

    def _classify_problem(
        self,
        *,
        positive_cell_counts: dict[str, int],
        overlay_layer_count: int,
    ) -> str:
        problems: list[str] = []
        if overlay_layer_count > 1:
            problems.append("overlay_styling_problem")
        problems.append("extent_zoom_problem")
        if any(int(value) > 0 and int(value) <= 12 for value in positive_cell_counts.values()):
            problems.append("upstream_stored_geometry_sparsity")
        else:
            problems.append("display_rendering_problem")
        if len(problems) == 1:
            return problems[0]
        return "mixed_problem:" + "+".join(dict.fromkeys(problems))

    def _mode_subtitle(self, base: str, rendering_mode: str) -> str:
        if rendering_mode == TECHNICAL_MODE:
            suffix = "technical exact view | stored rasters and exact footprint support only"
        else:
            suffix = "publication display-only view | conservative styling derived from stored forecast geometry only"
        return f"{base} | {suffix}"

    def _display_note_lines(
        self,
        *,
        case_id: str,
        rendering_mode: str,
        layer_note: str,
        sparse_layers: list[str],
        pygnome_item: dict[str, Any] | None = None,
    ) -> list[str]:
        lines = [
            f"{case_id.replace('CASE_', '')} legacy support-only case.",
            layer_note,
            "Display refinement is derived from stored forecast geometry only; accepted scoreable products are unchanged.",
            "PyGNOME remains comparator-only and is not truth.",
        ]
        if rendering_mode == DISPLAY_MODE:
            lines.insert(
                2,
                "Publication display-only mode uses dissolved stored support polygons, lighter fills, and clearer outlines for readability without inventing new extent.",
            )
        else:
            lines.insert(
                2,
                "Technical exact mode keeps exact stored raster cells and exact stored support geometry with no smoothing.",
            )
        if sparse_layers:
            lines.append("Sparse stored geometry remains visible in: " + ", ".join(sorted(sparse_layers)) + ".")
        if pygnome_item is not None:
            lines.append(self.prototype_helper._pygnome_forcing_sentence(pygnome_item))
        return lines

    def _render_density_underlay(
        self,
        ax: plt.Axes,
        *,
        raster_path: Path,
        color: str,
        alpha_low: float,
        alpha_high: float,
        zorder: float,
    ) -> tuple[bool, int]:
        info = self.prototype_helper._load_raster_mask(raster_path)
        array = np.asarray(info["array"], dtype=float)
        array[~np.isfinite(array)] = 0.0
        mask = array > 0.0
        positive_count = int(np.count_nonzero(mask))
        if positive_count <= 0:
            return False, 0
        positive_values = array[mask]
        max_value = float(np.max(positive_values))
        normalized = np.zeros_like(array, dtype=float)
        if max_value > 0.0:
            normalized[mask] = positive_values / max_value
        else:
            normalized[mask] = 1.0
        cmap = LinearSegmentedColormap.from_list(
            f"review_{raster_path.stem}",
            [
                (0.0, (1.0, 1.0, 1.0, 0.0)),
                (0.40, to_rgba(color, alpha=alpha_low)),
                (1.0, to_rgba(color, alpha=alpha_high)),
            ],
        )
        masked = np.ma.masked_where(~mask, normalized)
        left, bottom, right, top = [float(value) for value in info["bounds"]]
        ax.imshow(
            masked,
            extent=(left, right, bottom, top),
            origin="upper",
            cmap=cmap,
            interpolation="nearest",
            vmin=0.0,
            vmax=1.0,
            transform=ccrs.PlateCarree(),
            zorder=zorder,
        )
        return True, positive_count

    def _draw_geometry_layer(
        self,
        ax: plt.Axes,
        *,
        raster_path: Path,
        color: str,
        rendering_mode: str,
        fill_alpha: float,
        edge_width: float,
        hatch: str | None = None,
        linestyle: str = "-",
        zorder: float,
    ) -> tuple[bool, int]:
        info = self.prototype_helper._load_raster_mask(raster_path)
        polygons = info.get("footprint_polygons") or []
        cell_boxes = info.get("positive_cell_boxes") or []
        positive_count = int(np.count_nonzero(info["mask"]))
        if positive_count <= 0:
            return False, 0
        drew = False
        if polygons:
            for coordinates in polygons:
                ax.add_patch(
                    Polygon(
                        coordinates,
                        closed=True,
                        facecolor=to_rgba(color, fill_alpha),
                        edgecolor=color,
                        linewidth=edge_width,
                        linestyle=linestyle,
                        hatch=hatch,
                        joinstyle="round",
                        zorder=zorder,
                        transform=ccrs.PlateCarree(),
                    )
                )
                drew = True
        else:
            for bounds in cell_boxes:
                ax.add_patch(
                    Rectangle(
                        (bounds[0], bounds[1]),
                        bounds[2] - bounds[0],
                        bounds[3] - bounds[1],
                        facecolor=to_rgba(color, fill_alpha),
                        edgecolor=color,
                        linewidth=edge_width,
                        linestyle=linestyle,
                        hatch=hatch,
                        zorder=zorder,
                        transform=ccrs.PlateCarree(),
                    )
                )
                drew = True
        if rendering_mode == TECHNICAL_MODE and positive_count <= 196:
            for bounds in cell_boxes:
                ax.add_patch(
                    Rectangle(
                        (bounds[0], bounds[1]),
                        bounds[2] - bounds[0],
                        bounds[3] - bounds[1],
                        facecolor="none",
                        edgecolor=to_rgba(color, alpha=0.52),
                        linewidth=0.42,
                        zorder=zorder + 0.15,
                        transform=ccrs.PlateCarree(),
                    )
                )
        return drew, positive_count

    def _draw_source_point(self, ax: plt.Axes, source_point: tuple[float, float] | None) -> None:
        if source_point is None:
            return
        ax.scatter(
            [source_point[0]],
            [source_point[1]],
            marker="*",
            s=210,
            c="#1d9b1d",
            edgecolors="#111827",
            linewidths=1.0,
            zorder=15,
            transform=ccrs.PlateCarree(),
        )

    def _render_comparison_board(
        self,
        *,
        case_id: str,
        rendering_mode: str,
        overlay_paths: dict[int, Path],
    ) -> Path:
        output_path = self._review_case_dir(case_id, rendering_mode) / BOARD_FILENAME
        fig = plt.figure(figsize=(15.2, 5.8), dpi=self._dpi(), facecolor="#ffffff")
        grid = fig.add_gridspec(2, 3, height_ratios=[0.9, 0.1], left=0.05, right=0.98, top=0.88, bottom=0.08, wspace=0.06)
        for idx, hour in enumerate(REQUIRED_HOURS):
            ax = fig.add_subplot(grid[0, idx])
            ax.imshow(plt.imread(overlay_paths[int(hour)]))
            ax.axis("off")
            ax.set_title(f"T+{int(hour)} h", fontsize=11.0, fontweight="bold", loc="left")
        footer_ax = fig.add_subplot(grid[1, :])
        footer_ax.axis("off")
        footer_ax.text(
            0.0,
            0.5,
            textwrap.fill(
                "Review board reusing the stored-geometry overlay figures only. This panel is display-review output and does not replace accepted scoreable products.",
                width=140,
            ),
            ha="left",
            va="center",
            fontsize=8.6,
            color="#334155",
            transform=footer_ax.transAxes,
        )
        fig.suptitle(
            f"{case_id.replace('CASE_', '')} | PyGNOME vs ensemble review board",
            x=0.05,
            y=0.96,
            ha="left",
            fontsize=18,
            fontweight="bold",
        )
        fig.text(
            0.05,
            0.925,
            f"prototype_2016 | {rendering_mode} | existing stored outputs only",
            ha="left",
            va="top",
            fontsize=10,
            color="#475569",
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return output_path

    def _render_phase2_display_mode(
        self,
        *,
        case_id: str,
        hour: int,
        output_path: Path,
        title_suffix: str,
    ) -> dict[str, Any]:
        source_paths = self._phase2_source_paths(case_id, hour)
        self._require_paths(source_paths["probability"], source_paths["p50"], source_paths["p90"])
        source_point = self._source_point(case_id)
        plot_bounds = self._resolve_figure_bounds(
            case_id,
            raster_paths=[source_paths["probability"], source_paths["p50"], source_paths["p90"]],
            source_point=source_point,
        )
        case_label = case_id.replace("CASE_", "")
        fig, ax = self._figure_frame(
            f"{case_label} | {title_suffix}",
            self._mode_subtitle("Prototype 2016 legacy support review", DISPLAY_MODE),
            display_bounds=plot_bounds,
        )
        side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])

        probability_drawn, probability_cells = self._render_density_underlay(
            ax,
            raster_path=source_paths["probability"],
            color="#d97706",
            alpha_low=0.10,
            alpha_high=0.32,
            zorder=3.0,
        )
        p50_drawn, p50_cells = self._draw_geometry_layer(
            ax,
            raster_path=source_paths["p50"],
            color=MODEL_STYLES["opendrift_p50"]["color"],
            rendering_mode=DISPLAY_MODE,
            fill_alpha=0.16,
            edge_width=1.6,
            zorder=7.0,
        )
        p90_drawn, p90_cells = self._draw_geometry_layer(
            ax,
            raster_path=source_paths["p90"],
            color=MODEL_STYLES["opendrift_p90"]["color"],
            rendering_mode=DISPLAY_MODE,
            fill_alpha=0.00,
            edge_width=1.8,
            hatch="////",
            zorder=8.0,
        )
        self._draw_source_point(ax, source_point)
        locator_ax = add_prototype_2016_geoaxes(
            fig,
            figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
            self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
            show_grid_labels=False,
            add_scale_bar=False,
            add_north_arrow=False,
        )
        self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
        legend_handles = [
            Line2D(
                [0],
                [0],
                marker="*",
                linestyle="None",
                markerfacecolor="#1d9b1d",
                markeredgecolor="#111827",
                markersize=13,
                label="Drifter-of-record release point",
            )
        ]
        if probability_drawn:
            legend_handles.append(
                Patch(
                    facecolor=to_rgba("#d97706", 0.24),
                    edgecolor="none",
                    label="Stored member-occupancy probability field",
                )
            )
        if p50_drawn:
            legend_handles.append(
                Patch(
                    facecolor=to_rgba(MODEL_STYLES["opendrift_p50"]["color"], 0.16),
                    edgecolor=MODEL_STYLES["opendrift_p50"]["color"],
                    label="Ensemble p50 footprint",
                )
            )
        if p90_drawn:
            legend_handles.append(
                Patch(
                    facecolor=to_rgba("#ffffff", 0.0),
                    edgecolor=MODEL_STYLES["opendrift_p90"]["color"],
                    hatch="////",
                    label="Ensemble p90 footprint",
                )
            )
        ax.legend(
            handles=legend_handles,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.09),
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=8.3,
            ncol=2,
        )
        sparse_layers = [
            name
            for name, count in {
                "probability field": probability_cells,
                "p50 footprint": p50_cells,
                "p90 footprint": p90_cells,
            }.items()
            if 0 < int(count) <= 12
        ]
        self._draw_side_panel(
            side_ax,
            "Interpretation",
            self._display_note_lines(
                case_id=case_id,
                rendering_mode=DISPLAY_MODE,
                layer_note="Stored member-occupancy probability shading is paired with exact p50/p90 support footprints.",
                sparse_layers=sparse_layers,
            ),
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return {
            "geometry_render_mode": "display_only_conservative_shape",
            "density_render_mode": "stored_probability_underlay" if probability_drawn else "omitted",
            "stored_geometry_status": "nonempty" if (p50_drawn or p90_drawn) else "empty_stored_artifact",
            "extent_mode": self.extent_mode,
            "plot_bounds_wgs84": plot_bounds,
            "positive_cell_counts": {
                "probability": probability_cells,
                "p50": p50_cells,
                "p90": p90_cells,
            },
        }

    def _render_pygnome_display_mode(
        self,
        *,
        case_id: str,
        hour: int,
        output_path: Path,
    ) -> dict[str, Any]:
        item = self._load_case_item(case_id)
        if item is None:
            raise RuntimeError(self._case_benchmark_skip_message(case_id) or f"Benchmark artifacts unavailable for {case_id}.")
        pair_row = item["pairings_by_hour"]["deterministic"][int(hour)]
        pygnome_footprint = Path(pair_row["pygnome_footprint_path_resolved"])
        pygnome_density = pair_row.get("pygnome_density_path_resolved")
        self._require_paths(pygnome_footprint)
        if pygnome_density:
            self._require_paths(Path(pygnome_density))
        source_point = self._source_point(case_id)
        plot_bounds = self._resolve_figure_bounds(
            case_id,
            raster_paths=[pygnome_footprint, Path(pygnome_density) if pygnome_density else None],
            source_point=source_point,
        )
        case_label = case_id.replace("CASE_", "")
        fig, ax = self._figure_frame(
            f"{case_label} | PyGNOME comparator {int(hour)} h",
            self._mode_subtitle("Prototype 2016 legacy support review", DISPLAY_MODE),
            display_bounds=plot_bounds,
        )
        side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])
        density_drawn, density_cells = self._render_density_underlay(
            ax,
            raster_path=Path(pygnome_density) if pygnome_density else pygnome_footprint,
            color=MODEL_STYLES["pygnome"]["mid_color"],
            alpha_low=0.10,
            alpha_high=0.30,
            zorder=3.0,
        )
        geometry_drawn, geometry_cells = self._draw_geometry_layer(
            ax,
            raster_path=pygnome_footprint,
            color=MODEL_STYLES["pygnome"]["color"],
            rendering_mode=DISPLAY_MODE,
            fill_alpha=0.08,
            edge_width=1.8,
            zorder=8.0,
        )
        self._draw_source_point(ax, source_point)
        locator_ax = add_prototype_2016_geoaxes(
            fig,
            figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
            self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
            show_grid_labels=False,
            add_scale_bar=False,
            add_north_arrow=False,
        )
        self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
        handles = [
            Line2D(
                [0],
                [0],
                marker="*",
                linestyle="None",
                markerfacecolor="#1d9b1d",
                markeredgecolor="#111827",
                markersize=13,
                label="Drifter-of-record release point",
            )
        ]
        if density_drawn:
            handles.append(
                Patch(
                    facecolor=to_rgba(MODEL_STYLES["pygnome"]["mid_color"], 0.18),
                    edgecolor="none",
                    label="Stored PyGNOME density field",
                )
            )
        if geometry_drawn:
            handles.append(
                Line2D(
                    [0],
                    [0],
                    color=MODEL_STYLES["pygnome"]["color"],
                    linewidth=1.8,
                    label="PyGNOME exact footprint outline",
                )
            )
        ax.legend(
            handles=handles,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.09),
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=8.3,
            ncol=2,
        )
        sparse_layers = [name for name, count in {"PyGNOME density": density_cells, "PyGNOME footprint": geometry_cells}.items() if 0 < int(count) <= 12]
        self._draw_side_panel(
            side_ax,
            "Interpretation",
            self._display_note_lines(
                case_id=case_id,
                rendering_mode=DISPLAY_MODE,
                layer_note="Stored deterministic PyGNOME density shading and exact footprint support are shown for comparator context only.",
                sparse_layers=sparse_layers,
                pygnome_item=item,
            ),
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return {
            "geometry_render_mode": "display_only_conservative_shape",
            "density_render_mode": "stored_pygnome_density_underlay" if density_drawn else "omitted",
            "stored_geometry_status": "nonempty" if geometry_drawn else "empty_stored_artifact",
            "extent_mode": self.extent_mode,
            "plot_bounds_wgs84": plot_bounds,
            "positive_cell_counts": {
                "pygnome_density": density_cells,
                "pygnome_footprint": geometry_cells,
            },
        }

    def _render_pygnome_vs_ensemble_display_mode(
        self,
        *,
        case_id: str,
        hour: int,
        output_path: Path,
        pygnome_footprint_path: Path,
        pygnome_density_path: Path | None,
        p50_path: Path,
        p90_path: Path,
        note_suffix: str,
    ) -> dict[str, Any]:
        item = self._load_case_item(case_id)
        if item is None:
            raise RuntimeError(self._case_benchmark_skip_message(case_id) or f"Benchmark artifacts unavailable for {case_id}.")
        self._require_paths(pygnome_footprint_path, p50_path, p90_path)
        if pygnome_density_path:
            self._require_paths(pygnome_density_path)
        source_point = self._source_point(case_id)
        plot_bounds = self._resolve_figure_bounds(
            case_id,
            raster_paths=[pygnome_footprint_path, pygnome_density_path, p50_path, p90_path],
            source_point=source_point,
        )
        case_label = case_id.replace("CASE_", "")
        fig, ax = self._figure_frame(
            f"{case_label} | PyGNOME vs ensemble {note_suffix}",
            self._mode_subtitle("Prototype 2016 legacy support review", DISPLAY_MODE),
            display_bounds=plot_bounds,
        )
        side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])

        density_drawn, density_cells = self._render_density_underlay(
            ax,
            raster_path=pygnome_density_path or pygnome_footprint_path,
            color=MODEL_STYLES["pygnome"]["mid_color"],
            alpha_low=0.08,
            alpha_high=0.24,
            zorder=3.0,
        )
        py_drawn, py_cells = self._draw_geometry_layer(
            ax,
            raster_path=pygnome_footprint_path,
            color=MODEL_STYLES["pygnome"]["color"],
            rendering_mode=DISPLAY_MODE,
            fill_alpha=0.04,
            edge_width=1.8,
            zorder=9.0,
        )
        p50_drawn, p50_cells = self._draw_geometry_layer(
            ax,
            raster_path=p50_path,
            color=MODEL_STYLES["opendrift_p50"]["color"],
            rendering_mode=DISPLAY_MODE,
            fill_alpha=0.15,
            edge_width=1.5,
            zorder=7.0,
        )
        p90_drawn, p90_cells = self._draw_geometry_layer(
            ax,
            raster_path=p90_path,
            color=MODEL_STYLES["opendrift_p90"]["color"],
            rendering_mode=DISPLAY_MODE,
            fill_alpha=0.00,
            edge_width=1.8,
            hatch="////",
            zorder=8.0,
        )
        self._draw_source_point(ax, source_point)
        locator_ax = add_prototype_2016_geoaxes(
            fig,
            figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
            self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
            show_grid_labels=False,
            add_scale_bar=False,
            add_north_arrow=False,
        )
        self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
        handles = [
            Line2D(
                [0],
                [0],
                marker="*",
                linestyle="None",
                markerfacecolor="#1d9b1d",
                markeredgecolor="#111827",
                markersize=13,
                label="Drifter-of-record release point",
            )
        ]
        if density_drawn:
            handles.append(
                Patch(
                    facecolor=to_rgba(MODEL_STYLES["pygnome"]["mid_color"], 0.18),
                    edgecolor="none",
                    label="Stored PyGNOME density field",
                )
            )
        if py_drawn:
            handles.append(Line2D([0], [0], color=MODEL_STYLES["pygnome"]["color"], linewidth=1.8, label="PyGNOME outline"))
        if p50_drawn:
            handles.append(
                Patch(
                    facecolor=to_rgba(MODEL_STYLES["opendrift_p50"]["color"], 0.15),
                    edgecolor=MODEL_STYLES["opendrift_p50"]["color"],
                    label="Ensemble p50 footprint",
                )
            )
        if p90_drawn:
            handles.append(
                Patch(
                    facecolor=to_rgba("#ffffff", 0.0),
                    edgecolor=MODEL_STYLES["opendrift_p90"]["color"],
                    hatch="////",
                    label="Ensemble p90 footprint",
                )
            )
        ax.legend(
            handles=handles,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.09),
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=8.0,
            ncol=2,
        )
        hour_token = int("".join(char for char in note_suffix if char.isdigit()) or "72")
        self._draw_side_panel(
            side_ax,
            "FSS summary",
            [
                *self._score_box_lines(item, hour=hour_token, case_id=case_id),
                "Display-only review uses clearer overlay separation while keeping the same stored comparator geometry.",
            ],
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=self._dpi())
        plt.close(fig)
        return {
            "geometry_render_mode": "display_only_conservative_shape",
            "density_render_mode": "stored_pygnome_density_underlay" if density_drawn else "omitted",
            "stored_geometry_status": "nonempty" if (py_drawn or p50_drawn or p90_drawn) else "empty_stored_artifact",
            "extent_mode": self.extent_mode,
            "plot_bounds_wgs84": plot_bounds,
            "positive_cell_counts": {
                "pygnome_density": density_cells,
                "pygnome_footprint": py_cells,
                "p50": p50_cells,
                "p90": p90_cells,
            },
        }

    def _generate_exact_mode_outputs(self, case_id: str) -> dict[str, Path]:
        self._active_rendering_mode = TECHNICAL_MODE
        case_dir = self._review_case_dir(case_id, TECHNICAL_MODE)
        case_dir.mkdir(parents=True, exist_ok=True)
        outputs: dict[str, Path] = {}
        item = self._load_case_item(case_id)

        for hour in REQUIRED_HOURS:
            output_path = case_dir / f"ensemble_probability_{int(hour)}h.png"
            render_info = super()._render_phase2_probability_figure(
                case_id=case_id,
                hour=int(hour),
                output_path=output_path,
                title_suffix=f"Ensemble member-occupancy footprint {int(hour)} h",
            )
            outputs[f"ensemble_probability_{int(hour)}h"] = output_path
            phase2_paths = self._phase2_source_paths(case_id, int(hour))
            source_paths = list(phase2_paths.values())
            counts = {
                "probability": self._continuous_positive_count(phase2_paths["probability"]),
                "p50": self._continuous_positive_count(phase2_paths["p50"]),
                "p90": self._continuous_positive_count(phase2_paths["p90"]),
            }
            notes = f"Technical exact review figure for stored prototype_2016 ensemble occupancy products at T+{int(hour)} h."
            self._record_figure(
                case_id=case_id,
                figure_id=f"ensemble_probability_{int(hour)}h",
                output_path=output_path,
                figure_type="ensemble_probability_map",
                source_paths=source_paths,
                notes=notes,
                geometry_render_mode=render_info["geometry_render_mode"],
                density_render_mode=render_info["density_render_mode"],
                stored_geometry_status=render_info["stored_geometry_status"],
                extent_mode=render_info["extent_mode"],
                plot_bounds_wgs84=render_info["plot_bounds_wgs84"],
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id=f"ensemble_probability_{int(hour)}h",
                figure_type="ensemble_probability_map",
                rendering_mode=TECHNICAL_MODE,
                source_paths=source_paths,
                geometry_source_types=["member_occupancy_probability_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(positive_cell_counts=counts, overlay_layer_count=2),
                positive_cell_counts=counts,
                plot_bounds=render_info["plot_bounds_wgs84"],
                notes=notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{TECHNICAL_MODE}/ensemble_probability_{int(hour)}h",
                rendering_mode=TECHNICAL_MODE,
                text="Technical exact view from stored member-occupancy probability raster cells plus exact p50/p90 footprint support. No smoothing or rerun.",
            )

        consolidated_output = case_dir / "ensemble_consolidated_72h.png"
        consolidated_render_info = super()._render_phase2_probability_figure(
            case_id=case_id,
            hour=72,
            output_path=consolidated_output,
            title_suffix="Consolidated ensemble member-occupancy footprint 72 h",
        )
        outputs["ensemble_consolidated_72h"] = consolidated_output
        consolidated_paths = self._phase2_source_paths(case_id, 72)
        consolidated_counts = {
            "probability": self._continuous_positive_count(consolidated_paths["probability"]),
            "p50": self._continuous_positive_count(consolidated_paths["p50"]),
            "p90": self._continuous_positive_count(consolidated_paths["p90"]),
        }
        consolidated_notes = "Technical exact review figure for stored 72 h ensemble occupancy products."
        self._record_figure(
            case_id=case_id,
            figure_id="ensemble_consolidated_72h",
            output_path=consolidated_output,
            figure_type="ensemble_consolidated_map",
            source_paths=list(consolidated_paths.values()),
            notes=consolidated_notes,
            geometry_render_mode=consolidated_render_info["geometry_render_mode"],
            density_render_mode=consolidated_render_info["density_render_mode"],
            stored_geometry_status=consolidated_render_info["stored_geometry_status"],
            extent_mode=consolidated_render_info["extent_mode"],
            plot_bounds_wgs84=consolidated_render_info["plot_bounds_wgs84"],
        )
        self._record_review_audit(
            case_id=case_id,
            figure_id="ensemble_consolidated_72h",
            figure_type="ensemble_consolidated_map",
            rendering_mode=TECHNICAL_MODE,
            source_paths=list(consolidated_paths.values()),
            geometry_source_types=["member_occupancy_probability_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
            extent_strategy=self.extent_mode,
            root_problem_classification=self._classify_problem(positive_cell_counts=consolidated_counts, overlay_layer_count=2),
            positive_cell_counts=consolidated_counts,
            plot_bounds=consolidated_render_info["plot_bounds_wgs84"],
            notes=consolidated_notes,
        )
        self._append_caption(
            figure_id=f"{case_id}/{TECHNICAL_MODE}/ensemble_consolidated_72h",
            rendering_mode=TECHNICAL_MODE,
            text="Technical exact consolidated 72 h view from stored probability support and exact p50/p90 occupancy masks only.",
        )

        if item is None:
            return outputs

        for hour in REQUIRED_HOURS:
            pair_row = item["pairings_by_hour"]["deterministic"][int(hour)]
            pygnome_footprint = Path(pair_row["pygnome_footprint_path_resolved"])
            pygnome_density = pair_row.get("pygnome_density_path_resolved")

            py_output = case_dir / f"pygnome_{int(hour)}h.png"
            py_render_info = super()._render_pygnome_single(case_id=case_id, hour=int(hour), output_path=py_output)
            outputs[f"pygnome_{int(hour)}h"] = py_output
            py_sources = [pygnome_footprint, *( [Path(pygnome_density)] if pygnome_density else [])]
            py_counts = {
                "pygnome_density": self._continuous_positive_count(Path(pygnome_density) if pygnome_density else pygnome_footprint),
                "pygnome_footprint": self._continuous_positive_count(pygnome_footprint),
            }
            py_notes = f"Technical exact review figure for stored prototype_2016 PyGNOME comparator products at T+{int(hour)} h."
            self._record_figure(
                case_id=case_id,
                figure_id=f"pygnome_{int(hour)}h",
                output_path=py_output,
                figure_type="pygnome_single_map",
                source_paths=py_sources,
                notes=py_notes,
                geometry_render_mode=py_render_info["geometry_render_mode"],
                density_render_mode=py_render_info["density_render_mode"],
                stored_geometry_status=py_render_info["stored_geometry_status"],
                extent_mode=py_render_info["extent_mode"],
                plot_bounds_wgs84=py_render_info["plot_bounds_wgs84"],
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id=f"pygnome_{int(hour)}h",
                figure_type="pygnome_single_map",
                rendering_mode=TECHNICAL_MODE,
                source_paths=py_sources,
                geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(positive_cell_counts=py_counts, overlay_layer_count=1),
                positive_cell_counts=py_counts,
                plot_bounds=py_render_info["plot_bounds_wgs84"],
                notes=py_notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{TECHNICAL_MODE}/pygnome_{int(hour)}h",
                rendering_mode=TECHNICAL_MODE,
                text="Technical exact view from stored PyGNOME comparator density and exact footprint support only.",
            )

            overlay_output = case_dir / f"pygnome_vs_ensemble_{int(hour)}h.png"
            overlay_render_info = super()._render_pygnome_vs_ensemble(
                case_id=case_id,
                hour=int(hour),
                output_path=overlay_output,
                pygnome_footprint_path=pygnome_footprint,
                pygnome_density_path=Path(pygnome_density) if pygnome_density else None,
                p50_path=Path(item["pairings_by_hour"]["ensemble_p50"][int(hour)]["opendrift_footprint_path_resolved"]),
                p90_path=Path(item["pairings_by_hour"]["ensemble_p90"][int(hour)]["opendrift_footprint_path_resolved"]),
                note_suffix=f"{int(hour)} h",
            )
            outputs[f"pygnome_vs_ensemble_{int(hour)}h"] = overlay_output
            overlay_sources = [
                pygnome_footprint,
                *( [Path(pygnome_density)] if pygnome_density else []),
                Path(item["pairings_by_hour"]["ensemble_p50"][int(hour)]["opendrift_footprint_path_resolved"]),
                Path(item["pairings_by_hour"]["ensemble_p90"][int(hour)]["opendrift_footprint_path_resolved"]),
            ]
            overlay_counts = {
                "pygnome_density": py_counts["pygnome_density"],
                "pygnome_footprint": py_counts["pygnome_footprint"],
                "p50": self._continuous_positive_count(Path(item["pairings_by_hour"]["ensemble_p50"][int(hour)]["opendrift_footprint_path_resolved"])),
                "p90": self._continuous_positive_count(Path(item["pairings_by_hour"]["ensemble_p90"][int(hour)]["opendrift_footprint_path_resolved"])),
            }
            overlay_notes = f"Technical exact comparison overlay for stored PyGNOME and ensemble footprint products at T+{int(hour)} h."
            self._record_figure(
                case_id=case_id,
                figure_id=f"pygnome_vs_ensemble_{int(hour)}h",
                output_path=overlay_output,
                figure_type="pygnome_vs_ensemble_overlay",
                source_paths=overlay_sources,
                notes=overlay_notes,
                geometry_render_mode=overlay_render_info["geometry_render_mode"],
                density_render_mode=overlay_render_info["density_render_mode"],
                stored_geometry_status=overlay_render_info["stored_geometry_status"],
                extent_mode=overlay_render_info["extent_mode"],
                plot_bounds_wgs84=overlay_render_info["plot_bounds_wgs84"],
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id=f"pygnome_vs_ensemble_{int(hour)}h",
                figure_type="pygnome_vs_ensemble_overlay",
                rendering_mode=TECHNICAL_MODE,
                source_paths=overlay_sources,
                geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(positive_cell_counts=overlay_counts, overlay_layer_count=3),
                positive_cell_counts=overlay_counts,
                plot_bounds=overlay_render_info["plot_bounds_wgs84"],
                notes=overlay_notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{TECHNICAL_MODE}/pygnome_vs_ensemble_{int(hour)}h",
                rendering_mode=TECHNICAL_MODE,
                text="Technical exact overlay from stored PyGNOME comparator outputs plus exact ensemble p50/p90 support masks only.",
            )

        super()._render_consolidated_pygnome_products(case_id=case_id, case_dir=case_dir)
        outputs["pygnome_consolidated_72h"] = case_dir / "pygnome_consolidated_72h.png"
        outputs["pygnome_vs_ensemble_consolidated_72h"] = case_dir / "pygnome_vs_ensemble_consolidated_72h.png"
        pygnome_sources = [
            Path(item["pairings_by_hour"]["deterministic"][hour]["pygnome_footprint_path_resolved"])
            for hour in REQUIRED_HOURS
        ]
        pygnome_sources.extend(
            [
                Path(item["pairings_by_hour"]["deterministic"][hour]["pygnome_density_path_resolved"])
                for hour in REQUIRED_HOURS
                if item["pairings_by_hour"]["deterministic"][hour].get("pygnome_density_path_resolved")
            ]
        )
        overlay_sources = [
            *pygnome_sources,
            *[
                Path(item["pairings_by_hour"]["ensemble_p50"][hour]["opendrift_footprint_path_resolved"])
                for hour in REQUIRED_HOURS
            ],
            *[
                Path(item["pairings_by_hour"]["ensemble_p90"][hour]["opendrift_footprint_path_resolved"])
                for hour in REQUIRED_HOURS
            ],
        ]
        self._record_review_audit(
            case_id=case_id,
            figure_id="pygnome_consolidated_72h",
            figure_type="pygnome_consolidated_map",
            rendering_mode=TECHNICAL_MODE,
            source_paths=pygnome_sources,
            geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster"],
            extent_strategy=self.extent_mode,
            root_problem_classification="mixed_problem:display_rendering_problem+extent_zoom_problem",
            positive_cell_counts={},
            plot_bounds=None,
            notes="Technical exact consolidated PyGNOME comparator map built from stored 24/48/72 benchmark artifacts only.",
        )
        self._record_review_audit(
            case_id=case_id,
            figure_id="pygnome_vs_ensemble_consolidated_72h",
            figure_type="pygnome_vs_ensemble_consolidated_overlay",
            rendering_mode=TECHNICAL_MODE,
            source_paths=overlay_sources,
            geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
            extent_strategy=self.extent_mode,
            root_problem_classification="mixed_problem:overlay_styling_problem+extent_zoom_problem",
            positive_cell_counts={},
            plot_bounds=None,
            notes="Technical exact consolidated overlay built from stored 24/48/72 benchmark artifacts only.",
        )
        self._append_caption(
            figure_id=f"{case_id}/{TECHNICAL_MODE}/pygnome_consolidated_72h",
            rendering_mode=TECHNICAL_MODE,
            text="Technical exact consolidated PyGNOME comparator view built from stored 24/48/72 benchmark artifacts only.",
        )
        self._append_caption(
            figure_id=f"{case_id}/{TECHNICAL_MODE}/pygnome_vs_ensemble_consolidated_72h",
            rendering_mode=TECHNICAL_MODE,
            text="Technical exact consolidated overlay built from stored 24/48/72 PyGNOME and ensemble footprint artifacts only.",
        )
        return outputs

    def _generate_display_mode_outputs(self, case_id: str) -> dict[str, Path]:
        self._active_rendering_mode = DISPLAY_MODE
        case_dir = self._review_case_dir(case_id, DISPLAY_MODE)
        case_dir.mkdir(parents=True, exist_ok=True)
        outputs: dict[str, Path] = {}
        item = self._load_case_item(case_id)

        for hour in REQUIRED_HOURS:
            output_path = case_dir / f"ensemble_probability_{int(hour)}h.png"
            render_info = self._render_phase2_display_mode(
                case_id=case_id,
                hour=int(hour),
                output_path=output_path,
                title_suffix=f"Ensemble member-occupancy footprint {int(hour)} h",
            )
            outputs[f"ensemble_probability_{int(hour)}h"] = output_path
            phase2_paths = self._phase2_source_paths(case_id, int(hour))
            source_paths = list(phase2_paths.values())
            notes = f"Publication display-only review figure for stored prototype_2016 ensemble occupancy products at T+{int(hour)} h."
            self._record_figure(
                case_id=case_id,
                figure_id=f"ensemble_probability_{int(hour)}h",
                output_path=output_path,
                figure_type="ensemble_probability_map",
                source_paths=source_paths,
                notes=notes,
                geometry_render_mode=render_info["geometry_render_mode"],
                density_render_mode=render_info["density_render_mode"],
                stored_geometry_status=render_info["stored_geometry_status"],
                extent_mode=render_info["extent_mode"],
                plot_bounds_wgs84=render_info["plot_bounds_wgs84"],
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id=f"ensemble_probability_{int(hour)}h",
                figure_type="ensemble_probability_map",
                rendering_mode=DISPLAY_MODE,
                source_paths=source_paths,
                geometry_source_types=["member_occupancy_probability_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(
                    positive_cell_counts=render_info["positive_cell_counts"],
                    overlay_layer_count=2,
                ),
                positive_cell_counts=render_info["positive_cell_counts"],
                plot_bounds=render_info["plot_bounds_wgs84"],
                notes=notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{DISPLAY_MODE}/ensemble_probability_{int(hour)}h",
                rendering_mode=DISPLAY_MODE,
                text="Publication display-only view derived from stored ensemble probability raster plus exact p50/p90 support footprints. Accepted scoreable products are unchanged.",
            )

        consolidated_output = case_dir / "ensemble_consolidated_72h.png"
        consolidated_render_info = self._render_phase2_display_mode(
            case_id=case_id,
            hour=72,
            output_path=consolidated_output,
            title_suffix="Consolidated ensemble member-occupancy footprint 72 h",
        )
        outputs["ensemble_consolidated_72h"] = consolidated_output
        consolidated_paths = self._phase2_source_paths(case_id, 72)
        consolidated_notes = "Publication display-only review figure for stored 72 h ensemble occupancy products."
        self._record_figure(
            case_id=case_id,
            figure_id="ensemble_consolidated_72h",
            output_path=consolidated_output,
            figure_type="ensemble_consolidated_map",
            source_paths=list(consolidated_paths.values()),
            notes=consolidated_notes,
            geometry_render_mode=consolidated_render_info["geometry_render_mode"],
            density_render_mode=consolidated_render_info["density_render_mode"],
            stored_geometry_status=consolidated_render_info["stored_geometry_status"],
            extent_mode=consolidated_render_info["extent_mode"],
            plot_bounds_wgs84=consolidated_render_info["plot_bounds_wgs84"],
        )
        self._record_review_audit(
            case_id=case_id,
            figure_id="ensemble_consolidated_72h",
            figure_type="ensemble_consolidated_map",
            rendering_mode=DISPLAY_MODE,
            source_paths=list(consolidated_paths.values()),
            geometry_source_types=["member_occupancy_probability_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
            extent_strategy=self.extent_mode,
            root_problem_classification=self._classify_problem(
                positive_cell_counts=consolidated_render_info["positive_cell_counts"],
                overlay_layer_count=2,
            ),
            positive_cell_counts=consolidated_render_info["positive_cell_counts"],
            plot_bounds=consolidated_render_info["plot_bounds_wgs84"],
            notes=consolidated_notes,
        )
        self._append_caption(
            figure_id=f"{case_id}/{DISPLAY_MODE}/ensemble_consolidated_72h",
            rendering_mode=DISPLAY_MODE,
            text="Publication display-only consolidated 72 h ensemble view from the same stored support rasters and masks only.",
        )

        if item is None:
            return outputs

        for hour in REQUIRED_HOURS:
            pair_row = item["pairings_by_hour"]["deterministic"][int(hour)]
            pygnome_footprint = Path(pair_row["pygnome_footprint_path_resolved"])
            pygnome_density = Path(pair_row["pygnome_density_path_resolved"]) if pair_row.get("pygnome_density_path_resolved") else None
            p50_path = Path(item["pairings_by_hour"]["ensemble_p50"][int(hour)]["opendrift_footprint_path_resolved"])
            p90_path = Path(item["pairings_by_hour"]["ensemble_p90"][int(hour)]["opendrift_footprint_path_resolved"])

            py_output = case_dir / f"pygnome_{int(hour)}h.png"
            py_render_info = self._render_pygnome_display_mode(
                case_id=case_id,
                hour=int(hour),
                output_path=py_output,
            )
            outputs[f"pygnome_{int(hour)}h"] = py_output
            py_sources = [pygnome_footprint, *( [pygnome_density] if pygnome_density else [])]
            py_notes = f"Publication display-only review figure for stored prototype_2016 PyGNOME comparator products at T+{int(hour)} h."
            self._record_figure(
                case_id=case_id,
                figure_id=f"pygnome_{int(hour)}h",
                output_path=py_output,
                figure_type="pygnome_single_map",
                source_paths=py_sources,
                notes=py_notes,
                geometry_render_mode=py_render_info["geometry_render_mode"],
                density_render_mode=py_render_info["density_render_mode"],
                stored_geometry_status=py_render_info["stored_geometry_status"],
                extent_mode=py_render_info["extent_mode"],
                plot_bounds_wgs84=py_render_info["plot_bounds_wgs84"],
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id=f"pygnome_{int(hour)}h",
                figure_type="pygnome_single_map",
                rendering_mode=DISPLAY_MODE,
                source_paths=py_sources,
                geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(
                    positive_cell_counts=py_render_info["positive_cell_counts"],
                    overlay_layer_count=1,
                ),
                positive_cell_counts=py_render_info["positive_cell_counts"],
                plot_bounds=py_render_info["plot_bounds_wgs84"],
                notes=py_notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{DISPLAY_MODE}/pygnome_{int(hour)}h",
                rendering_mode=DISPLAY_MODE,
                text="Publication display-only PyGNOME comparator view derived from stored PyGNOME density and exact stored support only.",
            )

            overlay_output = case_dir / f"pygnome_vs_ensemble_{int(hour)}h.png"
            overlay_render_info = self._render_pygnome_vs_ensemble_display_mode(
                case_id=case_id,
                hour=int(hour),
                output_path=overlay_output,
                pygnome_footprint_path=pygnome_footprint,
                pygnome_density_path=pygnome_density,
                p50_path=p50_path,
                p90_path=p90_path,
                note_suffix=f"{int(hour)} h",
            )
            outputs[f"pygnome_vs_ensemble_{int(hour)}h"] = overlay_output
            overlay_sources = [pygnome_footprint, *( [pygnome_density] if pygnome_density else []), p50_path, p90_path]
            overlay_notes = f"Publication display-only review overlay for stored PyGNOME and ensemble footprint products at T+{int(hour)} h."
            self._record_figure(
                case_id=case_id,
                figure_id=f"pygnome_vs_ensemble_{int(hour)}h",
                output_path=overlay_output,
                figure_type="pygnome_vs_ensemble_overlay",
                source_paths=overlay_sources,
                notes=overlay_notes,
                geometry_render_mode=overlay_render_info["geometry_render_mode"],
                density_render_mode=overlay_render_info["density_render_mode"],
                stored_geometry_status=overlay_render_info["stored_geometry_status"],
                extent_mode=overlay_render_info["extent_mode"],
                plot_bounds_wgs84=overlay_render_info["plot_bounds_wgs84"],
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id=f"pygnome_vs_ensemble_{int(hour)}h",
                figure_type="pygnome_vs_ensemble_overlay",
                rendering_mode=DISPLAY_MODE,
                source_paths=overlay_sources,
                geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(
                    positive_cell_counts=overlay_render_info["positive_cell_counts"],
                    overlay_layer_count=3,
                ),
                positive_cell_counts=overlay_render_info["positive_cell_counts"],
                plot_bounds=overlay_render_info["plot_bounds_wgs84"],
                notes=overlay_notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{DISPLAY_MODE}/pygnome_vs_ensemble_{int(hour)}h",
                rendering_mode=DISPLAY_MODE,
                text="Publication display-only overlay using stored PyGNOME comparator geometry plus exact ensemble p50/p90 support, with lighter fills and clearer separation only.",
            )

        pygnome_footprints = [
            Path(item["pairings_by_hour"]["deterministic"][hour]["pygnome_footprint_path_resolved"])
            for hour in REQUIRED_HOURS
        ]
        pygnome_density_paths = [
            Path(item["pairings_by_hour"]["deterministic"][hour]["pygnome_density_path_resolved"])
            for hour in REQUIRED_HOURS
            if item["pairings_by_hour"]["deterministic"][hour].get("pygnome_density_path_resolved")
        ]
        p50_paths = [
            Path(item["pairings_by_hour"]["ensemble_p50"][hour]["opendrift_footprint_path_resolved"])
            for hour in REQUIRED_HOURS
        ]
        p90_paths = [
            Path(item["pairings_by_hour"]["ensemble_p90"][hour]["opendrift_footprint_path_resolved"])
            for hour in REQUIRED_HOURS
        ]
        with tempfile.TemporaryDirectory(prefix=f"{case_id.lower()}_display_review_") as tmpdir:
            tmp_root = Path(tmpdir)
            pygnome_footprint_array, pygnome_bounds, pygnome_crs = self._build_composite_raster(pygnome_footprints, density_mode="sum")
            pygnome_density_array, _, _ = self._build_composite_raster(pygnome_density_paths or pygnome_footprints, density_mode="max")
            p50_array, p50_bounds, p50_crs = self._build_composite_raster(p50_paths, density_mode="sum")
            p90_array, p90_bounds, p90_crs = self._build_composite_raster(p90_paths, density_mode="sum")

            pygnome_footprint_tmp = self._write_temp_raster(tmp_root / "pygnome_footprint_72h.tif", array=pygnome_footprint_array, bounds=pygnome_bounds, crs=pygnome_crs)
            pygnome_density_tmp = self._write_temp_raster(tmp_root / "pygnome_density_72h.tif", array=pygnome_density_array, bounds=pygnome_bounds, crs=pygnome_crs)
            p50_tmp = self._write_temp_raster(tmp_root / "ensemble_p50_72h.tif", array=p50_array, bounds=p50_bounds, crs=p50_crs)
            p90_tmp = self._write_temp_raster(tmp_root / "ensemble_p90_72h.tif", array=p90_array, bounds=p90_bounds, crs=p90_crs)

            pygnome_consolidated_output = case_dir / "pygnome_consolidated_72h.png"
            source_point = self._source_point(case_id)
            plot_bounds = self._resolve_figure_bounds(
                case_id,
                raster_paths=[pygnome_footprint_tmp, pygnome_density_tmp],
                source_point=source_point,
            )
            case_label = case_id.replace("CASE_", "")
            fig, ax = self._figure_frame(
                f"{case_label} | Consolidated PyGNOME comparator 72 h",
                self._mode_subtitle("Prototype 2016 legacy support review", DISPLAY_MODE),
                display_bounds=plot_bounds,
            )
            side_ax = fig.add_axes([0.83, 0.12, 0.14, 0.76])
            density_drawn, density_cells = self._render_density_underlay(
                ax,
                raster_path=pygnome_density_tmp,
                color=MODEL_STYLES["pygnome"]["mid_color"],
                alpha_low=0.10,
                alpha_high=0.30,
                zorder=3.0,
            )
            footprint_drawn, footprint_cells = self._draw_geometry_layer(
                ax,
                raster_path=pygnome_footprint_tmp,
                color=MODEL_STYLES["pygnome"]["color"],
                rendering_mode=DISPLAY_MODE,
                fill_alpha=0.08,
                edge_width=1.8,
                zorder=8.0,
            )
            self._draw_source_point(ax, source_point)
            locator_ax = add_prototype_2016_geoaxes(
                fig,
                figure_relative_inset_rect(ax, [0.74, 0.74, 0.22, 0.22]),
                self.prototype_helper._load_prototype_map_context()["full_bounds_wgs84"],
                show_grid_labels=False,
                add_scale_bar=False,
                add_north_arrow=False,
            )
            self.prototype_helper._draw_locator(locator_ax, plot_bounds, self._display_bounds(case_id))
            ax.legend(
                handles=[
                    Line2D([0], [0], marker="*", linestyle="None", markerfacecolor="#1d9b1d", markeredgecolor="#111827", markersize=13, label="Drifter-of-record release point"),
                    Patch(facecolor=to_rgba(MODEL_STYLES["pygnome"]["mid_color"], 0.18), edgecolor="none", label="Stored PyGNOME density field"),
                    Line2D([0], [0], color=MODEL_STYLES["pygnome"]["color"], linewidth=1.8, label="PyGNOME exact footprint outline"),
                ],
                loc="upper center",
                bbox_to_anchor=(0.5, -0.09),
                frameon=True,
                framealpha=0.98,
                facecolor="#ffffff",
                edgecolor="#cbd5e1",
                fontsize=8.3,
                ncol=2,
            )
            self._draw_side_panel(
                side_ax,
                "Interpretation",
                self._display_note_lines(
                    case_id=case_id,
                    rendering_mode=DISPLAY_MODE,
                    layer_note="Consolidated display-only comparator view built from stored 24/48/72 PyGNOME artifacts only.",
                    sparse_layers=[name for name, count in {"PyGNOME density": density_cells, "PyGNOME footprint": footprint_cells}.items() if 0 < int(count) <= 12],
                    pygnome_item=item,
                ),
            )
            fig.savefig(pygnome_consolidated_output, dpi=self._dpi())
            plt.close(fig)
            outputs["pygnome_consolidated_72h"] = pygnome_consolidated_output
            pygnome_consolidated_sources = [*pygnome_footprints, *pygnome_density_paths]
            pygnome_consolidated_notes = "Publication display-only consolidated PyGNOME comparator review, derived from stored 24/48/72 artifacts only."
            self._record_figure(
                case_id=case_id,
                figure_id="pygnome_consolidated_72h",
                output_path=pygnome_consolidated_output,
                figure_type="pygnome_consolidated_map",
                source_paths=pygnome_consolidated_sources,
                notes=pygnome_consolidated_notes,
                geometry_render_mode="display_only_conservative_shape",
                density_render_mode="stored_pygnome_density_underlay" if density_drawn else "omitted",
                stored_geometry_status="nonempty" if footprint_drawn else "empty_stored_artifact",
                extent_mode=self.extent_mode,
                plot_bounds_wgs84=plot_bounds,
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id="pygnome_consolidated_72h",
                figure_type="pygnome_consolidated_map",
                rendering_mode=DISPLAY_MODE,
                source_paths=pygnome_consolidated_sources,
                geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(
                    positive_cell_counts={"pygnome_density": density_cells, "pygnome_footprint": footprint_cells},
                    overlay_layer_count=1,
                ),
                positive_cell_counts={"pygnome_density": density_cells, "pygnome_footprint": footprint_cells},
                plot_bounds=plot_bounds,
                notes=pygnome_consolidated_notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{DISPLAY_MODE}/pygnome_consolidated_72h",
                rendering_mode=DISPLAY_MODE,
                text="Publication display-only consolidated PyGNOME comparator view derived from stored 24/48/72 artifacts only.",
            )

            consolidated_overlay_output = case_dir / "pygnome_vs_ensemble_consolidated_72h.png"
            consolidated_overlay_render_info = self._render_pygnome_vs_ensemble_display_mode(
                case_id=case_id,
                hour=72,
                output_path=consolidated_overlay_output,
                pygnome_footprint_path=pygnome_footprint_tmp,
                pygnome_density_path=pygnome_density_tmp,
                p50_path=p50_tmp,
                p90_path=p90_tmp,
                note_suffix="consolidated 72 h",
            )
            outputs["pygnome_vs_ensemble_consolidated_72h"] = consolidated_overlay_output
            consolidated_overlay_sources = [*pygnome_footprints, *pygnome_density_paths, *p50_paths, *p90_paths]
            consolidated_overlay_notes = "Publication display-only consolidated overlay from stored PyGNOME comparator and ensemble footprints only."
            self._record_figure(
                case_id=case_id,
                figure_id="pygnome_vs_ensemble_consolidated_72h",
                output_path=consolidated_overlay_output,
                figure_type="pygnome_vs_ensemble_consolidated_overlay",
                source_paths=consolidated_overlay_sources,
                notes=consolidated_overlay_notes,
                geometry_render_mode=consolidated_overlay_render_info["geometry_render_mode"],
                density_render_mode=consolidated_overlay_render_info["density_render_mode"],
                stored_geometry_status=consolidated_overlay_render_info["stored_geometry_status"],
                extent_mode=consolidated_overlay_render_info["extent_mode"],
                plot_bounds_wgs84=consolidated_overlay_render_info["plot_bounds_wgs84"],
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id="pygnome_vs_ensemble_consolidated_72h",
                figure_type="pygnome_vs_ensemble_consolidated_overlay",
                rendering_mode=DISPLAY_MODE,
                source_paths=consolidated_overlay_sources,
                geometry_source_types=["pygnome_footprint_mask", "pygnome_density_raster", "occupancy_mask_p50", "occupancy_mask_p90"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(
                    positive_cell_counts=consolidated_overlay_render_info["positive_cell_counts"],
                    overlay_layer_count=3,
                ),
                positive_cell_counts=consolidated_overlay_render_info["positive_cell_counts"],
                plot_bounds=consolidated_overlay_render_info["plot_bounds_wgs84"],
                notes=consolidated_overlay_notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{DISPLAY_MODE}/pygnome_vs_ensemble_consolidated_72h",
                rendering_mode=DISPLAY_MODE,
                text="Publication display-only consolidated overlay using stored PyGNOME comparator outputs plus exact ensemble p50/p90 support only.",
            )
        return outputs

    def _maybe_review_drifter_overlay(self, case_id: str) -> None:
        selection, drifter_track_df = self._load_drifter_track(case_id)
        if drifter_track_df.empty:
            return
        p50_72h = self._ensemble_dir(case_id) / "mask_p50_72h.tif"
        p90_72h = self._ensemble_dir(case_id) / "mask_p90_72h.tif"
        self._require_paths(p50_72h, p90_72h)
        for rendering_mode in RENDERING_MODES:
            self._active_rendering_mode = rendering_mode
            output_path = self._review_case_dir(case_id, rendering_mode) / "drifter_vs_ensemble_72h.png"
            plot_legacy_drifter_track_ensemble_overlay(
                output_file=str(output_path),
                drifter_track_df=drifter_track_df,
                p50_mask_path=str(p50_72h),
                p90_mask_path=str(p90_72h),
                corners=list(self._display_bounds(case_id)),
                title=f"{case_id.replace('CASE_', '')} drifter vs ensemble footprints (72 h legacy support review)",
                extent_mode=self.extent_mode,
                locator_bounds=self._display_bounds(case_id),
            )
            plot_bounds = self._resolve_figure_bounds(
                case_id,
                raster_paths=[p50_72h, p90_72h],
                trajectory_points=list(zip(drifter_track_df["lon"], drifter_track_df["lat"])),
                source_point=(float(selection["start_lon"]), float(selection["start_lat"])),
            )
            counts = {
                "drifter_track_vertices": int(len(drifter_track_df)),
                "p50": self._continuous_positive_count(p50_72h),
                "p90": self._continuous_positive_count(p90_72h),
            }
            notes = "Review overlay of the observed drifter-of-record track against stored 72 h ensemble footprints."
            self._record_figure(
                case_id=case_id,
                figure_id="drifter_vs_ensemble_72h",
                output_path=output_path,
                figure_type="drifter_track_ensemble_overlay",
                source_paths=[self._drifter_csv_path(case_id), p50_72h, p90_72h],
                notes=notes,
                geometry_render_mode="observed_track_line_plus_exact_stored_raster" if rendering_mode == TECHNICAL_MODE else "display_only_observed_track_plus_support",
                density_render_mode="not_applicable",
                stored_geometry_status="mixed_nonempty_or_empty_stored_artifacts",
                extent_mode=self.extent_mode,
                plot_bounds_wgs84=plot_bounds,
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id="drifter_vs_ensemble_72h",
                figure_type="drifter_track_ensemble_overlay",
                rendering_mode=rendering_mode,
                source_paths=[self._drifter_csv_path(case_id), p50_72h, p90_72h],
                geometry_source_types=["observed_drifter_track", "occupancy_mask_p50", "occupancy_mask_p90"],
                extent_strategy=self.extent_mode,
                root_problem_classification=self._classify_problem(positive_cell_counts=counts, overlay_layer_count=3),
                positive_cell_counts=counts,
                plot_bounds=plot_bounds,
                notes=notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{rendering_mode}/drifter_vs_ensemble_72h",
                rendering_mode=rendering_mode,
                text="Review overlay of the observed drifter-of-record track against the stored 72 h ensemble footprints only.",
            )

    def _export_case(self, case_id: str) -> None:
        exact_outputs = self._generate_exact_mode_outputs(case_id)
        display_outputs = self._generate_display_mode_outputs(case_id)
        self._maybe_review_drifter_overlay(case_id)
        for rendering_mode, outputs in ((TECHNICAL_MODE, exact_outputs), (DISPLAY_MODE, display_outputs)):
            self._active_rendering_mode = rendering_mode
            required_overlay_keys = [f"pygnome_vs_ensemble_{int(hour)}h" for hour in REQUIRED_HOURS]
            missing_overlay_keys = [key for key in required_overlay_keys if key not in outputs]
            if missing_overlay_keys:
                raise FileNotFoundError(
                    f"Display review could not build required stored-geometry overlay outputs for {case_id} "
                    f"in {rendering_mode}: {', '.join(missing_overlay_keys)}"
                )
            overlay_paths = {int(hour): outputs[f"pygnome_vs_ensemble_{int(hour)}h"] for hour in REQUIRED_HOURS}
            board_output = self._render_comparison_board(
                case_id=case_id,
                rendering_mode=rendering_mode,
                overlay_paths=overlay_paths,
            )
            board_notes = "Support summary board built from the already rendered review overlay figures only."
            self._record_figure(
                case_id=case_id,
                figure_id="pygnome_vs_ensemble_board_24_48_72h",
                output_path=board_output,
                figure_type="comparison_board",
                source_paths=list(overlay_paths.values()),
                notes=board_notes,
                geometry_render_mode="review_board_reuse",
                density_render_mode="not_applicable",
                stored_geometry_status="nonempty",
                extent_mode="reused_review_figures",
                plot_bounds_wgs84=None,
            )
            self._record_review_audit(
                case_id=case_id,
                figure_id="pygnome_vs_ensemble_board_24_48_72h",
                figure_type="comparison_board",
                rendering_mode=rendering_mode,
                source_paths=list(overlay_paths.values()),
                geometry_source_types=["reused_review_overlay_pngs"],
                extent_strategy="reused_review_figures",
                root_problem_classification="display_rendering_problem",
                positive_cell_counts={},
                plot_bounds=None,
                notes=board_notes,
            )
            self._append_caption(
                figure_id=f"{case_id}/{rendering_mode}/pygnome_vs_ensemble_board_24_48_72h",
                rendering_mode=rendering_mode,
                text="Review board assembled from the already generated T+24/T+48/T+72 overlay figures only.",
            )

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for case_id in self.case_ids:
            logger.info("Exporting prototype_2016 display review figures for %s", case_id)
            self._export_case(case_id)

        audit_csv = self.output_dir / AUDIT_FILENAME
        captions_md = self.output_dir / CAPTIONS_FILENAME
        manifest_json = self.output_dir / MANIFEST_FILENAME
        _write_csv(
            audit_csv,
            self.audit_rows,
            columns=[
                "case_id",
                "figure_id",
                "figure_type",
                "rendering_mode",
                "source_paths",
                "geometry_source_types",
                "extent_strategy",
                "root_problem_classification",
                "positive_cell_counts",
                "plot_bounds_wgs84",
                "placeholder_geometry_used",
                "science_rerun_performed",
                "pygnome_rerun_performed",
                "notes",
            ],
        )
        captions_md.write_text(
            "# Prototype 2016 Display Review Captions\n\n" + "\n".join(self.caption_lines) + "\n",
            encoding="utf-8",
        )
        manifest_payload = {
            "phase": PHASE,
            "workflow_mode": "prototype_2016",
            "generated_at_utc": _utc_now_iso(),
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "configured_case_ids": list(self.case_ids),
            "rendering_modes_supported": list(RENDERING_MODES),
            "extent_modes_supported": [
                PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
                PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
            ],
            "default_extent_mode": self.extent_mode,
            "font_family": self.font_family,
            "figure_count": len(self.figure_rows),
            "missing_figure_count": len(self.missing_rows),
            "figures": self.figure_rows,
            "missing_figures": self.missing_rows,
            "audit_csv": _relative_to_repo(self.repo_root, audit_csv),
            "captions_md": _relative_to_repo(self.repo_root, captions_md),
            "notes": [
                "Read-only prototype_2016 display review built from existing stored outputs only.",
                "No OpenDrift ensemble rerun was triggered by this phase.",
                "No PyGNOME rerun was triggered by this phase.",
                "Technical exact mode preserves exact stored raster cells and exact stored support geometry.",
                "Publication display-only mode keeps the same stored support while improving zoom and overlay readability.",
                "prototype_2016 remains legacy support-only and PyGNOME remains comparator-only.",
            ],
        }
        _write_json(manifest_json, manifest_payload)
        return {
            "phase": PHASE,
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "manifest_json": _relative_to_repo(self.repo_root, manifest_json),
            "audit_csv": _relative_to_repo(self.repo_root, audit_csv),
            "captions_md": _relative_to_repo(self.repo_root, captions_md),
            "figure_count": len(self.figure_rows),
            "missing_figure_count": len(self.missing_rows),
            "generated_figures": [row["relative_path"] for row in self.figure_rows],
        }


def run_prototype_2016_display_review(
    repo_root: str | Path = ".",
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    return Prototype2016DisplayReviewService(repo_root=repo_root, output_dir=output_dir).run()
