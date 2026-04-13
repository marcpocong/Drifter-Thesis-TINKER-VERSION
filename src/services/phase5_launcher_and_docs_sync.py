"""Phase 5 reproducibility, launcher, documentation, and packaging sync."""

from __future__ import annotations

import hashlib
import json
import platform
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.core.artifact_status import get_artifact_status
from src.services.mindoro_primary_validation_metadata import (
    MINDORO_BASE_CASE_CONFIG_PATH,
    MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH,
    MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH,
    MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR,
    MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID,
    MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID,
    MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
    MINDORO_SHARED_IMAGERY_CAVEAT,
)
from src.services.dwh_phase3c_metadata import (
    DWH_BASE_CASE_CONFIG_PATH,
    DWH_PHASE3C_FINAL_NOTE_PATH,
    DWH_PHASE3C_FINAL_OUTPUT_DIR,
    DWH_PHASE3C_FORCING_STACK,
)

PHASE = "phase5_launcher_and_docs_sync"
OUTPUT_DIR = Path("output") / "final_reproducibility_package"
FINAL_VALIDATION_DIR = Path("output") / "final_validation_package"
PHASE1_AUDIT_JSON = Path("output") / "phase1_finalization_audit" / "phase1_finalization_status.json"
PHASE2_AUDIT_JSON = Path("output") / "phase2_finalization_audit" / "phase2_finalization_status.json"
PHASE4_MANIFEST_JSON = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_run_manifest.json"
PHASE4_VERDICT_MD = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_final_verdict.md"
FINAL_VALIDATION_MANIFEST_JSON = FINAL_VALIDATION_DIR / "final_validation_manifest.json"
FINAL_VALIDATION_CASE_REGISTRY_CSV = FINAL_VALIDATION_DIR / "final_validation_case_registry.csv"
LAUNCHER_MATRIX_PATH = Path("config") / "launcher_matrix.json"

DOC_PATHS = [
    Path("README.md"),
    Path("docs") / "PHASE_STATUS.md",
    Path("docs") / "ARCHITECTURE.md",
    Path("docs") / "OUTPUT_CATALOG.md",
    Path("docs") / "FIGURE_GALLERY.md",
    Path("docs") / "QUICKSTART.md",
    Path("docs") / "COMMAND_MATRIX.md",
    Path("docs") / "LAUNCHER_USER_GUIDE.md",
    Path("docs") / "MINDORO_PRIMARY_VALIDATION_MIGRATION.md",
    Path("docs") / "DWH_PHASE3C_FINAL.md",
    Path("docs") / "UI_GUIDE.md",
]

OPTIONAL_MANIFEST_PATHS = [
    {
        "manifest_id": "phase4_dwh_optional",
        "phase_id": "phase4",
        "track_id": "dwh_phase4_appendix_pilot",
        "case_id": "CASE_DWH_RETRO_2010_72H",
        "path": Path("output") / "phase4" / "CASE_DWH_RETRO_2010_72H" / "phase4_run_manifest.json",
        "optional": True,
        "description": "Optional DWH Phase 4 appendix pilot manifest.",
    }
]
PROTOTYPE_OUTPUT_DIRS = [
    (
        Path("output") / "prototype_2021_pygnome_similarity",
        "prototype_pygnome_similarity_summary",
        "prototype_2021_pygnome_similarity",
        get_artifact_status("prototype_2021_support").panel_text,
    ),
    (
        Path("output") / "prototype_2016_pygnome_similarity",
        "prototype_legacy_phase3a",
        "prototype_2016_pygnome_similarity",
        get_artifact_status("prototype_2016_support").panel_text,
    ),
]
PROTOTYPE_2016_FINAL_OUTPUT_DIR = Path("output") / "2016 Legacy Runs FINAL Figures"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8-sig") as handle:
        return json.load(handle) or {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if columns is not None:
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


def _resolve_repo_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _path_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mtime_utc(path: Path) -> str:
    if not path.exists():
        return ""
    return pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC").isoformat()


def _safe_package_version(distribution_name: str) -> str:
    try:
        return importlib_metadata.version(distribution_name)
    except importlib_metadata.PackageNotFoundError:
        return ""


def _coerce_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def load_launcher_matrix(path: str | Path = LAUNCHER_MATRIX_PATH) -> dict[str, Any]:
    matrix_path = Path(path)
    with open(matrix_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle) or {}

    categories = payload.get("categories") or []
    entries = payload.get("entries") or []
    if not categories or not entries:
        raise ValueError(f"Launcher matrix is missing categories or entries: {matrix_path}")

    category_ids = {str(item.get("category_id") or "") for item in categories}
    seen_entry_ids: set[str] = set()
    for entry in entries:
        entry_id = str(entry.get("entry_id") or "").strip()
        if not entry_id:
            raise ValueError(f"Launcher entry without entry_id in {matrix_path}")
        if entry_id in seen_entry_ids:
            raise ValueError(f"Duplicate launcher entry_id '{entry_id}' in {matrix_path}")
        seen_entry_ids.add(entry_id)
        category_id = str(entry.get("category_id") or "").strip()
        if category_id not in category_ids:
            raise ValueError(f"Launcher entry '{entry_id}' references unknown category '{category_id}'")
        if not entry.get("steps"):
            raise ValueError(f"Launcher entry '{entry_id}' does not define any steps.")
    return payload


class Phase5LauncherAndDocsSyncService:
    def __init__(
        self,
        repo_root: str | Path = ".",
        output_dir: str | Path | None = None,
        launcher_matrix_path: str | Path = LAUNCHER_MATRIX_PATH,
    ):
        self.repo_root = Path(repo_root).resolve()
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_DIR
        self.launcher_matrix_path = _resolve_repo_path(self.repo_root, launcher_matrix_path)
        self.launcher_matrix = load_launcher_matrix(self.launcher_matrix_path)
        self.final_validation_manifest = _read_json(self.repo_root / FINAL_VALIDATION_MANIFEST_JSON)
        self.phase1_audit = _read_json(self.repo_root / PHASE1_AUDIT_JSON)
        self.phase2_audit = _read_json(self.repo_root / PHASE2_AUDIT_JSON)
        self.phase4_manifest = _read_json(self.repo_root / PHASE4_MANIFEST_JSON)
        self.final_validation_case_registry = _read_csv(self.repo_root / FINAL_VALIDATION_CASE_REGISTRY_CSV)
        self.phase4_verdict_text = self._safe_read_text(self.repo_root / PHASE4_VERDICT_MD)
        self.docs_updated = list(DOC_PATHS)

    def _safe_read_text(self, path: Path) -> str:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")

    def _collect_software_versions(self) -> list[dict[str, Any]]:
        rows = [
            {
                "component_type": "runtime",
                "component_name": "python",
                "version": platform.python_version(),
                "source": "platform.python_version",
                "notes": "Version inside the active execution environment.",
            }
        ]
        for package_name, distribution_name in (
            ("numpy", "numpy"),
            ("pandas", "pandas"),
            ("xarray", "xarray"),
            ("PyYAML", "PyYAML"),
            ("streamlit", "streamlit"),
            ("opendrift", "opendrift"),
            ("geopandas", "geopandas"),
            ("rasterio", "rasterio"),
            ("shapely", "shapely"),
            ("pyproj", "pyproj"),
            ("fiona", "fiona"),
        ):
            rows.append(
                {
                    "component_type": "python_package",
                    "component_name": package_name,
                    "version": _safe_package_version(distribution_name),
                    "source": "importlib.metadata",
                    "notes": "",
                }
            )
        rows.append(
            {
                "component_type": "launcher_catalog",
                "component_name": "launcher_matrix_catalog_version",
                "version": _coerce_text(self.launcher_matrix.get("catalog_version")),
                "source": _relative_to_repo(self.repo_root, self.launcher_matrix_path),
                "notes": "Source-of-truth launcher/menu matrix used by start.ps1 and Phase 5 sync.",
            }
        )
        return rows

    def _collect_case_registry(self) -> list[dict[str, Any]]:
        prototype_2021_status = get_artifact_status("prototype_2021_support")
        prototype_2016_status = get_artifact_status("prototype_2016_support")
        rows: list[dict[str, Any]] = [
            {
                "case_id": "prototype_2021",
                "workflow_mode": "prototype_2021",
                "mode_label": prototype_2021_status.panel_label,
                "description": prototype_2021_status.provenance_label,
                "config_path": "config/prototype_2021_cases.yaml",
                "case_freeze_amendment_path": "",
                "primary_launcher_entry_id": "prototype_2021_bundle",
                "launcher_alias_entry_id": "",
                "primary_output_root": "output/CASE_202103*",
                "reportable_track_ids": "",
                "appendix_or_support_track_ids": "prototype_pygnome_similarity_summary",
                "notes": prototype_2021_status.panel_text,
            },
            {
                "case_id": "prototype_2016",
                "workflow_mode": "prototype_2016",
                "mode_label": prototype_2016_status.panel_label,
                "description": prototype_2016_status.provenance_label,
                "config_path": "config/settings.yaml",
                "case_freeze_amendment_path": "",
                "primary_launcher_entry_id": "prototype_legacy_bundle",
                "launcher_alias_entry_id": "prototype_legacy_final_figures",
                "primary_output_root": PROTOTYPE_2016_FINAL_OUTPUT_DIR.as_posix(),
                "reportable_track_ids": "",
                "appendix_or_support_track_ids": "prototype_legacy_phase3a;prototype_legacy_phase4_weathering;prototype_legacy_phase5_packaging",
                "notes": prototype_2016_status.panel_text,
            }
        ]
        for config_rel_path in (
            Path("config") / "case_mindoro_retro_2023.yaml",
            Path("config") / "case_dwh_retro_2010_72h.yaml",
        ):
            config_path = self.repo_root / config_rel_path
            config = _read_yaml(config_path)
            case_id = _coerce_text(config.get("case_id")) or config_path.stem
            track_rows = self.final_validation_case_registry[
                self.final_validation_case_registry.get("case_id", pd.Series(dtype=str)) == case_id
            ]
            track_ids = [
                _coerce_text(value)
                for value in track_rows.get("track_id", pd.Series(dtype=str)).tolist()
                if _coerce_text(value)
            ]
            appendix_ids = [
                _coerce_text(value)
                for value in track_rows.get("track_id", pd.Series(dtype=str)).tolist()
                if _coerce_text(value) and "appendix" in _coerce_text(value).lower()
            ]
            if case_id == "CASE_MINDORO_RETRO_2023":
                track_ids = sorted(set(track_ids + ["phase4"]))
                appendix_ids = sorted(set(appendix_ids + ["appendix_sensitivity"]))
            rows.append(
                {
                    "case_id": case_id,
                    "workflow_mode": _coerce_text(config.get("workflow_mode")),
                    "mode_label": _coerce_text(config.get("mode_label")),
                    "description": _coerce_text(config.get("description")),
                    "config_path": str(config_rel_path),
                    "case_freeze_amendment_path": (
                        str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH)
                        if case_id == "CASE_MINDORO_RETRO_2023"
                        else str(DWH_PHASE3C_FINAL_NOTE_PATH)
                        if case_id == "CASE_DWH_RETRO_2010_72H"
                        else ""
                    ),
                    "primary_launcher_entry_id": (
                        MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID
                        if case_id == "CASE_MINDORO_RETRO_2023"
                        else "dwh_reportable_bundle"
                        if case_id == "CASE_DWH_RETRO_2010_72H"
                        else ""
                    ),
                    "launcher_alias_entry_id": (
                        MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID
                        if case_id == "CASE_MINDORO_RETRO_2023"
                        else ""
                    ),
                    "primary_output_root": f"output/{case_id}",
                    "reportable_track_ids": ";".join(track_ids),
                    "appendix_or_support_track_ids": ";".join(appendix_ids),
                    "notes": (
                        (
                            _coerce_text((config.get("notes") or [""])[0] if isinstance(config.get("notes"), list) else "")
                            + (" | Curated final output: output/Phase 3C DWH Final Output" if case_id == "CASE_DWH_RETRO_2010_72H" else "")
                        ).strip(" |")
                    ),
                }
            )
        return rows

    def _collect_config_snapshot_index(self) -> list[dict[str, Any]]:
        tracked_paths: list[tuple[Path, str]] = []
        for path in sorted((self.repo_root / "config").glob("*")):
            if path.is_file():
                tracked_paths.append((path, "config"))
        tracked_paths.extend(
            (
                (self.repo_root / "start.ps1", "launcher"),
                (self.repo_root / "docker-compose.yml", "container_runtime"),
                (self.repo_root / "docker" / "pipeline" / "pyproject.toml", "container_runtime"),
                (self.repo_root / ".gitignore", "repo_hygiene"),
                (self.launcher_matrix_path, "launcher_catalog"),
            )
        )
        for path in DOC_PATHS:
            tracked_paths.append((self.repo_root / path, "documentation"))
        ui_root = self.repo_root / "ui"
        if ui_root.exists():
            for path in sorted(ui_root.rglob("*")):
                if path.is_file():
                    tracked_paths.append((path, "ui"))

        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for path, snapshot_class in tracked_paths:
            rel = _relative_to_repo(self.repo_root, path)
            if rel in seen:
                continue
            seen.add(rel)
            rows.append(
                {
                    "relative_path": rel,
                    "snapshot_class": snapshot_class,
                    "exists": path.exists(),
                    "size_bytes": path.stat().st_size if path.exists() else 0,
                    "modified_utc": _mtime_utc(path),
                    "sha256": _path_sha256(path) if path.exists() else "",
                }
            )
        return rows

    def _scan_manifest_paths(self) -> list[Path]:
        manifest_paths: list[Path] = []
        output_root = self.repo_root / "output"
        if output_root.exists():
            for suffix in ("*manifest.json", "*status.json"):
                manifest_paths.extend(sorted(output_root.rglob(suffix)))
        return sorted({path.resolve() for path in manifest_paths})

    def _infer_track_from_path(self, path: Path) -> tuple[str, str, str]:
        rel = _relative_to_repo(self.repo_root, path)
        rel_lower = rel.lower()
        mindoro_crossmodel = get_artifact_status("mindoro_crossmodel_comparator")
        mindoro_primary = get_artifact_status("mindoro_primary_validation")
        mindoro_legacy = get_artifact_status("mindoro_legacy_march6")
        mindoro_support = get_artifact_status("mindoro_legacy_support")
        dwh_deterministic = get_artifact_status("dwh_deterministic_transfer")
        dwh_ensemble = get_artifact_status("dwh_ensemble_transfer")
        dwh_comparator = get_artifact_status("dwh_crossmodel_comparator")
        prototype_2021 = get_artifact_status("prototype_2021_support")
        prototype_2016 = get_artifact_status("prototype_2016_support")
        if "phase1_finalization_audit" in rel_lower:
            return "phase1", "phase1_regional_baseline", "Phase 1 regional baseline"
        if "phase2_finalization_audit" in rel_lower:
            return "phase2", "phase2_machine_readable_forecast", "Phase 2 machine-readable forecast"
        if "phase4" in rel_lower:
            if "case_dwh" in rel_lower:
                return "phase4", "dwh_phase4_appendix_pilot", "DWH Phase 4 appendix pilot"
            return "phase4", "mindoro_phase4", "Mindoro Phase 4"
        if "phase 3b march13-14 final output" in rel_lower:
            return "phase3b", "B1", get_artifact_status("mindoro_primary_validation").label
        if "final_validation_package" in rel_lower:
            return "phase5", "final_validation_package", "Final validation package"
        if "trajectory_gallery_panel" in rel_lower:
            return "phase5", "trajectory_gallery_panel", "Trajectory gallery panel pack"
        if "figure_package_publication" in rel_lower:
            return "phase5", "figure_package_publication", "Publication-grade figure package"
        if "prototype_2021_pygnome_similarity" in rel_lower:
            return "prototype", "prototype_pygnome_similarity_summary", prototype_2021.label
        if "prototype_2016_pygnome_similarity" in rel_lower:
            return "phase3a", "prototype_legacy_phase3a", prototype_2016.label
        if "output/case_2016-" in rel_lower and "/weathering/" in rel_lower:
            return "phase4", "prototype_legacy_phase4_weathering", "Prototype 2016 legacy Phase 4 weathering"
        if "output/2016 legacy runs final figures" in rel_lower:
            return "phase5", "prototype_legacy_phase5_packaging", "Prototype 2016 legacy final package"
        if "trajectory_gallery" in rel_lower:
            return "phase5", "trajectory_gallery", "Trajectory gallery"
        if "final_reproducibility_package" in rel_lower:
            return "phase5", "phase5_sync", "Phase 5 reproducibility package"
        if "phase3c_external_case_run" in rel_lower:
            return "phase3c", "C1", dwh_deterministic.label
        if "phase3c_external_case_ensemble_comparison" in rel_lower:
            return "phase3c", "C2", dwh_ensemble.label
        if "phase3c_dwh_pygnome_comparator" in rel_lower:
            return "phase3c", "C3", dwh_comparator.label
        if "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison" in rel_lower:
            return "phase3a", "A", mindoro_crossmodel.label
        if "phase3b_extended_public_scored_march13_14_reinit" in rel_lower:
            return "phase3b", "B1", mindoro_primary.label
        if "phase3b" in rel_lower and "extended" not in rel_lower and "multidate" not in rel_lower:
            return "phase3b", "B2", mindoro_legacy.label
        if "public_obs_appendix" in rel_lower or "multidate_public" in rel_lower:
            return "phase3b", "B3", mindoro_support.label
        if "phase3b_extended_public" in rel_lower:
            return "phase3b", "B1", mindoro_primary.label
        return "", "", ""

    def _collect_manifest_index(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for path in self._scan_manifest_paths():
            payload = _read_json(path)
            payload_dict = payload if isinstance(payload, dict) else {}
            phase_id, track_id, track_label = self._infer_track_from_path(path)
            rows.append(
                {
                    "manifest_id": path.stem,
                    "phase_id": phase_id,
                    "track_id": track_id,
                    "track_label": track_label,
                    "relative_path": _relative_to_repo(self.repo_root, path),
                    "exists": True,
                    "optional": False,
                    "manifest_type": _coerce_text(
                        payload_dict.get("manifest_type") or payload_dict.get("phase") or path.stem
                    ),
                    "case_id": _coerce_text(payload_dict.get("case_id") or payload_dict.get("run_name")),
                    "generated_at_utc": _coerce_text(payload_dict.get("generated_at_utc")),
                    "notes": "",
                }
            )

        known_paths = {row["relative_path"] for row in rows}
        for spec in OPTIONAL_MANIFEST_PATHS:
            path = self.repo_root / spec["path"]
            rel = _relative_to_repo(self.repo_root, path)
            if rel in known_paths:
                continue
            rows.append(
                {
                    "manifest_id": spec["manifest_id"],
                    "phase_id": spec["phase_id"],
                    "track_id": spec["track_id"],
                    "track_label": spec["description"],
                    "relative_path": rel,
                    "exists": path.exists(),
                    "optional": bool(spec["optional"]),
                    "manifest_type": "expected_optional_manifest",
                    "case_id": spec["case_id"],
                    "generated_at_utc": "",
                    "notes": "Optional future work artifact.",
                }
            )

        rows.sort(key=lambda row: (row["phase_id"], row["track_id"], row["relative_path"]))
        return rows

    def _collect_log_index(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        logs_dir = self.repo_root / "logs"
        for pattern in ("*.log", "*.txt"):
            for path in sorted(logs_dir.glob(pattern)) if logs_dir.exists() else []:
                rows.append(
                    {
                        "relative_path": _relative_to_repo(self.repo_root, path),
                        "exists": True,
                        "size_bytes": path.stat().st_size,
                        "modified_utc": _mtime_utc(path),
                        "log_kind": path.suffix.lstrip("."),
                    }
                )
        if not rows:
            rows.append(
                {
                    "relative_path": "logs/",
                    "exists": False,
                    "size_bytes": 0,
                    "modified_utc": "",
                    "log_kind": "missing_optional_directory",
                }
            )
        return rows

    def _phase5_row(self) -> dict[str, Any]:
        return {
            "phase_id": "phase5",
            "track_id": "phase5_sync",
            "track_label": "Phase 5 reproducibility / launcher / docs sync",
            "readiness_status": "implemented_reportable_non_scientific_sync_layer",
            "scientifically_reportable": False,
            "scientifically_frozen": False,
            "inherited_provisional": False,
            "main_blocker": "",
            "reportable_now": True,
            "reportability_scope": "non_scientific_reproducibility_and_documentation_layer",
            "summary": "Launcher, docs, and reproducibility packaging are synchronized around the current repo state without rerunning expensive science.",
            "evidence_path": _relative_to_repo(self.repo_root, self.output_dir / "phase5_final_verdict.md"),
        }

    def _phase5_ui_row(self) -> dict[str, Any]:
        return {
            "phase_id": "phase5",
            "track_id": "phase5_read_only_dashboard",
            "track_label": "Phase 5 read-only local dashboard",
            "readiness_status": "implemented_reportable_non_scientific_read_only_ui",
            "scientifically_reportable": False,
            "scientifically_frozen": False,
            "inherited_provisional": True,
            "main_blocker": "Interactive run controls remain intentionally deferred; the first UI version is read-only by design.",
            "reportable_now": True,
            "reportability_scope": "read_only_local_dashboard_for_existing_outputs",
            "summary": "The local dashboard is now available as a read-only Phase 5 exploration layer built on the current packaging outputs and publication-grade figures.",
            "evidence_path": "docs/UI_GUIDE.md",
        }

    def _build_phase_status_registry(self) -> list[dict[str, Any]]:
        phase1_overall = self.phase1_audit.get("overall_verdict") or {}
        phase2_overall = self.phase2_audit.get("overall_verdict") or {}
        phase4_overall = self.phase4_manifest.get("overall_verdict") or {}
        final_headlines = self.final_validation_manifest.get("headlines") or {}
        mindoro_crossmodel = get_artifact_status("mindoro_crossmodel_comparator")
        mindoro_primary = get_artifact_status("mindoro_primary_validation")
        mindoro_legacy = get_artifact_status("mindoro_legacy_march6")
        mindoro_support = get_artifact_status("mindoro_legacy_support")
        dwh_deterministic = get_artifact_status("dwh_deterministic_transfer")
        dwh_ensemble = get_artifact_status("dwh_ensemble_transfer")
        dwh_comparator = get_artifact_status("dwh_crossmodel_comparator")
        phase4_oil = get_artifact_status("mindoro_phase4_oil_budget")
        upstream_blocker = (
            _coerce_text(phase4_overall.get("biggest_remaining_phase4_blocker"))
            or _coerce_text(phase2_overall.get("biggest_remaining_phase2_provisional_item"))
            or _coerce_text(phase1_overall.get("biggest_remaining_blocker"))
        )

        def _headline_note(headline_key: str, fallback: str) -> str:
            headline = final_headlines.get(headline_key) or {}
            if not headline:
                return fallback
            values = []
            for key in ("fss_1km", "fss_3km", "fss_5km", "fss_10km"):
                if key in headline:
                    values.append(f"{key}={headline[key]:.4f}")
            return f"{fallback} " + (", ".join(values) if values else "")

        rows = [
            {
                "phase_id": "phase1",
                "track_id": "phase1_regional_baseline",
                "track_label": "Phase 1 regional transport baseline selection",
                "readiness_status": "architecture_audited_production_rerun_needed",
                "scientifically_reportable": False,
                "scientifically_frozen": False,
                "inherited_provisional": False,
                "main_blocker": _coerce_text(phase1_overall.get("biggest_remaining_blocker")),
                "reportable_now": False,
                "reportability_scope": "not_yet_scientifically_reportable",
                "summary": "Architecture audited; the final 2016-2022 production rerun is still needed.",
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / PHASE1_AUDIT_JSON),
            },
            {
                "phase_id": "phase2",
                "track_id": "phase2_machine_readable_forecast",
                "track_label": "Phase 2 machine-readable forecast products",
                "readiness_status": "scientifically_usable_not_frozen",
                "scientifically_reportable": True,
                "scientifically_frozen": bool(phase2_overall.get("scientifically_frozen")),
                "inherited_provisional": True,
                "main_blocker": _coerce_text(phase2_overall.get("biggest_remaining_phase2_provisional_item")),
                "reportable_now": True,
                "reportability_scope": "scientifically_usable_official_forecast_products",
                "summary": "Scientifically usable as implemented, but not yet frozen.",
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / PHASE2_AUDIT_JSON),
            },
            {
                "phase_id": "phase3a",
                "track_id": "A",
                "track_label": mindoro_crossmodel.label,
                "readiness_status": "scientifically_informative_comparator",
                "scientifically_reportable": True,
                "scientifically_frozen": False,
                "inherited_provisional": True,
                "main_blocker": "Comparator-only track with a shared-imagery caveat; upstream Mindoro transport baseline is still not frozen.",
                "reportable_now": True,
                "reportability_scope": "comparative_benchmark_discussion",
                "summary": _headline_note(
                    "mindoro_crossmodel_top",
                    mindoro_crossmodel.dashboard_summary,
                ),
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            },
            {
                "phase_id": "phase3b",
                "track_id": "B1",
                "track_label": mindoro_primary.label,
                "readiness_status": "scientifically_reportable_primary_validation",
                "scientifically_reportable": True,
                "scientifically_frozen": False,
                "inherited_provisional": True,
                "main_blocker": upstream_blocker or "Primary validation remains inherited-provisional from upstream transport state and carries a shared-imagery caveat.",
                "reportable_now": True,
                "reportability_scope": "main_text_primary_validation",
                "summary": _headline_note(
                    "mindoro_primary_reinit",
                    mindoro_primary.dashboard_summary,
                ),
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            },
            {
                "phase_id": "phase3b",
                "track_id": "B2",
                "track_label": mindoro_legacy.label,
                "readiness_status": "scientifically_reportable_legacy_reference",
                "scientifically_reportable": True,
                "scientifically_frozen": False,
                "inherited_provisional": True,
                "main_blocker": upstream_blocker or "Legacy sparse reference; upstream transport baseline still not frozen.",
                "reportable_now": True,
                "reportability_scope": "legacy_sparse_reference",
                "summary": _headline_note(
                    "mindoro_legacy_march6",
                    mindoro_legacy.dashboard_summary,
                ),
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            },
            {
                "phase_id": "phase3b",
                "track_id": "B3",
                "track_label": mindoro_support.label,
                "readiness_status": "scientifically_reportable_legacy_reference",
                "scientifically_reportable": True,
                "scientifically_frozen": False,
                "inherited_provisional": True,
                "main_blocker": upstream_blocker or "Legacy broader-support reference; upstream transport baseline still not frozen.",
                "reportable_now": True,
                "reportability_scope": "legacy_broader_support_reference",
                "summary": _headline_note(
                    "mindoro_legacy_broader_support",
                    mindoro_support.dashboard_summary,
                ),
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            },
            {
                "phase_id": "phase3c",
                "track_id": "C1",
                "track_label": dwh_deterministic.label,
                "readiness_status": "scientifically_reportable_transfer_validation",
                "scientifically_reportable": True,
                "scientifically_frozen": True,
                "inherited_provisional": False,
                "main_blocker": "",
                "reportable_now": True,
                "reportability_scope": "external_rich_data_transfer_validation",
                "summary": _headline_note(
                    "dwh_deterministic_event",
                    dwh_deterministic.dashboard_summary,
                ),
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            },
            {
                "phase_id": "phase3c",
                "track_id": "C2",
                "track_label": dwh_ensemble.label,
                "readiness_status": "scientifically_reportable_transfer_validation",
                "scientifically_reportable": True,
                "scientifically_frozen": True,
                "inherited_provisional": False,
                "main_blocker": "",
                "reportable_now": True,
                "reportability_scope": "external_rich_data_transfer_validation",
                "summary": _headline_note(
                    "dwh_ensemble_p50_event",
                    dwh_ensemble.dashboard_summary,
                ),
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            },
            {
                "phase_id": "phase3c",
                "track_id": "C3",
                "track_label": dwh_comparator.label,
                "readiness_status": "scientifically_reportable_comparator",
                "scientifically_reportable": True,
                "scientifically_frozen": True,
                "inherited_provisional": False,
                "main_blocker": "",
                "reportable_now": True,
                "reportability_scope": "cross_model_comparator",
                "summary": _headline_note(
                    "dwh_pygnome_event",
                    dwh_comparator.dashboard_summary,
                ),
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            },
            {
                "phase_id": "phase4",
                "track_id": "mindoro_phase4",
                "track_label": phase4_oil.label,
                "readiness_status": "scientifically_reportable_inherited_provisional",
                "scientifically_reportable": bool(phase4_overall.get("scientifically_reportable_now")),
                "scientifically_frozen": False,
                "inherited_provisional": bool(phase4_overall.get("provisional_inherited_from_transport", True)),
                "main_blocker": _coerce_text(phase4_overall.get("biggest_remaining_phase4_blocker")),
                "reportable_now": bool(phase4_overall.get("scientifically_reportable_now")),
                "reportability_scope": "mindoro_oil_type_and_shoreline_interpretation",
                "summary": phase4_oil.dashboard_summary,
                "evidence_path": _relative_to_repo(self.repo_root, self.repo_root / PHASE4_MANIFEST_JSON),
            },
            self._phase5_row(),
            self._phase5_ui_row(),
        ]
        return rows

    def _collect_output_catalog_rows(self, phase5_artifacts: dict[str, Path]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        def add_row(
            phase_id: str,
            track_id: str,
            artifact_group: str,
            artifact_type: str,
            path: Path,
            notes: str,
            source_manifest: str = "",
        ) -> None:
            rows.append(
                {
                    "phase_id": phase_id,
                    "track_id": track_id,
                    "artifact_group": artifact_group,
                    "artifact_type": artifact_type,
                    "relative_path": _relative_to_repo(self.repo_root, path),
                    "exists": path.exists(),
                    "source_manifest": source_manifest,
                    "notes": notes,
                }
            )

        final_validation_artifacts = self.final_validation_manifest.get("artifacts") or {}
        for artifact_type, rel_path in sorted(final_validation_artifacts.items()):
            add_row(
                "phase5",
                "final_validation_package",
                "final_validation_package",
                artifact_type,
                _resolve_repo_path(self.repo_root, rel_path),
                "Frozen final validation package artifact reused by Phase 5.",
                _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            )

        for rel_path in sorted(self.final_validation_manifest.get("inputs_preserved") or []):
            source_path = _resolve_repo_path(self.repo_root, rel_path)
            phase_id, track_id, _ = self._infer_track_from_path(source_path)
            add_row(
                phase_id or "phase3",
                track_id or "supporting_input",
                "scientific_input_reuse",
                source_path.name,
                source_path,
                "Scientific input preserved and reused by the final validation package.",
                _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
            )

        for output_path in sorted((self.repo_root / "output" / "phase1_finalization_audit").glob("*")):
            add_row("phase1", "phase1_regional_baseline", "phase1_audit", output_path.name, output_path, "Read-only Phase 1 audit artifact.")
        for output_path in sorted((self.repo_root / "output" / "phase2_finalization_audit").glob("*")):
            add_row("phase2", "phase2_machine_readable_forecast", "phase2_audit", output_path.name, output_path, "Read-only Phase 2 audit artifact.")

        phase4_artifacts = self.phase4_manifest.get("artifacts") or {}
        for artifact_type, rel_path in sorted(phase4_artifacts.items()):
            add_row(
                "phase4",
                "mindoro_phase4",
                "phase4",
                artifact_type,
                _resolve_repo_path(self.repo_root, rel_path),
                "Mindoro Phase 4 artifact built on the current reportable transport framework.",
                _relative_to_repo(self.repo_root, self.repo_root / PHASE4_MANIFEST_JSON),
            )

        trajectory_gallery_dir = self.repo_root / "output" / "trajectory_gallery"
        if trajectory_gallery_dir.exists():
            for output_path in sorted(trajectory_gallery_dir.glob("*")):
                add_row(
                    "phase5",
                    "trajectory_gallery",
                    "trajectory_gallery",
                    output_path.name,
                    output_path,
                    "Read-only trajectory gallery artifact built from existing outputs for panel inspection.",
                )
        trajectory_gallery_panel_dir = self.repo_root / "output" / "trajectory_gallery_panel"
        if trajectory_gallery_panel_dir.exists():
            for output_path in sorted(trajectory_gallery_panel_dir.rglob("*")):
                if output_path.is_dir():
                    continue
                add_row(
                    "phase5",
                    "trajectory_gallery_panel",
                    "trajectory_gallery_panel",
                    output_path.name,
                    output_path,
                    "Read-only polished panel gallery artifact built from existing outputs for non-technical review.",
                )
        figure_package_publication_dir = self.repo_root / "output" / "figure_package_publication"
        if figure_package_publication_dir.exists():
            for output_path in sorted(figure_package_publication_dir.rglob("*")):
                if output_path.is_dir():
                    continue
                add_row(
                    "phase5",
                    "figure_package_publication",
                    "figure_package_publication",
                    output_path.name,
                    output_path,
                    "Read-only publication-grade figure package artifact built from existing outputs for canonical defense and paper presentation.",
                )
        mindoro_final_output_dir = self.repo_root / MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR
        if mindoro_final_output_dir.exists():
            for output_path in sorted(mindoro_final_output_dir.rglob("*")):
                if output_path.is_dir():
                    continue
                add_row(
                    "phase3b",
                    "B1",
                    "phase3b_march13_14_final_output",
                    output_path.name,
                    output_path,
                    "Read-only curated export of the promoted B1 family for thesis-facing Phase 3B delivery.",
                    _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
                )
        dwh_final_output_dir = self.repo_root / DWH_PHASE3C_FINAL_OUTPUT_DIR
        if dwh_final_output_dir.exists():
            for output_path in sorted(dwh_final_output_dir.rglob("*")):
                if output_path.is_dir():
                    continue
                add_row(
                    "phase3c",
                    "C1/C2/C3",
                    "phase3c_dwh_final_output",
                    output_path.name,
                    output_path,
                    "Read-only curated export of the frozen DWH Phase 3C external transfer-validation family.",
                    _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
                )
        for prototype_dir_rel, track_id, artifact_group, description in PROTOTYPE_OUTPUT_DIRS:
            prototype_similarity_dir = self.repo_root / prototype_dir_rel
            if prototype_similarity_dir.exists():
                for output_path in sorted(prototype_similarity_dir.rglob("*")):
                    if output_path.is_dir():
                        continue
                    add_row(
                        "phase3a" if track_id == "prototype_legacy_phase3a" else "prototype",
                        track_id,
                        artifact_group,
                        output_path.name,
                        output_path,
                        description,
                    )

        for weathering_dir in sorted((self.repo_root / "output").glob("CASE_2016-*/weathering")):
            if not weathering_dir.exists():
                continue
            for output_path in sorted(weathering_dir.rglob("*")):
                if output_path.is_dir():
                    continue
                add_row(
                    "phase4",
                    "prototype_legacy_phase4_weathering",
                    "prototype_2016_weathering",
                    output_path.name,
                    output_path,
                    "Prototype 2016 legacy Phase 4 oil weathering and fate artifact.",
                )

        prototype_2016_final_dir = self.repo_root / PROTOTYPE_2016_FINAL_OUTPUT_DIR
        if prototype_2016_final_dir.exists():
            for output_path in sorted(prototype_2016_final_dir.rglob("*")):
                if output_path.is_dir():
                    continue
                add_row(
                    "phase5",
                    "prototype_legacy_phase5_packaging",
                    "prototype_2016_final_package",
                    output_path.name,
                    output_path,
                    "Curated prototype_2016 legacy Phase 5 package artifact built from stored Phase 3A/Phase 4 outputs only.",
                )

        for artifact_type, path in sorted(phase5_artifacts.items()):
            if artifact_type == "final_output_catalog":
                continue
            add_row("phase5", "phase5_sync", "phase5_sync", artifact_type, path, "Phase 5 reproducibility/package sync artifact.")

        rows.sort(key=lambda row: (row["phase_id"], row["track_id"], row["artifact_group"], row["artifact_type"]))
        return rows

    def _build_summary_markdown(
        self,
        phase_status_rows: list[dict[str, Any]],
        phase5_artifacts: dict[str, Path],
    ) -> str:
        safe_entry_ids = [
            f"`{_coerce_text(entry.get('entry_id'))}`"
            for entry in self.launcher_matrix.get("entries", [])
            if bool(entry.get("safe_default"))
        ]
        promotion = self.final_validation_manifest.get("mindoro_primary_validation_promotion") or {}
        final_output_dir = _coerce_text(
            promotion.get("final_output_export_dir")
            or self.final_validation_manifest.get("phase3b_march13_14_final_output", {}).get("output_dir")
        )
        lines = [
            "# Final Reproducibility Summary",
            "",
            "This package synchronizes launcher/menu behavior, documentation, and reproducibility indexes around the current local repo state without rerunning the expensive scientific branches by default.",
            "",
            "## Launcher Entrypoint",
            "",
            "- PowerShell entrypoint: `./start.ps1 -List -NoPause`",
            f"- Source-of-truth launcher matrix: `{_relative_to_repo(self.repo_root, self.launcher_matrix_path)}`",
            f"- Safe read-only launcher IDs: {', '.join(safe_entry_ids)}",
            "",
            "## Phase Status Highlights",
            "",
        ]
        for row in phase_status_rows:
            lines.append(f"- `{row['phase_id']}` / `{row['track_id']}`: {row['summary']}")
        lines.extend(
            [
                "",
                "## Packaging Sync Scope",
                "",
                "- Existing scientific Mindoro and DWH outputs were reused and not recomputed here.",
                "- The existing `output/final_validation_package/` bundle was reused rather than rebuilt from scratch.",
                (
                    f"- Mindoro keeps the frozen base case definition in `{MINDORO_BASE_CASE_CONFIG_PATH.as_posix()}` "
                    f"and records the promoted March 13 -> March 14 Phase 3B primary row through the separate "
                    f"`{MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH.as_posix()}` amendment file."
                ),
                (
                    f"- The thesis-facing B1 title is `{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE}`, and the "
                    "separate focused 2016-2023 Mindoro drifter rerun selected the same `cmems_era5` recipe "
                    "without rewriting the stored B1 raw provenance."
                ),
                (
                    f"- The curated read-only B1 export now lives under `{final_output_dir or MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR.as_posix()}` "
                    "and packages the publication figures, canonical scientific source PNGs, summary CSV, decision note, and local manifest."
                ),
                (
                    f"- The curated read-only DWH export now lives under `{DWH_PHASE3C_FINAL_OUTPUT_DIR.as_posix()}` and packages "
                    "the Phase 3C observation context figures, deterministic baseline figures, ensemble extension figures, "
                    "PyGNOME comparator figures, canonical scientific source PNGs, and summary/manifests without rerunning science."
                ),
                f"- `prototype_2016` is cataloged here as a legacy Phase 1 / 2 / 3A / 4 / 5 support lane, with the dedicated curated package rooted at `{PROTOTYPE_2016_FINAL_OUTPUT_DIR.as_posix()}` and repo-wide `phase5_sync` kept as a separate cross-repo reproducibility layer.",
                "- Mindoro Phase 4 now participates in the reproducibility/package layer via the current `phase4_run_manifest.json` and verdict bundle.",
                "- The static `output/trajectory_gallery/` bundle now participates in the reproducibility/package layer as a read-only technical figure set.",
                "- The static `output/trajectory_gallery_panel/` bundle now participates in the reproducibility/package layer as the polished panel-ready figure pack.",
                "- The static `output/figure_package_publication/` bundle now participates in the reproducibility/package layer as the canonical publication-grade presentation package.",
                "- The new `ui/` layer now participates as a read-only local dashboard over the existing packaged outputs and figures rather than as a rerun control surface.",
                "",
                "## Key Artifacts",
                "",
                f"- Phase status registry: `{_relative_to_repo(self.repo_root, phase5_artifacts['final_phase_status_registry'])}`",
                f"- Reproducibility manifest: `{_relative_to_repo(self.repo_root, phase5_artifacts['final_reproducibility_manifest'])}`",
                f"- Packaging sync memo: `{_relative_to_repo(self.repo_root, phase5_artifacts['phase5_packaging_sync_memo'])}`",
                f"- Launcher guide: `{_relative_to_repo(self.repo_root, phase5_artifacts['launcher_user_guide'])}`",
                "- UI guide: `docs/UI_GUIDE.md`",
                "- Trajectory gallery manifest: `output/trajectory_gallery/trajectory_gallery_manifest.json`",
                "- Panel gallery manifest: `output/trajectory_gallery_panel/panel_figure_manifest.json`",
                "- Publication figure manifest: `output/figure_package_publication/publication_figure_manifest.json`",
                f"- Curated B1 final-output export: `{final_output_dir or MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR.as_posix()}`",
                f"- Curated DWH final-output export: `{DWH_PHASE3C_FINAL_OUTPUT_DIR.as_posix()}`",
            ]
        )
        return "\n".join(lines)

    def _build_packaging_sync_memo(self, manifest_index_rows: list[dict[str, Any]]) -> str:
        missing_optional = [
            row["relative_path"]
            for row in manifest_index_rows
            if not bool(row.get("exists")) and bool(row.get("optional"))
        ]
        lines = [
            "# Phase 5 Packaging Sync Memo",
            "",
            "Phase 5 reuses the existing final validation package, Phase 1 audit, Phase 2 audit, Mindoro Phase 4 bundle, and DWH Phase 3C outputs to build a synchronized reproducibility/package layer.",
            "",
            "## What Was Reused",
            "",
            f"- Existing final validation manifest: `{_relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON)}`",
            f"- Existing Phase 1 audit: `{_relative_to_repo(self.repo_root, self.repo_root / PHASE1_AUDIT_JSON)}`",
            f"- Existing Phase 2 audit: `{_relative_to_repo(self.repo_root, self.repo_root / PHASE2_AUDIT_JSON)}`",
            f"- Existing Mindoro Phase 4 manifest: `{_relative_to_repo(self.repo_root, self.repo_root / PHASE4_MANIFEST_JSON)}`",
            f"- Frozen Mindoro base case definition: `{MINDORO_BASE_CASE_CONFIG_PATH.as_posix()}`",
            f"- Mindoro primary-validation amendment file: `{MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH.as_posix()}`",
            f"- Mindoro drifter-confirmation candidate baseline: `{MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH.as_posix()}`",
            f"- Mindoro curated final-output export: `{MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR.as_posix()}`",
            f"- DWH Phase 3C final-governance note: `{DWH_PHASE3C_FINAL_NOTE_PATH.as_posix()}`",
            f"- DWH curated final-output export: `{DWH_PHASE3C_FINAL_OUTPUT_DIR.as_posix()}`",
            "- Existing trajectory gallery outputs under `output/trajectory_gallery/` when present.",
            "- Existing polished panel gallery outputs under `output/trajectory_gallery_panel/` when present.",
            "- Existing publication-grade figure package outputs under `output/figure_package_publication/` when present.",
            "- Existing read-only dashboard source files under `ui/` and guidance in `docs/UI_GUIDE.md` when present.",
            "",
            "## Guardrails",
            "",
            "- No scientific score tables were recomputed here.",
            "- No finished Mindoro or DWH scientific outputs were overwritten.",
            "- The March 3 -> March 6 Mindoro base case YAML remains frozen; the promoted March 13 -> March 14 row is recorded as an amendment rather than a silent rewrite.",
            f"- `{MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE}` remains tied to B1, and {MINDORO_SHARED_IMAGERY_CAVEAT.lower()}",
            f"- DWH Phase 3C remains a separate external transfer-validation lane under `{DWH_BASE_CASE_CONFIG_PATH.as_posix()}` with forcing fixed to `{DWH_PHASE3C_FORCING_STACK}` and no thesis-facing drifter baseline.",
            "- The separate focused 2016-2023 Mindoro drifter rerun now supplies the active B1 recipe-provenance story, not the raw generation history of the stored March 13 -> March 14 science bundle.",
            f"- The legacy `prototype_2016` lane is framed as Phase 1 / 2 / 3A / 4 / 5 support, with its dedicated curated package rooted at `{PROTOTYPE_2016_FINAL_OUTPUT_DIR.as_posix()}`; it has no thesis-facing Phase 3B or Phase 3C.",
            "- The launcher/menu is now organized around current track categories instead of the older monolithic Mindoro full-chain story.",
            "- The first dashboard version is intentionally read-only and does not add scientific run buttons.",
        ]
        if missing_optional:
            lines.extend(
                [
                    "",
                    "## Optional Future Work Still Missing",
                    "",
                    *[f"- `{path}`" for path in missing_optional],
                ]
            )
        return "\n".join(lines)

    def _build_phase5_verdict(self, overall_verdict: dict[str, Any]) -> str:
        optional_lines = [
            f"- `{item['work_id']}`: {_coerce_text(item.get('label'))} [{_coerce_text(item.get('status'))}]"
            for item in overall_verdict.get("optional_future_work", [])
        ]
        lines = [
            "# Phase 5 Final Verdict",
            "",
            "Phase 5 is reportable now as the launcher/docs/reproducibility synchronization layer for the current repo state.",
            "",
            "## Direct Answers",
            "",
            f"- Is Phase 5 reportable now? {'Yes' if overall_verdict['phase5_reportable_now'] else 'No'}; it is reportable as a non-scientific reproducibility/package layer rather than as a scientific result.",
            f"- Is the launcher/menu now honest and current? {'Yes' if overall_verdict['launcher_menu_honest_and_current'] else 'No'}.",
            "- Were existing scientific outputs recomputed here? No.",
            f"- Does the project still need a later final Phase 1 production rerun for the frozen baseline story? {'Yes' if overall_verdict['requires_phase1_production_rerun_for_full_freeze'] else 'No'}.",
            f"- Does legacy recipe-family drift still leak into official-mode behavior? {'Yes' if overall_verdict['legacy_recipe_drift_leaks_into_official_mode'] else 'No'}.",
            "",
            "## What Remains Provisional",
            "",
            f"- Phase 1 frozen baseline story complete: {overall_verdict['phase1_frozen_story_complete']}",
            f"- Phase 2 scientifically usable: {overall_verdict['phase2_scientifically_usable']}",
            f"- Phase 2 scientifically frozen: {overall_verdict['phase2_scientifically_frozen']}",
            f"- Mindoro Phase 4 scientifically reportable now: {overall_verdict['phase4_scientifically_reportable_now']}",
            f"- Mindoro Phase 4 inherited provisional: {overall_verdict['phase4_inherited_provisional']}",
            "- Read-only local dashboard: implemented as a Phase 5 exploration layer over existing outputs, with no scientific run controls.",
            f"- Biggest remaining project-science blocker: {overall_verdict['biggest_remaining_project_scientific_blocker']}",
            "",
            "## Optional Future Work",
            "",
        ]
        if optional_lines:
            lines.extend(optional_lines)
        else:
            lines.append("- No optional future-work items are currently recorded in the launcher matrix.")
        return "\n".join(lines)

    def _build_launcher_user_guide(self) -> str:
        entries = sorted(
            self.launcher_matrix.get("entries", []),
            key=lambda item: (
                int(item.get("menu_order", 0)),
                _coerce_text(item.get("entry_id")),
            ),
        )
        safe_entries = [
            entry
            for entry in entries
            if bool(entry.get("safe_default"))
        ]
        lines = [
            "# Launcher User Guide",
            "",
            "Use the PowerShell launcher from the repository root. The launcher is now organized around honest current tracks instead of the old single Mindoro full-chain menu.",
            "",
            "## Safe First Commands",
            "",
            "- `./start.ps1 -List -NoPause` shows the current menu catalog without starting Docker work.",
            "- `./start.ps1 -Help -NoPause` prints guidance and safe entry IDs.",
        ]
        for entry in safe_entries:
            lines.append(
                f"- `./start.ps1 -Entry {_coerce_text(entry.get('entry_id'))} -NoPause` runs the safe read-only entry "
                f"`{_coerce_text(entry.get('label'))}`."
            )
        lines.extend(
            [
            "",
            "## Entry Catalog",
            "",
        ])
        for category in self.launcher_matrix.get("categories", []):
            category_id = _coerce_text(category.get("category_id"))
            lines.append(f"### {_coerce_text(category.get('label'))}")
            lines.append("")
            lines.append(_coerce_text(category.get("description")))
            lines.append("")
            for entry in entries:
                if _coerce_text(entry.get("category_id")) != category_id:
                    continue
                workflow_mode = _coerce_text(entry.get("workflow_mode"))
                rerun_cost = _coerce_text(entry.get("rerun_cost"))
                safe_default = bool(entry.get("safe_default"))
                steps = entry.get("steps") or []
                step_labels = ", ".join(_coerce_text(step.get("phase")) for step in steps)
                lines.append(
                    f"- `{_coerce_text(entry.get('entry_id'))}`: {_coerce_text(entry.get('label'))}. "
                    f"Workflow mode = `{workflow_mode}`. Cost = `{rerun_cost}`. "
                    f"Safe read-only default = `{str(safe_default).lower()}`. "
                    f"Phases = {step_labels}."
                )
                if _coerce_text(entry.get("notes")):
                    lines.append(f"  Note: {_coerce_text(entry.get('notes'))}")
                lines.append(
                    f"  Run with: `./start.ps1 -Entry {_coerce_text(entry.get('entry_id'))} -NoPause`"
                )
            lines.append("")

        lines.extend(
            [
                "## Guardrails",
                "",
                "- `prototype_2016` remains available for debugging and regression only; it is not the final Phase 1 study.",
                f"- `prototype_2016` is thesis-facing only as Phase 1 / 2 / 3A / 4 / 5, with its dedicated curated package rooted at `{PROTOTYPE_2016_FINAL_OUTPUT_DIR.as_posix()}` and no thesis-facing 3B/3C lane. Repo-wide `phase5_sync` remains a separate cross-repo reproducibility layer.",
                "- `mindoro_reportable_core` and `dwh_reportable_bundle` are intentional scientific reruns and are not safe defaults.",
                "- The read-only utilities do not recompute scientific scores and are the safest launcher options for routine status refreshes.",
                "",
                "## Optional Future Work",
                "",
            ]
        )

        optional_items = self.launcher_matrix.get("optional_future_work") or []
        if optional_items:
            for item in optional_items:
                lines.append(
                    f"- `{_coerce_text(item.get('work_id'))}`: {_coerce_text(item.get('label'))} "
                    f"[{_coerce_text(item.get('status'))}]"
                )
        else:
            lines.append("- No optional future-work items are currently recorded.")

        lines.extend(
            [
                "",
                "## Matrix Source",
                "",
                f"- Catalog file: `{_relative_to_repo(self.repo_root, self.launcher_matrix_path)}`",
                f"- Entrypoint script: `{_coerce_text(self.launcher_matrix.get('entrypoint'))}`",
                f"- Catalog version: `{_coerce_text(self.launcher_matrix.get('catalog_version'))}`",
            ]
        )
        return "\n".join(lines)

    def _build_overall_verdict(
        self,
        phase_status_rows: list[dict[str, Any]],
        manifest_index_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        phase1_row = next(row for row in phase_status_rows if row["phase_id"] == "phase1")
        phase2_row = next(row for row in phase_status_rows if row["phase_id"] == "phase2")
        phase4_row = next(row for row in phase_status_rows if row["phase_id"] == "phase4")
        missing_optional_artifacts = [
            row["relative_path"]
            for row in manifest_index_rows
            if not bool(row.get("exists")) and bool(row.get("optional"))
        ]
        return {
            "phase5_reportable_now": True,
            "launcher_menu_honest_and_current": True,
            "final_validation_package_reused_without_recompute": True,
            "requires_phase1_production_rerun_for_full_freeze": bool(
                self.phase2_audit.get("overall_verdict", {}).get(
                    "requires_phase1_production_rerun_for_full_freeze",
                    True,
                )
            ),
            "phase1_frozen_story_complete": False,
            "phase2_scientifically_usable": bool(phase2_row["reportable_now"]),
            "phase2_scientifically_frozen": bool(phase2_row["scientifically_frozen"]),
            "phase4_scientifically_reportable_now": bool(phase4_row["reportable_now"]),
            "phase4_inherited_provisional": bool(phase4_row["inherited_provisional"]),
            "legacy_recipe_drift_leaks_into_official_mode": bool(
                self.phase2_audit.get("overall_verdict", {}).get(
                    "legacy_recipe_drift_leaks_into_official_mode",
                    True,
                )
            ),
            "biggest_remaining_project_scientific_blocker": _coerce_text(phase1_row["main_blocker"]),
            "optional_future_work": self.launcher_matrix.get("optional_future_work") or [],
            "missing_optional_artifacts": missing_optional_artifacts,
        }

    def _build_final_manifest(
        self,
        generated_at_utc: str,
        phase5_artifacts: dict[str, Path],
        phase_status_rows: list[dict[str, Any]],
        manifest_index_rows: list[dict[str, Any]],
        overall_verdict: dict[str, Any],
    ) -> dict[str, Any]:
        promotion = self.final_validation_manifest.get("mindoro_primary_validation_promotion") or {}
        dwh_freeze = self.final_validation_manifest.get("dwh_phase3c_freeze") or {}
        return {
            "phase": PHASE,
            "generated_at_utc": generated_at_utc,
            "repo_root": str(self.repo_root),
            "output_dir": _relative_to_repo(self.repo_root, self.output_dir),
            "launcher_catalog": {
                "path": _relative_to_repo(self.repo_root, self.launcher_matrix_path),
                "entrypoint": _coerce_text(self.launcher_matrix.get("entrypoint")),
                "catalog_version": _coerce_text(self.launcher_matrix.get("catalog_version")),
                "safe_read_only_entry_ids": [
                    _coerce_text(entry.get("entry_id"))
                    for entry in self.launcher_matrix.get("entries", [])
                    if bool(entry.get("safe_default"))
                ],
            },
            "final_validation_package_sync": {
                "reused_existing_package": True,
                "package_manifest_path": _relative_to_repo(self.repo_root, self.repo_root / FINAL_VALIDATION_MANIFEST_JSON),
                "package_case_registry_path": _relative_to_repo(
                    self.repo_root,
                    self.repo_root / FINAL_VALIDATION_CASE_REGISTRY_CSV,
                ),
                "scientific_scores_recomputed": False,
            },
            "mindoro_primary_validation_promotion": {
                "base_case_definition_path": MINDORO_BASE_CASE_CONFIG_PATH.as_posix(),
                "case_freeze_amendment_path": MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH.as_posix(),
                "primary_launcher_entry_id": MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID,
                "launcher_alias_entry_id": MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID,
                "base_case_window": "2023-03-03_to_2023-03-06",
                "promoted_primary_window": "2023-03-13_to_2023-03-14",
                "legacy_row_retained": "B2",
                "thesis_phase_title": _coerce_text(promotion.get("thesis_phase_title")) or MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
                "thesis_phase_subtitle": _coerce_text(promotion.get("thesis_phase_subtitle")),
                "shared_imagery_caveat": _coerce_text(promotion.get("shared_imagery_caveat")) or MINDORO_SHARED_IMAGERY_CAVEAT,
                "dual_provenance_confirmation": promotion.get("dual_provenance_confirmation") or {},
                "final_output_export_dir": _coerce_text(promotion.get("final_output_export_dir"))
                or _coerce_text(self.final_validation_manifest.get("phase3b_march13_14_final_output", {}).get("output_dir")),
            },
            "dwh_phase3c_freeze": {
                "base_case_definition_path": _coerce_text(dwh_freeze.get("base_case_definition_path")) or DWH_BASE_CASE_CONFIG_PATH.as_posix(),
                "final_note_path": _coerce_text(dwh_freeze.get("final_note_path")) or DWH_PHASE3C_FINAL_NOTE_PATH.as_posix(),
                "forcing_stack": _coerce_text(dwh_freeze.get("forcing_stack")) or DWH_PHASE3C_FORCING_STACK,
                "final_output_export_dir": _coerce_text(dwh_freeze.get("final_output_export_dir"))
                or DWH_PHASE3C_FINAL_OUTPUT_DIR.as_posix(),
            },
            "docs_updated": [
                _relative_to_repo(self.repo_root, self.repo_root / path) for path in self.docs_updated
            ],
            "artifacts": {
                key: _relative_to_repo(self.repo_root, path)
                for key, path in phase5_artifacts.items()
            },
            "phase_status_rows": phase_status_rows,
            "manifest_index_summary": {
                "row_count": len(manifest_index_rows),
                "missing_optional_artifacts": overall_verdict["missing_optional_artifacts"],
            },
            "phase5_verdict": overall_verdict,
        }

    def run(self) -> dict[str, Any]:
        generated_at_utc = pd.Timestamp.now(tz="UTC").isoformat()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        safe_read_only_entry_ids = [
            _coerce_text(entry.get("entry_id"))
            for entry in self.launcher_matrix.get("entries", [])
            if bool(entry.get("safe_default"))
        ]

        phase5_artifacts = {
            "software_versions": self.output_dir / "software_versions.csv",
            "final_case_registry": self.output_dir / "final_case_registry.csv",
            "final_config_snapshot_index": self.output_dir / "final_config_snapshot_index.csv",
            "final_manifest_index": self.output_dir / "final_manifest_index.csv",
            "final_output_catalog": self.output_dir / "final_output_catalog.csv",
            "final_log_index": self.output_dir / "final_log_index.csv",
            "final_phase_status_registry": self.output_dir / "final_phase_status_registry.csv",
            "final_reproducibility_summary": self.output_dir / "final_reproducibility_summary.md",
            "final_reproducibility_manifest": self.output_dir / "final_reproducibility_manifest.json",
            "phase5_packaging_sync_memo": self.output_dir / "phase5_packaging_sync_memo.md",
            "phase5_final_verdict": self.output_dir / "phase5_final_verdict.md",
            "launcher_user_guide": self.output_dir / "launcher_user_guide.md",
        }

        software_rows = self._collect_software_versions()
        case_rows = self._collect_case_registry()
        config_snapshot_rows = self._collect_config_snapshot_index()
        log_rows = self._collect_log_index()
        phase_status_rows = self._build_phase_status_registry()

        _write_csv(
            phase5_artifacts["software_versions"],
            software_rows,
            columns=["component_type", "component_name", "version", "source", "notes"],
        )
        _write_csv(
            phase5_artifacts["final_case_registry"],
            case_rows,
            columns=[
                "case_id",
                "workflow_mode",
                "mode_label",
                "description",
                "config_path",
                "case_freeze_amendment_path",
                "primary_launcher_entry_id",
                "launcher_alias_entry_id",
                "primary_output_root",
                "reportable_track_ids",
                "appendix_or_support_track_ids",
                "notes",
            ],
        )
        _write_csv(
            phase5_artifacts["final_config_snapshot_index"],
            config_snapshot_rows,
            columns=[
                "relative_path",
                "snapshot_class",
                "exists",
                "size_bytes",
                "modified_utc",
                "sha256",
            ],
        )
        _write_csv(
            phase5_artifacts["final_log_index"],
            log_rows,
            columns=["relative_path", "exists", "size_bytes", "modified_utc", "log_kind"],
        )
        _write_csv(
            phase5_artifacts["final_phase_status_registry"],
            phase_status_rows,
            columns=[
                "phase_id",
                "track_id",
                "track_label",
                "readiness_status",
                "scientifically_reportable",
                "scientifically_frozen",
                "inherited_provisional",
                "main_blocker",
                "reportable_now",
                "reportability_scope",
                "summary",
                "evidence_path",
            ],
        )

        output_catalog_rows = self._collect_output_catalog_rows(phase5_artifacts)
        _write_csv(
            phase5_artifacts["final_output_catalog"],
            output_catalog_rows,
            columns=[
                "phase_id",
                "track_id",
                "artifact_group",
                "artifact_type",
                "relative_path",
                "exists",
                "source_manifest",
                "notes",
            ],
        )

        launcher_guide = self._build_launcher_user_guide()
        _write_text(phase5_artifacts["launcher_user_guide"], launcher_guide)

        summary = self._build_summary_markdown(phase_status_rows, phase5_artifacts)
        _write_text(phase5_artifacts["final_reproducibility_summary"], summary)

        placeholder_manifest = self._build_final_manifest(
            generated_at_utc,
            phase5_artifacts,
            phase_status_rows,
            [],
            self._build_overall_verdict(phase_status_rows, []),
        )
        _write_json(phase5_artifacts["final_reproducibility_manifest"], placeholder_manifest)

        manifest_index_rows = self._collect_manifest_index()
        _write_csv(
            phase5_artifacts["final_manifest_index"],
            manifest_index_rows,
            columns=[
                "manifest_id",
                "phase_id",
                "track_id",
                "track_label",
                "relative_path",
                "exists",
                "optional",
                "manifest_type",
                "case_id",
                "generated_at_utc",
                "notes",
            ],
        )

        overall_verdict = self._build_overall_verdict(phase_status_rows, manifest_index_rows)
        packaging_sync_memo = self._build_packaging_sync_memo(manifest_index_rows)
        phase5_verdict = self._build_phase5_verdict(overall_verdict)
        summary = self._build_summary_markdown(phase_status_rows, phase5_artifacts)
        final_manifest = self._build_final_manifest(
            generated_at_utc,
            phase5_artifacts,
            phase_status_rows,
            manifest_index_rows,
            overall_verdict,
        )

        _write_text(phase5_artifacts["phase5_packaging_sync_memo"], packaging_sync_memo)
        _write_text(phase5_artifacts["phase5_final_verdict"], phase5_verdict)
        _write_text(phase5_artifacts["final_reproducibility_summary"], summary)
        _write_json(phase5_artifacts["final_reproducibility_manifest"], final_manifest)

        return {
            "output_dir": str(self.output_dir),
            "launcher_entrypoint": "./start.ps1 -List -NoPause",
            "safe_read_only_entry_ids": safe_read_only_entry_ids,
            "menu_categories": [
                _coerce_text(item.get("label"))
                for item in self.launcher_matrix.get("categories", [])
            ],
            "docs_updated": [
                _relative_to_repo(self.repo_root, self.repo_root / path) for path in self.docs_updated
            ],
            "software_versions_csv": str(phase5_artifacts["software_versions"]),
            "final_case_registry_csv": str(phase5_artifacts["final_case_registry"]),
            "final_config_snapshot_index_csv": str(phase5_artifacts["final_config_snapshot_index"]),
            "final_manifest_index_csv": str(phase5_artifacts["final_manifest_index"]),
            "final_output_catalog_csv": str(phase5_artifacts["final_output_catalog"]),
            "final_log_index_csv": str(phase5_artifacts["final_log_index"]),
            "final_phase_status_registry_csv": str(phase5_artifacts["final_phase_status_registry"]),
            "final_reproducibility_summary_md": str(phase5_artifacts["final_reproducibility_summary"]),
            "final_reproducibility_manifest_json": str(phase5_artifacts["final_reproducibility_manifest"]),
            "phase5_packaging_sync_memo_md": str(phase5_artifacts["phase5_packaging_sync_memo"]),
            "phase5_final_verdict_md": str(phase5_artifacts["phase5_final_verdict"]),
            "launcher_user_guide_md": str(phase5_artifacts["launcher_user_guide"]),
            "phase_status_rows": phase_status_rows,
            "overall_verdict": overall_verdict,
        }


def run_phase5_launcher_and_docs_sync(
    repo_root: str | Path = ".",
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    return Phase5LauncherAndDocsSyncService(repo_root=repo_root, output_dir=output_dir).run()
