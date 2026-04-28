import unittest
from pathlib import Path


CONFIG_PATH = Path("config/ensemble.yaml")
ENSEMBLE_SERVICE_PATH = Path("src/services/ensemble.py")
INGESTION_SERVICE_PATH = Path("src/services/ingestion.py")
PHASE3C_SERVICE_PATH = Path("src/services/phase3c_external_case_ensemble_comparison.py")


def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def _extract_block(lines: list[str], key: str, indent: int = 0) -> str:
    prefix = (" " * indent) + f"{key}:"
    start = None
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            start = index
            break
    if start is None:
        raise AssertionError(f"Could not find block `{key}` at indent {indent}.")

    block = [lines[start]]
    for line in lines[start + 1 :]:
        stripped = line.strip()
        if stripped:
            current_indent = len(line) - len(line.lstrip(" "))
            if current_indent <= indent:
                break
        block.append(line)
    return "\n".join(block)


class EnsembleConfigConsistencyTests(unittest.TestCase):
    def test_official_ensemble_config_matches_thesis_facing_design(self):
        lines = _read_lines(CONFIG_PATH)

        official = _extract_block(lines, "official_forecast")
        deterministic = _extract_block(lines, "deterministic", indent=2)
        ensemble = _extract_block(lines, "ensemble", indent=2)
        products = _extract_block(lines, "official_products")

        self.assertIn("transport_model: oceandrift", official)
        self.assertIn("require_wave_forcing: true", official)
        self.assertIn("enable_stokes_drift: true", official)
        self.assertIn("snapshot_hours:\n    - 24\n    - 48\n    - 72", official)
        self.assertIn("probability_thresholds:\n    - 0.5\n    - 0.9", official)

        self.assertIn("start_time_offset_hours: 0", deterministic)
        self.assertIn("wind_factor: 1.0", deterministic)
        self.assertIn("horizontal_diffusivity_m2s: 2.0", deterministic)

        self.assertIn("ensemble_size: 50", ensemble)
        self.assertIn("wind_factor_min: 0.8", ensemble)
        self.assertIn("wind_factor_max: 1.2", ensemble)
        self.assertIn(
            "start_time_offset_hours:\n      - -3\n      - -2\n      - -1\n      - 0\n      - 1\n      - 2\n      - 3",
            ensemble,
        )
        self.assertIn("horizontal_diffusivity_m2s_min: 1.0", ensemble)
        self.assertIn("horizontal_diffusivity_m2s_max: 10.0", ensemble)

        self.assertIn("snapshot_hours:\n    - 24\n    - 48\n    - 72", products)
        self.assertIn("probability_thresholds:\n    - 0.5\n    - 0.9", products)

    def test_old_monte_carlo_ranges_are_not_active_in_official_config_paths(self):
        text = CONFIG_PATH.read_text(encoding="utf-8")
        legacy_block = _extract_block(_read_lines(CONFIG_PATH), "legacy_perturbations_inactive")
        active_text = text.replace(legacy_block, "")

        self.assertNotIn("\nperturbations:\n", text)
        self.assertNotIn("diffusivity_min: 0.01", active_text)
        self.assertNotIn("diffusivity_max: 0.1", active_text)
        self.assertNotIn("wind_uncertainty_min: 0.5", active_text)
        self.assertNotIn("wind_uncertainty_max: 2.0", active_text)

    def test_legacy_perturbations_are_inactive_and_excluded_from_reportable_lanes(self):
        text = CONFIG_PATH.read_text(encoding="utf-8")
        legacy = _extract_block(_read_lines(CONFIG_PATH), "legacy_perturbations_inactive")
        ensemble_service = ENSEMBLE_SERVICE_PATH.read_text(encoding="utf-8")
        ingestion_service = INGESTION_SERVICE_PATH.read_text(encoding="utf-8")
        phase3c_service = PHASE3C_SERVICE_PATH.read_text(encoding="utf-8")

        self.assertIn("active: false", legacy)
        self.assertIn("Not thesis-facing", legacy)
        self.assertIn("official/reportable lanes", legacy)

        self.assertNotIn('self.config["perturbations"]', ensemble_service)
        self.assertIn("get_active_legacy_perturbations(self.config)", ensemble_service)
        self.assertNotIn('ensemble_cfg.get("perturbations")', ingestion_service)
        self.assertIn("legacy_perturbations.get(\"active\", False)", ingestion_service)
        self.assertIn("get_official_ensemble_block", phase3c_service)
        self.assertIn("load_ensemble_config", phase3c_service)
        self.assertNotIn("\nperturbations:\n", text)


if __name__ == "__main__":
    unittest.main()
