import json
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER_MATRIX_PATH = REPO_ROOT / "config" / "launcher_matrix.json"
ABSOLUTE_LOCAL_LINK_PATTERN = re.compile(r"(?i)(?:^|[\(\s])/?[a-z]:/users/")


class LauncherMatrixMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = json.loads(LAUNCHER_MATRIX_PATH.read_text(encoding="utf-8"))
        cls.entries = cls.matrix["entries"]
        cls.entry_map = {entry["entry_id"]: entry for entry in cls.entries}
        cls.role_groups = cls.matrix.get("role_groups") or []

    def test_all_entries_have_required_fields(self):
        required_fields = (
            "entry_id",
            "label",
            "category_id",
            "workflow_mode",
            "rerun_cost",
            "safe_default",
            "steps",
            "thesis_role",
            "manuscript_section",
            "claim_boundary",
            "run_kind",
            "recommended_for",
            "confirms_before_run",
            "thesis_facing",
            "reportable",
            "experimental_only",
        )

        for entry in self.entries:
            for field in required_fields:
                self.assertIn(field, entry, f"{entry.get('entry_id', '<missing>')} is missing '{field}'")
            self.assertTrue(entry["entry_id"])
            self.assertTrue(entry["label"])
            self.assertTrue(entry["category_id"])
            self.assertTrue(entry["workflow_mode"])
            self.assertTrue(entry["rerun_cost"])
            self.assertTrue(entry["thesis_role"])
            self.assertTrue(entry["manuscript_section"])
            self.assertNotIn("dra" + "ft_section", entry)
            self.assertTrue(entry["claim_boundary"])
            self.assertTrue(entry["run_kind"])
            self.assertTrue(entry["recommended_for"])
            self.assertIsInstance(entry["safe_default"], bool)
            self.assertIsInstance(entry["confirms_before_run"], bool)
            self.assertIsInstance(entry["thesis_facing"], bool)
            self.assertIsInstance(entry["reportable"], bool)
            self.assertIsInstance(entry["experimental_only"], bool)
            self.assertIsInstance(entry["steps"], list)
            self.assertTrue(entry["steps"], f"{entry['entry_id']} must define at least one step")
            if entry.get("alias_of"):
                self.assertIn("menu_hidden", entry, f"{entry['entry_id']} alias must declare menu_hidden metadata")
                self.assertTrue(entry["menu_hidden"], f"{entry['entry_id']} alias must stay hidden from the default menu")

    def test_primary_evidence_group_contains_only_final_manuscript_entries(self):
        primary_entry_ids = {
            entry["entry_id"]
            for entry in self.entries
            if entry.get("thesis_role") == "primary_evidence"
        }

        self.assertEqual(
            primary_entry_ids,
            {
                "phase1_mindoro_focus_provenance",
                "mindoro_phase3b_primary_public_validation",
                "dwh_reportable_bundle",
                "mindoro_reportable_core",
            },
        )
        self.assertFalse(self.entry_map["mindoro_reportable_core"]["default_panel_path"])
        self.assertTrue(self.entry_map["mindoro_reportable_core"]["intentional_full_evidence_support_bundle"])

    def test_archive_and_hidden_experimental_metadata_stays_non_thesis_facing(self):
        hidden_experimental_ids = {
            "phase3b_mindoro_march3_4_philsa_5000_experiment",
            "mindoro_mar09_12_multisource_experiment",
            "phase3b_mindoro_march13_14_reinit_5000_experiment",
        }

        for entry in self.entries:
            if entry.get("archive_status"):
                self.assertFalse(entry["thesis_facing"], entry["entry_id"])
                self.assertFalse(entry["reportable"], entry["entry_id"])
                self.assertIn("archive_registry_id", entry, entry["entry_id"])
                self.assertIn(
                    entry.get("launcher_visibility"),
                    {"visible_archive", "hidden_alias", "hidden_experimental", "read_only_only"},
                    entry["entry_id"],
                )

        for entry_id in hidden_experimental_ids:
            entry = self.entry_map[entry_id]
            self.assertTrue(entry["menu_hidden"], entry_id)
            self.assertTrue(entry["experimental_only"], entry_id)
            self.assertEqual(entry["archive_status"], "experimental_only", entry_id)
            self.assertEqual(entry["launcher_visibility"], "hidden_experimental", entry_id)
            self.assertFalse(entry["thesis_facing"], entry_id)
            self.assertFalse(entry["reportable"], entry_id)

    def test_role_groups_define_launcher_home_lanes(self):
        group_ids = {group.get("GroupId") for group in self.role_groups}
        self.assertTrue(self.role_groups, "launcher_matrix.json should define role_groups for the home menu")
        self.assertEqual(
            group_ids,
            {
                "main_thesis_evidence",
                "support_context",
                "archive_provenance",
                "legacy_debug",
                "read_only_governance",
            },
        )

    def test_new_aliases_exist_and_old_ids_remain(self):
        self.assertIn("phase1_mindoro_focus_provenance", self.entry_map)
        self.assertIn("phase1_mindoro_focus_pre_spill_experiment", self.entry_map)
        self.assertEqual(
            self.entry_map["phase1_mindoro_focus_pre_spill_experiment"].get("alias_of"),
            "phase1_mindoro_focus_provenance",
        )

        self.assertIn("phase1_regional_reference_rerun", self.entry_map)
        self.assertIn("phase1_production_rerun", self.entry_map)
        self.assertEqual(
            self.entry_map["phase1_production_rerun"].get("alias_of"),
            "phase1_regional_reference_rerun",
        )

    def test_canonical_entry_set_stays_stable_and_aliases_stay_hidden(self):
        canonical_entry_ids = (
            "phase1_mindoro_focus_provenance",
            "mindoro_phase3b_primary_public_validation",
            "mindoro_reportable_core",
            "dwh_reportable_bundle",
            "phase1_regional_reference_rerun",
            "mindoro_phase4_only",
            "mindoro_appendix_sensitivity_bundle",
            "b1_drifter_context_panel",
            "phase5_sync",
            "prototype_legacy_final_figures",
        )
        alias_targets = {
            "phase1_mindoro_focus_pre_spill_experiment": "phase1_mindoro_focus_provenance",
            "phase1_production_rerun": "phase1_regional_reference_rerun",
            "mindoro_march13_14_noaa_reinit_stress_test": "mindoro_phase3b_primary_public_validation",
        }

        for entry_id in canonical_entry_ids:
            self.assertIn(entry_id, self.entry_map)
            self.assertFalse(self.entry_map[entry_id].get("alias_of"), entry_id)

        for alias_id, target_id in alias_targets.items():
            self.assertIn(alias_id, self.entry_map)
            self.assertEqual(self.entry_map[alias_id].get("alias_of"), target_id)
            self.assertTrue(self.entry_map[alias_id].get("menu_hidden"), alias_id)

    def test_aliases_point_to_existing_entries_and_duplicate_steps_safely(self):
        for entry in self.entries:
            alias_of = entry.get("alias_of")
            if not alias_of:
                continue

            self.assertIn(alias_of, self.entry_map, f"{entry['entry_id']} points to missing alias target {alias_of}")
            target = self.entry_map[alias_of]
            self.assertEqual(entry["workflow_mode"], target["workflow_mode"])
            self.assertEqual(entry["rerun_cost"], target["rerun_cost"])
            self.assertEqual(entry["safe_default"], target["safe_default"])
            self.assertEqual(entry["steps"], target["steps"])

    def test_no_local_absolute_links_in_readme_or_docs(self):
        markdown_paths = [REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").rglob("*.md"))]
        offenders = []

        for path in markdown_paths:
            text = path.read_text(encoding="utf-8")
            if ABSOLUTE_LOCAL_LINK_PATTERN.search(text):
                offenders.append(str(path.relative_to(REPO_ROOT)))

        self.assertFalse(offenders, f"Found local absolute paths in markdown: {offenders}")

    def test_panel_safe_entries_remain_read_only_or_packaging_only(self):
        panel_safe_entry_ids = (
            "b1_drifter_context_panel",
            "phase1_audit",
            "phase2_audit",
            "final_validation_package",
            "phase5_sync",
            "trajectory_gallery",
            "trajectory_gallery_panel",
            "figure_package_publication",
        )

        for entry_id in panel_safe_entry_ids:
            entry = self.entry_map[entry_id]
            self.assertTrue(entry["safe_default"], entry_id)
            self.assertIn(entry["run_kind"], {"read_only", "packaging_only"}, entry_id)

    def test_b1_claim_boundary_mentions_independent_noaa_products(self):
        boundary = self.entry_map["mindoro_phase3b_primary_public_validation"]["claim_boundary"].lower()
        self.assertIn("independent noaa-published day-specific observation products", boundary)
        self.assertIn("neighborhood-scale", boundary)
        self.assertIn("only main philippine public-observation validation claim", boundary)

    def test_b1_drifter_context_entry_stays_read_only_and_not_direct(self):
        entry = self.entry_map["b1_drifter_context_panel"]
        self.assertTrue(entry["safe_default"])
        self.assertEqual(entry["run_kind"], "read_only")
        self.assertEqual(entry["recommended_for"], "panel")
        self.assertIn("not the direct", entry["claim_boundary"].lower())

    def test_pygnome_entries_keep_comparator_only_wording(self):
        offenders = []

        for entry in self.entries:
            payload = json.dumps(entry).lower()
            if "pygnome" not in payload:
                continue

            combined = " ".join(
                str(entry.get(field, ""))
                for field in ("label", "description", "notes", "claim_boundary")
            ).lower()
            if "comparator-only" not in combined and "comparator only" not in combined:
                offenders.append(entry["entry_id"])

        self.assertFalse(offenders, f"PyGNOME entries lost comparator-only wording: {offenders}")

    def test_dwh_entry_mentions_external_transfer_validation_not_recalibration(self):
        combined = " ".join(
            str(self.entry_map["dwh_reportable_bundle"].get(field, ""))
            for field in ("description", "notes", "claim_boundary")
        ).lower()
        self.assertIn("external transfer", combined)
        self.assertIn("not mindoro recalibration", combined)

    def test_launcher_docs_list_canonical_entries_and_dashboard_shortcut(self):
        doc_paths = (
            REPO_ROOT / "README.md",
            REPO_ROOT / "PANEL_QUICK_START.md",
            REPO_ROOT / "docs" / "COMMAND_MATRIX.md",
            REPO_ROOT / "docs" / "LAUNCHER_USER_GUIDE.md",
        )
        combined = "\n".join(path.read_text(encoding="utf-8") for path in doc_paths)
        lowered = combined.lower()

        for entry_id in (
            "phase1_mindoro_focus_provenance",
            "mindoro_phase3b_primary_public_validation",
            "mindoro_reportable_core",
            "dwh_reportable_bundle",
            "phase1_regional_reference_rerun",
            "mindoro_phase4_only",
            "mindoro_appendix_sensitivity_bundle",
            "b1_drifter_context_panel",
            "phase5_sync",
            "prototype_legacy_final_figures",
        ):
            self.assertIn(entry_id, combined)

        self.assertIn("panel option `1`", lowered)
        self.assertTrue("u` / `ui`" in lowered or "`u`, `ui`" in lowered)
        self.assertIn("shortcut, not a separate launcher entry id", lowered)

    def test_panel_option_labels_match_command_matrix_and_quick_start(self):
        start_text = (REPO_ROOT / "start.ps1").read_text(encoding="utf-8")
        command_matrix_text = (REPO_ROOT / "docs" / "COMMAND_MATRIX.md").read_text(encoding="utf-8")
        panel_quick_start_text = (REPO_ROOT / "PANEL_QUICK_START.md").read_text(encoding="utf-8")

        exact_panel_labels = (
            "Open read-only dashboard",
            "Verify paper numbers against stored scorecards",
            "Rebuild publication figures from stored outputs",
            "Refresh final validation package from stored outputs",
            "Refresh final reproducibility package / command documentation",
            "Show paper-to-output registry",
            "View B1 drifter provenance/context",
            "View data sources and provenance registry",
        )

        for label in exact_panel_labels:
            self.assertIn(label, start_text)
            self.assertIn(label, command_matrix_text)
            self.assertIn(label, panel_quick_start_text)


if __name__ == "__main__":
    unittest.main()
