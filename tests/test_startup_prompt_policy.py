import os
import io
import unittest
from unittest import mock
from contextlib import redirect_stdout

import src.__main__ as entrypoint
from src.utils import startup_prompt_policy as policy


class StartupPromptPolicyTests(unittest.TestCase):
    def test_noninteractive_defaults_are_applied_silently(self):
        probe = {
            "should_prompt_wait_budget": True,
            "has_eligible_input_cache": True,
        }
        with mock.patch.dict(os.environ, {}, clear=False), mock.patch.object(
            policy,
            "build_phase_probe",
            return_value=probe,
        ):
            resolved = policy.resolve_run_startup_env(
                workflow_mode="mindoro_retro_2023",
                phase="official_phase3b",
                interactive=False,
                output=io.StringIO(),
                apply=False,
            )

        self.assertEqual(resolved["FORCING_SOURCE_BUDGET_SECONDS"], "300")
        self.assertEqual(resolved["INPUT_CACHE_POLICY"], "reuse_if_valid")
        self.assertEqual(resolved["RUN_STARTUP_PROMPTS_RESOLVED"], "1")

    def test_interactive_prompt_can_choose_longer_budget_and_force_refresh(self):
        answers = iter(["3", "2"])
        probe = {
            "should_prompt_wait_budget": True,
            "has_eligible_input_cache": True,
        }
        with mock.patch.dict(os.environ, {}, clear=False), mock.patch.object(
            policy,
            "build_phase_probe",
            return_value=probe,
        ):
            resolved = policy.resolve_run_startup_env(
                workflow_mode="mindoro_retro_2023",
                phase="official_phase3b",
                interactive=True,
                input_func=lambda prompt: next(answers),
                output=io.StringIO(),
                apply=False,
            )

        self.assertEqual(resolved["FORCING_SOURCE_BUDGET_SECONDS"], "600")
        self.assertEqual(resolved["INPUT_CACHE_POLICY"], "force_refresh")

    def test_explicit_envs_suppress_prompts(self):
        with mock.patch.dict(
            os.environ,
            {
                "FORCING_SOURCE_BUDGET_SECONDS": "120",
                "INPUT_CACHE_POLICY": "force_refresh",
            },
            clear=False,
        ), mock.patch.object(
            policy,
            "build_phase_probe",
            return_value={
                "should_prompt_wait_budget": True,
                "has_eligible_input_cache": True,
            },
        ):
            resolved = policy.resolve_run_startup_env(
                workflow_mode="mindoro_retro_2023",
                phase="official_phase3b",
                interactive=True,
                input_func=lambda prompt: self.fail("interactive prompt should not be used"),
                output=io.StringIO(),
                apply=False,
            )

        self.assertEqual(resolved["FORCING_SOURCE_BUDGET_SECONDS"], "120")
        self.assertEqual(resolved["INPUT_CACHE_POLICY"], "force_refresh")

    def test_prep_force_refresh_alias_only_applies_when_new_policy_is_unset(self):
        with mock.patch.dict(
            os.environ,
            {
                "PREP_FORCE_REFRESH": "1",
                "INPUT_CACHE_POLICY": "default",
            },
            clear=False,
        ), mock.patch.object(
            policy,
            "build_phase_probe",
            return_value={
                "should_prompt_wait_budget": False,
                "has_eligible_input_cache": False,
            },
        ):
            resolved = policy.resolve_run_startup_env(
                workflow_mode="mindoro_retro_2023",
                phase="official_phase3b",
                interactive=False,
                output=io.StringIO(),
                apply=False,
            )

        self.assertEqual(resolved["INPUT_CACHE_POLICY"], "force_refresh")

    def test_read_only_launcher_entry_probe_stays_non_prompting(self):
        payload = policy.build_launcher_entry_probe("phase5_sync")

        self.assertEqual(payload["entry_id"], "phase5_sync")
        self.assertFalse(payload["should_prompt_wait_budget"])
        self.assertFalse(payload["has_eligible_input_cache"])

    def test_prototype_legacy_bundle_probe_includes_ensemble_reuse_prompt_flag(self):
        payload = policy.build_launcher_entry_probe("prototype_legacy_bundle")

        self.assertEqual(payload["entry_id"], "prototype_legacy_bundle")
        self.assertTrue(payload["should_prompt_wait_budget"])
        self.assertTrue(payload["should_prompt_prototype_2016_ensemble_policy"])

    def test_noninteractive_startup_state_reports_default_sources(self):
        probe = {
            "should_prompt_wait_budget": True,
            "has_eligible_input_cache": True,
        }
        with mock.patch.dict(os.environ, {}, clear=False), mock.patch.object(
            policy,
            "build_phase_probe",
            return_value=probe,
        ):
            state = policy.resolve_run_startup_state(
                workflow_mode="mindoro_retro_2023",
                phase="official_phase3b",
                interactive=False,
                output=io.StringIO(),
                apply=False,
            )

        self.assertEqual(state["wait_budget_source"], "non_interactive_default")
        self.assertEqual(state["input_cache_policy_source"], "non_interactive_default")
        self.assertEqual(state["prompting_skipped_reason"], "non_interactive_runtime")

    def test_launcher_prompt_coverage_audit_marks_all_promptable_pipeline_entries(self):
        audit_rows = policy.audit_launcher_startup_prompt_coverage()

        self.assertTrue(audit_rows)
        for row in audit_rows:
            has_promptable_pipeline_phase = bool(row["has_promptable_pipeline_phase"])
            self.assertEqual(bool(row["should_prompt_wait_budget"]), has_promptable_pipeline_phase)


class MainStartupPromptIntegrationTests(unittest.TestCase):
    def test_main_resolves_startup_prompt_env_before_dispatch(self):
        with mock.patch.dict(
            os.environ,
            {
                "PIPELINE_PHASE": "phase1_production_rerun",
                "WORKFLOW_MODE": "phase1_regional_2016_2022",
            },
            clear=False,
        ), mock.patch.object(
            entrypoint,
            "_resolve_startup_prompt_env_once",
            return_value=None,
        ) as mock_resolve, mock.patch.object(
            entrypoint,
            "run_phase1_production_rerun_phase",
        ) as mock_phase:
            entrypoint.main()

        mock_resolve.assert_called_once_with("phase1_production_rerun")
        mock_phase.assert_called_once_with()

    def test_main_prints_direct_run_startup_banner_for_noninteractive_promptable_phase(self):
        startup_state = {
            "probe": {"should_prompt_wait_budget": True},
            "input_cache_policy": "reuse_if_valid",
            "input_cache_policy_source": "non_interactive_default",
            "forcing_source_budget_seconds": "300",
            "wait_budget_source": "explicit_env",
            "prompting_skipped_reason": "non_interactive_runtime",
            "matching_launcher_entry_id": "phase1_mindoro_focus_pre_spill_experiment",
        }
        stdout = io.StringIO()

        with mock.patch.dict(
            os.environ,
            {
                "PIPELINE_PHASE": "phase1_production_rerun",
                "WORKFLOW_MODE": "phase1_mindoro_focus_pre_spill_2016_2023",
            },
            clear=False,
        ), mock.patch.object(
            entrypoint,
            "_resolve_startup_prompt_env_once",
            return_value=startup_state,
        ), mock.patch.object(
            entrypoint,
            "run_phase1_production_rerun_phase",
        ) as mock_phase, redirect_stdout(stdout):
            entrypoint.main()

        output = stdout.getvalue()
        self.assertIn("Startup policy:", output)
        self.assertIn("INPUT_CACHE_POLICY=reuse_if_valid", output)
        self.assertIn("FORCING_SOURCE_BUDGET_SECONDS=300", output)
        self.assertIn("non-interactive", output)
        self.assertIn(".\\start.ps1 -Entry phase1_mindoro_focus_pre_spill_experiment", output)
        mock_phase.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
