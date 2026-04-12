"""
Custom exceptions for the application.
"""

from typing import Any

from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_ENV,
    FORCING_OUTAGE_SKIP_EXIT_CODE,
    FORCING_OUTAGE_SKIP_PAYLOAD_PREFIX,
)

PREP_OUTAGE_DECISION_EXIT_CODE = 86
BENCHMARK_SKIP_EXIT_CODE = 87
PREP_OUTAGE_PAYLOAD_PREFIX = "PREP_OUTAGE_PAYLOAD="
PREP_OUTAGE_PROMPT_SUPPORTED_ENV = "PREP_OUTAGE_PROMPT_SUPPORTED"
PREP_REUSE_APPROVED_SOURCE_ENV = "PREP_REUSE_APPROVED_SOURCE"
PREP_REUSE_APPROVED_ONCE_ENV = "PREP_REUSE_APPROVED_ONCE"
PREP_FORCE_REFRESH_ENV = "PREP_FORCE_REFRESH"

class ValidationPipelineError(Exception):
    """Base exception for validation pipeline."""
    pass

class DataLoadingError(ValidationPipelineError):
    """Raised when data loading fails."""
    pass

class SimulationError(ValidationPipelineError):
    """Raised when simulation fails."""
    pass


class BenchmarkCaseSkipped(ValidationPipelineError):
    """Raised when a benchmark case is intentionally skipped before scoring."""

    pass


class ForcingProviderAcquisitionError(DataLoadingError):
    """Raised when a forcing-provider acquisition returned a standardized failure record."""

    def __init__(self, record: dict[str, Any]) -> None:
        self.record = dict(record or {})
        source_id = str(self.record.get("source_id") or "unknown")
        message = str(self.record.get("error") or f"Forcing-provider acquisition failed for {source_id}.")
        super().__init__(message)


class PrepOutageDecisionRequired(ValidationPipelineError):
    """Raised when a required prep input hit an outage and a validated same-case cache is available."""

    def __init__(
        self,
        *,
        run_name: str,
        source_id: str,
        cache_path: str,
        validation: dict[str, Any],
        error: str,
    ) -> None:
        self.run_name = str(run_name)
        self.source_id = str(source_id)
        self.cache_path = str(cache_path)
        self.validation = dict(validation or {})
        self.error = str(error)
        super().__init__(
            f"Required prep input '{self.source_id}' hit a remote-service outage, "
            f"but a validated same-case cache is available at {self.cache_path}."
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "run_name": self.run_name,
            "source_id": self.source_id,
            "cache_path": self.cache_path,
            "validation": self.validation,
            "error": self.error,
        }


class ForcingOutagePhaseSkipped(ValidationPipelineError):
    """Raised when a phase is intentionally skipped after a forcing outage in degraded mode."""

    def __init__(
        self,
        *,
        phase: str,
        workflow_mode: str,
        forcing_outage_policy: str,
        reason: str,
        missing_forcing_factors: list[str] | None = None,
        skipped_recipe_ids: list[str] | None = None,
        skipped_branch_ids: list[str] | None = None,
        rerun_required: bool = True,
        manifest_path: str = "",
        budget_seconds: int | None = None,
        elapsed_seconds: float | None = None,
        budget_exhausted: bool = False,
        failure_stage: str = "",
    ) -> None:
        self.phase = str(phase)
        self.workflow_mode = str(workflow_mode)
        self.forcing_outage_policy = str(forcing_outage_policy)
        self.reason = str(reason)
        self.missing_forcing_factors = [str(value) for value in (missing_forcing_factors or []) if str(value).strip()]
        self.skipped_recipe_ids = [str(value) for value in (skipped_recipe_ids or []) if str(value).strip()]
        self.skipped_branch_ids = [str(value) for value in (skipped_branch_ids or []) if str(value).strip()]
        self.rerun_required = bool(rerun_required)
        self.manifest_path = str(manifest_path or "")
        self.budget_seconds = None if budget_seconds is None else int(budget_seconds)
        self.elapsed_seconds = None if elapsed_seconds is None else float(elapsed_seconds)
        self.budget_exhausted = bool(budget_exhausted)
        self.failure_stage = str(failure_stage or "")
        super().__init__(
            f"Phase '{self.phase}' skipped in degraded mode after forcing outage. "
            f"Set {FORCING_OUTAGE_POLICY_ENV}=fail_hard to disable degraded continuation."
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "workflow_mode": self.workflow_mode,
            "forcing_outage_policy": self.forcing_outage_policy,
            "reason": self.reason,
            "missing_forcing_factors": self.missing_forcing_factors,
            "skipped_recipe_ids": self.skipped_recipe_ids,
            "skipped_branch_ids": self.skipped_branch_ids,
            "rerun_required": self.rerun_required,
            "manifest_path": self.manifest_path,
            "budget_seconds": self.budget_seconds,
            "elapsed_seconds": self.elapsed_seconds,
            "budget_exhausted": self.budget_exhausted,
            "failure_stage": self.failure_stage,
        }
