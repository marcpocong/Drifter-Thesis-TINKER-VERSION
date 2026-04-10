<#
.SYNOPSIS
    One-command launcher for the oil-spill forecasting workflows.

.DESCRIPTION
    Run from the repository root with:

        .\start.ps1

    The recommended menu option runs the current Mindoro workflow from the
    beginning: prep, Phase 1/2 forecast generation, Phase 3B scoring, public
    observation tracks, diagnostics, PyGNOME comparison, and latest recipe
    sensitivity outputs.
#>

$Host.UI.RawUI.WindowTitle = "Drifter-Validated Oil Spill Forecasting"
$OutputEncoding = [Console]::OutputEncoding = [Console]::InputEncoding = [System.Text.Encoding]::UTF8

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "   $Text" -ForegroundColor White
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Ensure-Directories {
    foreach ($dir in @("data", "data_processed", "output", "logs")) {
        if (!(Test-Path $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }
    }
}

function Invoke-DockerPhase {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [Parameter(Mandatory = $true)][string]$Description,
        [string]$WorkflowMode = "mindoro_retro_2023",
        [string]$Service = "pipeline"
    )

    Write-Host ""
    Write-Host ">>> $Description" -ForegroundColor Yellow
    Write-Host "    WORKFLOW_MODE=$WorkflowMode PIPELINE_PHASE=$Phase SERVICE=$Service" -ForegroundColor DarkGray

    docker-compose exec -T `
        -e WORKFLOW_MODE="$WorkflowMode" `
        -e PIPELINE_PHASE="$Phase" `
        $Service python -m src *>&1 | Out-Host

    if ($LASTEXITCODE -ne 0) {
        throw "Phase '$Phase' failed in service '$Service' with exit code $LASTEXITCODE."
    }
}

function Invoke-PhaseList {
    param(
        [Parameter(Mandatory = $true)][array]$Steps,
        [string]$WorkflowMode = "mindoro_retro_2023"
    )

    Ensure-Directories
    $logFile = "logs\run_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    Start-Transcript -Path $logFile -Append | Out-Null
    $startTime = Get-Date

    try {
        Write-Host "Starting Docker containers..." -ForegroundColor Yellow
        docker-compose up -d
        if ($LASTEXITCODE -ne 0) {
            throw "docker-compose up failed with exit code $LASTEXITCODE."
        }

        $index = 0
        foreach ($step in $Steps) {
            $index += 1
            Write-Host ""
            Write-Host "[$index/$($Steps.Count)]" -ForegroundColor Cyan -NoNewline
            Invoke-DockerPhase `
                -Phase $step.Phase `
                -Description $step.Description `
                -WorkflowMode $WorkflowMode `
                -Service $step.Service
        }

        $endTime = Get-Date
        $duration = $endTime - $startTime
        Write-Host ""
        Write-Host "[SUCCESS] Workflow completed." -ForegroundColor Green
        Write-Host ("Runtime: {0:D2}h {1:D2}m {2:D2}s" -f $duration.Hours, $duration.Minutes, $duration.Seconds) -ForegroundColor Yellow
        Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
    }
    catch {
        Write-Host ""
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
    }
    finally {
        Stop-Transcript | Out-Null
    }

    Write-Host ""
    Pause
}

function Get-MindoroFullSteps {
    return @(
        @{ Phase = "prep"; Service = "pipeline"; Description = "Prep inputs: forcing, ArcGIS layers, canonical grid, shoreline masks" },
        @{ Phase = "1_2"; Service = "pipeline"; Description = "Phase 1/2 official baseline recipe, deterministic control, and ensemble forecast" },
        @{ Phase = "3b"; Service = "pipeline"; Description = "Strict March 6 Phase 3B stress-test scoring" },
        @{ Phase = "public_obs_appendix"; Service = "pipeline"; Description = "Public observation appendix inventory and accepted masks" },
        @{ Phase = "phase3b_multidate_public"; Service = "pipeline"; Description = "Main multi-date public-observation validation" },
        @{ Phase = "phase3b_extended_public"; Service = "pipeline"; Description = "Extended public-observation guardrail/inventory" },
        @{ Phase = "phase3b_extended_public_scored"; Service = "pipeline"; Description = "Short extended March 7-9 appendix scoring" },
        @{ Phase = "horizon_survival_audit"; Service = "pipeline"; Description = "Horizon survival audit" },
        @{ Phase = "transport_retention_fix"; Service = "pipeline"; Description = "Transport-retention sensitivity and R1 selection evidence" },
        @{ Phase = "official_rerun_r1"; Service = "pipeline"; Description = "Official R1 rerun/rescore pack" },
        @{ Phase = "init_mode_sensitivity_r1"; Service = "pipeline"; Description = "Initialization sensitivity: B polygon vs A1 source point" },
        @{ Phase = "source_history_reconstruction_r1"; Service = "pipeline"; Description = "A2 source-history reconstruction sensitivity" },
        @{ Phase = "pygnome_public_comparison"; Service = "gnome"; Description = "PyGNOME/OpenDrift public-observation comparison" },
        @{ Phase = "ensemble_threshold_sensitivity"; Service = "pipeline"; Description = "Ensemble threshold calibration/sensitivity" },
        @{ Phase = "recipe_sensitivity_r1_multibranch"; Service = "pipeline"; Description = "R1 recipe/branch matrix vs fixed PyGNOME comparator" }
    )
}

function Get-MindoroCoreSteps {
    return @(
        @{ Phase = "prep"; Service = "pipeline"; Description = "Prep inputs" },
        @{ Phase = "1_2"; Service = "pipeline"; Description = "Phase 1/2 official baseline forecast generation" },
        @{ Phase = "3b"; Service = "pipeline"; Description = "Strict March 6 Phase 3B scoring" },
        @{ Phase = "phase3b_multidate_public"; Service = "pipeline"; Description = "Main multi-date public-observation validation" }
    )
}

function Get-PrototypeSteps {
    return @(
        @{ Phase = "prep"; Service = "pipeline"; Description = "Prep prototype inputs" },
        @{ Phase = "1_2"; Service = "pipeline"; Description = "Prototype Phase 1 transport validation and Phase 2 ensemble" },
        @{ Phase = "benchmark"; Service = "gnome"; Description = "Prototype/legacy Phase 3A benchmark" },
        @{ Phase = "3"; Service = "gnome"; Description = "Prototype/legacy Phase 3 weathering and PyGNOME comparison" },
        @{ Phase = "3b"; Service = "pipeline"; Description = "Prototype/legacy Phase 3B scoring" }
    )
}

function Show-Help {
    Clear-Host
    Write-Section "HELP"
    Write-Host ""
    Write-Host "Recommended command:" -ForegroundColor Yellow
    Write-Host "  .\start.ps1" -ForegroundColor Green
    Write-Host ""
    Write-Host "Recommended menu option:" -ForegroundColor Yellow
    Write-Host "  2. Run Mindoro FULL workflow from Phase 1" -ForegroundColor Green
    Write-Host ""
    Write-Host "What option 2 runs:" -ForegroundColor Yellow
    Write-Host "  prep -> 1_2 -> 3b -> public_obs_appendix -> phase3b_multidate_public"
    Write-Host "  -> phase3b_extended_public -> phase3b_extended_public_scored"
    Write-Host "  -> horizon_survival_audit -> transport_retention_fix -> official_rerun_r1"
    Write-Host "  -> init_mode_sensitivity_r1 -> source_history_reconstruction_r1"
    Write-Host "  -> pygnome_public_comparison -> ensemble_threshold_sensitivity"
    Write-Host "  -> recipe_sensitivity_r1_multibranch"
    Write-Host ""
    Write-Host "Outputs are written under:" -ForegroundColor Yellow
    Write-Host "  output\CASE_MINDORO_RETRO_2023\" -ForegroundColor Blue
    Write-Host ""
    Write-Host "Notes:" -ForegroundColor Yellow
    Write-Host "  - PyGNOME comparison runs in the 'gnome' container."
    Write-Host "  - The strict March 6 target remains unchanged."
    Write-Host "  - The legacy prototype_2016 workflow is still available from the menu."
    Write-Host ""
    Pause
}

function Show-Menu {
    while ($true) {
        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host ""
        Write-Host "  1. " -ForegroundColor Yellow -NoNewline; Write-Host "Help"
        Write-Host "  2. " -ForegroundColor Yellow -NoNewline; Write-Host "Run Mindoro FULL workflow from Phase 1 (recommended)"
        Write-Host "  3. " -ForegroundColor Yellow -NoNewline; Write-Host "Run Mindoro CORE workflow only"
        Write-Host "  4. " -ForegroundColor Yellow -NoNewline; Write-Host "Run legacy prototype_2016 workflow"
        Write-Host "  5. " -ForegroundColor Yellow -NoNewline; Write-Host "Exit"
        Write-Host ""
        $choice = Read-Host "Select an option (1-5, Enter = 2)"
        if ([string]::IsNullOrWhiteSpace($choice)) {
            $choice = "2"
        }

        switch ($choice) {
            "1" { Show-Help }
            "2" {
                Write-Section "MINDORO FULL WORKFLOW"
                Invoke-PhaseList -WorkflowMode "mindoro_retro_2023" -Steps (Get-MindoroFullSteps)
            }
            "3" {
                Write-Section "MINDORO CORE WORKFLOW"
                Invoke-PhaseList -WorkflowMode "mindoro_retro_2023" -Steps (Get-MindoroCoreSteps)
            }
            "4" {
                Write-Section "LEGACY PROTOTYPE WORKFLOW"
                Invoke-PhaseList -WorkflowMode "prototype_2016" -Steps (Get-PrototypeSteps)
            }
            "5" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
            default {
                Write-Host "Invalid option. Please choose 1, 2, 3, 4, or 5." -ForegroundColor Red
                Start-Sleep -Seconds 2
            }
        }
    }
}

Show-Menu
