<#
.SYNOPSIS
    Honest launcher for the current thesis workflow state.

.DESCRIPTION
    This launcher reads config/launcher_matrix.json and separates:
    - scientific / reportable rerun tracks
    - sensitivity / appendix tracks
    - read-only packaging / help utilities
    - legacy prototype tracks

    Safe first commands:
        .\start.ps1 -List -NoPause
        .\start.ps1 -Help -NoPause
        .\start.ps1 -Entry phase5_sync -NoPause
        .\start.ps1 -Entry trajectory_gallery -NoPause
        .\start.ps1 -Entry trajectory_gallery_panel -NoPause
        .\start.ps1 -Entry figure_package_publication -NoPause
#>

param(
    [switch]$List,
    [switch]$Help,
    [string]$Entry,
    [switch]$NoPause
)

$Host.UI.RawUI.WindowTitle = "Drifter-Validated Oil Spill Forecasting"
$OutputEncoding = [Console]::OutputEncoding = [Console]::InputEncoding = [System.Text.Encoding]::UTF8
$Script:RepoRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
$Script:LauncherMatrixPath = Join-Path $Script:RepoRoot "config\launcher_matrix.json"
$Script:LauncherMatrix = $null

Set-Location $Script:RepoRoot

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "   $Text" -ForegroundColor White
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Pause-IfNeeded {
    if (-not $NoPause) {
        Write-Host ""
        Pause
    }
}

function Ensure-Directories {
    foreach ($dir in @("data", "data_processed", "output", "logs")) {
        if (-not (Test-Path $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }
    }
}

function Write-ProcessLine {
    param([object]$Line)

    if ($Line -is [System.Management.Automation.ErrorRecord]) {
        $message = $Line.Exception.Message
    } else {
        $message = [string]$Line
    }

    if ($message -match '^\s*WARNING\b|^\s*WARN\b') {
        Write-Host $message -ForegroundColor DarkYellow
    } elseif ($message -match '^\s*ERROR\b|Traceback \(most recent call last\)') {
        Write-Host $message -ForegroundColor Red
    } else {
        Write-Host $message
    }
}

function Get-LauncherMatrix {
    if ($null -ne $Script:LauncherMatrix) {
        return $Script:LauncherMatrix
    }

    if (-not (Test-Path $Script:LauncherMatrixPath)) {
        throw "Missing launcher matrix: $Script:LauncherMatrixPath"
    }

    $payload = Get-Content $Script:LauncherMatrixPath -Raw | ConvertFrom-Json
    if (-not $payload.categories -or -not $payload.entries) {
        throw "Launcher matrix is missing categories or entries: $Script:LauncherMatrixPath"
    }

    $Script:LauncherMatrix = $payload
    return $Script:LauncherMatrix
}

function Get-LauncherEntries {
    $matrix = Get-LauncherMatrix
    return @($matrix.entries | Sort-Object menu_order, entry_id)
}

function Get-LauncherCategories {
    $matrix = Get-LauncherMatrix
    return @($matrix.categories)
}

function Get-LauncherEntryById {
    param([Parameter(Mandatory = $true)][string]$EntryId)

    $match = Get-LauncherEntries | Where-Object { $_.entry_id -eq $EntryId } | Select-Object -First 1
    if ($null -eq $match) {
        $known = (Get-LauncherEntries | ForEach-Object { $_.entry_id }) -join ", "
        throw "Unknown launcher entry '$EntryId'. Known entry IDs: $known"
    }
    return $match
}

function Invoke-DockerPhase {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][string]$WorkflowMode,
        [Parameter(Mandatory = $true)][string]$Service
    )

    Write-Host ""
    Write-Host ">>> $Description" -ForegroundColor Yellow
    Write-Host "    WORKFLOW_MODE=$WorkflowMode PIPELINE_PHASE=$Phase SERVICE=$Service" -ForegroundColor DarkGray

    docker-compose exec -T `
        -e WORKFLOW_MODE="$WorkflowMode" `
        -e PIPELINE_PHASE="$Phase" `
        $Service python -m src 2>&1 | ForEach-Object { Write-ProcessLine $_ }

    $phaseExitCode = $LASTEXITCODE
    if ($phaseExitCode -ne 0) {
        throw "Phase '$Phase' failed in service '$Service' with exit code $phaseExitCode."
    }
}

function Invoke-LauncherEntry {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    Ensure-Directories
    $entryId = [string]$LauncherEntry.entry_id
    $workflowMode = [string]$LauncherEntry.workflow_mode
    $logFile = "logs\run_${entryId}_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    $startTime = Get-Date

    Start-Transcript -Path $logFile -Append | Out-Null
    try {
        Write-Host "Starting Docker containers..." -ForegroundColor Yellow
        docker-compose up -d 2>&1 | ForEach-Object { Write-ProcessLine $_ }
        $composeExitCode = $LASTEXITCODE
        if ($composeExitCode -ne 0) {
            throw "docker-compose up failed with exit code $composeExitCode."
        }

        $steps = @($LauncherEntry.steps)
        $index = 0
        foreach ($step in $steps) {
            $index += 1
            Write-Host ""
            Write-Host "[$index/$($steps.Count)]" -ForegroundColor Cyan -NoNewline
            Invoke-DockerPhase `
                -Phase ([string]$step.phase) `
                -Description ([string]$step.description) `
                -WorkflowMode $workflowMode `
                -Service ([string]$step.service)
        }

        $duration = (Get-Date) - $startTime
        Write-Host ""
        Write-Host "[SUCCESS] Launcher entry completed." -ForegroundColor Green
        Write-Host "Entry ID: $entryId" -ForegroundColor Yellow
        Write-Host ("Runtime: {0:D2}h {1:D2}m {2:D2}s" -f $duration.Hours, $duration.Minutes, $duration.Seconds) -ForegroundColor Yellow
        Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
    }
    catch {
        Write-Host ""
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
        throw
    }
    finally {
        Stop-Transcript | Out-Null
    }
}

function Show-LauncherList {
    $matrix = Get-LauncherMatrix
    $entries = Get-LauncherEntries
    Clear-Host
    Write-Section "CURRENT LAUNCHER CATALOG"
    Write-Host ""
    Write-Host "Entrypoint: .\start.ps1" -ForegroundColor Green
    Write-Host "Catalog: $($matrix.catalog_version)" -ForegroundColor Yellow
    Write-Host "Safe first commands: .\start.ps1 -Entry phase5_sync -NoPause ; .\start.ps1 -Entry trajectory_gallery -NoPause ; .\start.ps1 -Entry trajectory_gallery_panel -NoPause ; .\start.ps1 -Entry figure_package_publication -NoPause" -ForegroundColor Yellow
    Write-Host ""

    foreach ($category in Get-LauncherCategories) {
        Write-Host "$($category.label)" -ForegroundColor Cyan
        Write-Host "  $($category.description)" -ForegroundColor DarkGray
        foreach ($launcherEntry in $entries | Where-Object { $_.category_id -eq $category.category_id }) {
            $safeTag = if ($launcherEntry.safe_default) { "safe" } else { "manual" }
            Write-Host ("  - {0} [{1}; cost={2}; mode={3}]" -f $launcherEntry.entry_id, $safeTag, $launcherEntry.rerun_cost, $launcherEntry.workflow_mode) -ForegroundColor White
            Write-Host "    $($launcherEntry.label)" -ForegroundColor Yellow
            Write-Host "    $($launcherEntry.description)" -ForegroundColor Gray
            Write-Host ("    phases: {0}" -f (($launcherEntry.steps | ForEach-Object { $_.phase }) -join ", ")) -ForegroundColor DarkGray
            if ($launcherEntry.notes) {
                Write-Host "    note: $($launcherEntry.notes)" -ForegroundColor DarkYellow
            }
        }
        Write-Host ""
    }

    if ($matrix.optional_future_work) {
        Write-Host "Optional future work not implemented in the launcher:" -ForegroundColor Cyan
        foreach ($item in $matrix.optional_future_work) {
            Write-Host ("  - {0} [{1}]" -f $item.label, $item.status) -ForegroundColor DarkGray
        }
    }

    Pause-IfNeeded
}

function Show-Help {
    $matrix = Get-LauncherMatrix
    $safeEntries = @($matrix.entries | Where-Object { $_.safe_default } | Sort-Object menu_order)
    Clear-Host
    Write-Section "LAUNCHER HELP"
    Write-Host ""
    Write-Host "Recommended read-only commands:" -ForegroundColor Yellow
    Write-Host "  .\start.ps1 -List -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Help -NoPause" -ForegroundColor Green
    foreach ($entry in $safeEntries) {
        Write-Host ("  .\start.ps1 -Entry {0} -NoPause" -f $entry.entry_id) -ForegroundColor Green
    }
    Write-Host ""
    Write-Host "Scientific rerun commands require explicit intent:" -ForegroundColor Yellow
    Write-Host "  .\start.ps1 -Entry mindoro_reportable_core -NoPause" -ForegroundColor Gray
    Write-Host "  .\start.ps1 -Entry dwh_reportable_bundle -NoPause" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Current status guardrails:" -ForegroundColor Yellow
    Write-Host "  - Phase 1 is architecture-audited, but the full 2016-2022 production rerun is still needed." -ForegroundColor White
    Write-Host "  - Phase 2 is scientifically usable, but not scientifically frozen." -ForegroundColor White
    Write-Host "  - Phase 4 is scientifically reportable now for Mindoro, but inherited-provisional from the upstream Phase 1/2 state." -ForegroundColor White
    Write-Host "  - Prototype mode remains available for debugging and regression only." -ForegroundColor White
    Write-Host ""
    Write-Host "Not implemented yet:" -ForegroundColor Yellow
    foreach ($item in $matrix.optional_future_work) {
        Write-Host ("  - {0} [{1}]" -f $item.label, $item.status) -ForegroundColor DarkGray
    }

    Pause-IfNeeded
}

function Show-Menu {
    while ($true) {
        $entries = Get-LauncherEntries
        $selectionMap = @{}
        $displayIndex = 1

        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host ""
        Write-Host "Choose a launcher entry. Read-only utilities are the safest first choice." -ForegroundColor Yellow
        Write-Host ""

        foreach ($category in Get-LauncherCategories) {
            Write-Host "$($category.label)" -ForegroundColor Cyan
            foreach ($launcherEntry in $entries | Where-Object { $_.category_id -eq $category.category_id }) {
                $selectionMap[[string]$displayIndex] = $launcherEntry.entry_id
                $safeTag = if ($launcherEntry.safe_default) { "safe" } else { "manual" }
                Write-Host ("  {0}. {1} [{2}; cost={3}]" -f $displayIndex, $launcherEntry.label, $safeTag, $launcherEntry.rerun_cost) -ForegroundColor White
                Write-Host ("     id={0} | mode={1}" -f $launcherEntry.entry_id, $launcherEntry.workflow_mode) -ForegroundColor DarkGray
                $displayIndex += 1
            }
            Write-Host ""
        }

        Write-Host "  L. List catalog only" -ForegroundColor Yellow
        Write-Host "  H. Help" -ForegroundColor Yellow
        Write-Host "  Q. Exit" -ForegroundColor Yellow
        Write-Host ""

        $choice = (Read-Host "Select an option").Trim()
        if ([string]::IsNullOrWhiteSpace($choice)) {
            continue
        }

        switch ($choice.ToUpperInvariant()) {
            "L" {
                Show-LauncherList
                continue
            }
            "H" {
                Show-Help
                continue
            }
            "Q" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
        }

        if ($selectionMap.ContainsKey($choice)) {
            $launcherEntry = Get-LauncherEntryById -EntryId $selectionMap[$choice]
            Write-Section $launcherEntry.label
            try {
                Invoke-LauncherEntry -LauncherEntry $launcherEntry
            }
            catch {
                Pause-IfNeeded
            }
            Pause-IfNeeded
            continue
        }

        Write-Host "Invalid option. Use a menu number, L, H, or Q." -ForegroundColor Red
        Start-Sleep -Seconds 2
    }
}

try {
    if ($List) {
        Show-LauncherList
        exit 0
    }

    if ($Help) {
        Show-Help
        exit 0
    }

    if ($Entry) {
        $launcherEntry = Get-LauncherEntryById -EntryId $Entry
        Write-Section $launcherEntry.label
        Invoke-LauncherEntry -LauncherEntry $launcherEntry
        Pause-IfNeeded
        exit 0
    }

    Show-Menu
}
catch {
    Write-Host ""
    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    Pause-IfNeeded
    exit 1
}
