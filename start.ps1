<#
.SYNOPSIS
    Honest launcher for the current thesis workflow state.

.DESCRIPTION
    This launcher reads config/launcher_matrix.json and separates:
    - scientific / reportable rerun tracks
    - sensitivity / appendix tracks
    - read-only packaging / help utilities
    - legacy prototype tracks

    Canonical paths:
        .\start.ps1 -List -NoPause
        .\start.ps1 -Help -NoPause
        .\start.ps1 -Entry <entry_id>
        docker-compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
#>

param(
    [switch]$List,
    [switch]$Help,
    [string]$Entry,
    [switch]$Panel,
    [switch]$NoPause
)

$Host.UI.RawUI.WindowTitle = "Drifter-Validated Oil Spill Forecasting"
$OutputEncoding = [Console]::OutputEncoding = [Console]::InputEncoding = [System.Text.Encoding]::UTF8
$Script:RepoRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
$Script:LauncherMatrixPath = Join-Path $Script:RepoRoot "config\launcher_matrix.json"
$Script:LauncherMatrix = $null
$Script:PrepOutageExitCode = 86
$Script:PrepOutagePayloadPrefix = "PREP_OUTAGE_PAYLOAD="
$Script:ForcingOutageSkipExitCode = 87
$Script:ForcingOutageSkipPayloadPrefix = "FORCING_OUTAGE_SKIP_PAYLOAD="
$Script:StartupPromptProbePrefix = "STARTUP_PROMPT_PROBE="

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

function Invoke-DockerPhaseCommand {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][string]$WorkflowMode,
        [Parameter(Mandatory = $true)][string]$Service,
        [hashtable]$ExtraEnv = @{}
    )

    Write-Host ""
    Write-Host ">>> $Description" -ForegroundColor Yellow
    Write-Host "    WORKFLOW_MODE=$WorkflowMode PIPELINE_PHASE=$Phase SERVICE=$Service" -ForegroundColor DarkGray

    $dockerArgs = @("exec", "-T")
    foreach ($key in ($ExtraEnv.Keys | Sort-Object)) {
        $dockerArgs += @("-e", "$key=$($ExtraEnv[$key])")
    }
    $dockerArgs += @(
        "-e", "WORKFLOW_MODE=$WorkflowMode",
        "-e", "PIPELINE_PHASE=$Phase",
        $Service,
        "python",
        "-m",
        "src"
    )

    $prepPayloadLine = $null
    $forcingSkipPayloadLine = $null
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Docker writes routine status lines to stderr, and PowerShell 5.1 will surface those as
        # ErrorRecord objects when ErrorActionPreference=Stop. Downgrade just this native call so
        # benign container/status chatter does not abort the launcher.
        $ErrorActionPreference = "Continue"
        & docker-compose @dockerArgs 2>&1 | ForEach-Object {
            $message = if ($_ -is [System.Management.Automation.ErrorRecord]) {
                $_.Exception.Message
            } else {
                [string]$_
            }
            if ($message.StartsWith($Script:PrepOutagePayloadPrefix)) {
                $prepPayloadLine = $message
            }
            if ($message.StartsWith($Script:ForcingOutageSkipPayloadPrefix)) {
                $forcingSkipPayloadLine = $message
            }
            Write-ProcessLine $message
        }
        $phaseExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    return @{
        ExitCode = $phaseExitCode
        PayloadLine = $prepPayloadLine
        ForcingSkipPayloadLine = $forcingSkipPayloadLine
    }
}

function Update-PrepManifestCancelledByUser {
    param(
        [Parameter(Mandatory = $true)][string]$RunName,
        [Parameter(Mandatory = $true)][string]$SourceId,
        [string]$CachePath,
        [string]$RemoteError,
        $Validation
    )

    $manifestPath = Join-Path $Script:RepoRoot "data\download_manifest.json"
    if (-not (Test-Path $manifestPath)) {
        return
    }

    try {
        $payload = ConvertFrom-JsonCompat -Json (Get-Content $manifestPath -Raw)
        if (-not $payload.ContainsKey($RunName)) {
            return
        }
        if (-not $payload[$RunName].ContainsKey("downloads")) {
            $payload[$RunName]["downloads"] = @{}
        }
        $payload[$RunName]["downloads"][$SourceId] = @{
            status = "cancelled_by_user"
            source_id = $SourceId
            path = $CachePath
            remote_error = $RemoteError
            validation = $Validation
        }
        $payload | ConvertTo-Json -Depth 20 | Set-Content -Path $manifestPath -Encoding UTF8
    }
    catch {
        Write-Host "WARNING - Failed to update download manifest after user cancellation: $($_.Exception.Message)" -ForegroundColor DarkYellow
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

function ConvertTo-Hashtable {
    param([object]$InputObject)

    $result = @{}
    if ($null -eq $InputObject) {
        return $result
    }

    if ($InputObject -is [System.Collections.IDictionary]) {
        foreach ($key in $InputObject.Keys) {
            $result[[string]$key] = [string]$InputObject[$key]
        }
        return $result
    }

    foreach ($property in $InputObject.PSObject.Properties) {
        $result[[string]$property.Name] = [string]$property.Value
    }
    return $result
}

function ConvertTo-NativeJsonData {
    param([object]$InputObject)

    if ($null -eq $InputObject) {
        return $null
    }

    if ($InputObject -is [System.Collections.IDictionary]) {
        $result = @{}
        foreach ($key in $InputObject.Keys) {
            $result[[string]$key] = ConvertTo-NativeJsonData -InputObject $InputObject[$key]
        }
        return $result
    }

    if ($InputObject -is [System.Array] -or $InputObject -is [System.Collections.IList]) {
        $items = @()
        foreach ($item in $InputObject) {
            $items += ,(ConvertTo-NativeJsonData -InputObject $item)
        }
        return ,$items
    }

    if (
        $InputObject -isnot [string] -and
        $InputObject -isnot [char] -and
        $InputObject.PSObject -and
        $InputObject.PSObject.Properties.Count -gt 0
    ) {
        $result = @{}
        foreach ($property in $InputObject.PSObject.Properties) {
            $result[[string]$property.Name] = ConvertTo-NativeJsonData -InputObject $property.Value
        }
        return $result
    }

    return $InputObject
}

function ConvertFrom-JsonCompat {
    param([Parameter(Mandatory = $true)][string]$Json)

    $parsed = $Json | ConvertFrom-Json
    return ConvertTo-NativeJsonData -InputObject $parsed
}

function Merge-Hashtables {
    param(
        [hashtable]$Base = @{},
        [hashtable]$Override = @{}
    )

    $merged = @{}
    foreach ($key in $Base.Keys) {
        $merged[$key] = $Base[$key]
    }
    foreach ($key in $Override.Keys) {
        $merged[$key] = $Override[$key]
    }
    return $merged
}

function Test-LauncherStartupPromptInteractive {
    if ($NoPause) {
        return $false
    }

    try {
        return [Environment]::UserInteractive -and -not [Console]::IsInputRedirected -and -not [Console]::IsOutputRedirected
    }
    catch {
        return $false
    }
}

function Get-ExplicitForcingBudgetSetting {
    $raw = [string]$env:FORCING_SOURCE_BUDGET_SECONDS
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }

    $parsed = 0
    if (-not [int]::TryParse($raw.Trim(), [ref]$parsed) -or $parsed -lt 0) {
        throw "FORCING_SOURCE_BUDGET_SECONDS must be a non-negative integer number of seconds."
    }
    return [string]$parsed
}

function Get-ExplicitInputCachePolicySetting {
    $raw = [string]$env:INPUT_CACHE_POLICY
    $normalized = $raw.Trim().ToLowerInvariant()
    switch ($normalized) {
        "" { break }
        "default" { break }
        "reuse_if_valid" { return "reuse_if_valid" }
        "force_refresh" { return "force_refresh" }
        default {
            throw "INPUT_CACHE_POLICY must be one of: default, reuse_if_valid, force_refresh."
        }
    }

    $prepForceRefresh = [string]$env:PREP_FORCE_REFRESH
    if ($prepForceRefresh.Trim().ToLowerInvariant() -in @("1", "true", "yes", "y", "on")) {
        return "force_refresh"
    }
    return $null
}

function Get-ExplicitPrototype2016EnsemblePolicySetting {
    $raw = [string]$env:PROTOTYPE_2016_ENSEMBLE_POLICY
    $normalized = $raw.Trim().ToLowerInvariant()
    switch ($normalized) {
        "" { return $null }
        "full_rerun" { return "full_rerun" }
        "reuse_if_valid" { return "reuse_if_valid" }
        default {
            throw "PROTOTYPE_2016_ENSEMBLE_POLICY must be one of: full_rerun, reuse_if_valid."
        }
    }
}

function Read-StartupWaitBudgetChoice {
    Write-Host ""
    Write-Host "Choose one forcing wait budget for this run:" -ForegroundColor Yellow
    Write-Host "  1. 120 seconds" -ForegroundColor White
    Write-Host "  2. 300 seconds (Recommended)" -ForegroundColor White
    Write-Host "  3. 600 seconds" -ForegroundColor White
    Write-Host "  4. 0 seconds (no hard cap)" -ForegroundColor White

    while ($true) {
        $choice = (Read-Host "Select wait budget [2]").Trim()
        switch ($choice) {
            "" { return "300" }
            "1" { return "120" }
            "2" { return "300" }
            "3" { return "600" }
            "4" { return "0" }
            "120" { return "120" }
            "300" { return "300" }
            "600" { return "600" }
            "0" { return "0" }
            default {
                Write-Host "Enter 1, 2, 3, or 4." -ForegroundColor DarkYellow
            }
        }
    }
}

function Read-StartupInputCacheChoice {
    Write-Host ""
    Write-Host "Eligible validated files already exist in the persistent local input store for this run." -ForegroundColor Yellow
    Write-Host "  1. Reuse validated local inputs when available (Recommended)" -ForegroundColor White
    Write-Host "  2. Force refresh remote inputs and rewrite the persistent local store" -ForegroundColor White

    while ($true) {
        $choice = (Read-Host "Select input cache policy [1]").Trim().ToLowerInvariant()
        switch ($choice) {
            "" { return "reuse_if_valid" }
            "1" { return "reuse_if_valid" }
            "reuse" { return "reuse_if_valid" }
            "reuse_if_valid" { return "reuse_if_valid" }
            "2" { return "force_refresh" }
            "refresh" { return "force_refresh" }
            "force_refresh" { return "force_refresh" }
            default {
                Write-Host "Enter 1 or 2." -ForegroundColor DarkYellow
            }
        }
    }
}

function Read-StartupPrototype2016EnsembleChoice {
    Write-Host ""
    Write-Host "Prototype 2016 legacy bundle:" -ForegroundColor Yellow
    Write-Host "  1. Reuse valid ensemble outputs and refresh figures (Recommended for figure/layout-only updates)" -ForegroundColor White
    Write-Host "  2. Rerun full legacy bundle" -ForegroundColor White

    while ($true) {
        $choice = (Read-Host "Select 2016 ensemble policy [1]").Trim().ToLowerInvariant()
        switch ($choice) {
            "" { return "reuse_if_valid" }
            "1" { return "reuse_if_valid" }
            "reuse" { return "reuse_if_valid" }
            "reuse_if_valid" { return "reuse_if_valid" }
            "2" { return "full_rerun" }
            "rerun" { return "full_rerun" }
            "full_rerun" { return "full_rerun" }
            default {
                Write-Host "Enter 1 or 2." -ForegroundColor DarkYellow
            }
        }
    }
}

function Invoke-StartupPromptProbe {
    param([Parameter(Mandatory = $true)][string]$EntryId)

    $probeLine = $null
    $dockerArgs = @(
        "exec", "-T",
        "pipeline",
        "python",
        "-m",
        "src.utils.startup_prompt_policy",
        "--probe-launcher-entry",
        $EntryId
    )

    & docker-compose @dockerArgs 2>&1 | ForEach-Object {
        $message = if ($_ -is [System.Management.Automation.ErrorRecord]) {
            $_.Exception.Message
        } else {
            [string]$_
        }
        if ($message.StartsWith($Script:StartupPromptProbePrefix)) {
            $probeLine = $message
        } else {
            Write-ProcessLine $message
        }
    }

    if ($LASTEXITCODE -ne 0) {
        throw "Startup prompt probe failed for launcher entry '$EntryId' with exit code $LASTEXITCODE."
    }
    if (-not $probeLine) {
        throw "Startup prompt probe for launcher entry '$EntryId' did not return a machine-readable payload."
    }

    $payloadJson = $probeLine.Substring($Script:StartupPromptProbePrefix.Length)
    try {
        return ConvertFrom-JsonCompat -Json $payloadJson
    }
    catch {
        throw "Startup prompt probe for launcher entry '$EntryId' returned unreadable JSON: $($_.Exception.Message)"
    }
}

function Resolve-LauncherStartupEnv {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    $resolved = @{
        "RUN_STARTUP_PROMPTS_RESOLVED" = "1"
        "RUN_STARTUP_TOKEN" = "startup_$([guid]::NewGuid().ToString('N'))"
    }

    $explicitBudget = Get-ExplicitForcingBudgetSetting
    $explicitCachePolicy = Get-ExplicitInputCachePolicySetting
    $explicitPrototype2016EnsemblePolicy = Get-ExplicitPrototype2016EnsemblePolicySetting
    $interactivePromptAllowed = Test-LauncherStartupPromptInteractive
    $readOnlyCategory = ([string]$LauncherEntry.category_id -eq "read_only_packaging_help_utilities")
    $probe = $null

    if ($interactivePromptAllowed -and -not $readOnlyCategory -and ($null -eq $explicitBudget -or $null -eq $explicitCachePolicy)) {
        $probe = Invoke-StartupPromptProbe -EntryId ([string]$LauncherEntry.entry_id)
    }

    if ($null -ne $explicitBudget) {
        $resolved["FORCING_SOURCE_BUDGET_SECONDS"] = $explicitBudget
    } elseif ($interactivePromptAllowed -and $probe -and [bool]$probe.should_prompt_wait_budget) {
        $resolved["FORCING_SOURCE_BUDGET_SECONDS"] = Read-StartupWaitBudgetChoice
    } else {
        $resolved["FORCING_SOURCE_BUDGET_SECONDS"] = "300"
    }

    if ($null -ne $explicitCachePolicy) {
        $resolved["INPUT_CACHE_POLICY"] = $explicitCachePolicy
    } elseif ($interactivePromptAllowed -and $probe -and [bool]$probe.has_eligible_input_cache) {
        $resolved["INPUT_CACHE_POLICY"] = Read-StartupInputCacheChoice
    } else {
        $resolved["INPUT_CACHE_POLICY"] = "reuse_if_valid"
    }

    if ($null -ne $explicitPrototype2016EnsemblePolicy) {
        $resolved["PROTOTYPE_2016_ENSEMBLE_POLICY"] = $explicitPrototype2016EnsemblePolicy
    } elseif ($interactivePromptAllowed -and $probe -and [bool]$probe.should_prompt_prototype_2016_ensemble_policy) {
        $resolved["PROTOTYPE_2016_ENSEMBLE_POLICY"] = Read-StartupPrototype2016EnsembleChoice
    } else {
        $resolved["PROTOTYPE_2016_ENSEMBLE_POLICY"] = "full_rerun"
    }

    return $resolved
}

function Invoke-DockerPhase {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][string]$WorkflowMode,
        [Parameter(Mandatory = $true)][string]$Service,
        [hashtable]$ExtraEnv = @{},
        [switch]$ReuseDecisionConsumed
    )

    $phaseEnv = @{}
    foreach ($key in $ExtraEnv.Keys) {
        $phaseEnv[$key] = $ExtraEnv[$key]
    }
    if ($Phase -eq "prep" -and $Service -eq "pipeline" -and -not $phaseEnv.ContainsKey("PREP_OUTAGE_PROMPT_SUPPORTED")) {
        $phaseEnv["PREP_OUTAGE_PROMPT_SUPPORTED"] = "1"
    }

    $phaseResult = Invoke-DockerPhaseCommand `
        -Phase $Phase `
        -Description $Description `
        -WorkflowMode $WorkflowMode `
        -Service $Service `
        -ExtraEnv $phaseEnv

    if ($phaseResult.ExitCode -eq 0) {
        return
    }

    if ($phaseResult.ExitCode -eq $Script:ForcingOutageSkipExitCode) {
        if (-not $phaseResult.ForcingSkipPayloadLine) {
            throw "Phase '$Phase' reported a degraded forcing skip, but no machine-readable payload was returned."
        }

        $payloadJson = $phaseResult.ForcingSkipPayloadLine.Substring($Script:ForcingOutageSkipPayloadPrefix.Length)
        try {
            $payload = ConvertFrom-JsonCompat -Json $payloadJson
        }
        catch {
            throw "Phase '$Phase' returned an unreadable degraded-skip payload: $($_.Exception.Message)"
        }

        Write-Host ""
        Write-Host "Phase '$Phase' was skipped after a temporary forcing-provider outage." -ForegroundColor Yellow
        if ($payload.reason) {
            Write-Host "Reason: $($payload.reason)" -ForegroundColor DarkGray
        }
        if ($payload.missing_forcing_factors) {
            Write-Host "Missing forcing factors: $($payload.missing_forcing_factors -join ', ')" -ForegroundColor DarkGray
        }
        if ($payload.skipped_recipe_ids) {
            Write-Host "Skipped recipes: $($payload.skipped_recipe_ids -join ', ')" -ForegroundColor DarkGray
        }
        if ($payload.skipped_branch_ids) {
            Write-Host "Skipped branches: $($payload.skipped_branch_ids -join ', ')" -ForegroundColor DarkGray
        }
        if ($null -ne $payload.budget_seconds) {
            Write-Host "Budget: $($payload.budget_seconds) seconds" -ForegroundColor DarkGray
        }
        if ($null -ne $payload.elapsed_seconds) {
            Write-Host "Elapsed: $($payload.elapsed_seconds) seconds" -ForegroundColor DarkGray
        }
        if ($payload.failure_stage) {
            Write-Host "Failure stage: $($payload.failure_stage)" -ForegroundColor DarkGray
        }
        if ($null -ne $payload.budget_exhausted) {
            Write-Host "Budget exhausted: $($payload.budget_exhausted)" -ForegroundColor DarkGray
        }
        if ($payload.manifest_path) {
            Write-Host "Manifest: $($payload.manifest_path)" -ForegroundColor DarkGray
        }
        return
    }

    if ($Phase -eq "prep" -and $Service -eq "pipeline" -and $phaseResult.ExitCode -eq $Script:PrepOutageExitCode) {
        if ($ReuseDecisionConsumed) {
            throw "Prep hit another outage after the single allowed cache-reuse decision for this phase. Rerun the launcher after the upstream service recovers."
        }
        if (-not $phaseResult.PayloadLine) {
            throw "Prep signaled a cache-reuse decision, but no machine-readable outage payload was returned."
        }

        $payloadJson = $phaseResult.PayloadLine.Substring($Script:PrepOutagePayloadPrefix.Length)
        try {
            $payload = ConvertFrom-JsonCompat -Json $payloadJson
        }
        catch {
            throw "Prep returned an unreadable outage payload: $($_.Exception.Message)"
        }

        Write-Host ""
        Write-Host "Prep paused because required source '$($payload.source_id)' is temporarily unavailable." -ForegroundColor Yellow
        Write-Host "Validated same-case cache: $($payload.cache_path)" -ForegroundColor DarkGray
        $validationSummary = if ($payload.validation.summary) { $payload.validation.summary } else { $payload.validation.reason }
        if ($validationSummary) {
            Write-Host "Cache validation: $validationSummary" -ForegroundColor DarkGray
        }
        if ($payload.error) {
            Write-Host "Remote error: $($payload.error)" -ForegroundColor DarkGray
        }

        while ($true) {
            $decision = (Read-Host "Type R to reuse validated cache, or C to cancel").Trim().ToUpperInvariant()
            if ($decision -eq "R") {
                $retryEnv = @{}
                foreach ($key in $phaseEnv.Keys) {
                    $retryEnv[$key] = $phaseEnv[$key]
                }
                $retryEnv["PREP_REUSE_APPROVED_SOURCE"] = [string]$payload.source_id
                $retryEnv["PREP_REUSE_APPROVED_ONCE"] = "1"
                Invoke-DockerPhase `
                    -Phase $Phase `
                    -Description "$Description (reuse approved for $($payload.source_id))" `
                    -WorkflowMode $WorkflowMode `
                    -Service $Service `
                    -ExtraEnv $retryEnv `
                    -ReuseDecisionConsumed
                return
            }
            if ($decision -eq "C") {
                Update-PrepManifestCancelledByUser `
                    -RunName ([string]$payload.run_name) `
                    -SourceId ([string]$payload.source_id) `
                    -CachePath ([string]$payload.cache_path) `
                    -RemoteError ([string]$payload.error) `
                    -Validation $payload.validation
                throw "Prep cancelled by user after remote-service outage for source '$($payload.source_id)'."
            }
            Write-Host "Enter R to reuse the validated same-case cache, or C to cancel." -ForegroundColor DarkYellow
        }
    }

    throw "Phase '$Phase' failed in service '$Service' with exit code $($phaseResult.ExitCode)."
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
        $previousErrorActionPreference = $ErrorActionPreference
        try {
            # Docker may emit "Container ... Running" on stderr even when the command succeeds.
            # Treat that output as normal launcher chatter instead of a terminating PowerShell error.
            $ErrorActionPreference = "Continue"
            docker-compose up -d 2>&1 | ForEach-Object { Write-ProcessLine $_ }
            $composeExitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $previousErrorActionPreference
        }
        if ($composeExitCode -ne 0) {
            throw "docker-compose up failed with exit code $composeExitCode."
        }

        $entryStartupEnv = Resolve-LauncherStartupEnv -LauncherEntry $LauncherEntry
        Write-Host ""
        Write-Host "Run-start policy:" -ForegroundColor Yellow
        Write-Host "  INPUT_CACHE_POLICY=$($entryStartupEnv['INPUT_CACHE_POLICY'])" -ForegroundColor DarkGray
        Write-Host "  FORCING_SOURCE_BUDGET_SECONDS=$($entryStartupEnv['FORCING_SOURCE_BUDGET_SECONDS'])" -ForegroundColor DarkGray
        if ($entryStartupEnv.ContainsKey("PROTOTYPE_2016_ENSEMBLE_POLICY")) {
            Write-Host "  PROTOTYPE_2016_ENSEMBLE_POLICY=$($entryStartupEnv['PROTOTYPE_2016_ENSEMBLE_POLICY'])" -ForegroundColor DarkGray
        }

        $steps = @($LauncherEntry.steps)
        $index = 0
        foreach ($step in $steps) {
            $index += 1
            $stepExtraEnv = Merge-Hashtables `
                -Base $entryStartupEnv `
                -Override (ConvertTo-Hashtable -InputObject $step.extra_env)
            Write-Host ""
            Write-Host "[$index/$($steps.Count)]" -ForegroundColor Cyan -NoNewline
            Invoke-DockerPhase `
                -Phase ([string]$step.phase) `
                -Description ([string]$step.description) `
                -WorkflowMode $workflowMode `
                -Service ([string]$step.service) `
                -ExtraEnv $stepExtraEnv
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

function Invoke-ReadOnlyUi {
    param([switch]$RestartPipeline)

    if ($RestartPipeline) {
        Write-Host "Restarting compose services for a clean UI refresh..." -ForegroundColor Yellow
    } else {
        Write-Host "Starting Docker containers..." -ForegroundColor Yellow
    }

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Docker may emit routine status lines to stderr even on success.
        $ErrorActionPreference = "Continue"
        if ($RestartPipeline) {
            docker-compose up -d 2>&1 | ForEach-Object { Write-ProcessLine $_ }
            $composeExitCode = $LASTEXITCODE
            if ($composeExitCode -eq 0) {
                docker-compose restart pipeline gnome 2>&1 | ForEach-Object { Write-ProcessLine $_ }
                $composeExitCode = $LASTEXITCODE
            }
        } else {
            docker-compose up -d 2>&1 | ForEach-Object { Write-ProcessLine $_ }
            $composeExitCode = $LASTEXITCODE
        }
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($composeExitCode -ne 0) {
        if ($RestartPipeline) {
            throw "docker-compose up/restart for the full compose stack failed with exit code $composeExitCode."
        }
        throw "docker-compose up -d failed with exit code $composeExitCode."
    }

    Write-Host ""
    Write-Host "Launching read-only Streamlit UI..." -ForegroundColor Yellow
    Write-Host "Open http://localhost:8501 while this process is running." -ForegroundColor DarkGray
    Write-Host "Press Ctrl+C to stop the UI and return to the launcher." -ForegroundColor DarkGray

    $uiArgs = @(
        "exec",
        "pipeline",
        "python",
        "-m",
        "streamlit",
        "run",
        "ui/app.py",
        "--server.address",
        "0.0.0.0",
        "--server.port",
        "8501"
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        # Keep Streamlit attached to this terminal so Ctrl+C behaves as expected.
        $ErrorActionPreference = "Continue"
        & docker-compose @uiArgs 2>&1 | ForEach-Object { Write-ProcessLine $_ }
        $uiExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($uiExitCode -ne 0) {
        throw "Read-only UI exited with code $uiExitCode."
    }
}

function Invoke-ContainerPythonScript {
    param(
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][string]$ScriptPath,
        [string]$Service = "pipeline",
        [string[]]$ScriptArgs = @(),
        [hashtable]$ExtraEnv = @{}
    )

    Write-Host ""
    Write-Host ">>> $Description" -ForegroundColor Yellow
    Write-Host "    SERVICE=$Service SCRIPT=$ScriptPath" -ForegroundColor DarkGray

    $dockerArgs = @("exec", "-T")
    foreach ($key in ($ExtraEnv.Keys | Sort-Object)) {
        $dockerArgs += @("-e", "$key=$($ExtraEnv[$key])")
    }
    $dockerArgs += @($Service, "python", $ScriptPath)
    if ($ScriptArgs) {
        $dockerArgs += $ScriptArgs
    }

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & docker-compose @dockerArgs 2>&1 | ForEach-Object { Write-ProcessLine $_ }
        $scriptExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($scriptExitCode -ne 0) {
        throw "Container Python script exited with code $scriptExitCode."
    }
}

function Show-LauncherList {
    $matrix = Get-LauncherMatrix
    $entries = Get-LauncherEntries
    Clear-Host
    Write-Section "CURRENT LAUNCHER CATALOG"
    Write-Host ""
    Write-Host "Recommended defense path: .\panel.ps1 or .\start.ps1 -Panel" -ForegroundColor Green
    Write-Host "Entrypoint: .\start.ps1" -ForegroundColor Green
    Write-Host "Catalog: $($matrix.catalog_version)" -ForegroundColor Yellow
    Write-Host "Panel mode: .\start.ps1 -Panel -NoPause" -ForegroundColor Yellow
    Write-Host "List entries: .\start.ps1 -List -NoPause" -ForegroundColor Yellow
    Write-Host "Launcher help: .\start.ps1 -Help -NoPause" -ForegroundColor Yellow
    Write-Host "Interactive run: .\start.ps1 -Entry <entry_id>" -ForegroundColor Yellow
    Write-Host "Prompt-free container run: docker-compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src" -ForegroundColor Yellow
    Write-Host "Read-only UI: docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" -ForegroundColor Yellow
    Write-Host "Full UI refresh: docker-compose up -d ; docker-compose restart pipeline gnome ; then rerun the same Streamlit command." -ForegroundColor Yellow
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
    $readOnlyEntries = @(
        $matrix.entries |
            Where-Object { $_.category_id -eq "read_only_packaging_help_utilities" -and $_.safe_default } |
            Sort-Object menu_order
    )
    Clear-Host
    Write-Section "LAUNCHER HELP"
    Write-Host ""
    Write-Host "Canonical startup paths:" -ForegroundColor Yellow
    Write-Host "  .\panel.ps1" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Panel -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -List -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Help -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry <entry_id>" -ForegroundColor Green
    Write-Host "  docker-compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src" -ForegroundColor Green
    Write-Host "  docker-compose exec -T pipeline python src/services/panel_review_check.py" -ForegroundColor Green
    Write-Host "  docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" -ForegroundColor Green
    Write-Host "  docker-compose up -d ; docker-compose restart pipeline gnome ; docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" -ForegroundColor Green
    Write-Host ""
    Write-Host "Recommended read-only launcher entries:" -ForegroundColor Yellow
    Write-Host "  .\panel.ps1  # recommended defense/panel path" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Panel -NoPause" -ForegroundColor Green
    foreach ($entry in $readOnlyEntries) {
        Write-Host ("  .\start.ps1 -Entry {0}" -f $entry.entry_id) -ForegroundColor Green
    }
    Write-Host ""
    Write-Host "Reportable launcher entries require explicit intent:" -ForegroundColor Yellow
    Write-Host "  .\start.ps1 -Entry phase1_production_rerun" -ForegroundColor Gray
    Write-Host "  .\start.ps1 -Entry mindoro_phase3b_primary_public_validation" -ForegroundColor Gray
    Write-Host "  .\start.ps1 -Entry mindoro_reportable_core" -ForegroundColor Gray
    Write-Host "  .\start.ps1 -Entry dwh_reportable_bundle" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Support, archive, and legacy entries stay available through -List:" -ForegroundColor Yellow
    Write-Host "  mindoro_phase4_only ; mindoro_appendix_sensitivity_bundle ; phase1_mindoro_focus_pre_spill_experiment" -ForegroundColor Gray
    Write-Host "  mindoro_march13_14_phase1_focus_trial ; mindoro_march6_recovery_sensitivity ; mindoro_march23_extended_public_stress_test" -ForegroundColor Gray
    Write-Host "  prototype_2021_bundle ; prototype_legacy_bundle" -ForegroundColor Gray
    Write-Host "  Compatibility alias only: mindoro_march13_14_noaa_reinit_stress_test" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Direct container runs only prompt when they have a TTY:" -ForegroundColor Yellow
    Write-Host "  docker-compose exec -e WORKFLOW_MODE=phase1_mindoro_focus_pre_spill_2016_2023 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src" -ForegroundColor Gray
    Write-Host "  docker-compose exec -T ... stays prompt-free and now prints the resolved startup policy instead." -ForegroundColor White
    Write-Host ""
    Write-Host "Current status guardrails:" -ForegroundColor Yellow
    Write-Host "  - Phase 1 dedicated 2016-2022 rerun outputs now exist and stage a candidate baseline; default spill-case adoption remains a manual promotion or BASELINE_SELECTION_PATH trial." -ForegroundColor White
    Write-Host "  - FORCING_OUTAGE_POLICY=default|continue_degraded|fail_hard controls forcing-only outage behavior. Reportable lanes fail hard by default; appendix/legacy/experimental lanes may continue in degraded mode." -ForegroundColor White
    Write-Host "  - Interactive launcher runs now ask once for the forcing wait budget and, when eligible caches already exist, whether to reuse validated inputs or force refresh." -ForegroundColor White
    Write-Host "  - Direct interactive docker-compose exec runs do the same once per run; no-TTY direct runs skip prompts and print the resolved policy instead." -ForegroundColor White
    Write-Host "  - Non-interactive launcher runs default silently to INPUT_CACHE_POLICY=reuse_if_valid and FORCING_SOURCE_BUDGET_SECONDS=300." -ForegroundColor White
    Write-Host "  - Persistent local input store means validated reusable inputs under data/drifters, data/forcing, data/arcgis, data/historical_validation_inputs, and data/local_input_store; output-local forcing/raw folders are only staging or legacy scratch." -ForegroundColor White
    Write-Host "  - INPUT_CACHE_POLICY=force_refresh bypasses validated local reuse, fetches fresh copies, and rewrites the persistent local input store for that run." -ForegroundColor White
    Write-Host "  - Current inventories and manifests now record reuse-vs-refresh, provider/source URL, local storage path, and validation status for rerun-facing inputs." -ForegroundColor White
    Write-Host "  - Drifter truth and ArcGIS/observation truth inputs stay hard requirements even when degraded forcing continuation is enabled." -ForegroundColor White
    Write-Host "  - Phase 2 is scientifically usable, but not scientifically frozen." -ForegroundColor White
    Write-Host "  - Phase 3B and Phase 3C are validation-only lanes: public-observation validation for Mindoro and external transfer validation for DWH." -ForegroundColor White
    Write-Host "  - Mindoro track semantics are locked as B1=only main-text primary row, A=same-case comparator-support, B2=March 6 legacy honesty, and B3=March 3-6 broader-support legacy context." -ForegroundColor White
    Write-Host "  - The Mindoro-focused Phase 1 rerun is confirmation-only for the recipe story and stays separate from canonical baseline governance and stored B1 provenance." -ForegroundColor White
    Write-Host "  - Outside prototype_2016, phase4_oiltype_and_shoreline, phase5_sync, the galleries, and the dashboard are support layers rather than main thesis phases." -ForegroundColor White
    Write-Host "  - DWH Phase 3C stays a separate external transfer-validation story with readiness-gated HYCOM GOFS 3.1 + ERA5 + CMEMS wave/Stokes forcing; observed masks remain truth and PyGNOME remains comparator-only." -ForegroundColor White
    Write-Host "  - Prototype mode remains available for debugging and regression only." -ForegroundColor White
    Write-Host ""
    Write-Host "Not implemented yet:" -ForegroundColor Yellow
    foreach ($item in $matrix.optional_future_work) {
        Write-Host ("  - {0} [{1}]" -f $item.label, $item.status) -ForegroundColor DarkGray
    }

    Pause-IfNeeded
}

function Show-PanelGuide {
    Clear-Host
    Write-Section "PANEL REVIEW GUIDE"
    Write-Host ""
    Write-Host "Panel quick start file:" -ForegroundColor Yellow
    Write-Host "  PANEL_QUICK_START.md" -ForegroundColor Green
    Write-Host "Detailed guide:" -ForegroundColor Yellow
    Write-Host "  docs/PANEL_REVIEW_GUIDE.md" -ForegroundColor Green
    Write-Host "Paper/output registry:" -ForegroundColor Yellow
    Write-Host "  docs/PAPER_OUTPUT_REGISTRY.md" -ForegroundColor Green
    Write-Host ""
    if (Test-Path "PANEL_QUICK_START.md") {
        Get-Content "PANEL_QUICK_START.md" | ForEach-Object { Write-Host $_ }
    } else {
        Write-Host "PANEL_QUICK_START.md is missing." -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "This panel mode verifies stored thesis-facing outputs against the manuscript." -ForegroundColor White
    Write-Host "It does not rerun expensive scientific simulations by default." -ForegroundColor White
    Write-Host "Full scientific reruns remain available through the advanced launcher for audit purposes." -ForegroundColor White
    Pause-IfNeeded
}

function Show-PaperOutputRegistry {
    Clear-Host
    Write-Section "PAPER-TO-OUTPUT REGISTRY"
    Write-Host ""
    Write-Host "Registry config: config\paper_output_registry.yaml" -ForegroundColor Yellow
    Write-Host "Registry guide:  docs\PAPER_OUTPUT_REGISTRY.md" -ForegroundColor Yellow
    Write-Host ""
    if (Test-Path "docs\PAPER_OUTPUT_REGISTRY.md") {
        Get-Content "docs\PAPER_OUTPUT_REGISTRY.md" | ForEach-Object { Write-Host $_ }
    } else {
        Write-Host "docs\PAPER_OUTPUT_REGISTRY.md is missing." -ForegroundColor Red
    }
    Pause-IfNeeded
}

function Invoke-PanelPaperVerification {
    Write-Section "VERIFY PAPER NUMBERS AGAINST STORED SCORECARDS"
    Invoke-ContainerPythonScript `
        -Description "Read-only paper-results verification from stored outputs only" `
        -ScriptPath "src/services/panel_review_check.py"
    Write-Host ""
    Write-Host "Panel verification outputs:" -ForegroundColor Yellow
    Write-Host "  output\panel_review_check\panel_results_match_check.csv" -ForegroundColor Green
    Write-Host "  output\panel_review_check\panel_results_match_check.json" -ForegroundColor Green
    Write-Host "  output\panel_review_check\panel_results_match_check.md" -ForegroundColor Green
    Write-Host "  output\panel_review_check\panel_review_manifest.json" -ForegroundColor Green
}

function Show-PanelMenu {
    while ($true) {
        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host "   PANEL REVIEW MODE" -ForegroundColor White
        Write-Host ""
        Write-Host "Recommended panel checks:" -ForegroundColor Yellow
        Write-Host "  1. Open read-only dashboard [READ-ONLY]" -ForegroundColor White
        Write-Host "     Opens the Streamlit dashboard over stored outputs only." -ForegroundColor DarkGray
        Write-Host "  2. Verify paper numbers against stored scorecards [READ-ONLY]" -ForegroundColor White
        Write-Host "     Writes only to output\panel_review_check\ and never reruns science." -ForegroundColor DarkGray
        Write-Host "  3. Rebuild publication figures from stored outputs [PACKAGING ONLY]" -ForegroundColor White
        Write-Host "     Uses the existing read-only figure-package builder." -ForegroundColor DarkGray
        Write-Host "  4. Refresh final validation package from stored outputs [PACKAGING ONLY]" -ForegroundColor White
        Write-Host "     Uses the existing read-only validation-package refresh." -ForegroundColor DarkGray
        Write-Host "  5. Refresh final reproducibility package / command documentation [PACKAGING ONLY]" -ForegroundColor White
        Write-Host "     Uses the existing read-only launcher/docs/package sync." -ForegroundColor DarkGray
        Write-Host "  6. Show paper-to-output registry [READ-ONLY]" -ForegroundColor White
        Write-Host "     Opens the plain-language manuscript/output map." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Advanced:" -ForegroundColor Yellow
        Write-Host "  A. Open full research launcher [ADVANCED ONLY]" -ForegroundColor White
        Write-Host "  H. Help / interpretation guide [READ-ONLY]" -ForegroundColor White
        Write-Host "  Q. Exit" -ForegroundColor White
        Write-Host ""

        $choice = (Read-Host "Select an option").Trim()
        if ([string]::IsNullOrWhiteSpace($choice)) {
            continue
        }

        switch ($choice.ToUpperInvariant()) {
            "1" {
                Write-Section "READ-ONLY DASHBOARD"
                try {
                    Invoke-ReadOnlyUi
                }
                catch {
                    Pause-IfNeeded
                }
                Pause-IfNeeded
                continue
            }
            "2" {
                try {
                    Invoke-PanelPaperVerification
                }
                catch {
                    Pause-IfNeeded
                }
                Pause-IfNeeded
                continue
            }
            "3" {
                $launcherEntry = Get-LauncherEntryById -EntryId "figure_package_publication"
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
            "4" {
                $launcherEntry = Get-LauncherEntryById -EntryId "final_validation_package"
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
            "5" {
                $launcherEntry = Get-LauncherEntryById -EntryId "phase5_sync"
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
            "6" {
                Show-PaperOutputRegistry
                continue
            }
            "A" {
                Show-Menu -ReturnToCaller
                continue
            }
            "H" {
                Show-PanelGuide
                continue
            }
            "Q" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
        }

        Write-Host "Invalid option. Use 1-6, A, H, or Q." -ForegroundColor Red
        Start-Sleep -Seconds 2
    }
}

function Show-Menu {
    param([switch]$ReturnToCaller)

    while ($true) {
        $entries = Get-LauncherEntries
        $selectionMap = @{}
        $displayIndex = 1

        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host ""
        Write-Host "For defense/panel review, use PANEL REVIEW MODE first." -ForegroundColor Yellow
        Write-Host "The scientific tracks below can be expensive and may rerun parts of the workflow. They are provided for audit and researcher use." -ForegroundColor DarkYellow
        Write-Host ""
        Write-Host "Choose a launcher entry. Read-only utilities are the safest first choice, or press P for panel review mode." -ForegroundColor Yellow
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

        Write-Host "  P. Panel review mode (recommended for defense)" -ForegroundColor Yellow
        Write-Host "  L. List catalog only" -ForegroundColor Yellow
        Write-Host "  U. Launch read-only UI" -ForegroundColor Yellow
        Write-Host "  R. Full restart read-only UI" -ForegroundColor Yellow
        Write-Host "  H. Help" -ForegroundColor Yellow
        if ($ReturnToCaller) {
            Write-Host "  B. Back" -ForegroundColor Yellow
        }
        Write-Host "  Q. Exit" -ForegroundColor Yellow
        Write-Host ""

        $choice = (Read-Host "Select an option").Trim()
        if ([string]::IsNullOrWhiteSpace($choice)) {
            continue
        }

        switch ($choice.ToUpperInvariant()) {
            "P" {
                Show-PanelMenu
                continue
            }
            "L" {
                Show-LauncherList
                continue
            }
            "U" {
                Write-Section "READ-ONLY UI"
                try {
                    Invoke-ReadOnlyUi
                }
                catch {
                    Pause-IfNeeded
                }
                Pause-IfNeeded
                continue
            }
            "R" {
                Write-Section "FULL UI REFRESH"
                try {
                    Invoke-ReadOnlyUi -RestartPipeline
                }
                catch {
                    Pause-IfNeeded
                }
                Pause-IfNeeded
                continue
            }
            "H" {
                Show-Help
                continue
            }
            "B" {
                if ($ReturnToCaller) {
                    return
                }
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

        if ($ReturnToCaller) {
            Write-Host "Invalid option. Use a menu number, P, U, R, L, H, B, or Q." -ForegroundColor Red
        } else {
            Write-Host "Invalid option. Use a menu number, P, U, R, L, H, or Q." -ForegroundColor Red
        }
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

    if ($Panel) {
        Show-PanelMenu
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
