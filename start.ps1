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
        .\start.ps1 -ListRole <thesis_role> -NoPause
        .\start.ps1 -ValidateMatrix -NoPause
        .\start.ps1 -Help -NoPause
        .\start.ps1 -Explain <entry_id> -NoPause
        .\start.ps1 -Entry <entry_id>
        docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
#>

param(
    [switch]$List,
    [switch]$Help,
    [switch]$ValidateMatrix,
    [string]$Entry,
    [string]$Explain,
    [string]$ListRole,
    [switch]$Panel,
    [switch]$DryRun,
    [switch]$ExportPlan,
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
$Script:ComposeMode = $null
$Script:ComposeModeChecked = $false
$Script:LauncherNotice = $null

Set-Location $Script:RepoRoot

function Resolve-ComposeMode {
    if ($Script:ComposeModeChecked) {
        return $Script:ComposeMode
    }

    $Script:ComposeModeChecked = $true
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        & docker compose version *> $null
        if ($LASTEXITCODE -eq 0) {
            $Script:ComposeMode = "docker_compose_v2"
            return $Script:ComposeMode
        }

        & docker-compose version *> $null
        if ($LASTEXITCODE -eq 0) {
            $Script:ComposeMode = "docker_compose_v1"
            return $Script:ComposeMode
        }
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    $Script:ComposeMode = $null
    return $null
}

function Get-ComposeMode {
    $mode = Resolve-ComposeMode
    if ($mode) {
        return $mode
    }

    throw "Docker Compose is required. Prefer 'docker compose'; older 'docker-compose' also works if installed."
}

function Get-ComposeCommandText {
    $mode = $Script:ComposeMode
    switch ($mode) {
        "docker_compose_v1" { return "docker-compose" }
        default { return "docker compose" }
    }
}

function Invoke-ComposeCommand {
    param([Parameter(Mandatory = $true)][string[]]$ComposeArgs)

    switch (Get-ComposeMode) {
        "docker_compose_v2" {
            & docker compose @ComposeArgs
            return
        }
        "docker_compose_v1" {
            & docker-compose @ComposeArgs
            return
        }
    }
}

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

function New-LauncherResult {
    param(
        [Parameter(Mandatory = $true)][string]$Status,
        [string]$Message = "",
        [int]$ExitCode = 0,
        [object]$LauncherEntry = $null,
        [string]$RequestedEntryId = "",
        [bool]$NoWorkflowExecuted = $false
    )

    return [pscustomobject]@{
        Status = $Status
        Message = $Message
        ExitCode = $ExitCode
        LauncherEntry = $LauncherEntry
        RequestedEntryId = $RequestedEntryId
        NoWorkflowExecuted = $NoWorkflowExecuted
    }
}

function Set-LauncherNotice {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [ValidateSet("info", "warning", "success", "error")][string]$Level = "info"
    )

    $Script:LauncherNotice = @{
        Message = $Message
        Level = $Level
    }
}

function Show-LauncherNotice {
    if ($null -eq $Script:LauncherNotice) {
        return
    }

    $notice = $Script:LauncherNotice
    $Script:LauncherNotice = $null

    $color = switch ([string]$notice.Level) {
        "success" { "Green" }
        "error" { "Red" }
        "warning" { "Yellow" }
        default { "Cyan" }
    }

    Write-Host ""
    Write-Host ([string]$notice.Message) -ForegroundColor $color
    Write-Host ""
}

function Write-LauncherCancelledMessage {
    param([string]$Message = "Cancelled. No workflow was executed.")

    Write-Host ""
    Write-Host $Message -ForegroundColor Yellow
}

function Test-LauncherDryRunRequested {
    if ($DryRun) {
        return $true
    }

    $envValue = [string](Get-Item -Path "Env:LAUNCHER_DRY_RUN" -ErrorAction SilentlyContinue).Value
    return ($envValue.Trim().ToLowerInvariant() -in @("1", "true", "yes", "y", "on"))
}

function Test-LauncherInputBuffered {
    try {
        return ([Console]::In.Peek() -ne -1)
    }
    catch {
        return $false
    }
}

function Get-ComposeUnavailableMessage {
    param([string]$ActionLabel = "This launcher action")

    return ("{0} requires Docker Compose, but Docker is currently unavailable. Start Docker Desktop or install Docker Compose, then try again." -f $ActionLabel)
}

function Write-ComposeUnavailableMessage {
    param([string]$ActionLabel = "This launcher action")

    $message = Get-ComposeUnavailableMessage -ActionLabel $ActionLabel
    Write-Host ""
    Write-Host $message -ForegroundColor Yellow
    return $message
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
        Invoke-ComposeCommand -ComposeArgs $dockerArgs 2>&1 | ForEach-Object {
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

function Get-LauncherCategoryById {
    param([Parameter(Mandatory = $true)][string]$CategoryId)

    return Get-LauncherCategories | Where-Object { $_.category_id -eq $CategoryId } | Select-Object -First 1
}

function Get-LauncherCategoryLabel {
    param([string]$CategoryId)

    $category = Get-LauncherCategoryById -CategoryId ([string]$CategoryId)
    if ($null -ne $category) {
        return [string]$category.label
    }

    return [string]$CategoryId
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

function Resolve-LauncherEntryRequest {
    param([Parameter(Mandatory = $true)][string]$EntryId)

    $requestedEntry = Get-LauncherEntryById -EntryId $EntryId
    $canonicalEntry = $requestedEntry
    $visited = @{}

    while ($canonicalEntry.alias_of) {
        $currentId = [string]$canonicalEntry.entry_id
        if ($visited.ContainsKey($currentId)) {
            throw "Launcher alias cycle detected for entry '$EntryId'."
        }

        $visited[$currentId] = $true
        $canonicalEntry = Get-LauncherEntryById -EntryId ([string]$canonicalEntry.alias_of)
    }

    return [pscustomobject]@{
        RequestedEntry = $requestedEntry
        RequestedEntryId = [string]$requestedEntry.entry_id
        CanonicalEntry = $canonicalEntry
        CanonicalEntryId = [string]$canonicalEntry.entry_id
        AliasUsed = ([string]$requestedEntry.entry_id -ne [string]$canonicalEntry.entry_id)
    }
}

function Get-VisibleLauncherEntries {
    return @(
        Get-LauncherEntries |
            Where-Object { -not [bool]$_.menu_hidden } |
            Sort-Object menu_order, entry_id
    )
}

function Get-HiddenLauncherEntries {
    return @(
        Get-LauncherEntries |
            Where-Object { [bool]$_.menu_hidden } |
            Sort-Object menu_order, entry_id
    )
}

function Get-ValidThesisRoles {
    return @(
        "primary_evidence",
        "support_context",
        "comparator_support",
        "archive_provenance",
        "legacy_support",
        "read_only_governance"
    )
}

function Format-ThesisRoleLabel {
    param([string]$Role)

    switch ([string]$Role) {
        "primary_evidence" { return "Primary evidence" }
        "support_context" { return "Support/context" }
        "comparator_support" { return "Comparator support" }
        "archive_provenance" { return "Archive/provenance" }
        "legacy_support" { return "Legacy support" }
        "read_only_governance" { return "Read-only governance" }
        default { return ([string]$Role) }
    }
}

function Format-RunKindLabel {
    param([string]$RunKind)

    switch ([string]$RunKind) {
        "read_only" { return "Read-only" }
        "packaging_only" { return "Packaging-only" }
        "scientific_rerun" { return "Scientific rerun" }
        "comparator_rerun" { return "Comparator rerun" }
        "archive_rerun" { return "Archive/support rerun" }
        default { return ([string]$RunKind) }
    }
}

function Test-LauncherEntryReadOnlyLike {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    return ([string]$LauncherEntry.run_kind -in @("read_only", "packaging_only"))
}

function Test-LauncherEntryNeedsPreview {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    if ([bool]$LauncherEntry.confirms_before_run) {
        return $true
    }

    if (-not [bool]$LauncherEntry.safe_default) {
        return $true
    }

    return ([string]$LauncherEntry.rerun_cost -ne "cheap_read_only")
}

function Test-ConsolePromptAvailable {
    try {
        if ([Console]::IsInputRedirected) {
            return (Test-LauncherInputBuffered)
        }

        return [Environment]::UserInteractive
    }
    catch {
        return $false
    }
}

function Get-LauncherEntryOutputWarning {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    switch ([string]$LauncherEntry.run_kind) {
        "read_only" { return "Uses stored manifests or artifacts only. No scientific rerun should occur." }
        "packaging_only" { return "Rebuilds packaging/docs from stored outputs only. No scientific rerun should occur." }
        "comparator_rerun" { return "May rerun support/comparator workflows and write new comparator artifacts under output/. PyGNOME remains comparator-only." }
        "archive_rerun" { return "May rerun archive, appendix, or provenance workflows and write new support artifacts under output/ without changing the thesis claim boundary." }
        default { return "May rerun scientific phases and write new workflow artifacts under output/. Continue only when you intentionally want that rerun." }
    }
}

function New-LauncherChoiceResult {
    param(
        [Parameter(Mandatory = $true)][string]$Action,
        [string]$RawInput = "",
        [string]$Message = "",
        [string[]]$AllowedOptions = @(),
        [object]$ResolvedEntry = $null
    )

    return [pscustomobject]@{
        Action = $Action
        RawInput = $RawInput
        Message = $Message
        AllowedOptions = @(
            $AllowedOptions |
                Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
                Select-Object -Unique
        )
        ResolvedEntry = $ResolvedEntry
    }
}

function Get-LauncherAllowedOptionsText {
    param([string[]]$AllowedOptions)

    return (
        @(
            $AllowedOptions |
                Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
                Select-Object -Unique
        ) -join ", "
    )
}

function Resolve-LauncherChoice {
    param(
        [string]$InputText,
        [string[]]$AllowedActions = @(),
        [ValidateSet("ignore", "back", "cancel", "quit")][string]$BlankAction = "ignore",
        [hashtable]$SelectionMap = @{},
        [string]$GroupId = "",
        [string]$ConfirmationEntryId = "",
        [string[]]$AllowedOptions = @()
    )

    $normalizedAllowedActions = @(
        $AllowedActions |
            ForEach-Object { ([string]$_).Trim().ToLowerInvariant() } |
            Where-Object { $_ } |
            Select-Object -Unique
    )

    $trimmedInput = ([string]$InputText).Trim()
    $upperInput = $trimmedInput.ToUpperInvariant()

    if ([string]::IsNullOrWhiteSpace($trimmedInput)) {
        switch ($BlankAction) {
            "back" { return New-LauncherChoiceResult -Action "back" -AllowedOptions $AllowedOptions }
            "cancel" { return New-LauncherChoiceResult -Action "cancel" -AllowedOptions $AllowedOptions }
            "quit" { return New-LauncherChoiceResult -Action "quit" -AllowedOptions $AllowedOptions }
            default { return New-LauncherChoiceResult -Action "ignore" -AllowedOptions $AllowedOptions }
        }
    }

    if (($normalizedAllowedActions -contains "back") -and $upperInput -in @("0", "B", "BACK")) {
        return New-LauncherChoiceResult -Action "back" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "cancel") -and $upperInput -in @("C", "CANCEL")) {
        return New-LauncherChoiceResult -Action "cancel" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "quit") -and $upperInput -in @("Q", "QUIT", "EXIT")) {
        return New-LauncherChoiceResult -Action "quit" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "help") -and $upperInput -in @("H", "HELP")) {
        return New-LauncherChoiceResult -Action "help" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "list") -and $upperInput -in @("L", "LIST")) {
        return New-LauncherChoiceResult -Action "list" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "panel") -and $upperInput -in @("P", "PANEL")) {
        return New-LauncherChoiceResult -Action "panel" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "ui") -and $upperInput -in @("U", "UI")) {
        return New-LauncherChoiceResult -Action "ui" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "restart") -and $upperInput -in @("R", "RESTART")) {
        return New-LauncherChoiceResult -Action "restart" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "explain") -and $upperInput -in @("X", "E", "I", "EXPLAIN", "INSPECT")) {
        return New-LauncherChoiceResult -Action "explain" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "search") -and $upperInput -in @("S", "SEARCH")) {
        return New-LauncherChoiceResult -Action "search" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "export") -and $upperInput -in @("E", "EXPORT", "EXPORTPLAN", "EXPORT_PLAN")) {
        return New-LauncherChoiceResult -Action "export" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if (($normalizedAllowedActions -contains "more") -and $upperInput -in @("M", "MORE")) {
        return New-LauncherChoiceResult -Action "more" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
    }
    if ($normalizedAllowedActions -contains "run") {
        $matchesEntryId = $false
        if (-not [string]::IsNullOrWhiteSpace($ConfirmationEntryId)) {
            $matchesEntryId = $trimmedInput.Equals([string]$ConfirmationEntryId, [System.StringComparison]::InvariantCultureIgnoreCase)
        }
        if ($upperInput -in @("RUN", "Y", "YES") -or $matchesEntryId) {
            return New-LauncherChoiceResult -Action "run" -RawInput $trimmedInput -AllowedOptions $AllowedOptions
        }
    }
    if ($normalizedAllowedActions -contains "entry") {
        try {
            $resolvedEntry = Resolve-LauncherEntryReference `
                -Reference $trimmedInput `
                -SelectionMap $SelectionMap `
                -GroupId $GroupId `
                -ThrowIfUnknown
            return New-LauncherChoiceResult -Action "entry" -RawInput $trimmedInput -AllowedOptions $AllowedOptions -ResolvedEntry $resolvedEntry
        }
        catch {
            return New-LauncherChoiceResult -Action "invalid" -RawInput $trimmedInput -AllowedOptions $AllowedOptions -Message $_.Exception.Message
        }
    }

    $allowedText = Get-LauncherAllowedOptionsText -AllowedOptions $AllowedOptions
    $invalidMessage = if ($allowedText) {
        "Invalid option '$trimmedInput'. Allowed options: $allowedText."
    } else {
        "Invalid option '$trimmedInput'."
    }
    return New-LauncherChoiceResult -Action "invalid" -RawInput $trimmedInput -AllowedOptions $AllowedOptions -Message $invalidMessage
}

function Write-LauncherEntrySummary {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$DisplayKey,
        [switch]$IncludeHiddenMarker
    )

    $prefix = if ($DisplayKey) { "  $DisplayKey. " } else { "  - " }
    Write-Host ("{0}{1}" -f $prefix, $LauncherEntry.label) -ForegroundColor White
    Write-Host ("     id={0}" -f $LauncherEntry.entry_id) -ForegroundColor DarkGray
    Write-Host ("     category={0} | thesis role={1}" -f (Get-LauncherCategoryLabel -CategoryId ([string]$LauncherEntry.category_id)), (Format-ThesisRoleLabel -Role ([string]$LauncherEntry.thesis_role))) -ForegroundColor Yellow
    Write-Host ("     draft={0}" -f ([string]$LauncherEntry.draft_section)) -ForegroundColor Yellow

    $safetyTag = if ([bool]$LauncherEntry.safe_default) { "safe-default" } else { "explicit-confirm" }
    $tags = @(
        (Format-RunKindLabel -RunKind ([string]$LauncherEntry.run_kind)),
        ("cost={0}" -f [string]$LauncherEntry.rerun_cost),
        $safetyTag,
        ("for={0}" -f [string]$LauncherEntry.recommended_for)
    )
    if ($IncludeHiddenMarker -and [bool]$LauncherEntry.menu_hidden) {
        $tags += "hidden-from-default-menu"
    }
    if ($LauncherEntry.alias_of) {
        $tags += ("alias-of={0}" -f [string]$LauncherEntry.alias_of)
    }

    Write-Host ("     tags={0}" -f ($tags -join " | ")) -ForegroundColor DarkGray
    Write-Host ("     boundary={0}" -f [string]$LauncherEntry.claim_boundary) -ForegroundColor Gray
}

function Get-LauncherEntrySafetyText {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    if ([bool]$LauncherEntry.safe_default) {
        return "safe default"
    }

    return "explicit confirmation required"
}

function Get-LauncherEntryShortStepSummary {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    return ((@($LauncherEntry.steps) | ForEach-Object { "{0}:{1}" -f [string]$_.service, [string]$_.phase }) -join " -> ")
}

function Get-LauncherEntryInteractiveCommand {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    return (".\start.ps1 -Entry {0}" -f [string]$LauncherEntry.entry_id)
}

function Get-LauncherEntryExpectedOutputDirs {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    $entryId = [string]$LauncherEntry.entry_id
    $knownDirs = switch ($entryId) {
        "phase1_mindoro_focus_provenance" {
            @("output/phase1_mindoro_focus_pre_spill_2016_2023")
            break
        }
        "mindoro_phase3b_primary_public_validation" {
            @(
                "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public",
                "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit",
                "output/Phase 3B March13-14 Final Output"
            )
            break
        }
        "dwh_reportable_bundle" {
            @(
                "output/CASE_DWH_RETRO_2010_72H",
                "output/Phase 3C DWH Final Output"
            )
            break
        }
        "mindoro_reportable_core" {
            @(
                "output/CASE_MINDORO_RETRO_2023",
                "output/Phase 3B March13-14 Final Output",
                "output/phase4/CASE_MINDORO_RETRO_2023"
            )
            break
        }
        "phase1_regional_reference_rerun" {
            @("output/phase1_production_rerun")
            break
        }
        "mindoro_phase4_only" {
            @("output/phase4/CASE_MINDORO_RETRO_2023")
            break
        }
        "mindoro_appendix_sensitivity_bundle" {
            @(
                "output/CASE_MINDORO_RETRO_2023",
                "output/phase3b_public_obs_appendix",
                "output/horizon_survival_audit",
                "output/transport_retention_fix"
            )
            break
        }
        "mindoro_march13_14_phase1_focus_trial" {
            @("output/CASE_MINDORO_RETRO_2023/mindoro_march13_14_phase1_focus_trial")
            break
        }
        "mindoro_march6_recovery_sensitivity" {
            @("output/CASE_MINDORO_RETRO_2023/march6_recovery_sensitivity")
            break
        }
        "mindoro_march23_extended_public_stress_test" {
            @(
                "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public",
                "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march23"
            )
            break
        }
        "phase3b_mindoro_march3_4_philsa_5000_experiment" {
            @("output/CASE_MINDORO_RETRO_2023/phase3b_philsa_march3_4_5000_experiment")
            break
        }
        "mindoro_mar09_12_multisource_experiment" {
            @("output/CASE_MINDORO_RETRO_2023/experiments/mar09_11_12_multisource")
            break
        }
        "phase3b_mindoro_march13_14_reinit_5000_experiment" {
            @(
                "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_5000_experiment",
                "output/CASE_MINDORO_RETRO_2023/phase3b_march13_14_element_count_sensitivity"
            )
            break
        }
        "phase1_audit" {
            @("output/phase1_finalization_audit")
            break
        }
        "phase2_audit" {
            @("output/phase2_finalization_audit")
            break
        }
        "b1_drifter_context_panel" {
            @("output/panel_drifter_context")
            break
        }
        "final_validation_package" {
            @("output/final_validation_package")
            break
        }
        "phase5_sync" {
            @("output/final_reproducibility_package")
            break
        }
        "trajectory_gallery" {
            @("output/trajectory_gallery")
            break
        }
        "trajectory_gallery_panel" {
            @("output/trajectory_gallery_panel")
            break
        }
        "figure_package_publication" {
            @("output/figure_package_publication")
            break
        }
        "prototype_legacy_final_figures" {
            @("output/2016 Legacy Runs FINAL Figures")
            break
        }
        "prototype_2021_bundle" {
            @("output/CASE_20210305T180000Z", "output/prototype_2021_pygnome_similarity")
            break
        }
        "prototype_legacy_bundle" {
            @(
                "output/2016 Legacy Runs FINAL Figures",
                "output/prototype_2016_pygnome_similarity"
            )
            break
        }
        default {
            @()
        }
    }

    $dirs = @($knownDirs)
    if ($LauncherEntry.expected_output_dirs) {
        $dirs += @($LauncherEntry.expected_output_dirs | ForEach-Object { [string]$_ })
    }

    return @(
        $dirs |
            Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
            ForEach-Object { ([string]$_).Replace("\", "/") } |
            Select-Object -Unique
    )
}

function Get-LauncherEntryMayWriteOutputsText {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    switch ([string]$LauncherEntry.run_kind) {
        "read_only" { return "May write read-only audit/panel artifacts only; it must not rewrite science." }
        "packaging_only" { return "May write packaging/docs/figure artifacts from stored outputs only; it must not recompute science." }
        "comparator_rerun" { return "May write comparator-support outputs; PyGNOME/Track A remain comparator-only and are not observational truth." }
        "archive_rerun" { return "May write archive/provenance/support outputs; these stay outside the main claim." }
        default { return "May write scientific rerun outputs under output/; run only for intentional researcher/audit work." }
    }
}

function Get-LauncherStartupPromptText {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    if (Test-LauncherDryRunRequested) {
        return "No. -DryRun skips Docker/probe prompts and uses the resolved default/passthrough environment."
    }

    if ($NoPause) {
        return "No. -NoPause/non-interactive launcher use resolves the default/passthrough environment without prompts."
    }

    if (Test-LauncherEntryReadOnlyLike -LauncherEntry $LauncherEntry) {
        return "No startup science prompts expected for this read-only/package entry."
    }

    return "Possible in interactive launcher runs after Docker starts; prompt-free docker compose commands use the environment shown here."
}

function Get-LauncherEnvPreview {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    return Resolve-LauncherStartupEnv -LauncherEntry $LauncherEntry -SkipInteractiveProbe
}

function Write-LauncherEnvironmentPreview {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [hashtable]$StartupEnv = $null
    )

    if ($null -eq $StartupEnv) {
        $StartupEnv = Get-LauncherEnvPreview -LauncherEntry $LauncherEntry
    }

    Write-Host "Environment variables that will be passed:" -ForegroundColor White
    foreach ($key in ($StartupEnv.Keys | Sort-Object)) {
        Write-Host ("  {0}={1}" -f $key, $StartupEnv[$key]) -ForegroundColor DarkGray
    }

    $stepsWithExtraEnv = @(
        @($LauncherEntry.steps) |
            Where-Object { $_.extra_env } |
            ForEach-Object {
                $step = $_
                $extra = ConvertTo-Hashtable -InputObject $step.extra_env
                if ($extra.Keys.Count -gt 0) {
                    [pscustomobject]@{
                        Phase = [string]$step.phase
                        ExtraEnv = $extra
                    }
                }
            }
    )

    if ($stepsWithExtraEnv) {
        Write-Host "  Step-specific overrides:" -ForegroundColor DarkGray
        foreach ($stepEnv in $stepsWithExtraEnv) {
            foreach ($key in ($stepEnv.ExtraEnv.Keys | Sort-Object)) {
                Write-Host ("    {0}: {1}={2}" -f $stepEnv.Phase, $key, $stepEnv.ExtraEnv[$key]) -ForegroundColor DarkGray
            }
        }
    }
}

function Write-LauncherPromptFreeCommands {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [hashtable]$StartupEnv = $null
    )

    if ($null -eq $StartupEnv) {
        $StartupEnv = Get-LauncherEnvPreview -LauncherEntry $LauncherEntry
    }

    Write-Host ("  [prep] {0} up -d" -f (Get-ComposeCommandText)) -ForegroundColor DarkGray
    $stepIndex = 0
    foreach ($step in @($LauncherEntry.steps)) {
        $stepIndex += 1
        $stepExtraEnv = Merge-Hashtables `
            -Base $StartupEnv `
            -Override (ConvertTo-Hashtable -InputObject $step.extra_env)
        Write-Host ("  [{0}/{1}] {2}" -f $stepIndex, @($LauncherEntry.steps).Count, (Get-LauncherStepCommandPreview -LauncherEntry $LauncherEntry -Step $step -ExtraEnv $stepExtraEnv)) -ForegroundColor DarkGray
    }
}

function Write-LauncherEntryPreviewContent {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = "",
        [hashtable]$StartupEnv = $null,
        [switch]$IncludeNotes
    )

    $startupEnv = $StartupEnv
    if ($null -eq $startupEnv) {
        $startupEnv = Get-LauncherEnvPreview -LauncherEntry $LauncherEntry
    }
    Write-Host ("Entry ID: {0}" -f [string]$LauncherEntry.entry_id) -ForegroundColor Yellow
    if (-not [string]::IsNullOrWhiteSpace($RequestedEntryId) -and ([string]$RequestedEntryId -ne [string]$LauncherEntry.entry_id)) {
        Write-Host ("Requested alias: {0}" -f [string]$RequestedEntryId) -ForegroundColor DarkGray
    }
    Write-Host ("Canonical entry ID: {0}" -f [string]$LauncherEntry.entry_id) -ForegroundColor White
    Write-Host ("Label: {0}" -f [string]$LauncherEntry.label) -ForegroundColor White
    Write-Host ("Category: {0}" -f (Get-LauncherCategoryLabel -CategoryId ([string]$LauncherEntry.category_id))) -ForegroundColor White
    Write-Host ("Thesis role: {0}" -f (Format-ThesisRoleLabel -Role ([string]$LauncherEntry.thesis_role))) -ForegroundColor White
    Write-Host ("Draft section: {0}" -f [string]$LauncherEntry.draft_section) -ForegroundColor White
    Write-Host ("Claim boundary: {0}" -f [string]$LauncherEntry.claim_boundary) -ForegroundColor White
    Write-Host ("Run kind: {0}" -f (Format-RunKindLabel -RunKind ([string]$LauncherEntry.run_kind))) -ForegroundColor White
    Write-Host ("Rerun cost: {0}" -f [string]$LauncherEntry.rerun_cost) -ForegroundColor White
    Write-Host ("Services and phases: {0}" -f (Get-LauncherEntryShortStepSummary -LauncherEntry $LauncherEntry)) -ForegroundColor White
    $safetyText = Get-LauncherEntrySafetyText -LauncherEntry $LauncherEntry
    Write-Host ("Safety / recommended for: {0} / {1}" -f $safetyText, [string]$LauncherEntry.recommended_for) -ForegroundColor White
    Write-Host ("Startup prompts: {0}" -f (Get-LauncherStartupPromptText -LauncherEntry $LauncherEntry)) -ForegroundColor White
    Write-Host ("May write outputs: {0}" -f (Get-LauncherEntryMayWriteOutputsText -LauncherEntry $LauncherEntry)) -ForegroundColor White
    Write-Host "Steps that would run:" -ForegroundColor White
    $stepIndex = 0
    foreach ($step in @($LauncherEntry.steps)) {
        $stepIndex += 1
        Write-Host ("  {0}. {1}:{2} - {3}" -f $stepIndex, [string]$step.service, [string]$step.phase, [string]$step.description) -ForegroundColor DarkGray
    }
    Write-LauncherEnvironmentPreview -LauncherEntry $LauncherEntry -StartupEnv $startupEnv
    Write-Host "Exact interactive command:" -ForegroundColor White
    Write-Host ("  {0}" -f (Get-LauncherEntryInteractiveCommand -LauncherEntry $LauncherEntry)) -ForegroundColor Green
    Write-Host "Exact prompt-free docker compose command sequence:" -ForegroundColor White
    Write-LauncherPromptFreeCommands -LauncherEntry $LauncherEntry -StartupEnv $startupEnv
    Write-Host "Expected output directories:" -ForegroundColor White
    $expectedOutputDirs = Get-LauncherEntryExpectedOutputDirs -LauncherEntry $LauncherEntry
    if ($expectedOutputDirs) {
        foreach ($dir in $expectedOutputDirs) {
            Write-Host ("  {0}" -f $dir) -ForegroundColor DarkGray
        }
    } else {
        Write-Host "  Entry-specific directories are determined by the phase handlers and manifests under output/." -ForegroundColor DarkGray
    }
    Write-Host ("Output warning: {0}" -f (Get-LauncherEntryOutputWarning -LauncherEntry $LauncherEntry)) -ForegroundColor Yellow
    if ($IncludeNotes -and $LauncherEntry.notes) {
        Write-Host ("Notes: {0}" -f [string]$LauncherEntry.notes) -ForegroundColor DarkGray
    }
}

function Write-LauncherEntryCompactPreview {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = ""
    )

    Write-Host ""
    Write-Host "Inspect preview:" -ForegroundColor Cyan
    Write-Host ("  Entry ID: {0}" -f [string]$LauncherEntry.entry_id) -ForegroundColor Yellow
    if (-not [string]::IsNullOrWhiteSpace($RequestedEntryId) -and ([string]$RequestedEntryId -ne [string]$LauncherEntry.entry_id)) {
        Write-Host ("  Requested alias: {0}" -f [string]$RequestedEntryId) -ForegroundColor DarkGray
    }
    Write-Host ("  Label: {0}" -f [string]$LauncherEntry.label) -ForegroundColor White
    Write-Host ("  Category: {0}" -f (Get-LauncherCategoryLabel -CategoryId ([string]$LauncherEntry.category_id))) -ForegroundColor White
    Write-Host ("  Thesis role: {0}" -f (Format-ThesisRoleLabel -Role ([string]$LauncherEntry.thesis_role))) -ForegroundColor White
    Write-Host ("  Manuscript section: {0}" -f [string]$LauncherEntry.draft_section) -ForegroundColor White
    Write-Host ("  Claim boundary: {0}" -f [string]$LauncherEntry.claim_boundary) -ForegroundColor White
    Write-Host ("  Run kind: {0}" -f (Format-RunKindLabel -RunKind ([string]$LauncherEntry.run_kind))) -ForegroundColor White
    Write-Host ("  Rerun cost: {0}" -f [string]$LauncherEntry.rerun_cost) -ForegroundColor White
    Write-Host ("  Safety / confirmation: {0}" -f (Get-LauncherEntrySafetyText -LauncherEntry $LauncherEntry)) -ForegroundColor White
    Write-Host ("  Recommended for: {0}" -f [string]$LauncherEntry.recommended_for) -ForegroundColor White
    Write-Host ("  Step summary: {0}" -f (Get-LauncherEntryShortStepSummary -LauncherEntry $LauncherEntry)) -ForegroundColor DarkGray
    Write-Host ("  May write outputs: {0}" -f (Get-LauncherEntryMayWriteOutputsText -LauncherEntry $LauncherEntry)) -ForegroundColor White
    Write-Host ("  Interactive command: {0}" -f (Get-LauncherEntryInteractiveCommand -LauncherEntry $LauncherEntry)) -ForegroundColor DarkGray
}

function Show-LauncherEntryPreview {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = "",
        [hashtable]$StartupEnv = $null
    )

    Clear-Host
    Write-Section "ENTRY PREVIEW"
    Write-Host ""
    Write-LauncherEntryPreviewContent `
        -LauncherEntry $LauncherEntry `
        -RequestedEntryId $RequestedEntryId `
        -StartupEnv $StartupEnv `
        -IncludeNotes
}

function Confirm-LauncherEntryRun {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = ""
    )

    if (-not (Test-LauncherEntryNeedsPreview -LauncherEntry $LauncherEntry)) {
        return (New-LauncherResult -Status "confirmed" -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId)
    }

    if (-not (Test-ConsolePromptAvailable)) {
        throw "Launcher entry '$([string]$LauncherEntry.entry_id)' requires interactive confirmation. Run .\start.ps1 -Explain $([string]$LauncherEntry.entry_id) or .\start.ps1 -Entry $([string]$LauncherEntry.entry_id) -DryRun first, then rerun it from an interactive PowerShell session."
    }

    Show-LauncherEntryPreview -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId
    Write-Host ""

    $confirmationPrompt = if (Test-LauncherEntryReadOnlyLike -LauncherEntry $LauncherEntry) {
        "Confirm [Y/YES=run | C=cancel | Enter=cancel]"
    } else {
        "Confirm [RUN/Y/YES/$([string]$LauncherEntry.entry_id)=run | C=cancel | Enter=cancel]"
    }
    $allowedOptions = if (Test-LauncherEntryReadOnlyLike -LauncherEntry $LauncherEntry) {
        @("Y", "YES", "C", "CANCEL", "Enter")
    } else {
        @("RUN", "Y", "YES", [string]$LauncherEntry.entry_id, "C", "CANCEL", "Enter")
    }

    if (Test-LauncherEntryReadOnlyLike -LauncherEntry $LauncherEntry) {
        while ($true) {
            $choice = Resolve-LauncherChoice `
                -InputText (Read-Host $confirmationPrompt) `
                -AllowedActions @("run", "cancel") `
                -BlankAction "cancel" `
                -AllowedOptions $allowedOptions
            switch ($choice.Action) {
                "run" {
                    return (New-LauncherResult -Status "confirmed" -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId)
                }
                "cancel" {
                    return (New-LauncherResult -Status "cancelled" -Message "Cancelled. No workflow was executed." -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId -NoWorkflowExecuted $true)
                }
            }
            Write-Host ("Invalid option '{0}'. Allowed options: {1}." -f $choice.RawInput, (Get-LauncherAllowedOptionsText -AllowedOptions $allowedOptions)) -ForegroundColor DarkYellow
        }
    }

    while ($true) {
        $choice = Resolve-LauncherChoice `
            -InputText (Read-Host $confirmationPrompt) `
            -AllowedActions @("run", "cancel") `
            -BlankAction "cancel" `
            -ConfirmationEntryId ([string]$LauncherEntry.entry_id) `
            -AllowedOptions $allowedOptions
        switch ($choice.Action) {
            "run" {
                return (New-LauncherResult -Status "confirmed" -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId)
            }
            "cancel" {
                return (New-LauncherResult -Status "cancelled" -Message "Cancelled. No workflow was executed." -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId -NoWorkflowExecuted $true)
            }
        }
        Write-Host ("Invalid option '{0}'. Allowed options: {1}." -f $choice.RawInput, (Get-LauncherAllowedOptionsText -AllowedOptions $allowedOptions)) -ForegroundColor DarkYellow
    }
}

function Get-DefaultLauncherRoleGroups {
    return @(
        @{
            menu_order = 1
            MenuKey = "1"
            GroupId = "main_thesis_evidence"
            Label = "Main thesis evidence / reportable"
            Description = "Intentional reruns for the defended reportable evidence lanes."
            thesis_roles = @("primary_evidence")
        },
        @{
            menu_order = 2
            MenuKey = "2"
            GroupId = "support_context"
            Label = "Support/context and appendix"
            Description = "Support, comparator, and appendix lanes outside the main-text claim."
            thesis_roles = @("support_context", "comparator_support")
        },
        @{
            menu_order = 3
            MenuKey = "3"
            GroupId = "archive_provenance"
            Label = "Archive/provenance"
            Description = "Archive, provenance, and governance reruns kept outside the default defense path."
            thesis_roles = @("archive_provenance")
        },
        @{
            menu_order = 4
            MenuKey = "4"
            GroupId = "legacy_debug"
            Label = "Legacy/debug"
            Description = "Legacy prototype support and debug paths."
            thesis_roles = @("legacy_support")
        },
        @{
            menu_order = 5
            MenuKey = "5"
            GroupId = "read_only_governance"
            Label = "Read-only dashboard / packaging / audits / docs"
            Description = "Defense-safe dashboard, packaging, audit, and documentation surfaces built from stored outputs only."
            thesis_roles = @("read_only_governance")
        }
    )
}

function Get-LauncherRoleGroups {
    $matrix = Get-LauncherMatrix
    $roleGroups = @($matrix.role_groups)
    if ($roleGroups) {
        return @($roleGroups | Sort-Object menu_order, MenuKey, GroupId)
    }

    return Get-DefaultLauncherRoleGroups
}

function Get-LauncherRoleGroupById {
    param([Parameter(Mandatory = $true)][string]$GroupId)

    $match = Get-LauncherRoleGroups | Where-Object {
        ([string]$_.GroupId) -eq $GroupId -or ([string]$_.group_id) -eq $GroupId
    } | Select-Object -First 1
    if ($null -eq $match) {
        throw "Unknown launcher role group '$GroupId'."
    }

    return $match
}

function Get-LauncherRoleGroupThesisRoles {
    param([Parameter(Mandatory = $true)]$RoleGroup)

    $roles = @($RoleGroup.thesis_roles)
    if ($roles) {
        return @($roles | ForEach-Object { [string]$_ })
    }

    $roleGroupId = if ($RoleGroup.PSObject.Properties.Name -contains "GroupId" -and $RoleGroup.GroupId) {
        [string]$RoleGroup.GroupId
    } else {
        [string]$RoleGroup.group_id
    }

    switch ($roleGroupId) {
        "main_thesis_evidence" { return @("primary_evidence") }
        "support_context" { return @("support_context", "comparator_support") }
        "archive_provenance" { return @("archive_provenance") }
        "legacy_debug" { return @("legacy_support") }
        "read_only_governance" { return @("read_only_governance") }
        default { return @() }
    }
}

function Get-LauncherEntriesForRoleGroup {
    param([Parameter(Mandatory = $true)][string]$GroupId)

    $entries = Get-VisibleLauncherEntries
    $roleGroup = Get-LauncherRoleGroupById -GroupId $GroupId
    $roles = Get-LauncherRoleGroupThesisRoles -RoleGroup $roleGroup
    return @($entries | Where-Object { ([string]$_.thesis_role) -in $roles })
}

function Test-LauncherEntryMatchesRoleGroup {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [Parameter(Mandatory = $true)][string]$GroupId
    )

    try {
        $roleGroup = Get-LauncherRoleGroupById -GroupId $GroupId
        $roles = Get-LauncherRoleGroupThesisRoles -RoleGroup $roleGroup
        return ([string]$LauncherEntry.thesis_role -in $roles)
    }
    catch {
        return $false
    }
}

function Resolve-LauncherEntryReference {
    param(
        [Parameter(Mandatory = $true)][string]$Reference,
        [hashtable]$SelectionMap = @{},
        [string]$GroupId = "",
        [switch]$ThrowIfUnknown
    )

    $trimmedReference = ([string]$Reference).Trim()
    if ([string]::IsNullOrWhiteSpace($trimmedReference)) {
        if ($ThrowIfUnknown) {
            throw "Enter a visible menu number or an entry ID from this section."
        }
        return $null
    }

    $requestedEntryId = $null
    if ($SelectionMap.ContainsKey($trimmedReference)) {
        $requestedEntryId = [string]$SelectionMap[$trimmedReference]
    }

    if (-not $requestedEntryId) {
        $launcherEntry = Get-LauncherEntries | Where-Object { $_.entry_id -eq $trimmedReference } | Select-Object -First 1
        if ($null -eq $launcherEntry) {
            if ($ThrowIfUnknown) {
                $menuNumbers = (($SelectionMap.Keys | Sort-Object {[int]$_}) -join ", ")
                if ([string]::IsNullOrWhiteSpace($menuNumbers)) {
                    throw "Unknown selection '$trimmedReference'. Use an entry ID from this section."
                }
                throw "Unknown selection '$trimmedReference'. Use a visible menu number ($menuNumbers) or an entry ID from this section."
            }
            return $null
        }

        $requestedEntryId = [string]$launcherEntry.entry_id
    }

    $resolvedEntry = Resolve-LauncherEntryRequest -EntryId $requestedEntryId
    if ($GroupId -and -not (Test-LauncherEntryMatchesRoleGroup -LauncherEntry $resolvedEntry.CanonicalEntry -GroupId $GroupId)) {
        if ($ThrowIfUnknown) {
            throw "Entry '$trimmedReference' is not part of this section. Use a visible menu number or an entry ID shown in this menu."
        }
        return $null
    }

    return $resolvedEntry
}

function Invoke-LauncherRoleGroupInspectMode {
    param(
        [Parameter(Mandatory = $true)][string]$GroupId,
        [Parameter(Mandatory = $true)][hashtable]$SelectionMap,
        [Parameter(Mandatory = $true)][string]$Label
    )

    $selectionSummary = (($SelectionMap.Keys | Sort-Object {[int]$_}) | ForEach-Object {
        "{0}={1}" -f $_, $SelectionMap[$_]
    }) -join " | "
    $lastPreview = $null

    Write-Host ""
    Write-Host ("Inspect mode for {0}" -f $Label) -ForegroundColor Yellow
    Write-Host "No workflow will run from this prompt." -ForegroundColor White
    Write-Host "Enter a visible menu number or an entry ID from this section." -ForegroundColor White
    Write-Host "Press Enter, B, BACK, or 0 to return to this section." -ForegroundColor White
    Write-Host "Type C to cancel inspect mode, or Q to exit the launcher." -ForegroundColor White
    if (-not [string]::IsNullOrWhiteSpace($selectionSummary)) {
        Write-Host ("Visible choices: {0}" -f $selectionSummary) -ForegroundColor DarkGray
    }

    while ($true) {
        $allowedActions = @("entry", "back", "cancel", "quit")
        $allowedOptions = @("number", "entry_id", "B", "BACK", "0", "C", "CANCEL", "Q", "QUIT", "EXIT")
        $prompt = "Inspect [number | entry_id | Enter/B=back | C=cancel | Q=quit]"
        if ($null -ne $lastPreview) {
            $allowedActions += @("more", "export")
            $allowedOptions += @("M", "MORE", "E", "EXPORT", "EXPORT_PLAN")
            $prompt = "Inspect [number | entry_id | M=more | E=export plan | Enter/B=back | C=cancel | Q=quit]"
        }

        $choice = Resolve-LauncherChoice `
            -InputText (Read-Host $prompt) `
            -AllowedActions $allowedActions `
            -BlankAction "back" `
            -SelectionMap $SelectionMap `
            -GroupId $GroupId `
            -AllowedOptions $allowedOptions

        switch ($choice.Action) {
            "back" { return }
            "cancel" {
                Set-LauncherNotice -Message "Cancelled. No workflow was executed." -Level "warning"
                return
            }
            "quit" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
            "more" {
                Write-Host ""
                Write-Host "Full preview:" -ForegroundColor Cyan
                Write-LauncherEntryPreviewContent `
                    -LauncherEntry $lastPreview.CanonicalEntry `
                    -RequestedEntryId $lastPreview.RequestedEntryId `
                    -IncludeNotes
                Write-Host ""
                Write-Host "Inspect mode remains active. Enter another number or entry ID, or press Enter/B to return to this section." -ForegroundColor DarkGray
                continue
            }
            "export" {
                $exportedPlan = Export-LauncherRunPlan `
                    -LauncherEntry $lastPreview.CanonicalEntry `
                    -RequestedEntryId $lastPreview.RequestedEntryId
                Write-Host ""
                Write-Host "Run plan exported without executing science:" -ForegroundColor Green
                Write-Host ("  Markdown: {0}" -f $exportedPlan.MarkdownPath) -ForegroundColor DarkGray
                Write-Host ("  JSON:     {0}" -f $exportedPlan.JsonPath) -ForegroundColor DarkGray
                Write-Host "Inspect mode remains active. Enter another number or entry ID, or press Enter/B to return to this section." -ForegroundColor DarkGray
                continue
            }
            "entry" {
                $lastPreview = $choice.ResolvedEntry
                Write-LauncherEntryCompactPreview `
                    -LauncherEntry $lastPreview.CanonicalEntry `
                    -RequestedEntryId $lastPreview.RequestedEntryId
                Write-Host "  Type M or MORE for the full thesis-facing preview of this entry." -ForegroundColor DarkGray
                Write-Host "  Type E or EXPORT to write output\launcher_plans\<entry_id>.md/.json without running science." -ForegroundColor DarkGray
                Write-Host "  Inspect mode remains active. Enter another number or entry ID, or press Enter/B to return to this section." -ForegroundColor DarkGray
                continue
            }
            "invalid" {
                Write-Host ("Invalid inspect option '{0}'. Allowed options: {1}." -f $choice.RawInput, (Get-LauncherAllowedOptionsText -AllowedOptions $allowedOptions)) -ForegroundColor DarkYellow
                continue
            }
        }
    }
}

function Search-LauncherEntries {
    param(
        [Parameter(Mandatory = $true)][string]$Query,
        [string]$GroupId = ""
    )

    $normalizedQuery = ([string]$Query).Trim().ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($normalizedQuery)) {
        return @()
    }

    $candidateEntries = if ($GroupId) {
        Get-LauncherEntriesForRoleGroup -GroupId $GroupId
    } else {
        Get-VisibleLauncherEntries
    }

    $hiddenAliasMatches = @()
    if (-not $GroupId) {
        $hiddenAliasMatches = @(
            Get-HiddenLauncherEntries |
                Where-Object {
                    ([string]$_.entry_id).ToLowerInvariant().Contains($normalizedQuery) -or
                    ([string]$_.label).ToLowerInvariant().Contains($normalizedQuery) -or
                    ([string]$_.notes).ToLowerInvariant().Contains($normalizedQuery)
                }
        )
    }

    $allCandidates = @($candidateEntries) + @($hiddenAliasMatches)
    return @(
        $allCandidates |
            Where-Object {
                $entry = $_
                $searchText = @(
                    [string]$entry.entry_id,
                    [string]$entry.label,
                    [string]$entry.thesis_role,
                    [string]$entry.run_kind,
                    [string]$entry.category_id,
                    (Get-LauncherCategoryLabel -CategoryId ([string]$entry.category_id)),
                    [string]$entry.notes,
                    [string]$entry.claim_boundary,
                    [string]$entry.description
                ) -join " "
                $searchText.ToLowerInvariant().Contains($normalizedQuery)
            } |
            Sort-Object menu_order, entry_id
    )
}

function Write-LauncherSearchResults {
    param(
        [Parameter(Mandatory = $true)][array]$Results,
        [Parameter(Mandatory = $true)][hashtable]$SelectionMap
    )

    if (-not $Results) {
        Write-Host "No matching launcher entries." -ForegroundColor DarkYellow
        return
    }

    $index = 0
    foreach ($entry in $Results) {
        $index += 1
        $SelectionMap[[string]$index] = [string]$entry.entry_id
        $hiddenTag = if ([bool]$entry.menu_hidden) { " [hidden alias]" } else { "" }
        Write-Host ("  {0}. {1}{2}" -f $index, [string]$entry.entry_id, $hiddenTag) -ForegroundColor Yellow
        Write-Host ("     role={0} | run={1} | cost={2}" -f [string]$entry.thesis_role, [string]$entry.run_kind, [string]$entry.rerun_cost) -ForegroundColor White
        Write-Host ("     boundary={0}" -f [string]$entry.claim_boundary) -ForegroundColor Gray
    }
}

function Invoke-LauncherSearchMode {
    param(
        [string]$GroupId = "",
        [string]$Label = "all visible launcher entries"
    )

    $lastPreview = $null
    $selectionMap = @{}

    Write-Host ""
    Write-Host ("Search mode for {0}" -f $Label) -ForegroundColor Yellow
    Write-Host "No workflow will run from search mode." -ForegroundColor White
    Write-Host "Searches entry_id, label, thesis_role, run_kind, category, notes, description, and claim boundary." -ForegroundColor White
    Write-Host "Use B/BACK to return, C/CANCEL to cancel, or Q/QUIT to exit." -ForegroundColor White

    while ($true) {
        $allowedActions = @("back", "cancel", "quit")
        $allowedOptions = @("search text", "number", "entry_id", "B", "BACK", "0", "C", "CANCEL", "Q", "QUIT", "EXIT")
        $prompt = "Search [text | number/entry_id=inspect | B=back | C=cancel | Q=quit]"
        if ($null -ne $lastPreview) {
            $allowedActions += @("more", "export")
            $allowedOptions += @("M", "MORE", "E", "EXPORT", "EXPORT_PLAN")
            $prompt = "Search [text | number/entry_id=inspect | M=more | E=export | B=back | C=cancel | Q=quit]"
        }

        $inputText = Read-Host $prompt
        $choice = Resolve-LauncherChoice `
            -InputText $inputText `
            -AllowedActions $allowedActions `
            -BlankAction "back" `
            -AllowedOptions $allowedOptions

        switch ($choice.Action) {
            "back" { return }
            "cancel" {
                Set-LauncherNotice -Message "Cancelled. No workflow was executed." -Level "warning"
                return
            }
            "quit" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
            "more" {
                Write-Host ""
                Write-Host "Full preview:" -ForegroundColor Cyan
                Write-LauncherEntryPreviewContent `
                    -LauncherEntry $lastPreview.CanonicalEntry `
                    -RequestedEntryId $lastPreview.RequestedEntryId `
                    -IncludeNotes
                continue
            }
            "export" {
                $exportedPlan = Export-LauncherRunPlan `
                    -LauncherEntry $lastPreview.CanonicalEntry `
                    -RequestedEntryId $lastPreview.RequestedEntryId
                Write-Host ""
                Write-Host "Run plan exported without executing science:" -ForegroundColor Green
                Write-Host ("  Markdown: {0}" -f $exportedPlan.MarkdownPath) -ForegroundColor DarkGray
                Write-Host ("  JSON:     {0}" -f $exportedPlan.JsonPath) -ForegroundColor DarkGray
                continue
            }
        }

        $trimmedInput = ([string]$inputText).Trim()
        if ($selectionMap.ContainsKey($trimmedInput)) {
            $resolvedEntry = Resolve-LauncherEntryReference `
                -Reference $trimmedInput `
                -SelectionMap $selectionMap `
                -GroupId $GroupId `
                -ThrowIfUnknown
            $lastPreview = $resolvedEntry
            Write-LauncherEntryCompactPreview `
                -LauncherEntry $lastPreview.CanonicalEntry `
                -RequestedEntryId $lastPreview.RequestedEntryId
            Write-Host "  Type M or MORE for full preview; E or EXPORT writes a read-only run plan." -ForegroundColor DarkGray
            continue
        }

        $directResolvedEntry = Resolve-LauncherEntryReference `
            -Reference $trimmedInput `
            -GroupId $GroupId
        if ($directResolvedEntry) {
            $lastPreview = $directResolvedEntry
            Write-LauncherEntryCompactPreview `
                -LauncherEntry $lastPreview.CanonicalEntry `
                -RequestedEntryId $lastPreview.RequestedEntryId
            Write-Host "  Type M or MORE for full preview; E or EXPORT writes a read-only run plan." -ForegroundColor DarkGray
            continue
        }

        $selectionMap = @{}
        $results = Search-LauncherEntries -Query $trimmedInput -GroupId $GroupId
        Write-Host ""
        Write-Host ("Search results for '{0}':" -f $trimmedInput) -ForegroundColor Cyan
        Write-LauncherSearchResults -Results $results -SelectionMap $selectionMap
        if ($results) {
            Write-Host "Enter a result number or entry ID to inspect it, or enter a new search." -ForegroundColor DarkGray
        }
    }
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

    Invoke-ComposeCommand -ComposeArgs $dockerArgs 2>&1 | ForEach-Object {
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
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [switch]$SkipInteractiveProbe
    )

    $resolved = @{
        "RUN_STARTUP_PROMPTS_RESOLVED" = "1"
        "RUN_STARTUP_TOKEN" = "startup_$([guid]::NewGuid().ToString('N'))"
    }

    $explicitBudget = Get-ExplicitForcingBudgetSetting
    $explicitCachePolicy = Get-ExplicitInputCachePolicySetting
    $explicitPrototype2016EnsemblePolicy = Get-ExplicitPrototype2016EnsemblePolicySetting
    $interactivePromptAllowed = (-not $SkipInteractiveProbe) -and (Test-LauncherStartupPromptInteractive)
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

    foreach ($passthroughKey in @("DEFENSE_SMOKE_TEST", "DEFENSE_NO_SCIENCE")) {
        $passthroughValue = [string](Get-Item -Path ("Env:" + $passthroughKey) -ErrorAction SilentlyContinue).Value
        if (-not [string]::IsNullOrWhiteSpace($passthroughValue)) {
            $resolved[$passthroughKey] = $passthroughValue
        }
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
        return (New-LauncherResult -Status "phase_completed")
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
        return (New-LauncherResult -Status "phase_skipped")
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
            $decision = Resolve-LauncherChoice `
                -InputText (Read-Host "Type R to reuse validated cache, or C to cancel") `
                -AllowedActions @("cancel") `
                -AllowedOptions @("R", "C", "CANCEL")
            if ($decision.RawInput.ToUpperInvariant() -eq "R") {
                $retryEnv = @{}
                foreach ($key in $phaseEnv.Keys) {
                    $retryEnv[$key] = $phaseEnv[$key]
                }
                $retryEnv["PREP_REUSE_APPROVED_SOURCE"] = [string]$payload.source_id
                $retryEnv["PREP_REUSE_APPROVED_ONCE"] = "1"
                return (Invoke-DockerPhase `
                    -Phase $Phase `
                    -Description "$Description (reuse approved for $($payload.source_id))" `
                    -WorkflowMode $WorkflowMode `
                    -Service $Service `
                    -ExtraEnv $retryEnv `
                    -ReuseDecisionConsumed)
            }
            if ($decision.Action -eq "cancel") {
                Update-PrepManifestCancelledByUser `
                    -RunName ([string]$payload.run_name) `
                    -SourceId ([string]$payload.source_id) `
                    -CachePath ([string]$payload.cache_path) `
                    -RemoteError ([string]$payload.error) `
                    -Validation $payload.validation
                return (New-LauncherResult -Status "cancelled" -Message ("Cancelled. Prep reuse was declined for source '{0}'." -f [string]$payload.source_id))
            }
            Write-Host "Invalid option. Allowed options: R, C, CANCEL." -ForegroundColor DarkYellow
        }
    }

    throw "Phase '$Phase' failed in service '$Service' with exit code $($phaseResult.ExitCode)."
}

function Start-LauncherTranscript {
    param([Parameter(Mandatory = $true)][string]$Path)

    try {
        Start-Transcript -Path $Path -Append | Out-Null
        return $true
    }
    catch {
        Write-Host "WARNING - Transcript could not be started. Continuing without transcript logging." -ForegroundColor DarkYellow
        Write-Host ("  {0}" -f $_.Exception.Message) -ForegroundColor Yellow
        return $false
    }
}

function Stop-LauncherTranscriptSafe {
    param([bool]$TranscriptStarted = $false)

    if (-not $TranscriptStarted) {
        return
    }

    try {
        Stop-Transcript | Out-Null
    }
    catch {
        Write-Host "WARNING - Transcript could not be stopped cleanly." -ForegroundColor DarkYellow
        Write-Host ("  {0}" -f $_.Exception.Message) -ForegroundColor Yellow
    }
}

function Get-LauncherStepCommandPreview {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [Parameter(Mandatory = $true)]$Step,
        [hashtable]$ExtraEnv = @{}
    )

    $parts = @((Get-ComposeCommandText), "exec", "-T")
    foreach ($key in ($ExtraEnv.Keys | Sort-Object)) {
        $parts += @("-e", ("{0}={1}" -f $key, $ExtraEnv[$key]))
    }
    $parts += @(
        "-e", ("WORKFLOW_MODE={0}" -f [string]$LauncherEntry.workflow_mode),
        "-e", ("PIPELINE_PHASE={0}" -f [string]$Step.phase),
        [string]$Step.service,
        "python",
        "-m",
        "src"
    )

    return ($parts -join " ")
}

function New-LauncherRunPlan {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = "",
        [hashtable]$StartupEnv = $null
    )

    if ($null -eq $StartupEnv) {
        $StartupEnv = Get-LauncherEnvPreview -LauncherEntry $LauncherEntry
    }

    $stepPlans = @()
    $stepIndex = 0
    foreach ($step in @($LauncherEntry.steps)) {
        $stepIndex += 1
        $stepExtraEnv = Merge-Hashtables `
            -Base $StartupEnv `
            -Override (ConvertTo-Hashtable -InputObject $step.extra_env)
        $stepPlans += [pscustomobject]@{
            index = $stepIndex
            service = [string]$step.service
            phase = [string]$step.phase
            description = [string]$step.description
            environment = $stepExtraEnv
            prompt_free_command = Get-LauncherStepCommandPreview -LauncherEntry $LauncherEntry -Step $step -ExtraEnv $stepExtraEnv
        }
    }

    return [pscustomobject]@{
        generated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        entry_id = [string]$LauncherEntry.entry_id
        requested_alias = if ((-not [string]::IsNullOrWhiteSpace($RequestedEntryId)) -and ([string]$RequestedEntryId -ne [string]$LauncherEntry.entry_id)) { [string]$RequestedEntryId } else { "" }
        canonical_entry_id = [string]$LauncherEntry.entry_id
        label = [string]$LauncherEntry.label
        category = Get-LauncherCategoryLabel -CategoryId ([string]$LauncherEntry.category_id)
        thesis_role = [string]$LauncherEntry.thesis_role
        thesis_role_label = Format-ThesisRoleLabel -Role ([string]$LauncherEntry.thesis_role)
        claim_boundary = [string]$LauncherEntry.claim_boundary
        run_kind = [string]$LauncherEntry.run_kind
        rerun_cost = [string]$LauncherEntry.rerun_cost
        safe_default = [bool]$LauncherEntry.safe_default
        requires_explicit_confirmation = [bool](Test-LauncherEntryNeedsPreview -LauncherEntry $LauncherEntry)
        services_and_phases = Get-LauncherEntryShortStepSummary -LauncherEntry $LauncherEntry
        startup_environment = $StartupEnv
        startup_prompts = Get-LauncherStartupPromptText -LauncherEntry $LauncherEntry
        may_write_outputs = Get-LauncherEntryMayWriteOutputsText -LauncherEntry $LauncherEntry
        expected_output_directories = @(Get-LauncherEntryExpectedOutputDirs -LauncherEntry $LauncherEntry)
        output_warning = Get-LauncherEntryOutputWarning -LauncherEntry $LauncherEntry
        interactive_command = Get-LauncherEntryInteractiveCommand -LauncherEntry $LauncherEntry
        prompt_free_prepare_command = ("{0} up -d" -f (Get-ComposeCommandText))
        prompt_free_steps = $stepPlans
        no_workflow_executed = $true
    }
}

function ConvertTo-LauncherMarkdownList {
    param([object[]]$Items)

    if (-not $Items) {
        return @("- None documented")
    }

    return @($Items | ForEach-Object { "- {0}" -f [string]$_ })
}

function Export-LauncherRunPlan {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = ""
    )

    $startupEnv = Get-LauncherEnvPreview -LauncherEntry $LauncherEntry
    $plan = New-LauncherRunPlan `
        -LauncherEntry $LauncherEntry `
        -RequestedEntryId $RequestedEntryId `
        -StartupEnv $startupEnv
    $outputDir = Join-Path $Script:RepoRoot "output\launcher_plans"
    if (-not (Test-Path -LiteralPath $outputDir)) {
        New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
    }

    $entryId = [string]$LauncherEntry.entry_id
    $jsonPath = Join-Path $outputDir ("{0}.json" -f $entryId)
    $markdownPath = Join-Path $outputDir ("{0}.md" -f $entryId)

    $plan | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $jsonPath -Encoding UTF8

    $requestedAliasForPlan = if ($plan.requested_alias) { $plan.requested_alias } else { "none" }
    $markdownLines = @(
        ("# Launcher Run Plan: {0}" -f $entryId),
        "",
        ('- Entry ID: `{0}`' -f $plan.entry_id),
        ('- Requested alias: `{0}`' -f $requestedAliasForPlan),
        ('- Canonical entry ID: `{0}`' -f $plan.canonical_entry_id),
        ("- Label: {0}" -f $plan.label),
        ('- Thesis role: `{0}` ({1})' -f $plan.thesis_role, $plan.thesis_role_label),
        ("- Claim boundary: {0}" -f $plan.claim_boundary),
        ('- Run kind: `{0}`' -f $plan.run_kind),
        ('- Rerun cost: `{0}`' -f $plan.rerun_cost),
        ('- Services and phases: `{0}`' -f $plan.services_and_phases),
        ('- Safe default: `{0}`' -f ([string]$plan.safe_default).ToLowerInvariant()),
        ('- Requires explicit confirmation: `{0}`' -f ([string]$plan.requires_explicit_confirmation).ToLowerInvariant()),
        ("- Startup prompts: {0}" -f $plan.startup_prompts),
        ("- May write outputs: {0}" -f $plan.may_write_outputs),
        ("- Output warning: {0}" -f $plan.output_warning),
        "",
        "## Startup Environment",
        ""
    )
    foreach ($key in ($startupEnv.Keys | Sort-Object)) {
        $markdownLines += ('- `{0}={1}`' -f $key, $startupEnv[$key])
    }
    $markdownLines += @(
        "",
        "## Expected Output Directories",
        ""
    )
    $markdownLines += ConvertTo-LauncherMarkdownList -Items $plan.expected_output_directories
    $markdownLines += @(
        "",
        "## Exact Commands",
        "",
        ('Interactive launcher command: `{0}`' -f $plan.interactive_command),
        "",
        "Prompt-free docker compose command sequence:",
        "",
        '```powershell',
        $plan.prompt_free_prepare_command
    )
    foreach ($stepPlan in @($plan.prompt_free_steps)) {
        $markdownLines += [string]$stepPlan.prompt_free_command
    }
    $markdownLines += @(
        '```',
        "",
        "No workflow was executed while exporting this plan."
    )

    $markdownLines | Set-Content -LiteralPath $markdownPath -Encoding UTF8

    return [pscustomobject]@{
        JsonPath = $jsonPath
        MarkdownPath = $markdownPath
    }
}

function Show-LauncherDryRunPlan {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = ""
    )

    $entryStartupEnv = Resolve-LauncherStartupEnv -LauncherEntry $LauncherEntry -SkipInteractiveProbe
    Show-LauncherEntryPreview -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId -StartupEnv $entryStartupEnv
    Write-Host ""
    Write-Host "Dry-run startup policy:" -ForegroundColor Yellow
    Write-Host "  INPUT_CACHE_POLICY=$($entryStartupEnv['INPUT_CACHE_POLICY'])" -ForegroundColor DarkGray
    Write-Host "  FORCING_SOURCE_BUDGET_SECONDS=$($entryStartupEnv['FORCING_SOURCE_BUDGET_SECONDS'])" -ForegroundColor DarkGray
    if ($entryStartupEnv.ContainsKey("PROTOTYPE_2016_ENSEMBLE_POLICY")) {
        Write-Host "  PROTOTYPE_2016_ENSEMBLE_POLICY=$($entryStartupEnv['PROTOTYPE_2016_ENSEMBLE_POLICY'])" -ForegroundColor DarkGray
    }

    Write-Host ""
    Write-Host "Exact commands that would run:" -ForegroundColor Yellow
    Write-LauncherPromptFreeCommands -LauncherEntry $LauncherEntry -StartupEnv $entryStartupEnv

    Write-Host ""
    Write-Host "Dry run only. No Docker commands were executed and no outputs were modified." -ForegroundColor Green
    Write-Host "No workflow was executed." -ForegroundColor Green
}

function Invoke-LauncherEntry {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$RequestedEntryId = ""
    )

    if (Test-LauncherDryRunRequested) {
        Show-LauncherDryRunPlan -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId
        if ($ExportPlan) {
            $exportedPlan = Export-LauncherRunPlan -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId
            Write-Host ""
            Write-Host "Run plan exported without executing science:" -ForegroundColor Green
            Write-Host ("  Markdown: {0}" -f $exportedPlan.MarkdownPath) -ForegroundColor DarkGray
            Write-Host ("  JSON:     {0}" -f $exportedPlan.JsonPath) -ForegroundColor DarkGray
        }
        return (New-LauncherResult -Status "dry_run" -Message "Dry run only. No Docker commands were executed." -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId -NoWorkflowExecuted $true)
    }

    $confirmationResult = Confirm-LauncherEntryRun -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId
    if ($confirmationResult.Status -eq "cancelled") {
        Write-LauncherCancelledMessage -Message $confirmationResult.Message
        return $confirmationResult
    }

    if (-not (Resolve-ComposeMode)) {
        $message = Write-ComposeUnavailableMessage -ActionLabel ("Launcher entry '{0}'" -f [string]$LauncherEntry.entry_id)
        return (New-LauncherResult -Status "docker_unavailable" -Message $message -ExitCode 2 -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId -NoWorkflowExecuted $true)
    }

    Ensure-Directories
    $entryId = [string]$LauncherEntry.entry_id
    $workflowMode = [string]$LauncherEntry.workflow_mode
    $logFile = "logs\run_${entryId}_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    $startTime = Get-Date
    $transcriptStarted = Start-LauncherTranscript -Path $logFile

    try {
        Write-Host "Starting Docker containers..." -ForegroundColor Yellow
        $previousErrorActionPreference = $ErrorActionPreference
        try {
            # Docker may emit "Container ... Running" on stderr even when the command succeeds.
            # Treat that output as normal launcher chatter instead of a terminating PowerShell error.
            $ErrorActionPreference = "Continue"
            Invoke-ComposeCommand -ComposeArgs @("up", "-d") 2>&1 | ForEach-Object { Write-ProcessLine $_ }
            $composeExitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $previousErrorActionPreference
        }
        if ($composeExitCode -ne 0) {
            throw ("{0} up -d failed with exit code {1}." -f (Get-ComposeCommandText), $composeExitCode)
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
            $phaseInvocationResult = Invoke-DockerPhase `
                -Phase ([string]$step.phase) `
                -Description ([string]$step.description) `
                -WorkflowMode $workflowMode `
                -Service ([string]$step.service) `
                -ExtraEnv $stepExtraEnv
            if ($phaseInvocationResult -and $phaseInvocationResult.Status -eq "cancelled") {
                Write-Host ""
                Write-Host ($phaseInvocationResult.Message) -ForegroundColor Yellow
                Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
                return (New-LauncherResult -Status "cancelled" -Message $phaseInvocationResult.Message -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId)
            }
        }

        $duration = (Get-Date) - $startTime
        Write-Host ""
        Write-Host "[SUCCESS] Launcher entry completed." -ForegroundColor Green
        Write-Host "Entry ID: $entryId" -ForegroundColor Yellow
        Write-Host ("Runtime: {0:D2}h {1:D2}m {2:D2}s" -f $duration.Hours, $duration.Minutes, $duration.Seconds) -ForegroundColor Yellow
        Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
        return (New-LauncherResult -Status "success" -Message "Launcher entry completed." -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId)
    }
    catch {
        Write-Host ""
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "Log saved to: $logFile" -ForegroundColor DarkGray
        return (New-LauncherResult -Status "failed" -Message $_.Exception.Message -ExitCode 1 -LauncherEntry $LauncherEntry -RequestedEntryId $RequestedEntryId)
    }
    finally {
        Stop-LauncherTranscriptSafe -TranscriptStarted $transcriptStarted
    }
}

function Start-DelayedBrowserLaunch {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [string]$Label = "read-only dashboard page",
        [int]$WaitTimeoutSeconds = 30
    )

    $escapedUrl = $Url.Replace("'", "''")
    $launcherScript = @"
`$targetUrl = '$escapedUrl'
`$deadline = (Get-Date).AddSeconds($WaitTimeoutSeconds)
while ((Get-Date) -lt `$deadline) {
    try {
        `$client = New-Object System.Net.Sockets.TcpClient
        `$async = `$client.BeginConnect('127.0.0.1', 8501, `$null, `$null)
        if (`$async.AsyncWaitHandle.WaitOne(1000)) {
            `$client.EndConnect(`$async)
            `$client.Close()
            break
        }
        `$client.Close()
    }
    catch {
    }
    Start-Sleep -Milliseconds 500
}
Start-Process `$targetUrl
"@

    try {
        Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-Command", $launcherScript) -WindowStyle Hidden | Out-Null
        Write-Host ("Opening {0} in your browser when the local UI is ready:" -f $Label) -ForegroundColor DarkGray
        Write-Host ("  {0}" -f $Url) -ForegroundColor DarkGray
    }
    catch {
        Write-Host ("Could not schedule automatic browser opening for {0}." -f $Label) -ForegroundColor DarkYellow
        Write-Host ("Open this URL manually once the UI starts: {0}" -f $Url) -ForegroundColor Yellow
    }
}

function Test-ReadOnlyUiHealth {
    param([int]$Port = 8501)

    $healthUrl = "http://127.0.0.1:$Port/_stcore/health"
    try {
        $response = Invoke-WebRequest `
            -UseBasicParsing `
            -Uri $healthUrl `
            -TimeoutSec 2 `
            -ErrorAction Stop
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300)
    }
    catch {
        return $false
    }
}

function Test-ReadOnlyUiProcessRunning {
    $probeCommand = "(pgrep -f 'streamlit run ui/app.py' >/dev/null 2>&1) || (ps -ef 2>/dev/null | grep -F 'streamlit run ui/app.py' | grep -v grep >/dev/null 2>&1)"
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        Invoke-ComposeCommand -ComposeArgs @("exec", "-T", "pipeline", "sh", "-lc", $probeCommand) *> $null
        $probeExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    return ($probeExitCode -eq 0)
}

function Stop-ReadOnlyUiProcess {
    param([switch]$Quiet)

    $stopCommand = "pkill -f 'streamlit run ui/app.py' >/dev/null 2>&1 || true"
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        Invoke-ComposeCommand -ComposeArgs @("exec", "-T", "pipeline", "sh", "-lc", $stopCommand) 2>&1 | ForEach-Object {
            if (-not $Quiet) {
                Write-ProcessLine $_
            }
        }
        $stopExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    return ($stopExitCode -eq 0)
}

function Invoke-ReadOnlyUi {
    param(
        [switch]$RestartPipeline,
        [string]$LandingPath = ""
    )

    if (-not (Resolve-ComposeMode)) {
        Write-ComposeUnavailableMessage -ActionLabel "The read-only dashboard"
        return $false
    }

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
            Invoke-ComposeCommand -ComposeArgs @("up", "-d") 2>&1 | ForEach-Object { Write-ProcessLine $_ }
            $composeExitCode = $LASTEXITCODE
            if ($composeExitCode -eq 0) {
                Invoke-ComposeCommand -ComposeArgs @("restart", "pipeline", "gnome") 2>&1 | ForEach-Object { Write-ProcessLine $_ }
                $composeExitCode = $LASTEXITCODE
            }
        } else {
            Invoke-ComposeCommand -ComposeArgs @("up", "-d") 2>&1 | ForEach-Object { Write-ProcessLine $_ }
            $composeExitCode = $LASTEXITCODE
        }
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($composeExitCode -ne 0) {
        if ($RestartPipeline) {
            throw ("{0} up/restart for the full compose stack failed with exit code {1}." -f (Get-ComposeCommandText), $composeExitCode)
        }
        throw ("{0} up -d failed with exit code {1}." -f (Get-ComposeCommandText), $composeExitCode)
    }

    $landingUrl = "http://localhost:8501"
    if ($LandingPath) {
        $landingUrl = "{0}/{1}" -f $landingUrl.TrimEnd("/"), $LandingPath.TrimStart("/")
    }

    if ((-not $RestartPipeline) -and (Test-ReadOnlyUiHealth)) {
        Write-Host ""
        Write-Host "Read-only Streamlit UI is already running." -ForegroundColor Green
        Write-Host ("Open {0}" -f $landingUrl) -ForegroundColor Yellow
        Write-Host "Use R / RESTART from panel mode if you need a clean dashboard process." -ForegroundColor DarkGray
        if ($LandingPath) {
            Start-DelayedBrowserLaunch -Url $landingUrl -Label "requested dashboard page"
        }
        return $true
    }

    if ((-not $RestartPipeline) -and (Test-ReadOnlyUiProcessRunning)) {
        Write-Host ""
        Write-Host "Found a stale Streamlit process in the pipeline container; stopping it before relaunch." -ForegroundColor Yellow
        [void](Stop-ReadOnlyUiProcess -Quiet)
        Start-Sleep -Seconds 1
    }

    Write-Host ""
    Write-Host "Launching read-only Streamlit UI..." -ForegroundColor Yellow
    Write-Host ("Open {0} while this process is running." -f $landingUrl) -ForegroundColor DarkGray
    Write-Host "Press Ctrl+C to stop the UI and return to the launcher." -ForegroundColor DarkGray
    if ($LandingPath) {
        Start-DelayedBrowserLaunch -Url $landingUrl -Label "requested dashboard page"
    }

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
        Invoke-ComposeCommand -ComposeArgs $uiArgs 2>&1 | ForEach-Object { Write-ProcessLine $_ }
        $uiExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($uiExitCode -ne 0) {
        if (Test-ReadOnlyUiHealth) {
            Write-Host ""
            Write-Host "Read-only Streamlit UI is already running." -ForegroundColor Green
            Write-Host ("Open {0}" -f $landingUrl) -ForegroundColor Yellow
            Write-Host "Use R / RESTART from panel mode if you need a clean dashboard process." -ForegroundColor DarkGray
            return $true
        }
        throw "Read-only UI exited with code $uiExitCode."
    }

    return $true
}

function Invoke-ContainerPythonScript {
    param(
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][string]$ScriptPath,
        [string]$Service = "pipeline",
        [string[]]$ScriptArgs = @(),
        [hashtable]$ExtraEnv = @{}
    )

    if (-not (Resolve-ComposeMode)) {
        Write-ComposeUnavailableMessage -ActionLabel $Description
        return $false
    }

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
        Invoke-ComposeCommand -ComposeArgs $dockerArgs 2>&1 | ForEach-Object { Write-ProcessLine $_ }
        $scriptExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($scriptExitCode -ne 0) {
        throw "Container Python script exited with code $scriptExitCode."
    }

    return $true
}

function Get-DisplayPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction SilentlyContinue
    if ($resolved) {
        return $resolved.Path
    }

    return (Join-Path $Script:RepoRoot $Path)
}

function Open-FileInDefaultApp {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Label
    )

    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction SilentlyContinue
    if (-not $resolved) {
        Write-Host "$Label was not found." -ForegroundColor DarkYellow
        Write-Host "  $(Get-DisplayPath -Path $Path)" -ForegroundColor Yellow
        return
    }

    try {
        Invoke-Item -LiteralPath $resolved.Path
        Write-Host "Opened $Label in your default app:" -ForegroundColor Yellow
        Write-Host "  $($resolved.Path)" -ForegroundColor Green
    }
    catch {
        Write-Host "Could not open $Label automatically. Use this path instead:" -ForegroundColor DarkYellow
        Write-Host "  $($resolved.Path)" -ForegroundColor Green
    }
}

function Show-PanelVerificationQuickActions {
    param([Parameter(Mandatory = $true)][array]$PanelOutputs)

    while ($true) {
        Write-Host ""
        Write-Host "Open another verification file?" -ForegroundColor Yellow
        Write-Host ("  1. {0} (recommended)" -f $PanelOutputs[0].Label) -ForegroundColor White
        Write-Host "  2. Readable summary (.md)" -ForegroundColor White
        Write-Host "  3. JSON report" -ForegroundColor White
        Write-Host "  4. Output folder" -ForegroundColor White
        if ($PanelOutputs[1].Path -ne $PanelOutputs[0].Path) {
            Write-Host ("  5. {0}" -f $PanelOutputs[1].Label) -ForegroundColor White
        }
        Write-Host "  Enter. Return to panel menu" -ForegroundColor White
        Write-Host ""

        $choice = (Read-Host "Open").Trim()
        switch ($choice.ToUpperInvariant()) {
            "" { return }
            "1" { Open-FileInDefaultApp -Path $PanelOutputs[0].Path -Label $PanelOutputs[0].Label }
            "2" { Open-FileInDefaultApp -Path $PanelOutputs[2].Path -Label $PanelOutputs[2].Label }
            "3" { Open-FileInDefaultApp -Path $PanelOutputs[3].Path -Label $PanelOutputs[3].Label }
            "4" { Open-FileInDefaultApp -Path $PanelOutputs[5].Path -Label $PanelOutputs[5].Label }
            "5" {
                if ($PanelOutputs[1].Path -ne $PanelOutputs[0].Path) {
                    Open-FileInDefaultApp -Path $PanelOutputs[1].Path -Label $PanelOutputs[1].Label
                } else {
                    Write-Host "There is no separate fallback file for this run." -ForegroundColor DarkYellow
                }
            }
            default {
                if ($PanelOutputs[1].Path -ne $PanelOutputs[0].Path) {
                    Write-Host "Use 1-5, or press Enter to return." -ForegroundColor Red
                } else {
                    Write-Host "Use 1-4, or press Enter to return." -ForegroundColor Red
                }
            }
        }
    }
}

function Export-CsvToExcelWorkbook {
    param(
        [Parameter(Mandatory = $true)][string]$CsvPath,
        [Parameter(Mandatory = $true)][string]$WorkbookPath
    )

    $resolvedCsv = Resolve-Path -LiteralPath $CsvPath -ErrorAction SilentlyContinue
    if (-not $resolvedCsv) {
        Write-Host "Could not create the Excel workbook because the CSV report was not found." -ForegroundColor DarkYellow
        Write-Host "  $(Get-DisplayPath -Path $CsvPath)" -ForegroundColor Yellow
        return $null
    }

    $resolvedWorkbookPath = Get-DisplayPath -Path $WorkbookPath
    $workbookDir = Split-Path -Parent $resolvedWorkbookPath
    if (-not (Test-Path -LiteralPath $workbookDir)) {
        New-Item -ItemType Directory -Force -Path $workbookDir | Out-Null
    }

    Remove-Item -LiteralPath $resolvedWorkbookPath -Force -ErrorAction SilentlyContinue

    $excel = $null
    $workbook = $null
    try {
        $excel = New-Object -ComObject Excel.Application
        $excel.Visible = $false
        $excel.DisplayAlerts = $false

        $workbook = $excel.Workbooks.Open($resolvedCsv.Path)
        $worksheet = $workbook.Worksheets.Item(1)
        $worksheet.Name = "Panel Review Check"
        $worksheet.Rows.Item(1).Font.Bold = $true
        $worksheet.Application.ActiveWindow.SplitRow = 1
        $worksheet.Application.ActiveWindow.FreezePanes = $true
        $worksheet.Columns.AutoFit() | Out-Null

        # 51 = xlOpenXMLWorkbook (.xlsx)
        $workbook.SaveAs($resolvedWorkbookPath, 51)
        $workbook.Close($false)
        $excel.Quit()

        return $resolvedWorkbookPath
    }
    catch {
        Remove-Item -LiteralPath $resolvedWorkbookPath -Force -ErrorAction SilentlyContinue
        Write-Host "Excel workbook creation failed. Falling back to the CSV report." -ForegroundColor DarkYellow
        Write-Host "  $($_.Exception.Message)" -ForegroundColor Yellow
        return $null
    }
    finally {
        if ($workbook) {
            try {
                [void]$workbook.Close($false)
            }
            catch {
            }
            [System.Runtime.InteropServices.Marshal]::ReleaseComObject($workbook) | Out-Null
        }
        if ($excel) {
            try {
                [void]$excel.Quit()
            }
            catch {
            }
            [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
        }
        [GC]::Collect()
        [GC]::WaitForPendingFinalizers()
    }
}

function Invoke-LauncherMatrixValidation {
    Clear-Host
    Write-Section "LAUNCHER MATRIX VALIDATION"
    Write-Host ""
    Write-Host "Validating config\launcher_matrix.json without Docker or science execution..." -ForegroundColor Yellow
    Write-Host ""

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & python -m src.utils.validate_launcher_matrix 2>&1 | ForEach-Object { Write-ProcessLine $_ }
        $validationExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    Write-Host ""
    if ($validationExitCode -eq 0) {
        Write-Host "[SUCCESS] Launcher matrix validation passed." -ForegroundColor Green
        return (New-LauncherResult -Status "success" -Message "Launcher matrix validation passed." -ExitCode 0 -NoWorkflowExecuted $true)
    }

    Write-Host "[FAIL] Launcher matrix validation found issues." -ForegroundColor Red
    return (New-LauncherResult -Status "failed" -Message "Launcher matrix validation found issues." -ExitCode $validationExitCode -NoWorkflowExecuted $true)
}

function Show-LauncherList {
    param([string]$Role)

    $matrix = Get-LauncherMatrix
    $compose = Get-ComposeCommandText
    $normalizedRole = [string]$Role

    if ($normalizedRole) {
        $normalizedRole = $normalizedRole.Trim().ToLowerInvariant()
        if ($normalizedRole -notin (Get-ValidThesisRoles)) {
            throw "Unknown thesis role '$Role'. Valid roles: $((Get-ValidThesisRoles) -join ', ')"
        }
    }

    Clear-Host
    Write-Section "CURRENT LAUNCHER CATALOG"
    Write-Host ""
    Write-Host "Defense default: .\panel.ps1 or .\start.ps1 -Panel" -ForegroundColor Green
    Write-Host "Full launcher: .\start.ps1  # researcher/audit path" -ForegroundColor Yellow
    Write-Host "Catalog: $($matrix.catalog_version)" -ForegroundColor Yellow
    Write-Host "List by role: .\start.ps1 -ListRole <thesis_role> -NoPause" -ForegroundColor Yellow
    Write-Host "Validate catalog: .\start.ps1 -ValidateMatrix -NoPause" -ForegroundColor Yellow
    Write-Host "Explain one entry: .\start.ps1 -Explain <entry_id> -NoPause" -ForegroundColor Yellow
    Write-Host "Dry-run one entry: .\start.ps1 -Entry <entry_id> -DryRun -NoPause" -ForegroundColor Yellow
    Write-Host ("Prompt-free container run: {0} exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src" -f $compose) -ForegroundColor Yellow
    Write-Host ("Read-only UI: {0} exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" -f $compose) -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Use user-facing entry IDs and thesis-role groupings here. Raw phase names are not the primary startup commands." -ForegroundColor White
    Write-Host "Shared controls: B/BACK/0=back, C/CANCEL=cancel, Q/QUIT/EXIT=exit, H/HELP=help, L/LIST=list, S/SEARCH=search, P/PANEL=panel, U/UI=dashboard, R/RESTART=dashboard restart when available." -ForegroundColor White
    Write-Host ""

    if ($normalizedRole) {
        $matchingEntries = @(
            Get-LauncherEntries |
                Where-Object { ([string]$_.thesis_role).ToLowerInvariant() -eq $normalizedRole } |
                Sort-Object menu_order, entry_id
        )
        Write-Host ("Filtered thesis role: {0}" -f (Format-ThesisRoleLabel -Role $normalizedRole)) -ForegroundColor Cyan
        Write-Host ""
        if (-not $matchingEntries) {
            Write-Host "No launcher entries currently match that thesis role." -ForegroundColor DarkYellow
        } else {
            foreach ($entry in $matchingEntries) {
                Write-LauncherEntrySummary -LauncherEntry $entry -IncludeHiddenMarker
                Write-Host ""
            }
        }
    } else {
        foreach ($group in Get-LauncherRoleGroups) {
            $groupEntries = Get-LauncherEntriesForRoleGroup -GroupId ([string]$group.GroupId)
            Write-Host ("{0}. {1}" -f [string]$group.MenuKey, [string]$group.Label) -ForegroundColor Cyan
            Write-Host ("   {0}" -f [string]$group.Description) -ForegroundColor DarkGray
            foreach ($entry in $groupEntries) {
                Write-LauncherEntrySummary -LauncherEntry $entry
                Write-Host ""
            }
        }

        $hiddenEntries = Get-HiddenLauncherEntries
        if ($hiddenEntries) {
            Write-Host "Hidden compatibility / experimental IDs" -ForegroundColor Cyan
            Write-Host "   These remain valid for older scripts or deliberate experiments, but they stay out of the default launcher menu." -ForegroundColor DarkGray
            foreach ($entry in $hiddenEntries) {
                Write-LauncherEntrySummary -LauncherEntry $entry -IncludeHiddenMarker
                Write-Host ""
            }
        }
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
    $compose = Get-ComposeCommandText

    Clear-Host
    Write-Section "LAUNCHER HELP"
    Write-Host ""
    Write-Host "Panel-safe default path:" -ForegroundColor Yellow
    Write-Host "  .\panel.ps1" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Panel -NoPause" -ForegroundColor Green
    Write-Host ""
    Write-Host "Full launcher / researcher-audit path:" -ForegroundColor Yellow
    Write-Host "  .\start.ps1" -ForegroundColor Green
    Write-Host "  .\start.ps1 -List -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -ListRole primary_evidence -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -ValidateMatrix -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -ExportPlan -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry <entry_id>" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry <entry_id> -DryRun -NoPause" -ForegroundColor Green
    Write-Host ""
    Write-Host "Preferred user-facing entry IDs:" -ForegroundColor Yellow
    Write-Host "  phase1_mindoro_focus_provenance" -ForegroundColor White
    Write-Host "  mindoro_phase3b_primary_public_validation" -ForegroundColor White
    Write-Host "  mindoro_reportable_core" -ForegroundColor White
    Write-Host "  dwh_reportable_bundle" -ForegroundColor White
    Write-Host "  mindoro_phase4_only" -ForegroundColor White
    Write-Host "  mindoro_appendix_sensitivity_bundle" -ForegroundColor White
    Write-Host "  phase1_regional_reference_rerun" -ForegroundColor White
    Write-Host "  mindoro_march13_14_phase1_focus_trial" -ForegroundColor White
    Write-Host "  mindoro_march6_recovery_sensitivity" -ForegroundColor White
    Write-Host "  mindoro_march23_extended_public_stress_test" -ForegroundColor White
    Write-Host "  b1_drifter_context_panel" -ForegroundColor White
    Write-Host "  phase1_audit" -ForegroundColor White
    Write-Host "  phase2_audit" -ForegroundColor White
    Write-Host "  final_validation_package" -ForegroundColor White
    Write-Host "  phase5_sync" -ForegroundColor White
    Write-Host "  trajectory_gallery" -ForegroundColor White
    Write-Host "  trajectory_gallery_panel" -ForegroundColor White
    Write-Host "  figure_package_publication" -ForegroundColor White
    Write-Host "  prototype_legacy_final_figures" -ForegroundColor White
    Write-Host "  prototype_2021_bundle" -ForegroundColor White
    Write-Host "  prototype_legacy_bundle" -ForegroundColor White
    Write-Host "  Read-only dashboard launch: panel option 1 or U/UI shortcut (no separate entry_id)" -ForegroundColor White
    Write-Host ""
    Write-Host "Hidden compatibility IDs still work, but they are no longer the preferred wording:" -ForegroundColor Yellow
    Write-Host "  phase1_mindoro_focus_pre_spill_experiment" -ForegroundColor Gray
    Write-Host "  phase1_production_rerun" -ForegroundColor Gray
    Write-Host "  mindoro_march13_14_noaa_reinit_stress_test" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Read-only / packaging-safe examples:" -ForegroundColor Yellow
    Write-Host "  .\start.ps1 -Entry b1_drifter_context_panel" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry phase1_audit" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry final_validation_package" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry phase5_sync" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry figure_package_publication" -ForegroundColor Green
    Write-Host ""
    Write-Host "Shared controls:" -ForegroundColor Yellow
    Write-Host "  B/BACK/0 = go back when a previous menu exists" -ForegroundColor White
    Write-Host "  C/CANCEL = cancel the current selection cleanly" -ForegroundColor White
    Write-Host "  Q/QUIT/EXIT = exit cleanly" -ForegroundColor White
    Write-Host "  H/HELP = show help; L/LIST = show the launcher catalog" -ForegroundColor White
    Write-Host "  S/SEARCH = search entry IDs, thesis roles, run kinds, categories, and notes" -ForegroundColor White
    Write-Host "  P/PANEL = panel path; U/UI and R/RESTART are read-only dashboard shortcuts when available" -ForegroundColor White
    Write-Host "  X/INSPECT inside a section = compact inline preview without running the entry" -ForegroundColor White
    Write-Host "  E/EXPORT after an inspect/search preview = export output\launcher_plans\<entry_id>.md/.json without running science" -ForegroundColor White
    Write-Host ""
    Write-Host "Intentional scientific rerun examples:" -ForegroundColor Yellow
    Write-Host "  .\start.ps1 -Entry phase1_mindoro_focus_provenance" -ForegroundColor Gray
    Write-Host "  .\start.ps1 -Entry mindoro_phase3b_primary_public_validation" -ForegroundColor Gray
    Write-Host "  .\start.ps1 -Entry dwh_reportable_bundle" -ForegroundColor Gray
    Write-Host "  .\start.ps1 -Entry mindoro_reportable_core" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Direct container commands:" -ForegroundColor Yellow
    Write-Host ("  {0} exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src" -f $compose) -ForegroundColor Green
    Write-Host ("  {0} exec -T pipeline python src/services/panel_review_check.py" -f $compose) -ForegroundColor Green
    Write-Host ("  {0} exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" -f $compose) -ForegroundColor Green
    Write-Host ("  {0} up -d ; {0} restart pipeline gnome ; {0} exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" -f $compose) -ForegroundColor Green
    Write-Host ""
    Write-Host "Guardrails:" -ForegroundColor Yellow
    Write-Host "  - Panel mode is the defense-safe default. The full launcher is for researcher/audit use." -ForegroundColor White
    Write-Host "  - Panel mode and read-only entries do not rerun science." -ForegroundColor White
    Write-Host "  - Use launcher entry IDs and role groups as the user-facing startup vocabulary. Raw phase names are secondary implementation details." -ForegroundColor White
    Write-Host "  - B1 is the only main Philippine public-observation validation claim, using independent March 13 and March 14 NOAA public-observation products." -ForegroundColor White
    Write-Host "  - Track A and every PyGNOME branch remain comparator-only support, never observational truth." -ForegroundColor White
    Write-Host "  - DWH is a separate external transfer-validation story, not Mindoro recalibration." -ForegroundColor White
    Write-Host "  - Mindoro Phase 4 oil-type and shoreline outputs remain support/context only." -ForegroundColor White
    Write-Host "  - prototype_2016 remains legacy/archive support only." -ForegroundColor White
    Write-Host "  - B1 supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy." -ForegroundColor White
    Write-Host "  - Non-interactive launcher runs default silently to INPUT_CACHE_POLICY=reuse_if_valid and FORCING_SOURCE_BUDGET_SECONDS=300." -ForegroundColor White
    Write-Host "  - Interactive launcher runs still ask once for the forcing wait budget and cache policy when the target workflow is eligible." -ForegroundColor White
    Write-Host ("  - Direct interactive {0} exec runs do the same once per run; the -T form stays prompt-free and prints the resolved startup policy instead." -f $compose) -ForegroundColor White
    Write-Host "  - Do not auto-promote output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml over config/phase1_baseline_selection.yaml." -ForegroundColor White
    Write-Host ""
    Write-Host "Not implemented yet:" -ForegroundColor Yellow
    foreach ($item in $matrix.optional_future_work) {
        Write-Host ("  - {0} [{1}]" -f $item.label, $item.status) -ForegroundColor DarkGray
    }

    Pause-IfNeeded
}

function Show-LauncherRoleGroupMenu {
    param(
        [Parameter(Mandatory = $true)][string]$GroupId,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Description
    )

    :roleGroupMenuLoop while ($true) {
        $entries = Get-LauncherEntriesForRoleGroup -GroupId $GroupId
        $selectionMap = @{}
        $displayIndex = 1

        Clear-Host
        Write-Section $Label
        Show-LauncherNotice
        Write-Host ""
        Write-Host $Description -ForegroundColor DarkGray
        Write-Host ""

        foreach ($entry in $entries) {
            $selectionMap[[string]$displayIndex] = $entry.entry_id
            Write-LauncherEntrySummary -LauncherEntry $entry -DisplayKey ([string]$displayIndex)
            Write-Host ""
            $displayIndex += 1
        }

        if ($GroupId -eq "read_only_governance") {
            Write-Host "Read-only extras:" -ForegroundColor Yellow
            Write-Host "  U. Open read-only dashboard" -ForegroundColor White
            Write-Host "  R. Full restart read-only UI" -ForegroundColor White
            Write-Host "  P. Panel review mode / manuscript verification" -ForegroundColor White
            Write-Host ""
        }

        Write-Host "  X. Inspect / explain entries in this section" -ForegroundColor Yellow
        Write-Host "  S. Search entries in this section" -ForegroundColor Yellow
        Write-Host "  L. List launcher catalog" -ForegroundColor Yellow
        Write-Host "  H. Help" -ForegroundColor Yellow
        Write-Host "  B. Back" -ForegroundColor Yellow
        Write-Host "  C. Cancel and return" -ForegroundColor Yellow
        Write-Host "  Q. Exit" -ForegroundColor Yellow
        Write-Host ""

        $allowedActions = @("entry", "explain", "search", "list", "help", "back", "cancel", "quit")
        $allowedOptions = @("visible menu number", "entry_id", "X", "INSPECT", "S", "SEARCH", "L", "LIST", "H", "HELP", "B", "BACK", "0", "C", "CANCEL", "Q", "QUIT", "EXIT")
        if ($GroupId -eq "read_only_governance") {
            $allowedActions += @("ui", "restart", "panel")
            $allowedOptions += @("U", "UI", "R", "RESTART", "P", "PANEL")
        }

        $choice = Resolve-LauncherChoice `
            -InputText (Read-Host "Select an option") `
            -AllowedActions $allowedActions `
            -BlankAction "ignore" `
            -SelectionMap $selectionMap `
            -GroupId $GroupId `
            -AllowedOptions $allowedOptions

        switch ($choice.Action) {
            "ignore" { continue roleGroupMenuLoop }
            "ui" {
                Write-Section "READ-ONLY UI"
                try {
                    [void](Invoke-ReadOnlyUi)
                }
                catch {
                    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                }
                Pause-IfNeeded
                continue roleGroupMenuLoop
            }
            "restart" {
                Write-Section "FULL UI REFRESH"
                try {
                    [void](Invoke-ReadOnlyUi -RestartPipeline)
                }
                catch {
                    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                }
                Pause-IfNeeded
                continue roleGroupMenuLoop
            }
            "panel" {
                Show-PanelMenu -ReturnToCaller
                continue roleGroupMenuLoop
            }
            "explain" {
                Invoke-LauncherRoleGroupInspectMode `
                    -GroupId $GroupId `
                    -SelectionMap $selectionMap `
                    -Label $Label
                continue roleGroupMenuLoop
            }
            "search" {
                Invoke-LauncherSearchMode -GroupId $GroupId -Label $Label
                continue roleGroupMenuLoop
            }
            "list" {
                Show-LauncherList
                continue roleGroupMenuLoop
            }
            "help" {
                Show-Help
                continue roleGroupMenuLoop
            }
            "back" { return }
            "cancel" {
                Set-LauncherNotice -Message "Cancelled. No workflow was executed." -Level "warning"
                return
            }
            "quit" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
            "entry" {
                $launcherEntry = $choice.ResolvedEntry.CanonicalEntry
                Write-Section $launcherEntry.label
                $entryResult = Invoke-LauncherEntry `
                    -LauncherEntry $launcherEntry `
                    -RequestedEntryId $choice.ResolvedEntry.RequestedEntryId
                if ($entryResult.Status -eq "cancelled" -and $entryResult.NoWorkflowExecuted) {
                    Set-LauncherNotice -Message $entryResult.Message -Level "warning"
                }
                Pause-IfNeeded
                continue roleGroupMenuLoop
            }
            "invalid" {
                Write-Host $choice.Message -ForegroundColor Red
                Start-Sleep -Seconds 2
                continue roleGroupMenuLoop
            }
        }
    }
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
    Write-Host "Panel mode and read-only entries do not rerun science." -ForegroundColor White
    Write-Host "Panel option 8 opens docs\DATA_SOURCES.md as a read-only provenance registry." -ForegroundColor White
    Write-Host "Panel reviewers can also open the B1 Drifter Provenance page to inspect the focused Phase 1 drifter records behind the selected B1 recipe without creating a new validation claim." -ForegroundColor White
    Write-Host "If no direct March 13-14 2023 accepted drifter segment is stored, that page says so explicitly." -ForegroundColor White
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

function Show-DataSourcesRegistry {
    Clear-Host
    Write-Section "DATA SOURCES AND PROVENANCE REGISTRY"
    Write-Host ""
    Write-Host "Registry file: docs\DATA_SOURCES.md" -ForegroundColor Yellow
    Write-Host "This action is read-only. It does not download data, rerun workflows, or rewrite science." -ForegroundColor White
    Write-Host ""
    if (Test-Path "docs\DATA_SOURCES.md") {
        Get-Content "docs\DATA_SOURCES.md" | ForEach-Object { Write-Host $_ }
        if (-not $NoPause) {
            Write-Host ""
            Write-Host "Opening the data-source registry in the default viewer..." -ForegroundColor Yellow
            Open-FileInDefaultApp -Path "docs\DATA_SOURCES.md" -Label "Data sources and provenance registry"
        }
    } else {
        Write-Host "docs\DATA_SOURCES.md is missing." -ForegroundColor Red
        Write-Host "No inventory builder was run from panel mode." -ForegroundColor DarkYellow
    }
    Pause-IfNeeded
}

function Invoke-PanelPaperVerification {
    Write-Section "VERIFY PAPER NUMBERS AGAINST STORED SCORECARDS"
    $scriptResult = Invoke-ContainerPythonScript `
        -Description "Read-only paper-results verification from stored outputs only" `
        -ScriptPath "src/services/panel_review_check.py"
    if (-not $scriptResult) {
        return $false
    }

    $csvPath = "output\panel_review_check\panel_results_match_check.csv"
    $jsonPath = "output\panel_review_check\panel_results_match_check.json"
    $markdownPath = "output\panel_review_check\panel_results_match_check.md"
    $manifestPath = "output\panel_review_check\panel_review_manifest.json"
    $workbookPath = "output\panel_review_check\panel_results_match_check.xlsx"
    $createdWorkbookPath = Export-CsvToExcelWorkbook -CsvPath $csvPath -WorkbookPath $workbookPath
    $preferredLabel = if ($createdWorkbookPath) { "Excel workbook" } else { "Spreadsheet report (.csv)" }
    $preferredPath = if ($createdWorkbookPath) { $workbookPath } else { $csvPath }

    $panelOutputs = @(
        @{ Label = $preferredLabel; Path = $preferredPath },
        @{ Label = "CSV export"; Path = $csvPath },
        @{ Label = "Readable markdown summary"; Path = $markdownPath },
        @{ Label = "Machine-readable JSON report"; Path = $jsonPath },
        @{ Label = "Run manifest"; Path = $manifestPath },
        @{ Label = "Panel review output folder"; Path = "output\panel_review_check" }
    )

    Write-Host ""
    Write-Host "Recommended review file:" -ForegroundColor Yellow
    Write-Host ("  {0}: {1}" -f $panelOutputs[0].Label, (Get-DisplayPath -Path $panelOutputs[0].Path)) -ForegroundColor Green
    Write-Host ""
    Write-Host "Also available:" -ForegroundColor Yellow
    foreach ($panelOutput in $panelOutputs) {
        if ($panelOutput.Path -ne $panelOutputs[0].Path) {
            Write-Host ("  {0}: {1}" -f $panelOutput.Label, (Get-DisplayPath -Path $panelOutput.Path)) -ForegroundColor Green
        }
    }

    if (-not $NoPause) {
        Write-Host ""
        Write-Host ("Opening the {0} for you now..." -f $preferredLabel.ToLowerInvariant()) -ForegroundColor Yellow
        Open-FileInDefaultApp `
            -Path $preferredPath `
            -Label $preferredLabel
        Show-PanelVerificationQuickActions -PanelOutputs $panelOutputs
    }

    return $true
}

function Invoke-PanelB1DrifterContext {
    $launcherEntry = Get-LauncherEntryById -EntryId "b1_drifter_context_panel"
    Write-Section $launcherEntry.label
    $entryResult = Invoke-LauncherEntry -LauncherEntry $launcherEntry
    if ($entryResult.Status -notin @("success", "dry_run")) {
        return $entryResult
    }
    Write-Host ""
    Write-Host "Next step: the read-only dashboard should open directly on 'B1 Drifter Provenance'." -ForegroundColor Yellow
    Write-Host "If it does not switch automatically, open http://localhost:8501/b1-drifter-provenance or click 'B1 Drifter Provenance' in the sidebar." -ForegroundColor DarkGray
    Write-Host "These drifter records support the selected transport recipe used by B1. They are not the direct March 13-14 public-observation truth mask." -ForegroundColor DarkGray
    Write-Host ("Stored context outputs: {0}" -f (Get-DisplayPath -Path "output\panel_drifter_context")) -ForegroundColor Green
    [void](Invoke-ReadOnlyUi -LandingPath "b1-drifter-provenance")
    return $entryResult
}

function Show-PanelMenu {
    param([switch]$ReturnToCaller)

    if ($NoPause) {
        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host "   PANEL REVIEW MODE" -ForegroundColor White
        Write-Host ""
        Write-Host "Non-interactive preview mode (-NoPause)." -ForegroundColor Yellow
        Write-Host "Use .\panel.ps1 or .\start.ps1 -Panel for the interactive defense menu." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Panel-safe actions:" -ForegroundColor Yellow
        Write-Host "  1. Open read-only dashboard" -ForegroundColor White
        Write-Host "  2. Verify paper numbers against stored scorecards" -ForegroundColor White
        Write-Host "  3. Rebuild publication figures from stored outputs" -ForegroundColor White
        Write-Host "  4. Refresh final validation package from stored outputs" -ForegroundColor White
        Write-Host "  5. Refresh final reproducibility package / command documentation" -ForegroundColor White
        Write-Host "  6. Show paper-to-output registry" -ForegroundColor White
        Write-Host "  7. View B1 drifter provenance/context" -ForegroundColor White
        Write-Host "  8. View data sources and provenance registry" -ForegroundColor White
        Write-Host ""
        Write-Host "Panel mode and read-only entries do not rerun science." -ForegroundColor Green
        Write-Host ""
        Write-Host "Smoke-test-safe examples:" -ForegroundColor Yellow
        Write-Host "  .\start.ps1 -Explain b1_drifter_context_panel -NoPause" -ForegroundColor Green
        Write-Host "  .\start.ps1 -Entry b1_drifter_context_panel -NoPause" -ForegroundColor Green
        Write-Host "  .\start.ps1 -Entry figure_package_publication -NoPause" -ForegroundColor Green
        Write-Host ""
        return
    }

    :panelMenuLoop while ($true) {
        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host "   PANEL REVIEW MODE" -ForegroundColor White
        Show-LauncherNotice
        Write-Host ""
        Write-Host "Recommended panel checks:" -ForegroundColor Yellow
        Write-Host "Panel mode and read-only entries do not rerun science." -ForegroundColor Green
        Write-Host ""
        Write-Host "  1. Open read-only dashboard [READ-ONLY]" -ForegroundColor White
        Write-Host "     Opens the Streamlit dashboard over stored outputs only." -ForegroundColor DarkGray
        Write-Host "  2. Verify paper numbers against stored scorecards [READ-ONLY]" -ForegroundColor White
        Write-Host "     Writes only to output\panel_review_check\, never reruns science, and opens the spreadsheet result." -ForegroundColor DarkGray
        Write-Host "  3. Rebuild publication figures from stored outputs [PACKAGING ONLY]" -ForegroundColor White
        Write-Host "     Uses the existing read-only figure-package builder." -ForegroundColor DarkGray
        Write-Host "  4. Refresh final validation package from stored outputs [PACKAGING ONLY]" -ForegroundColor White
        Write-Host "     Uses the existing read-only validation-package refresh." -ForegroundColor DarkGray
        Write-Host "  5. Refresh final reproducibility package / command documentation [PACKAGING ONLY]" -ForegroundColor White
        Write-Host "     Uses the existing read-only launcher/docs/package sync." -ForegroundColor DarkGray
        Write-Host "  6. Show paper-to-output registry [READ-ONLY]" -ForegroundColor White
        Write-Host "     Opens the plain-language manuscript/output map." -ForegroundColor DarkGray
        Write-Host "  7. View B1 drifter provenance/context [READ-ONLY]" -ForegroundColor White
        Write-Host "     Builds the stored-output-only drifter provenance context and opens the dashboard to the B1 Drifter Provenance page." -ForegroundColor DarkGray
        Write-Host "  8. View data sources and provenance registry [READ-ONLY]" -ForegroundColor White
        Write-Host "     Opens docs\DATA_SOURCES.md only; no downloads, reruns, or science rewrites." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Advanced:" -ForegroundColor Yellow
        Write-Host "  A. Open full research launcher [ADVANCED ONLY]" -ForegroundColor White
        Write-Host "  U. Open read-only dashboard shortcut [READ-ONLY]" -ForegroundColor White
        Write-Host "  R. Restart the read-only dashboard [READ-ONLY]" -ForegroundColor White
        Write-Host "  L. List launcher catalog" -ForegroundColor White
        Write-Host "  H. Help / interpretation guide [READ-ONLY]" -ForegroundColor White
        if ($ReturnToCaller) {
            Write-Host "  B. Back" -ForegroundColor White
            Write-Host "  C. Cancel and return" -ForegroundColor White
        } else {
            Write-Host "  B. Open launcher home" -ForegroundColor White
            Write-Host "  C. Cancel and open launcher home" -ForegroundColor White
        }
        Write-Host "  Q. Exit" -ForegroundColor White
        Write-Host ""

        $choice = Resolve-LauncherChoice `
            -InputText (Read-Host "Select an option") `
            -AllowedActions @("ui", "restart", "list", "help", "back", "cancel", "quit", "panel") `
            -BlankAction "ignore" `
            -AllowedOptions @("1", "2", "3", "4", "5", "6", "7", "8", "A", "U", "UI", "R", "RESTART", "L", "LIST", "H", "HELP", "B", "BACK", "0", "C", "CANCEL", "Q", "QUIT", "EXIT")

        switch ($choice.RawInput.ToUpperInvariant()) {
            "1" {
                Write-Section "READ-ONLY DASHBOARD"
                try {
                    [void](Invoke-ReadOnlyUi)
                }
                catch {
                    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "2" {
                try {
                    [void](Invoke-PanelPaperVerification)
                }
                catch {
                    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "3" {
                $launcherEntry = Get-LauncherEntryById -EntryId "figure_package_publication"
                Write-Section $launcherEntry.label
                $entryResult = Invoke-LauncherEntry -LauncherEntry $launcherEntry
                if ($entryResult.Status -eq "cancelled" -and $entryResult.NoWorkflowExecuted) {
                    Set-LauncherNotice -Message $entryResult.Message -Level "warning"
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "4" {
                $launcherEntry = Get-LauncherEntryById -EntryId "final_validation_package"
                Write-Section $launcherEntry.label
                $entryResult = Invoke-LauncherEntry -LauncherEntry $launcherEntry
                if ($entryResult.Status -eq "cancelled" -and $entryResult.NoWorkflowExecuted) {
                    Set-LauncherNotice -Message $entryResult.Message -Level "warning"
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "5" {
                $launcherEntry = Get-LauncherEntryById -EntryId "phase5_sync"
                Write-Section $launcherEntry.label
                $entryResult = Invoke-LauncherEntry -LauncherEntry $launcherEntry
                if ($entryResult.Status -eq "cancelled" -and $entryResult.NoWorkflowExecuted) {
                    Set-LauncherNotice -Message $entryResult.Message -Level "warning"
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "6" {
                Show-PaperOutputRegistry
                continue panelMenuLoop
            }
            "7" {
                try {
                    $entryResult = Invoke-PanelB1DrifterContext
                    if ($entryResult -and $entryResult.Status -eq "cancelled" -and $entryResult.NoWorkflowExecuted) {
                        Set-LauncherNotice -Message $entryResult.Message -Level "warning"
                    }
                }
                catch {
                    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "8" {
                Show-DataSourcesRegistry
                continue panelMenuLoop
            }
        }

        switch ($choice.Action) {
            "ignore" { continue panelMenuLoop }
            "ui" {
                Write-Section "READ-ONLY DASHBOARD"
                try {
                    [void](Invoke-ReadOnlyUi)
                }
                catch {
                    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "restart" {
                Write-Section "READ-ONLY DASHBOARD RESTART"
                try {
                    [void](Invoke-ReadOnlyUi -RestartPipeline)
                }
                catch {
                    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                }
                Pause-IfNeeded
                continue panelMenuLoop
            }
            "list" {
                Show-LauncherList
                continue panelMenuLoop
            }
            "help" {
                Show-PanelGuide
                continue panelMenuLoop
            }
            "panel" { continue panelMenuLoop }
            "back" {
                if ($ReturnToCaller) {
                    return
                }
                Show-Menu
                return
            }
            "cancel" {
                Set-LauncherNotice -Message "Cancelled. No workflow was executed." -Level "warning"
                if ($ReturnToCaller) {
                    return
                }
                Show-Menu
                return
            }
            "quit" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
            "invalid" {
                if ($choice.RawInput.ToUpperInvariant() -eq "A") {
                    if ($ReturnToCaller) {
                        Show-Menu -ReturnToCaller
                    } else {
                        Show-Menu
                    }
                    continue panelMenuLoop
                }
                Write-Host $choice.Message -ForegroundColor Red
                Start-Sleep -Seconds 2
                continue panelMenuLoop
            }
        }
    }
}

function Show-Menu {
    param([switch]$ReturnToCaller)

    :launcherHomeLoop while ($true) {
        $groups = Get-LauncherRoleGroups

        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Show-LauncherNotice
        Write-Host ""
        Write-Host "Panel mode is the defense-safe default." -ForegroundColor Yellow
        Write-Host "This full launcher is for intentional researcher/audit work and is organized by thesis role instead of raw phase names." -ForegroundColor DarkYellow
        Write-Host "Panel mode and read-only entries do not rerun science." -ForegroundColor Green
        Write-Host ""
        Write-Host "Choose a role-based path:" -ForegroundColor Yellow
        Write-Host ""

        Write-Host "  P. Panel review mode / defense-safe path" -ForegroundColor White
        Write-Host "     Safest route for panel review, dashboards, and stored-output checks." -ForegroundColor DarkGray
        foreach ($group in $groups) {
            Write-Host ("  {0}. {1}" -f [string]$group.MenuKey, [string]$group.Label) -ForegroundColor White
            Write-Host ("     {0}" -f [string]$group.Description) -ForegroundColor DarkGray
        }
        Write-Host ""
        Write-Host "  S. Search launcher entries" -ForegroundColor Yellow
        Write-Host "  L. List catalog only" -ForegroundColor Yellow
        Write-Host "  H. Help" -ForegroundColor Yellow
        if ($ReturnToCaller) {
            Write-Host "  B. Back" -ForegroundColor Yellow
            Write-Host "  C. Cancel and return" -ForegroundColor Yellow
        } else {
            Write-Host "  C. Cancel current selection" -ForegroundColor Yellow
        }
        Write-Host "  Q. Exit" -ForegroundColor Yellow
        Write-Host ""

        $choice = Resolve-LauncherChoice `
            -InputText (Read-Host "Select an option") `
            -AllowedActions @("panel", "search", "list", "help", "back", "cancel", "quit") `
            -BlankAction "ignore" `
            -AllowedOptions @("P", "PANEL", "1", "2", "3", "4", "5", "S", "SEARCH", "L", "LIST", "H", "HELP", "B", "BACK", "0", "C", "CANCEL", "Q", "QUIT", "EXIT")

        switch ($choice.Action) {
            "ignore" { continue launcherHomeLoop }
            "panel" {
                if ($ReturnToCaller) {
                    Show-PanelMenu -ReturnToCaller
                } else {
                    Show-PanelMenu
                }
                continue launcherHomeLoop
            }
            "search" {
                Invoke-LauncherSearchMode -Label "all visible launcher entries"
                continue launcherHomeLoop
            }
            "list" {
                Show-LauncherList
                continue launcherHomeLoop
            }
            "help" {
                Show-Help
                continue launcherHomeLoop
            }
            "back" {
                if ($ReturnToCaller) {
                    return
                }
                continue launcherHomeLoop
            }
            "cancel" {
                if ($ReturnToCaller) {
                    Set-LauncherNotice -Message "Cancelled. No workflow was executed." -Level "warning"
                    return
                }
                Set-LauncherNotice -Message "Cancelled. No workflow was executed." -Level "warning"
                continue launcherHomeLoop
            }
            "quit" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
        }

        $matchedGroup = $groups | Where-Object { [string]$_.MenuKey -eq [string]$choice.RawInput } | Select-Object -First 1
        if ($matchedGroup) {
            Show-LauncherRoleGroupMenu `
                -GroupId ([string]$matchedGroup.GroupId) `
                -Label ([string]$matchedGroup.Label) `
                -Description ([string]$matchedGroup.Description)
            continue launcherHomeLoop
        }

        if ($choice.Action -eq "invalid") {
            Write-Host $choice.Message -ForegroundColor Red
        } else {
            Write-Host "Invalid option. Use P, 1-5, S, L, H, C, or Q." -ForegroundColor Red
        }
        Start-Sleep -Seconds 2
        continue launcherHomeLoop
    }
}

try {
    if ($ValidateMatrix) {
        $validationResult = Invoke-LauncherMatrixValidation
        Pause-IfNeeded
        exit $validationResult.ExitCode
    }

    if ($List) {
        Show-LauncherList
        exit 0
    }

    if ($ListRole) {
        Show-LauncherList -Role $ListRole
        exit 0
    }

    if ($Help) {
        Show-Help
        exit 0
    }

    if ($Explain) {
        $resolvedEntry = Resolve-LauncherEntryRequest -EntryId $Explain
        Show-LauncherEntryPreview `
            -LauncherEntry $resolvedEntry.CanonicalEntry `
            -RequestedEntryId $resolvedEntry.RequestedEntryId
        if ($ExportPlan) {
            $exportedPlan = Export-LauncherRunPlan `
                -LauncherEntry $resolvedEntry.CanonicalEntry `
                -RequestedEntryId $resolvedEntry.RequestedEntryId
            Write-Host ""
            Write-Host "Run plan exported without executing science:" -ForegroundColor Green
            Write-Host ("  Markdown: {0}" -f $exportedPlan.MarkdownPath) -ForegroundColor DarkGray
            Write-Host ("  JSON:     {0}" -f $exportedPlan.JsonPath) -ForegroundColor DarkGray
        }
        Pause-IfNeeded
        exit 0
    }

    if ($Entry) {
        $resolvedEntry = Resolve-LauncherEntryRequest -EntryId $Entry
        Write-Section $resolvedEntry.CanonicalEntry.label
        $entryResult = Invoke-LauncherEntry `
            -LauncherEntry $resolvedEntry.CanonicalEntry `
            -RequestedEntryId $resolvedEntry.RequestedEntryId
        Pause-IfNeeded
        if ($entryResult.Status -in @("success", "dry_run", "cancelled")) {
            exit 0
        }
        if ($entryResult.Status -eq "docker_unavailable") {
            exit $entryResult.ExitCode
        }
        exit 1
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
