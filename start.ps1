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
        .\start.ps1 -Help -NoPause
        .\start.ps1 -Explain <entry_id> -NoPause
        .\start.ps1 -Entry <entry_id>
        docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
#>

param(
    [switch]$List,
    [switch]$Help,
    [string]$Entry,
    [string]$Explain,
    [string]$ListRole,
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
$Script:ComposeMode = $null
$Script:ComposeModeChecked = $false

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
    $mode = Resolve-ComposeMode
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

function Get-LauncherEntryById {
    param([Parameter(Mandatory = $true)][string]$EntryId)

    $match = Get-LauncherEntries | Where-Object { $_.entry_id -eq $EntryId } | Select-Object -First 1
    if ($null -eq $match) {
        $known = (Get-LauncherEntries | ForEach-Object { $_.entry_id }) -join ", "
        throw "Unknown launcher entry '$EntryId'. Known entry IDs: $known"
    }
    return $match
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
        return [Environment]::UserInteractive -and -not [Console]::IsInputRedirected
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

function Write-LauncherEntrySummary {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [string]$DisplayKey,
        [switch]$IncludeHiddenMarker
    )

    $prefix = if ($DisplayKey) { "  $DisplayKey. " } else { "  - " }
    Write-Host ("{0}{1}" -f $prefix, $LauncherEntry.label) -ForegroundColor White
    Write-Host ("     id={0}" -f $LauncherEntry.entry_id) -ForegroundColor DarkGray
    Write-Host ("     thesis role={0} | draft={1}" -f (Format-ThesisRoleLabel -Role ([string]$LauncherEntry.thesis_role)), ([string]$LauncherEntry.draft_section)) -ForegroundColor Yellow

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

function Show-LauncherEntryPreview {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    $stepsSummary = ($LauncherEntry.steps | ForEach-Object { "{0}:{1}" -f $_.service, $_.phase }) -join " -> "

    Clear-Host
    Write-Section "ENTRY PREVIEW"
    Write-Host ""
    Write-Host ("Entry ID: {0}" -f [string]$LauncherEntry.entry_id) -ForegroundColor Yellow
    if ($LauncherEntry.alias_of) {
        Write-Host ("Preferred / canonical ID: {0}" -f [string]$LauncherEntry.alias_of) -ForegroundColor DarkGray
    }
    Write-Host ("Label: {0}" -f [string]$LauncherEntry.label) -ForegroundColor White
    Write-Host ("Thesis role: {0}" -f (Format-ThesisRoleLabel -Role ([string]$LauncherEntry.thesis_role))) -ForegroundColor White
    Write-Host ("Draft section: {0}" -f [string]$LauncherEntry.draft_section) -ForegroundColor White
    Write-Host ("Claim boundary: {0}" -f [string]$LauncherEntry.claim_boundary) -ForegroundColor White
    Write-Host ("Run kind: {0}" -f (Format-RunKindLabel -RunKind ([string]$LauncherEntry.run_kind))) -ForegroundColor White
    Write-Host ("Rerun cost: {0}" -f [string]$LauncherEntry.rerun_cost) -ForegroundColor White
    $safetyText = if ([bool]$LauncherEntry.safe_default) { "safe default" } else { "explicit confirmation required" }
    Write-Host ("Safety: {0}" -f $safetyText) -ForegroundColor White
    Write-Host ("Recommended for: {0}" -f [string]$LauncherEntry.recommended_for) -ForegroundColor White
    Write-Host ("Steps/phases: {0}" -f $stepsSummary) -ForegroundColor White
    Write-Host ("Output warning: {0}" -f (Get-LauncherEntryOutputWarning -LauncherEntry $LauncherEntry)) -ForegroundColor Yellow
    if ($LauncherEntry.notes) {
        Write-Host ("Notes: {0}" -f [string]$LauncherEntry.notes) -ForegroundColor DarkGray
    }
}

function Confirm-LauncherEntryRun {
    param([Parameter(Mandatory = $true)]$LauncherEntry)

    if (-not (Test-LauncherEntryNeedsPreview -LauncherEntry $LauncherEntry)) {
        return
    }

    if (-not (Test-ConsolePromptAvailable)) {
        throw "Launcher entry '$([string]$LauncherEntry.entry_id)' requires interactive confirmation. Run .\start.ps1 -Explain $([string]$LauncherEntry.entry_id) first, then rerun it from an interactive PowerShell session."
    }

    Show-LauncherEntryPreview -LauncherEntry $LauncherEntry
    Write-Host ""

    if (Test-LauncherEntryReadOnlyLike -LauncherEntry $LauncherEntry) {
        while ($true) {
            $choice = (Read-Host "Type Y to continue, or press Enter to cancel").Trim()
            if ([string]::IsNullOrWhiteSpace($choice)) {
                throw "Launch cancelled before execution."
            }
            if ($choice.ToUpperInvariant() -in @("Y", "YES")) {
                return
            }
            Write-Host "Enter Y to continue, or press Enter to cancel." -ForegroundColor DarkYellow
        }
    }

    $entryId = [string]$LauncherEntry.entry_id
    while ($true) {
        $choice = (Read-Host ("Type RUN or {0} to continue, or press Enter to cancel" -f $entryId)).Trim()
        if ([string]::IsNullOrWhiteSpace($choice)) {
            throw "Launch cancelled before execution."
        }
        if ($choice -eq "RUN" -or $choice -eq $entryId) {
            return
        }
        Write-Host ("Enter RUN or {0} to continue, or press Enter to cancel." -f $entryId) -ForegroundColor DarkYellow
    }
}

function Get-LauncherRoleGroups {
    return @(
        @{
            MenuKey = "1"
            GroupId = "main_thesis_evidence"
            Label = "Main thesis evidence reruns"
            Description = "Intentional reruns for the main thesis evidence lanes."
        },
        @{
            MenuKey = "2"
            GroupId = "support_context"
            Label = "Support/context and appendix reruns"
            Description = "Support, comparator, and appendix reruns outside the main-text claim."
        },
        @{
            MenuKey = "3"
            GroupId = "archive_provenance"
            Label = "Archive/provenance reruns"
            Description = "Archive, provenance, and governance reruns kept outside the default defense path."
        },
        @{
            MenuKey = "4"
            GroupId = "legacy_debug"
            Label = "Legacy prototype/debug reruns"
            Description = "Legacy prototype support and debug paths."
        },
        @{
            MenuKey = "5"
            GroupId = "read_only_governance"
            Label = "Read-only packaging, audits, dashboard, and docs"
            Description = "Safe packaging and audit surfaces built from stored outputs only."
        }
    )
}

function Get-LauncherEntriesForRoleGroup {
    param([Parameter(Mandatory = $true)][string]$GroupId)

    $entries = Get-VisibleLauncherEntries
    switch ($GroupId) {
        "main_thesis_evidence" {
            return @($entries | Where-Object { $_.thesis_role -eq "primary_evidence" })
        }
        "support_context" {
            return @($entries | Where-Object { $_.thesis_role -in @("support_context", "comparator_support") })
        }
        "archive_provenance" {
            return @($entries | Where-Object { $_.thesis_role -eq "archive_provenance" })
        }
        "legacy_debug" {
            return @($entries | Where-Object { $_.thesis_role -eq "legacy_support" })
        }
        "read_only_governance" {
            return @($entries | Where-Object { $_.thesis_role -eq "read_only_governance" })
        }
        default {
            throw "Unknown launcher role group '$GroupId'."
        }
    }
}

function Test-LauncherEntryMatchesRoleGroup {
    param(
        [Parameter(Mandatory = $true)]$LauncherEntry,
        [Parameter(Mandatory = $true)][string]$GroupId
    )

    switch ($GroupId) {
        "main_thesis_evidence" {
            return ([string]$LauncherEntry.thesis_role -eq "primary_evidence")
        }
        "support_context" {
            return ([string]$LauncherEntry.thesis_role -in @("support_context", "comparator_support"))
        }
        "archive_provenance" {
            return ([string]$LauncherEntry.thesis_role -eq "archive_provenance")
        }
        "legacy_debug" {
            return ([string]$LauncherEntry.thesis_role -eq "legacy_support")
        }
        "read_only_governance" {
            return ([string]$LauncherEntry.thesis_role -eq "read_only_governance")
        }
        default {
            return $false
        }
    }
}

function Resolve-LauncherRoleGroupEntryReference {
    param(
        [Parameter(Mandatory = $true)][string]$Reference,
        [Parameter(Mandatory = $true)][hashtable]$SelectionMap,
        [Parameter(Mandatory = $true)][string]$GroupId,
        [switch]$ThrowIfUnknown
    )

    $trimmedReference = ([string]$Reference).Trim()
    if ([string]::IsNullOrWhiteSpace($trimmedReference)) {
        if ($ThrowIfUnknown) {
            throw "Enter a visible menu number or an entry ID from this section."
        }
        return $null
    }

    if ($SelectionMap.ContainsKey($trimmedReference)) {
        return Get-LauncherEntryById -EntryId $SelectionMap[$trimmedReference]
    }

    $launcherEntry = Get-LauncherEntries | Where-Object { $_.entry_id -eq $trimmedReference } | Select-Object -First 1
    if ($null -eq $launcherEntry) {
        if ($ThrowIfUnknown) {
            $menuNumbers = (($SelectionMap.Keys | Sort-Object {[int]$_}) -join ", ")
            throw "Unknown selection '$trimmedReference'. Use a visible menu number ($menuNumbers) or an entry ID from this section."
        }
        return $null
    }

    if (-not (Test-LauncherEntryMatchesRoleGroup -LauncherEntry $launcherEntry -GroupId $GroupId)) {
        if ($ThrowIfUnknown) {
            throw "Entry '$trimmedReference' is not part of this section. Use a visible menu number or an entry ID shown in this menu."
        }
        return $null
    }

    return $launcherEntry
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

    Confirm-LauncherEntryRun -LauncherEntry $LauncherEntry
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

function Invoke-ReadOnlyUi {
    param(
        [switch]$RestartPipeline,
        [string]$LandingPath = ""
    )

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
        Invoke-ComposeCommand -ComposeArgs $dockerArgs 2>&1 | ForEach-Object { Write-ProcessLine $_ }
        $scriptExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($scriptExitCode -ne 0) {
        throw "Container Python script exited with code $scriptExitCode."
    }
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
    Write-Host "Explain one entry: .\start.ps1 -Explain <entry_id> -NoPause" -ForegroundColor Yellow
    Write-Host ("Prompt-free container run: {0} exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src" -f $compose) -ForegroundColor Yellow
    Write-Host ("Read-only UI: {0} exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" -f $compose) -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Use user-facing entry IDs and thesis-role groupings here. Raw phase names are not the primary startup commands." -ForegroundColor White
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
    Write-Host "  .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause" -ForegroundColor Green
    Write-Host "  .\start.ps1 -Entry <entry_id>" -ForegroundColor Green
    Write-Host ""
    Write-Host "Preferred user-facing entry IDs:" -ForegroundColor Yellow
    Write-Host "  phase1_mindoro_focus_provenance" -ForegroundColor White
    Write-Host "  b1_drifter_context_panel" -ForegroundColor White
    Write-Host "  mindoro_phase3b_primary_public_validation" -ForegroundColor White
    Write-Host "  dwh_reportable_bundle" -ForegroundColor White
    Write-Host "  phase1_regional_reference_rerun" -ForegroundColor White
    Write-Host "  mindoro_phase4_only" -ForegroundColor White
    Write-Host ""
    Write-Host "Compatibility aliases still work, but they are no longer the preferred wording:" -ForegroundColor Yellow
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
    Write-Host "  - Use launcher entry IDs and role groups as the user-facing startup vocabulary. Raw phase names are secondary implementation details." -ForegroundColor White
    Write-Host "  - B1 is the only main-text primary Mindoro validation row, and the March 13 -> March 14 pair keeps the shared-imagery caveat explicit." -ForegroundColor White
    Write-Host "  - Track A and every PyGNOME branch remain comparator-only support, never observational truth." -ForegroundColor White
    Write-Host "  - DWH is a separate external transfer-validation story, not Mindoro recalibration." -ForegroundColor White
    Write-Host "  - Mindoro Phase 4 oil-type and shoreline outputs remain support/context only." -ForegroundColor White
    Write-Host "  - prototype_2016 remains legacy support only." -ForegroundColor White
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

    while ($true) {
        $entries = Get-LauncherEntriesForRoleGroup -GroupId $GroupId
        $selectionMap = @{}
        $displayIndex = 1

        Clear-Host
        Write-Section $Label
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

        Write-Host "  X. Explain a visible menu number or entry ID without running it" -ForegroundColor Yellow
        Write-Host "  B. Back" -ForegroundColor Yellow
        Write-Host "  Q. Exit" -ForegroundColor Yellow
        Write-Host ""

        $choice = (Read-Host "Select an option").Trim()
        if ([string]::IsNullOrWhiteSpace($choice)) {
            continue
        }

        switch ($choice.ToUpperInvariant()) {
            "U" {
                if ($GroupId -eq "read_only_governance") {
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
            }
            "R" {
                if ($GroupId -eq "read_only_governance") {
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
            }
            "P" {
                if ($GroupId -eq "read_only_governance") {
                    Show-PanelMenu
                    continue
                }
            }
            "X" {
                $entryReference = (Read-Host "Menu number or entry ID to explain").Trim()
                if (-not [string]::IsNullOrWhiteSpace($entryReference)) {
                    try {
                        $launcherEntry = Resolve-LauncherRoleGroupEntryReference `
                            -Reference $entryReference `
                            -SelectionMap $selectionMap `
                            -GroupId $GroupId `
                            -ThrowIfUnknown
                        Show-LauncherEntryPreview -LauncherEntry $launcherEntry
                        Pause-IfNeeded
                    }
                    catch {
                        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
                        Start-Sleep -Seconds 2
                    }
                }
                continue
            }
            "B" { return }
            "Q" {
                Write-Host ""
                Write-Host "Goodbye." -ForegroundColor DarkGray
                exit 0
            }
        }

        $launcherEntry = Resolve-LauncherRoleGroupEntryReference `
            -Reference $choice `
            -SelectionMap $selectionMap `
            -GroupId $GroupId
        if ($null -ne $launcherEntry) {
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

        if ($GroupId -eq "read_only_governance") {
            Write-Host "Invalid option. Use a visible menu number or entry ID, U, R, P, X, B, or Q." -ForegroundColor Red
        } else {
            Write-Host "Invalid option. Use a visible menu number or entry ID, X, B, or Q." -ForegroundColor Red
        }
        Start-Sleep -Seconds 2
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
    Write-Host "It does not rerun expensive scientific simulations by default." -ForegroundColor White
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

function Invoke-PanelPaperVerification {
    Write-Section "VERIFY PAPER NUMBERS AGAINST STORED SCORECARDS"
    Invoke-ContainerPythonScript `
        -Description "Read-only paper-results verification from stored outputs only" `
        -ScriptPath "src/services/panel_review_check.py"

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
}

function Invoke-PanelB1DrifterContext {
    $launcherEntry = Get-LauncherEntryById -EntryId "b1_drifter_context_panel"
    Write-Section $launcherEntry.label
    Invoke-LauncherEntry -LauncherEntry $launcherEntry
    Write-Host ""
    Write-Host "Next step: the read-only dashboard should open directly on 'B1 Drifter Provenance'." -ForegroundColor Yellow
    Write-Host "If it does not switch automatically, open http://localhost:8501/b1-drifter-provenance or click 'B1 Drifter Provenance' in the sidebar." -ForegroundColor DarkGray
    Write-Host "These drifter records support the selected transport recipe used by B1. They are not the direct March 13-14 public-observation truth mask." -ForegroundColor DarkGray
    Write-Host ("Stored context outputs: {0}" -f (Get-DisplayPath -Path "output\panel_drifter_context")) -ForegroundColor Green
    Invoke-ReadOnlyUi -LandingPath "b1-drifter-provenance"
}

function Show-PanelMenu {
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
        Write-Host ""
        Write-Host "Smoke-test-safe examples:" -ForegroundColor Yellow
        Write-Host "  .\start.ps1 -Explain b1_drifter_context_panel -NoPause" -ForegroundColor Green
        Write-Host "  .\start.ps1 -Entry b1_drifter_context_panel -NoPause" -ForegroundColor Green
        Write-Host "  .\start.ps1 -Entry figure_package_publication -NoPause" -ForegroundColor Green
        Write-Host ""
        return
    }

    while ($true) {
        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host "   PANEL REVIEW MODE" -ForegroundColor White
        Write-Host ""
        Write-Host "Recommended panel checks:" -ForegroundColor Yellow
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
            "7" {
                try {
                    Invoke-PanelB1DrifterContext
                }
                catch {
                    Pause-IfNeeded
                }
                Pause-IfNeeded
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

        Write-Host "Invalid option. Use 1-7, A, H, or Q." -ForegroundColor Red
        Start-Sleep -Seconds 2
    }
}

function Show-Menu {
    param([switch]$ReturnToCaller)

    while ($true) {
        $groups = Get-LauncherRoleGroups

        Clear-Host
        Write-Section "DRIFTER-VALIDATED OIL SPILL FORECASTING"
        Write-Host ""
        Write-Host "Panel mode is the defense-safe default." -ForegroundColor Yellow
        Write-Host "This full launcher is for intentional researcher/audit work and is organized by thesis role instead of raw phase names." -ForegroundColor DarkYellow
        Write-Host ""
        Write-Host "Choose a role-based path:" -ForegroundColor Yellow
        Write-Host ""

        Write-Host "  P. Panel review mode / defense-safe path" -ForegroundColor White
        foreach ($group in $groups) {
            Write-Host ("  {0}. {1}" -f [string]$group.MenuKey, [string]$group.Label) -ForegroundColor White
            Write-Host ("     {0}" -f [string]$group.Description) -ForegroundColor DarkGray
        }
        Write-Host ""
        Write-Host "  L. List catalog only" -ForegroundColor Yellow
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

        $matchedGroup = $groups | Where-Object { $_.MenuKey -eq $choice } | Select-Object -First 1
        if ($matchedGroup) {
            Show-LauncherRoleGroupMenu `
                -GroupId ([string]$matchedGroup.GroupId) `
                -Label ([string]$matchedGroup.Label) `
                -Description ([string]$matchedGroup.Description)
            continue
        }

        if ($ReturnToCaller) {
            Write-Host "Invalid option. Use P, 1-5, L, H, B, or Q." -ForegroundColor Red
        } else {
            Write-Host "Invalid option. Use P, 1-5, L, H, or Q." -ForegroundColor Red
        }
        Start-Sleep -Seconds 2
    }
}

try {
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
        $launcherEntry = Get-LauncherEntryById -EntryId $Explain
        Show-LauncherEntryPreview -LauncherEntry $launcherEntry
        Pause-IfNeeded
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
