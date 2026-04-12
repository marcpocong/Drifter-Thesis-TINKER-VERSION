param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [int]$IntervalSeconds = 60
)

$repo = (Resolve-Path $RepoRoot).Path
$phaseDir = Join-Path $repo "output\CASE_MINDORO_RETRO_2023\phase3b_extended_public_scored_march13_14_reinit"
$summaryCsv = Join-Path $phaseDir "march13_14_reinit_summary.csv"
$runManifestJson = Join-Path $phaseDir "march13_14_reinit_run_manifest.json"
$branchSurvivalCsv = Join-Path $phaseDir "march13_14_reinit_branch_survival_summary.csv"
$statusJson = Join-Path $phaseDir "watch_status.json"
$statusMd = Join-Path $phaseDir "watch_status.md"
$doneMarker = Join-Path $phaseDir "watch_complete.marker"

New-Item -ItemType Directory -Force -Path $phaseDir | Out-Null

function Get-MemberCount {
    param([string]$Pattern)
    return (Get-ChildItem $Pattern -ErrorAction SilentlyContinue | Measure-Object).Count
}

function Write-StatusFiles {
    param([hashtable]$Payload)

    $Payload | ConvertTo-Json -Depth 4 | Set-Content -Path $statusJson -Encoding utf8

    $lines = @(
        "# March 13 -> March 14 Reinit Watch Status",
        "",
        "- Checked at local: $($Payload.checked_at_local)",
        "- Checked at UTC: $($Payload.checked_at_utc)",
        "- Status: $($Payload.status)",
        "- R0 members: $($Payload.r0_member_count)/$($Payload.expected_member_count)",
        "- R1_previous members: $($Payload.r1_member_count)/$($Payload.expected_member_count)",
        "- Summary CSV exists: $($Payload.summary_exists)",
        "- Run manifest exists: $($Payload.run_manifest_exists)",
        "- Branch survival CSV exists: $($Payload.branch_survival_exists)",
        "- PyGNOME comparator ready: $($Payload.pygnome_comparator_ready)",
        "- Phase dir: $($Payload.phase_dir)"
    )

    Set-Content -Path $statusMd -Value ($lines -join "`n") -Encoding utf8
}

while ($true) {
    $r0Count = Get-MemberCount (Join-Path $phaseDir "R0\model_run\ensemble\member_*.nc")
    $r1Count = Get-MemberCount (Join-Path $phaseDir "R1_previous\model_run\ensemble\member_*.nc")
    $summaryExists = Test-Path $summaryCsv
    $manifestExists = Test-Path $runManifestJson
    $branchSurvivalExists = Test-Path $branchSurvivalCsv
    $complete = ($r1Count -ge 50) -and $summaryExists -and $manifestExists -and $branchSurvivalExists

    $status = "waiting_for_outputs"
    if ($r1Count -gt 0 -or $r0Count -gt 0) {
        $status = "running"
    }
    if ($complete) {
        $status = "complete"
    }

    $payload = [ordered]@{
        checked_at_local = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss zzz")
        checked_at_utc = ([DateTime]::UtcNow).ToString("yyyy-MM-ddTHH:mm:ssZ")
        status = $status
        expected_member_count = 50
        r0_member_count = $r0Count
        r1_member_count = $r1Count
        summary_exists = $summaryExists
        run_manifest_exists = $manifestExists
        branch_survival_exists = $branchSurvivalExists
        pygnome_comparator_ready = $complete
        phase_dir = $phaseDir
    }

    Write-StatusFiles -Payload $payload

    if ($complete) {
        Set-Content -Path $doneMarker -Value "complete $(Get-Date -Format o)" -Encoding ascii
        break
    }

    Start-Sleep -Seconds $IntervalSeconds
}
