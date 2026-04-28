param(
    [switch]$NoPause
)

$Script:RepoRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
Set-Location $Script:RepoRoot

& (Join-Path $Script:RepoRoot "start.ps1") -Panel -NoPause:$NoPause
exit $LASTEXITCODE
