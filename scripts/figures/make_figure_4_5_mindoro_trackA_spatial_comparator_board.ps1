param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Drawing

function Get-RepoRelativePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$AbsolutePath,
        [Parameter(Mandatory = $true)]
        [string]$Root
    )

    $resolvedAbsolute = [System.IO.Path]::GetFullPath($AbsolutePath)
    $resolvedRoot = [System.IO.Path]::GetFullPath($Root)
    $uriPath = [Uri]$resolvedAbsolute
    $uriRoot = [Uri]($resolvedRoot.TrimEnd("\") + "\")
    return [Uri]::UnescapeDataString($uriRoot.MakeRelativeUri($uriPath).ToString()).Replace("/", "\")
}

function Resolve-RequiredFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string]$RelativePath,
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [string[]]$CandidatePatterns = @()
    )

    $normalizedRelativePath = $RelativePath.Replace("/", "\")
    $absolute = Join-Path $Root $normalizedRelativePath
    if (Test-Path -LiteralPath $absolute) {
        return (Resolve-Path -LiteralPath $absolute).Path
    }

    $candidateMatches = @()
    foreach ($pattern in $CandidatePatterns) {
        $matchesForPattern = @(
            Get-ChildItem -Path $Root -Recurse -File -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -like $pattern } |
                Select-Object -ExpandProperty FullName
        )
        $candidateMatches += $matchesForPattern
    }
    $candidateMatches = @($candidateMatches | Sort-Object -Unique)
    $normalizedParent = [System.IO.Path]::GetDirectoryName($normalizedRelativePath)
    if ($candidateMatches.Length -gt 0 -and -not [string]::IsNullOrWhiteSpace($normalizedParent)) {
        $filteredCandidates = @(
            $candidateMatches | Where-Object {
                (Get-RepoRelativePath -AbsolutePath $_ -Root $Root).ToLower().Contains($normalizedParent.ToLower())
            }
        )
        if ($filteredCandidates.Length -eq 1) {
            return $filteredCandidates[0]
        }
    }
    if ($candidateMatches.Length -eq 1) {
        return $candidateMatches[0]
    }

    $candidateText = if ($candidateMatches.Length -gt 0) {
        ($candidateMatches | ForEach-Object { "  - $(Get-RepoRelativePath -AbsolutePath $_ -Root $Root)" }) -join [Environment]::NewLine
    } else {
        "  - none found"
    }

    throw @"
Missing required $Label.
Expected path:
  - $normalizedRelativePath
Candidate files found:
$candidateText
"@
}

function Get-FirstCsvRowByTrackId {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Rows,
        [Parameter(Mandatory = $true)]
        [string]$TrackId,
        [Parameter(Mandatory = $true)]
        [string]$CsvLabel
    )

    $matches = @($Rows | Where-Object { $_.track_id -eq $TrackId })
    if ($matches.Count -ne 1) {
        $available = ($Rows | ForEach-Object { $_.track_id } | Sort-Object -Unique) -join ", "
        throw "Expected exactly one row for track_id '$TrackId' in $CsvLabel. Available track_ids: $available"
    }
    return $matches[0]
}

function Get-YamlScalar {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Content,
        [Parameter(Mandatory = $true)]
        [string]$Key
    )

    $match = [regex]::Match($Content, "(?m)^" + [regex]::Escape($Key) + ":\s*(.+?)\s*$")
    if (-not $match.Success) {
        throw "Could not find YAML key '$Key' in scoring_grid.yaml."
    }
    return $match.Groups[1].Value.Trim()
}

function Normalize-FssValue {
    param(
        [Parameter(Mandatory = $true)]
        [double]$Value
    )

    if ([Math]::Abs($Value) -lt 0.00005) {
        return 0.0
    }
    return [Math]::Round($Value, 4)
}

function Convert-MetricRow {
    param(
        [Parameter(Mandatory = $true)]
        $Row
    )

    $fss3 = Normalize-FssValue -Value ([double]$Row.fss_3km)
    $fss5 = Normalize-FssValue -Value ([double]$Row.fss_5km)
    $fss10 = Normalize-FssValue -Value ([double]$Row.fss_10km)
    $meanFss = Normalize-FssValue -Value ([double]$Row.mean_fss)
    return [ordered]@{
        forecast_cells = [int]$Row.forecast_nonzero_cells
        nearest_distance_m = [Math]::Round([double]$Row.nearest_distance_to_obs_m, 2)
        fss_3km = $fss3
        fss_5km = $fss5
        fss_10km = $fss10
        mean_fss = $meanFss
    }
}

function Format-MetricBlock {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [hashtable]$Metrics
    )

    return @(
        $Label
        "Forecast cells: {0}" -f $Metrics.forecast_cells
        "Nearest distance: {0:N2} m" -f $Metrics.nearest_distance_m
        "FSS 3 km: {0:F4}" -f $Metrics.fss_3km
        "FSS 5 km: {0:F4}" -f $Metrics.fss_5km
        "FSS 10 km: {0:F4}" -f $Metrics.fss_10km
        "Mean FSS: {0:F4}" -f $Metrics.mean_fss
    ) -join [Environment]::NewLine
}

function Draw-TextCentered {
    param(
        [Parameter(Mandatory = $true)]
        [System.Drawing.Graphics]$Graphics,
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [System.Drawing.Font]$Font,
        [Parameter(Mandatory = $true)]
        [System.Drawing.Brush]$Brush,
        [Parameter(Mandatory = $true)]
        [int]$CanvasWidth,
        [Parameter(Mandatory = $true)]
        [single]$Y
    )

    $size = $Graphics.MeasureString($Text, $Font)
    $x = ($CanvasWidth - $size.Width) / 2.0
    $Graphics.DrawString($Text, $Font, $Brush, $x, $Y)
}

function Draw-LegendItem {
    param(
        [Parameter(Mandatory = $true)]
        [System.Drawing.Graphics]$Graphics,
        [Parameter(Mandatory = $true)]
        [int]$X,
        [Parameter(Mandatory = $true)]
        [int]$Y,
        [Parameter(Mandatory = $true)]
        [ValidateSet("obs", "opendrift", "pygnome", "seed")]
        [string]$Kind,
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [System.Drawing.Font]$Font
    )

    switch ($Kind) {
        "obs" {
            $fill = [System.Drawing.SolidBrush]::new([System.Drawing.Color]::FromArgb(180, 190, 200))
            $pen = [System.Drawing.Pen]::new([System.Drawing.Color]::FromArgb(70, 79, 89), 2)
            $Graphics.FillRectangle($fill, $X, $Y, 36, 22)
            $Graphics.DrawRectangle($pen, $X, $Y, 36, 22)
            $fill.Dispose()
            $pen.Dispose()
        }
        "opendrift" {
            $fill = [System.Drawing.SolidBrush]::new([System.Drawing.Color]::FromArgb(154, 205, 181))
            $pen = [System.Drawing.Pen]::new([System.Drawing.Color]::FromArgb(46, 139, 87), 2)
            $Graphics.FillRectangle($fill, $X, $Y, 36, 22)
            $Graphics.DrawRectangle($pen, $X, $Y, 36, 22)
            $fill.Dispose()
            $pen.Dispose()
        }
        "pygnome" {
            $fill = [System.Drawing.SolidBrush]::new([System.Drawing.Color]::FromArgb(215, 193, 238))
            $pen = [System.Drawing.Pen]::new([System.Drawing.Color]::FromArgb(155, 89, 214), 2)
            $Graphics.FillRectangle($fill, $X, $Y, 36, 22)
            $Graphics.DrawRectangle($pen, $X, $Y, 36, 22)
            $fill.Dispose()
            $pen.Dispose()
        }
        "seed" {
            $pen = [System.Drawing.Pen]::new([System.Drawing.Color]::FromArgb(219, 124, 0), 3)
            $pen.DashStyle = [System.Drawing.Drawing2D.DashStyle]::Dash
            $Graphics.DrawRectangle($pen, $X, $Y, 36, 22)
            $pen.Dispose()
        }
    }

    $Graphics.DrawString($Label, $Font, [System.Drawing.Brushes]::Black, [single]($X + 50), [single]($Y - 1))
}

$repoRootPath = (Resolve-Path -LiteralPath $RepoRoot).Path

$sourceBoardPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath "output\Phase 3B March13-14 Final Output\publication\comparator_pygnome\mindoro_crossmodel_board.png" `
    -Label "stored March 13 -> March 14 crossmodel board" `
    -CandidatePatterns @("mindoro_crossmodel_board.png", "mindoro_observed_masks_ensemble_pygnome_board.png")

$summaryCsvPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath "output\Phase 3B March13-14 Final Output\summary\comparator_pygnome\march13_14_reinit_crossmodel_summary.csv" `
    -Label "crossmodel summary CSV" `
    -CandidatePatterns @("march13_14_reinit_crossmodel_summary.csv")

$diagnosticsCsvPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath "output\Phase 3B March13-14 Final Output\summary\comparator_pygnome\march13_14_reinit_crossmodel_diagnostics.csv" `
    -Label "crossmodel diagnostics CSV" `
    -CandidatePatterns @("march13_14_reinit_crossmodel_diagnostics.csv")

$gridYamlPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath "data_processed\grids\scoring_grid.yaml" `
    -Label "scoring-grid metadata YAML" `
    -CandidatePatterns @("scoring_grid.yaml")

$summaryRows = @(Import-Csv -LiteralPath $summaryCsvPath)
$r1Row = Get-FirstCsvRowByTrackId -Rows $summaryRows -TrackId "R1_previous_reinit_p50" -CsvLabel (Get-RepoRelativePath -AbsolutePath $summaryCsvPath -Root $repoRootPath)
$pygnomeRow = Get-FirstCsvRowByTrackId -Rows $summaryRows -TrackId "pygnome_reinit_deterministic" -CsvLabel (Get-RepoRelativePath -AbsolutePath $summaryCsvPath -Root $repoRootPath)
$r0Rows = @($summaryRows | Where-Object { $_.track_id -eq "R0_reinit_p50" })

$observationMaskPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath $r1Row.observation_path `
    -Label "March 14 public observation mask raster" `
    -CandidatePatterns @("10b37c42a9754363a5f7b14199b077e6.tif")

$openDriftMaskPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath $r1Row.forecast_path `
    -Label "OpenDrift R1_previous mask_p50 raster" `
    -CandidatePatterns @("mask_p50_2023-03-14*localdate.tif")

$pygnomeMaskPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath $pygnomeRow.forecast_path `
    -Label "PyGNOME deterministic footprint raster" `
    -CandidatePatterns @("pygnome_footprint_mask_2023-03-14*localdate.tif")

$gridYamlContent = Get-Content -LiteralPath $gridYamlPath -Raw
$gridCrs = Get-YamlScalar -Content $gridYamlContent -Key "crs"
$gridResolution = [double](Get-YamlScalar -Content $gridYamlContent -Key "resolution")
$gridWidth = [int](Get-YamlScalar -Content $gridYamlContent -Key "width")
$gridHeight = [int](Get-YamlScalar -Content $gridYamlContent -Key "height")
$landMaskRelative = Get-YamlScalar -Content $gridYamlContent -Key "land_mask_path"
$seaMaskRelative = Get-YamlScalar -Content $gridYamlContent -Key "sea_mask_path"
$landMaskPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath $landMaskRelative `
    -Label "land mask raster" `
    -CandidatePatterns @("land_mask.tif")
$seaMaskPath = Resolve-RequiredFile `
    -Root $repoRootPath `
    -RelativePath $seaMaskRelative `
    -Label "sea mask raster" `
    -CandidatePatterns @("sea_mask.tif")

$openDriftMetrics = Convert-MetricRow -Row $r1Row
$pygnomeMetrics = Convert-MetricRow -Row $pygnomeRow

$primaryOutputDir = Join-Path $repoRootPath "output\figure_package_publication"
$secondaryOutputDir = Join-Path $repoRootPath "output\Phase 3B March13-14 Final Output\publication\comparator_pygnome"
New-Item -ItemType Directory -Force -Path $primaryOutputDir | Out-Null
New-Item -ItemType Directory -Force -Path $secondaryOutputDir | Out-Null

$outputStem = "Figure_4_5_Mindoro_TrackA_OpenDrift_PyGNOME_spatial_board"
$primaryPngPath = Join-Path $primaryOutputDir "$outputStem.png"
$primaryManifestPath = Join-Path $primaryOutputDir "$outputStem.json"
$secondaryPngPath = Join-Path $secondaryOutputDir "$outputStem.png"
$secondaryManifestPath = Join-Path $secondaryOutputDir "$outputStem.json"

$sourceBoard = [System.Drawing.Bitmap]::FromFile($sourceBoardPath)
$topCropHeight = 1380
$axisBandHeight = 86
$bottomBandHeight = 360
$canvasWidth = $sourceBoard.Width
$canvasHeight = $topCropHeight + $axisBandHeight + $bottomBandHeight

$canvas = [System.Drawing.Bitmap]::new($canvasWidth, $canvasHeight)
$graphics = [System.Drawing.Graphics]::FromImage($canvas)
$graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
$graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
$graphics.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit
$graphics.Clear([System.Drawing.Color]::White)

$graphics.DrawImage(
    $sourceBoard,
    [System.Drawing.Rectangle]::new(0, 0, $canvasWidth, $topCropHeight),
    [System.Drawing.Rectangle]::new(0, 0, $canvasWidth, $topCropHeight),
    [System.Drawing.GraphicsUnit]::Pixel
)

$figureLabelFont = [System.Drawing.Font]::new("Times New Roman", 30, [System.Drawing.FontStyle]::Bold)
$headerFont = [System.Drawing.Font]::new("Times New Roman", 28, [System.Drawing.FontStyle]::Bold)
$bodyFont = [System.Drawing.Font]::new("Times New Roman", 19)
$axisFont = [System.Drawing.Font]::new("Times New Roman", 22)

$darkBrush = [System.Drawing.SolidBrush]::new([System.Drawing.Color]::FromArgb(28, 37, 58))
$mutedBrush = [System.Drawing.SolidBrush]::new([System.Drawing.Color]::FromArgb(61, 76, 99))
$panelFillBrush = [System.Drawing.SolidBrush]::new([System.Drawing.Color]::FromArgb(252, 253, 255))
$dividerPen = [System.Drawing.Pen]::new([System.Drawing.Color]::FromArgb(210, 218, 229), 2)
$boxPen = [System.Drawing.Pen]::new([System.Drawing.Color]::FromArgb(173, 186, 204), 2)

$graphics.DrawString("Figure 4.5", $figureLabelFont, $mutedBrush, [single]144, [single]14)

$dividerY = $topCropHeight + 16
$graphics.DrawLine($dividerPen, 120, $dividerY, $canvasWidth - 120, $dividerY)
Draw-TextCentered `
    -Graphics $graphics `
    -Text "EPSG:32651 Easting (m)" `
    -Font $axisFont `
    -Brush ([System.Drawing.Brushes]::Black) `
    -CanvasWidth $canvasWidth `
    -Y ([single]($topCropHeight + 30))

$bottomY = $topCropHeight + $axisBandHeight
$legendX = 110
$legendWidth = 1160
$scoreX = 1350
$scoreWidth = 2025
$boxHeight = 304

$graphics.FillRectangle($panelFillBrush, $legendX, $bottomY + 18, $legendWidth, $boxHeight)
$graphics.DrawRectangle($boxPen, $legendX, $bottomY + 18, $legendWidth, $boxHeight)
$graphics.FillRectangle($panelFillBrush, $scoreX, $bottomY + 18, $scoreWidth, $boxHeight)
$graphics.DrawRectangle($boxPen, $scoreX, $bottomY + 18, $scoreWidth, $boxHeight)

$graphics.DrawString("Legend", $headerFont, $darkBrush, [single]($legendX + 18), [single]($bottomY + 28))
$graphics.DrawString("Table 4.9 values shown", $headerFont, $darkBrush, [single]($scoreX + 18), [single]($bottomY + 28))

Draw-LegendItem -Graphics $graphics -X ($legendX + 24) -Y ($bottomY + 88) -Kind "obs" -Label "March 14 independent public target observation" -Font $bodyFont
Draw-LegendItem -Graphics $graphics -X ($legendX + 24) -Y ($bottomY + 154) -Kind "opendrift" -Label "OpenDrift R1_previous footprint (mask_p50)" -Font $bodyFont
Draw-LegendItem -Graphics $graphics -X ($legendX + 24) -Y ($bottomY + 220) -Kind "pygnome" -Label "PyGNOME deterministic footprint" -Font $bodyFont
Draw-LegendItem -Graphics $graphics -X ($legendX + 24) -Y ($bottomY + 286) -Kind "seed" -Label "March 13 independent public seed observation" -Font $bodyFont

$openDriftScoreText = Format-MetricBlock -Label "OpenDrift R1_previous" -Metrics $openDriftMetrics
$pygnomeScoreText = Format-MetricBlock -Label "PyGNOME deterministic" -Metrics $pygnomeMetrics

$scoreColumnGap = 32
$scoreColumnWidth = [single](($scoreWidth - 40 - $scoreColumnGap) / 2.0)
$scoreLeftRect = [System.Drawing.RectangleF]::new([single]($scoreX + 20), [single]($bottomY + 76), $scoreColumnWidth, [single]($boxHeight - 90))
$scoreRightRect = [System.Drawing.RectangleF]::new([single]($scoreX + 20 + $scoreColumnWidth + $scoreColumnGap), [single]($bottomY + 76), $scoreColumnWidth, [single]($boxHeight - 90))
$scoreFormat = [System.Drawing.StringFormat]::new()
$scoreFormat.Alignment = [System.Drawing.StringAlignment]::Near
$scoreFormat.LineAlignment = [System.Drawing.StringAlignment]::Near
$graphics.DrawString($openDriftScoreText, $bodyFont, [System.Drawing.Brushes]::Black, $scoreLeftRect, $scoreFormat)
$graphics.DrawString($pygnomeScoreText, $bodyFont, [System.Drawing.Brushes]::Black, $scoreRightRect, $scoreFormat)

$canvas.SetResolution(300.0, 300.0)
$canvas.Save($primaryPngPath, [System.Drawing.Imaging.ImageFormat]::Png)
Copy-Item -LiteralPath $primaryPngPath -Destination $secondaryPngPath -Force

$noteTextManifest = "Comparator support only; March 13 is the public seed observation, March 14 is the public target observation, and PyGNOME is not the observational scoring reference."
$manifest = [ordered]@{
    figure_label = "Figure 4.5"
    title = "Mindoro Track A same-case OpenDrift-PyGNOME spatial comparator board"
    export_method = "cleaned thesis export from stored March 13 -> March 14 crossmodel publication board"
    source_file_paths_used = [ordered]@{
        source_board_png = (Get-RepoRelativePath -AbsolutePath $sourceBoardPath -Root $repoRootPath)
        observation_mask_tif = (Get-RepoRelativePath -AbsolutePath $observationMaskPath -Root $repoRootPath)
        opendrift_r1_previous_mask_tif = (Get-RepoRelativePath -AbsolutePath $openDriftMaskPath -Root $repoRootPath)
        pygnome_deterministic_mask_tif = (Get-RepoRelativePath -AbsolutePath $pygnomeMaskPath -Root $repoRootPath)
        summary_csv = (Get-RepoRelativePath -AbsolutePath $summaryCsvPath -Root $repoRootPath)
        diagnostics_csv = (Get-RepoRelativePath -AbsolutePath $diagnosticsCsvPath -Root $repoRootPath)
        scoring_grid_yaml = (Get-RepoRelativePath -AbsolutePath $gridYamlPath -Root $repoRootPath)
        land_mask_tif = (Get-RepoRelativePath -AbsolutePath $landMaskPath -Root $repoRootPath)
        sea_mask_tif = (Get-RepoRelativePath -AbsolutePath $seaMaskPath -Root $repoRootPath)
    }
    grid = [ordered]@{
        crs = $gridCrs
        resolution_m = $gridResolution
        width_cells = $gridWidth
        height_cells = $gridHeight
    }
    board_source_crop = [ordered]@{
        source_size_px = @($sourceBoard.Width, $sourceBoard.Height)
        retained_top_height_px = $topCropHeight
        axis_band_height_px = $axisBandHeight
        bottom_summary_band_height_px = $bottomBandHeight
        output_size_px = @($canvasWidth, $canvasHeight)
    }
    output_png_path = (Get-RepoRelativePath -AbsolutePath $primaryPngPath -Root $repoRootPath)
    copied_png_path = (Get-RepoRelativePath -AbsolutePath $secondaryPngPath -Root $repoRootPath)
    metrics_shown = [ordered]@{
        OpenDrift_R1_previous = $openDriftMetrics
        PyGNOME_deterministic = $pygnomeMetrics
    }
    omitted_visual_row = if ($r0Rows.Count -eq 1) {
        [ordered]@{
            track_id = $r0Rows[0].track_id
            reason = "archived/no target-date survival"
            empty_forecast_reason = $r0Rows[0].empty_forecast_reason
        }
    } else {
        $null
    }
    note = $noteTextManifest
}

$manifestJson = $manifest | ConvertTo-Json -Depth 8
[System.IO.File]::WriteAllText($primaryManifestPath, $manifestJson + [Environment]::NewLine)
Copy-Item -LiteralPath $primaryManifestPath -Destination $secondaryManifestPath -Force

$scoreFormat.Dispose()
$figureLabelFont.Dispose()
$headerFont.Dispose()
$bodyFont.Dispose()
$axisFont.Dispose()
$darkBrush.Dispose()
$mutedBrush.Dispose()
$panelFillBrush.Dispose()
$dividerPen.Dispose()
$boxPen.Dispose()
$graphics.Dispose()
$canvas.Dispose()
$sourceBoard.Dispose()

Write-Output "Created:"
Write-Output "  - $(Get-RepoRelativePath -AbsolutePath $primaryPngPath -Root $repoRootPath)"
Write-Output "  - $(Get-RepoRelativePath -AbsolutePath $primaryManifestPath -Root $repoRootPath)"
Write-Output "  - $(Get-RepoRelativePath -AbsolutePath $secondaryPngPath -Root $repoRootPath)"
Write-Output "  - $(Get-RepoRelativePath -AbsolutePath $secondaryManifestPath -Root $repoRootPath)"
