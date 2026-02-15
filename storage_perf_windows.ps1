[CmdletBinding(DefaultParameterSetName = 'Run')]
param(
    [Parameter(ParameterSetName = 'Run', Mandatory = $true)]
    [string]$Label,

    [Parameter(ParameterSetName = 'Run', Mandatory = $true)]
    [string]$TargetPath,

    [Parameter(ParameterSetName = 'Run')]
    [string]$DiskSpdPath = "diskspd.exe",

    [Parameter(ParameterSetName = 'Run')]
    [bool]$AutoInstallDiskSpd = $true,

    [Parameter(ParameterSetName = 'Run')]
    [string]$DiskSpdInstallDir = "$env:LOCALAPPDATA\diskspd",

    [Parameter(ParameterSetName = 'Run')]
    [string]$Size = "20G",

    [Parameter(ParameterSetName = 'Run')]
    [int]$DurationSeconds = 60,

    [Parameter(ParameterSetName = 'Run')]
    [int]$WarmupSeconds = 10,

    [Parameter(ParameterSetName = 'Run')]
    [int]$Repeat = 3,

    [Parameter(ParameterSetName = 'Run')]
    [string]$OutputDir = "results",

    [Parameter(ParameterSetName = 'Run')]
    [string]$WorkloadsJson = "",

    [Parameter(ParameterSetName = 'Run')]
    [string]$DiskInstance = "",

    [Parameter(ParameterSetName = 'Run')]
    [string[]]$ExtraDiskSpdArg = @(),

    [Parameter(ParameterSetName = 'Run')]
    [Parameter(ParameterSetName = 'Compare')]
    [bool]$GeneratePdf = $true,

    [Parameter(ParameterSetName = 'Run')]
    [Parameter(ParameterSetName = 'Compare')]
    [string]$PdfBrowserPath = "",

    [Parameter(ParameterSetName = 'Compare', Mandatory = $true)]
    [switch]$Compare,

    [Parameter(ParameterSetName = 'Compare', Mandatory = $true)]
    [string]$A,

    [Parameter(ParameterSetName = 'Compare', Mandatory = $true)]
    [string]$B,

    [Parameter(ParameterSetName = 'Compare')]
    [string]$CompareOutput = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-DefaultWorkloads {
    @(
        @{ name = "seq_read_1m_qd32"; rw = "read"; bs = "1M"; o = 32; t = 1; rwmixread = $null },
        @{ name = "seq_write_1m_qd32"; rw = "write"; bs = "1M"; o = 32; t = 1; rwmixread = $null },
        @{ name = "rand_read_4k_qd1"; rw = "randread"; bs = "4K"; o = 1; t = 1; rwmixread = $null },
        @{ name = "rand_read_4k_qd32"; rw = "randread"; bs = "4K"; o = 32; t = 1; rwmixread = $null },
        @{ name = "rand_write_4k_qd1"; rw = "randwrite"; bs = "4K"; o = 1; t = 1; rwmixread = $null },
        @{ name = "rand_write_4k_qd32"; rw = "randwrite"; bs = "4K"; o = 32; t = 1; rwmixread = $null },
        @{ name = "rand_rw_4k_70r_qd32"; rw = "randrw"; bs = "4K"; o = 32; t = 1; rwmixread = 70 },
        @{ name = "rand_rw_16k_70r_qd32"; rw = "randrw"; bs = "16K"; o = 32; t = 1; rwmixread = 70 },
        @{ name = "rand_read_4k_qd32_t4"; rw = "randread"; bs = "4K"; o = 32; t = 4; rwmixread = $null },
        @{ name = "rand_write_4k_qd32_t4"; rw = "randwrite"; bs = "4K"; o = 32; t = 4; rwmixread = $null },
        @{ name = "rand_rw_4k_70r_qd64_t4"; rw = "randrw"; bs = "4K"; o = 64; t = 4; rwmixread = 70 }
    )
}

function Resolve-DiskSpd {
    param(
        [string]$Binary,
        [bool]$AutoInstall,
        [string]$InstallDir
    )
    $cmd = Get-Command $Binary -ErrorAction SilentlyContinue
    if ($null -ne $cmd) {
        return $cmd.Source
    }
    if (Test-Path -LiteralPath $Binary) {
        return (Resolve-Path -LiteralPath $Binary).Path
    }

    $candidates = @(
        (Join-Path $PSScriptRoot "tools\diskspd"),
        $InstallDir,
        (Join-Path $env:ProgramFiles "diskspd"),
        (Join-Path ${env:ProgramFiles(x86)} "diskspd")
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

    foreach ($dir in $candidates) {
        $found = Find-DiskSpdInDirectory -Root $dir
        if ($null -ne $found) { return $found }
    }

    if (-not $AutoInstall) {
        throw "diskspd binary not found: $Binary. Auto-install is disabled."
    }

    Write-Host "diskspd not found. Attempting auto-install..."
    $installed = $false

    if (Try-InstallDiskSpdWithWinget) {
        $cmd = Get-Command "diskspd.exe" -ErrorAction SilentlyContinue
        if ($null -ne $cmd) { return $cmd.Source }
        foreach ($dir in $candidates) {
            $found = Find-DiskSpdInDirectory -Root $dir
            if ($null -ne $found) { return $found }
        }
        $installed = $true
    }

    $zipInstall = Install-DiskSpdFromZip -InstallDir $InstallDir
    if ($null -ne $zipInstall) {
        return $zipInstall
    }

    $msg = "diskspd binary not found and auto-install failed. "
    if ($installed) {
        $msg += "winget reported success but diskspd.exe is still unresolved. "
    }
    $msg += "Install manually from https://aka.ms/diskspd or pass -DiskSpdPath to a valid diskspd.exe."
    throw $msg
}

function Find-DiskSpdInDirectory {
    param([string]$Root)
    if ([string]::IsNullOrWhiteSpace($Root)) { return $null }
    if (-not (Test-Path -LiteralPath $Root)) { return $null }

    $bins = Get-ChildItem -LiteralPath $Root -Recurse -File -Filter "diskspd.exe" -ErrorAction SilentlyContinue
    if ($null -eq $bins -or $bins.Count -eq 0) { return $null }

    $best = $bins | Sort-Object `
        @{ Expression = {
                $p = $_.FullName.ToLowerInvariant()
                if ($p -match 'amd64|x64') { 0 }
                elseif ($p -match 'x86') { 2 }
                else { 1 }
            }
        },
        @{ Expression = { $_.FullName.Length } } | Select-Object -First 1
    if ($null -eq $best) { return $null }
    return $best.FullName
}

function Try-InstallDiskSpdWithWinget {
    $winget = Get-Command "winget" -ErrorAction SilentlyContinue
    if ($null -eq $winget) { return $false }

    $ids = @(
        "Microsoft.DiskSpd",
        "Microsoft.Diskspd",
        "DiskSpd.DiskSpd"
    )

    foreach ($id in $ids) {
        try {
            Write-Host "Trying winget package: $id"
            $args = @(
                "install",
                "--id", $id,
                "--exact",
                "--silent",
                "--accept-package-agreements",
                "--accept-source-agreements"
            )
            $proc = Start-Process -FilePath $winget.Source -ArgumentList $args -NoNewWindow -PassThru -Wait
            if ($proc.ExitCode -eq 0) {
                return $true
            }
        } catch {
            # Keep trying fallback ids.
        }
    }
    return $false
}

function Install-DiskSpdFromZip {
    param([string]$InstallDir)
    if ([string]::IsNullOrWhiteSpace($InstallDir)) { return $null }

    $urls = @(
        "https://aka.ms/diskspd",
        "https://github.com/microsoft/diskspd/releases/latest/download/DiskSpd.zip"
    )
    $tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("diskspd-install-" + [guid]::NewGuid().ToString("N"))
    $zipPath = Join-Path $tmpRoot "diskspd.zip"
    $extractDir = Join-Path $tmpRoot "extract"
    New-Item -ItemType Directory -Path $tmpRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

    try {
        foreach ($url in $urls) {
            try {
                Write-Host "Downloading DiskSpd from $url"
                Invoke-WebRequest -Uri $url -OutFile $zipPath
                Expand-Archive -LiteralPath $zipPath -DestinationPath $extractDir -Force
                $found = Find-DiskSpdInDirectory -Root $extractDir
                if ($null -eq $found) {
                    Remove-Item -LiteralPath $extractDir -Recurse -Force -ErrorAction SilentlyContinue
                    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null
                    continue
                }
                $installBin = Join-Path $InstallDir "diskspd.exe"
                Copy-Item -LiteralPath $found -Destination $installBin -Force
                return $installBin
            } catch {
                Remove-Item -LiteralPath $zipPath -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $extractDir -Recurse -Force -ErrorAction SilentlyContinue
                New-Item -ItemType Directory -Path $extractDir -Force | Out-Null
                continue
            }
        }
    } finally {
        Remove-Item -LiteralPath $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    return $null
}

function ConvertTo-Number {
    param([object]$Value)
    if ($null -eq $Value) { return $null }
    if ($Value -is [double] -or $Value -is [int] -or $Value -is [long] -or $Value -is [decimal]) {
        return [double]$Value
    }
    $out = 0.0
    if ([double]::TryParse($Value.ToString(), [ref]$out)) {
        return $out
    }
    return $null
}

function Get-Mean {
    param([double[]]$Values)
    if ($null -eq $Values -or $Values.Count -eq 0) { return $null }
    return ($Values | Measure-Object -Average).Average
}

function Get-Percentile {
    param(
        [double[]]$Values,
        [double]$Percent
    )
    if ($null -eq $Values -or $Values.Count -eq 0) { return $null }
    $sorted = $Values | Sort-Object
    if ($sorted.Count -eq 1) { return $sorted[0] }
    if ($Percent -le 0) { return $sorted[0] }
    if ($Percent -ge 100) { return $sorted[-1] }
    $rank = ($sorted.Count - 1) * ($Percent / 100.0)
    $low = [int][Math]::Floor($rank)
    $high = [int][Math]::Ceiling($rank)
    if ($low -eq $high) { return $sorted[$low] }
    $w = $rank - $low
    return $sorted[$low] + (($sorted[$high] - $sorted[$low]) * $w)
}

function Get-MetricSeries {
    param(
        [object[]]$Samples,
        [string]$Metric
    )
    $vals = @()
    foreach ($s in $Samples) {
        if ($s.PSObject.Properties.Name -contains $Metric) {
            $n = ConvertTo-Number $s.$Metric
            if ($null -ne $n) { $vals += $n }
        }
    }
    return $vals
}

function Load-Workloads {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) {
        return Get-DefaultWorkloads
    }
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Workloads JSON file not found: $Path"
    }
    $payload = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    if ($payload -isnot [System.Array]) {
        throw "Workloads JSON must be an array."
    }
    return $payload
}

function Select-DiskInstance {
    param(
        [hashtable]$SeriesByDisk,
        [string]$RequestedDisk
    )
    if ($SeriesByDisk.Count -eq 0) { return $null }
    if (-not [string]::IsNullOrWhiteSpace($RequestedDisk)) {
        if ($SeriesByDisk.ContainsKey($RequestedDisk)) {
            return $RequestedDisk
        }
        $requestedLower = $RequestedDisk.ToLowerInvariant()
        foreach ($k in $SeriesByDisk.Keys) {
            if ($k.ToLowerInvariant() -eq $requestedLower) {
                return $k
            }
        }
    }

    $bestDisk = $null
    $bestScore = [double]::NegativeInfinity
    foreach ($disk in $SeriesByDisk.Keys) {
        if ($disk -eq "_Total") { continue }
        $samples = $SeriesByDisk[$disk]
        $util = Get-Mean (Get-MetricSeries -Samples $samples -Metric "pct_disk_time")
        $xfer = Get-Mean (Get-MetricSeries -Samples $samples -Metric "disk_transfers_sec")
        $score = (ConvertTo-Number $util)
        if ($null -eq $score) { $score = 0.0 }
        if ($null -ne $xfer) { $score += ($xfer * 0.001) }
        if ($score -gt $bestScore) {
            $bestScore = $score
            $bestDisk = $disk
        }
    }
    if ($null -ne $bestDisk) { return $bestDisk }
    return ($SeriesByDisk.Keys | Select-Object -First 1)
}

function Parse-DiskSpdMetrics {
    param([string]$StdoutPath)
    # DiskSpd text output can vary by version. We parse robustly and fallback to blanks.
    $text = Get-Content -LiteralPath $StdoutPath -Raw
    $lines = $text -split "`r?`n"

    $readIops = $null
    $writeIops = $null
    $totalIops = $null
    $readMiB = $null
    $writeMiB = $null
    $totalMiB = $null
    $readLatUs = $null
    $writeLatUs = $null

    $context = ""
    foreach ($line in $lines) {
        $trim = $line.Trim()
        if ($trim -match '^(total|read|write):\s*$') {
            $context = $matches[1].ToLowerInvariant()
            continue
        }
        if ($trim -eq "") { continue }

        # Common DiskSpd "total/read/write:" table row pattern:
        # bytes  I/Os  MiB/s  I/O per s  AvgLat  LatStdDev  file
        if ($trim -match '^\s*([\d,]+(?:\.[0-9]+)?)\s*\|\s*([\d,]+(?:\.[0-9]+)?)\s*\|\s*([\d,]+(?:\.[0-9]+)?)\s*\|\s*([\d,]+(?:\.[0-9]+)?)\s*\|\s*([\d,]+(?:\.[0-9]+)?)') {
            $mibPerSec = ConvertTo-Number ($matches[3] -replace ',', '')
            $iops = ConvertTo-Number ($matches[4] -replace ',', '')
            $avgLatMs = ConvertTo-Number ($matches[5] -replace ',', '')
            if ($context -eq "read") {
                $readMiB = $mibPerSec
                $readIops = $iops
                if ($null -ne $avgLatMs) { $readLatUs = $avgLatMs * 1000.0 }
            } elseif ($context -eq "write") {
                $writeMiB = $mibPerSec
                $writeIops = $iops
                if ($null -ne $avgLatMs) { $writeLatUs = $avgLatMs * 1000.0 }
            } elseif ($context -eq "total") {
                $totalMiB = $mibPerSec
                $totalIops = $iops
            }
        }
    }

    # Fallback totals if only read/write were parsed.
    if ($null -eq $totalIops) {
        if ($null -ne $readIops -or $null -ne $writeIops) {
            $ri = if ($null -ne $readIops) { $readIops } else { 0.0 }
            $wi = if ($null -ne $writeIops) { $writeIops } else { 0.0 }
            $totalIops = $ri + $wi
        }
    }
    if ($null -eq $totalMiB) {
        if ($null -ne $readMiB -or $null -ne $writeMiB) {
            $rb = if ($null -ne $readMiB) { $readMiB } else { 0.0 }
            $wb = if ($null -ne $writeMiB) { $writeMiB } else { 0.0 }
            $totalMiB = $rb + $wb
        }
    }

    return [ordered]@{
        read_iops = $readIops
        write_iops = $writeIops
        total_iops = $totalIops
        read_bw_mib_s = $readMiB
        write_bw_mib_s = $writeMiB
        total_bw_mib_s = $totalMiB
        read_lat_mean_us = $readLatUs
        write_lat_mean_us = $writeLatUs
        read_p99_us = $null
        write_p99_us = $null
    }
}

function Build-DiskSpdArgs {
    param(
        [object]$Workload,
        [string]$Size,
        [int]$DurationSeconds,
        [int]$WarmupSeconds,
        [string]$TargetPath,
        [string[]]$ExtraDiskSpdArg
    )
    $args = @()
    $args += "-b$($Workload.bs)"
    $args += "-o$([int]$Workload.o)"
    $args += "-t$([int]$Workload.t)"
    $args += "-d$DurationSeconds"
    $args += "-W$WarmupSeconds"
    $args += "-L"
    $args += "-Sh"
    $args += "-c$Size"

    $rw = $Workload.rw.ToString().ToLowerInvariant()
    switch ($rw) {
        "read" {
            $args += "-w0"
        }
        "write" {
            $args += "-w100"
        }
        "randread" {
            $args += "-r"
            $args += "-w0"
        }
        "randwrite" {
            $args += "-r"
            $args += "-w100"
        }
        "randrw" {
            $args += "-r"
            $mix = if ($null -ne $Workload.rwmixread) { [int]$Workload.rwmixread } else { 70 }
            $writePct = 100 - $mix
            $args += "-w$writePct"
        }
        default {
            throw "Unsupported workload rw: $($Workload.rw)"
        }
    }

    if ($ExtraDiskSpdArg.Count -gt 0) {
        $args += $ExtraDiskSpdArg
    }
    $args += $TargetPath
    return ,$args
}

function Collect-CounterSnapshot {
    param(
        [string[]]$CounterPaths
    )
    $data = @()
    try {
        $sample = Get-Counter -Counter $CounterPaths -SampleInterval 1 -MaxSamples 1
    } catch {
        return $data
    }

    foreach ($cs in $sample.CounterSamples) {
        $instance = $cs.InstanceName
        if ([string]::IsNullOrWhiteSpace($instance)) { continue }
        $path = $cs.Path.ToLowerInvariant()
        $value = [double]$cs.CookedValue

        $metric = $null
        if ($path -like "*\physicaldisk(*)\disk reads/sec*") { $metric = "disk_reads_sec" }
        elseif ($path -like "*\physicaldisk(*)\disk writes/sec*") { $metric = "disk_writes_sec" }
        elseif ($path -like "*\physicaldisk(*)\disk transfers/sec*") { $metric = "disk_transfers_sec" }
        elseif ($path -like "*\physicaldisk(*)\disk read bytes/sec*") { $metric = "disk_read_bytes_sec" }
        elseif ($path -like "*\physicaldisk(*)\disk write bytes/sec*") { $metric = "disk_write_bytes_sec" }
        elseif ($path -like "*\physicaldisk(*)\avg. disk sec/read*") { $metric = "avg_disk_sec_read" }
        elseif ($path -like "*\physicaldisk(*)\avg. disk sec/write*") { $metric = "avg_disk_sec_write" }
        elseif ($path -like "*\physicaldisk(*)\avg. disk sec/transfer*") { $metric = "avg_disk_sec_transfer" }
        elseif ($path -like "*\physicaldisk(*)\current disk queue length*") { $metric = "queue_length" }
        elseif ($path -like "*\physicaldisk(*)\% disk time*") { $metric = "pct_disk_time" }
        elseif ($path -like "*\processor(_total)\% processor time*") { $metric = "cpu_pct_busy" }
        else { continue }

        $data += [pscustomobject]@{
            instance = $instance
            metric = $metric
            value = $value
            timestamp = $sample.Timestamp
        }
    }
    return $data
}

function Summarize-Counters {
    param(
        [object[]]$RawSamples,
        [string]$RequestedDisk
    )
    $seriesByDisk = @{}
    $cpuBusy = @()

    foreach ($s in $RawSamples) {
        if ($s.metric -eq "cpu_pct_busy") {
            $cpuBusy += [double]$s.value
            continue
        }
        if (-not $seriesByDisk.ContainsKey($s.instance)) {
            $seriesByDisk[$s.instance] = @()
        }
    }

    # Re-shape into per-instance, per-sample objects where each object can have several metrics.
    $byInstanceTime = @{}
    foreach ($s in $RawSamples) {
        if ($s.metric -eq "cpu_pct_busy") { continue }
        $key = "$($s.instance)|$($s.timestamp.ToUniversalTime().ToString("O"))"
        if (-not $byInstanceTime.ContainsKey($key)) {
            $byInstanceTime[$key] = [ordered]@{
                instance = $s.instance
                ts = $s.timestamp
            }
        }
        $byInstanceTime[$key][$s.metric] = [double]$s.value
    }

    foreach ($row in $byInstanceTime.Values) {
        $inst = $row.instance
        $seriesByDisk[$inst] += [pscustomobject]$row
    }

    $selected = Select-DiskInstance -SeriesByDisk $seriesByDisk -RequestedDisk $RequestedDisk
    $diskSamples = if ($null -ne $selected) { $seriesByDisk[$selected] } else { @() }

    $readIops = Get-MetricSeries -Samples $diskSamples -Metric "disk_reads_sec"
    $writeIops = Get-MetricSeries -Samples $diskSamples -Metric "disk_writes_sec"
    $readBps = Get-MetricSeries -Samples $diskSamples -Metric "disk_read_bytes_sec"
    $writeBps = Get-MetricSeries -Samples $diskSamples -Metric "disk_write_bytes_sec"
    $avgReadSec = Get-MetricSeries -Samples $diskSamples -Metric "avg_disk_sec_read"
    $avgWriteSec = Get-MetricSeries -Samples $diskSamples -Metric "avg_disk_sec_write"
    $awaitSec = Get-MetricSeries -Samples $diskSamples -Metric "avg_disk_sec_transfer"
    $util = Get-MetricSeries -Samples $diskSamples -Metric "pct_disk_time"
    $queue = Get-MetricSeries -Samples $diskSamples -Metric "queue_length"

    $result = [ordered]@{
        selected_device = $selected

        read_iops_counter = Get-Mean $readIops
        write_iops_counter = Get-Mean $writeIops
        read_bw_mib_s_counter = if ($readBps.Count -gt 0) { (Get-Mean $readBps) / 1MB } else { $null }
        write_bw_mib_s_counter = if ($writeBps.Count -gt 0) { (Get-Mean $writeBps) / 1MB } else { $null }

        read_lat_mean_us_counter = if ($avgReadSec.Count -gt 0) { (Get-Mean $avgReadSec) * 1e6 } else { $null }
        write_lat_mean_us_counter = if ($avgWriteSec.Count -gt 0) { (Get-Mean $avgWriteSec) * 1e6 } else { $null }

        io_await_mean_ms = if ($awaitSec.Count -gt 0) { (Get-Mean $awaitSec) * 1e3 } else { $null }
        io_await_p95_ms = if ($awaitSec.Count -gt 0) { (Get-Percentile -Values $awaitSec -Percent 95) * 1e3 } else { $null }
        io_await_max_ms = if ($awaitSec.Count -gt 0) { ($awaitSec | Measure-Object -Maximum).Maximum * 1e3 } else { $null }

        io_util_mean_pct = Get-Mean $util
        io_util_p95_pct = if ($util.Count -gt 0) { Get-Percentile -Values $util -Percent 95 } else { $null }
        io_util_max_pct = if ($util.Count -gt 0) { ($util | Measure-Object -Maximum).Maximum } else { $null }

        io_queue_mean = Get-Mean $queue
        io_queue_p95 = if ($queue.Count -gt 0) { Get-Percentile -Values $queue -Percent 95 } else { $null }

        # Windows does not expose Linux-style CPU iowait directly.
        cpu_iowait_mean_pct = $null
        cpu_iowait_p95_pct = $null
        cpu_iowait_max_pct = $null

        cpu_busy_mean_pct = Get-Mean $cpuBusy
        cpu_busy_p95_pct = if ($cpuBusy.Count -gt 0) { Get-Percentile -Values $cpuBusy -Percent 95 } else { $null }
    }
    return $result
}

function Run-Workload {
    param(
        [string]$DiskSpdExe,
        [object]$Workload,
        [int]$Iteration,
        [string]$TargetPath,
        [string]$Size,
        [int]$DurationSeconds,
        [int]$WarmupSeconds,
        [string]$RequestedDisk,
        [string[]]$ExtraDiskSpdArg,
        [string]$RawDir
    )
    $slug = $Workload.name
    $runPrefix = "{0}.run{1:d2}" -f $slug, $Iteration
    $stdoutPath = Join-Path $RawDir "$runPrefix.diskspd.stdout.log"
    $stderrPath = Join-Path $RawDir "$runPrefix.diskspd.stderr.log"
    $counterRawPath = Join-Path $RawDir "$runPrefix.counters.json"

    $counterPaths = @(
        '\PhysicalDisk(*)\Disk Reads/sec',
        '\PhysicalDisk(*)\Disk Writes/sec',
        '\PhysicalDisk(*)\Disk Transfers/sec',
        '\PhysicalDisk(*)\Disk Read Bytes/sec',
        '\PhysicalDisk(*)\Disk Write Bytes/sec',
        '\PhysicalDisk(*)\Avg. Disk sec/Read',
        '\PhysicalDisk(*)\Avg. Disk sec/Write',
        '\PhysicalDisk(*)\Avg. Disk sec/Transfer',
        '\PhysicalDisk(*)\Current Disk Queue Length',
        '\PhysicalDisk(*)\% Disk Time',
        '\Processor(_Total)\% Processor Time'
    )

    $args = Build-DiskSpdArgs -Workload $Workload -Size $Size -DurationSeconds $DurationSeconds -WarmupSeconds $WarmupSeconds -TargetPath $TargetPath -ExtraDiskSpdArg $ExtraDiskSpdArg

    $counterSamples = @()
    $start = Get-Date
    $proc = Start-Process -FilePath $DiskSpdExe -ArgumentList $args -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    while (-not $proc.HasExited) {
        $counterSamples += Collect-CounterSnapshot -CounterPaths $counterPaths
    }
    # Avoid PID race conditions: waiting by process object is more reliable than Wait-Process -Id.
    try {
        [void]$proc.WaitForExit()
    } catch {
        # If the process object is already finalized, continue and rely on captured logs/exit code.
    }
    $elapsed = (Get-Date) - $start

    $counterSamples | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $counterRawPath -Encoding UTF8

    $counterSummary = Summarize-Counters -RawSamples $counterSamples -RequestedDisk $RequestedDisk
    $diskspdSummary = Parse-DiskSpdMetrics -StdoutPath $stdoutPath
    $exitCode = $null
    try {
        $proc.Refresh()
        $exitCode = $proc.ExitCode
    } catch {
        $exitCode = $null
    }
    if ($null -eq $exitCode) {
        $exitCode = 0
    }

    $readIops = if ($null -ne $diskspdSummary.read_iops) { $diskspdSummary.read_iops } else { $counterSummary.read_iops_counter }
    $writeIops = if ($null -ne $diskspdSummary.write_iops) { $diskspdSummary.write_iops } else { $counterSummary.write_iops_counter }
    $totalIops = if ($null -ne $diskspdSummary.total_iops) { $diskspdSummary.total_iops } else {
        if ($null -ne $readIops -or $null -ne $writeIops) {
            $ri = if ($null -ne $readIops) { $readIops } else { 0.0 }
            $wi = if ($null -ne $writeIops) { $writeIops } else { 0.0 }
            $ri + $wi
        } else {
            $null
        }
    }
    $readBw = if ($null -ne $diskspdSummary.read_bw_mib_s) { $diskspdSummary.read_bw_mib_s } else { $counterSummary.read_bw_mib_s_counter }
    $writeBw = if ($null -ne $diskspdSummary.write_bw_mib_s) { $diskspdSummary.write_bw_mib_s } else { $counterSummary.write_bw_mib_s_counter }
    $totalBw = if ($null -ne $diskspdSummary.total_bw_mib_s) { $diskspdSummary.total_bw_mib_s } else {
        if ($null -ne $readBw -or $null -ne $writeBw) {
            $rb = if ($null -ne $readBw) { $readBw } else { 0.0 }
            $wb = if ($null -ne $writeBw) { $writeBw } else { 0.0 }
            $rb + $wb
        } else {
            $null
        }
    }
    $readLat = if ($null -ne $diskspdSummary.read_lat_mean_us) { $diskspdSummary.read_lat_mean_us } else { $counterSummary.read_lat_mean_us_counter }
    $writeLat = if ($null -ne $diskspdSummary.write_lat_mean_us) { $diskspdSummary.write_lat_mean_us } else { $counterSummary.write_lat_mean_us_counter }

    [ordered]@{
        label = $Label
        workload = $Workload.name
        iteration = $Iteration
        rw = $Workload.rw
        bs = $Workload.bs
        iodepth = [int]$Workload.o
        numjobs = [int]$Workload.t
        rwmixread = if ($null -ne $Workload.rwmixread) { [int]$Workload.rwmixread } else { "" }

        read_iops = $readIops
        write_iops = $writeIops
        total_iops = $totalIops

        read_bw_mib_s = $readBw
        write_bw_mib_s = $writeBw
        total_bw_mib_s = $totalBw

        read_lat_mean_us = $readLat
        write_lat_mean_us = $writeLat
        read_p99_us = $diskspdSummary.read_p99_us
        write_p99_us = $diskspdSummary.write_p99_us

        selected_device = $counterSummary.selected_device
        io_await_mean_ms = $counterSummary.io_await_mean_ms
        io_await_p95_ms = $counterSummary.io_await_p95_ms
        io_await_max_ms = $counterSummary.io_await_max_ms
        io_util_mean_pct = $counterSummary.io_util_mean_pct
        io_util_p95_pct = $counterSummary.io_util_p95_pct
        io_util_max_pct = $counterSummary.io_util_max_pct
        io_queue_mean = $counterSummary.io_queue_mean
        io_queue_p95 = $counterSummary.io_queue_p95

        cpu_iowait_mean_pct = $counterSummary.cpu_iowait_mean_pct
        cpu_iowait_p95_pct = $counterSummary.cpu_iowait_p95_pct
        cpu_iowait_max_pct = $counterSummary.cpu_iowait_max_pct
        cpu_busy_mean_pct = $counterSummary.cpu_busy_mean_pct
        cpu_busy_p95_pct = $counterSummary.cpu_busy_p95_pct

        fio_exit_code = $exitCode
        fio_error = if ($exitCode -eq 0) { "" } else { "diskspd exited with code $exitCode" }
        elapsed_s = [Math]::Round($elapsed.TotalSeconds, 3)
    }
}

function Write-SummaryCsv {
    param(
        [object[]]$Rows,
        [string]$Path
    )
    $Rows | Export-Csv -LiteralPath $Path -NoTypeInformation -Encoding UTF8
}

function Escape-Html {
    param([object]$Value)
    if ($null -eq $Value) { return "" }
    return [System.Net.WebUtility]::HtmlEncode($Value.ToString())
}

function Format-NullableNumber {
    param(
        [object]$Value,
        [string]$Format = "N2"
    )
    $n = ConvertTo-Number $Value
    if ($null -eq $n -or [double]::IsNaN($n) -or [double]::IsInfinity($n)) { return "-" }
    return $n.ToString($Format)
}

function Resolve-RunCsvPath {
    param([string]$Dir)
    $results = Join-Path $Dir "results.csv"
    if (Test-Path -LiteralPath $results) { return $results }
    $summary = Join-Path $Dir "summary.csv"
    if (Test-Path -LiteralPath $summary) { return $summary }
    return $null
}

function Resolve-PdfBrowser {
    param([string]$PreferredPath)
    if (-not [string]::IsNullOrWhiteSpace($PreferredPath)) {
        $cmd = Get-Command $PreferredPath -ErrorAction SilentlyContinue
        if ($null -ne $cmd) { return $cmd.Source }
        if (Test-Path -LiteralPath $PreferredPath) {
            return (Resolve-Path -LiteralPath $PreferredPath).Path
        }
    }

    $commandCandidates = @("msedge.exe", "chrome.exe", "msedge", "chrome", "chromium", "chromium-browser")
    foreach ($name in $commandCandidates) {
        $cmd = Get-Command $name -ErrorAction SilentlyContinue
        if ($null -ne $cmd) { return $cmd.Source }
    }

    $pathCandidates = @(
        (Join-Path $env:ProgramFiles "Microsoft\Edge\Application\msedge.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "Microsoft\Edge\Application\msedge.exe"),
        (Join-Path $env:ProgramFiles "Google\Chrome\Application\chrome.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "Google\Chrome\Application\chrome.exe")
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

    foreach ($path in $pathCandidates) {
        if (Test-Path -LiteralPath $path) { return $path }
    }
    return $null
}

function Convert-PathToFileUri {
    param([string]$Path)
    $fullPath = (Resolve-Path -LiteralPath $Path).Path
    $uri = New-Object System.Uri($fullPath)
    return $uri.AbsoluteUri
}

function Convert-HtmlToPdf {
    param(
        [string]$HtmlPath,
        [string]$PdfPath,
        [string]$BrowserPath = ""
    )
    if (-not (Test-Path -LiteralPath $HtmlPath)) {
        return $false
    }
    $browser = Resolve-PdfBrowser -PreferredPath $BrowserPath
    if ($null -eq $browser) {
        Write-Warning "PDF export skipped: no Edge/Chrome browser found."
        return $false
    }

    $htmlUri = Convert-PathToFileUri -Path $HtmlPath
    $pdfDir = Split-Path -Path $PdfPath -Parent
    if (-not [string]::IsNullOrWhiteSpace($pdfDir)) {
        New-Item -ItemType Directory -Path $pdfDir -Force | Out-Null
    }

    $args = @(
        "--headless",
        "--disable-gpu",
        "--no-first-run",
        "--no-default-browser-check",
        "--print-to-pdf=$PdfPath",
        $htmlUri
    )
    try {
        $proc = Start-Process -FilePath $browser -ArgumentList $args -NoNewWindow -PassThru -Wait
    } catch {
        Write-Warning "PDF export failed launching browser ($browser): $($_.Exception.Message)"
        return $false
    }
    if ($proc.ExitCode -ne 0) {
        Write-Warning "PDF export failed with browser exit code $($proc.ExitCode)."
        return $false
    }
    if (-not (Test-Path -LiteralPath $PdfPath)) {
        Write-Warning "PDF export completed without creating file: $PdfPath"
        return $false
    }
    return $true
}

function Write-RunDashboard {
    param(
        [object[]]$Rows,
        [hashtable]$Meta,
        [string]$Path
    )
    if ($null -eq $Rows -or $Rows.Count -eq 0) { return }

    $groupRows = @()
    $byWorkload = $Rows | Group-Object workload | Sort-Object Name
    foreach ($g in $byWorkload) {
        $groupRows += [pscustomobject]@{
            workload = $g.Name
            runs = $g.Count
            total_iops = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.total_iops }) | Where-Object { $null -ne $_ })
            total_bw_mib_s = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.total_bw_mib_s }) | Where-Object { $null -ne $_ })
            read_lat_mean_us = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.read_lat_mean_us }) | Where-Object { $null -ne $_ })
            write_lat_mean_us = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.write_lat_mean_us }) | Where-Object { $null -ne $_ })
            io_await_mean_ms = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.io_await_mean_ms }) | Where-Object { $null -ne $_ })
            io_util_mean_pct = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.io_util_mean_pct }) | Where-Object { $null -ne $_ })
            io_queue_mean = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.io_queue_mean }) | Where-Object { $null -ne $_ })
            cpu_busy_mean_pct = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.cpu_busy_mean_pct }) | Where-Object { $null -ne $_ })
        }
    }

    $overallIops = Get-Mean (($Rows | ForEach-Object { ConvertTo-Number $_.total_iops }) | Where-Object { $null -ne $_ })
    $overallBw = Get-Mean (($Rows | ForEach-Object { ConvertTo-Number $_.total_bw_mib_s }) | Where-Object { $null -ne $_ })
    $overallAwait = Get-Mean (($Rows | ForEach-Object { ConvertTo-Number $_.io_await_mean_ms }) | Where-Object { $null -ne $_ })
    $overallUtil = Get-Mean (($Rows | ForEach-Object { ConvertTo-Number $_.io_util_mean_pct }) | Where-Object { $null -ne $_ })
    $overallCpuBusy = Get-Mean (($Rows | ForEach-Object { ConvertTo-Number $_.cpu_busy_mean_pct }) | Where-Object { $null -ne $_ })

    $groupRowsHtml = New-Object System.Text.StringBuilder
    foreach ($row in $groupRows) {
        [void]$groupRowsHtml.AppendLine("<tr>")
        [void]$groupRowsHtml.AppendLine("<td>$(Escape-Html $row.workload)</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.runs 'N0'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.total_iops 'N1'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.total_bw_mib_s 'N1'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.read_lat_mean_us 'N1'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.write_lat_mean_us 'N1'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.io_await_mean_ms 'N2'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.io_util_mean_pct 'N2'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.io_queue_mean 'N2'))</td>")
        [void]$groupRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.cpu_busy_mean_pct 'N2'))</td>")
        [void]$groupRowsHtml.AppendLine("</tr>")
    }

    $rawRowsHtml = New-Object System.Text.StringBuilder
    $sortedRaw = $Rows | Sort-Object workload, iteration
    foreach ($row in $sortedRaw) {
        $errorText = if ([string]::IsNullOrWhiteSpace($row.fio_error)) { "" } else { $row.fio_error }
        [void]$rawRowsHtml.AppendLine("<tr>")
        [void]$rawRowsHtml.AppendLine("<td>$(Escape-Html $row.workload)</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.iteration 'N0'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.total_iops 'N1'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.total_bw_mib_s 'N1'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.read_lat_mean_us 'N1'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.write_lat_mean_us 'N1'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.io_await_mean_ms 'N2'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.io_util_mean_pct 'N2'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.io_queue_mean 'N2'))</td>")
        [void]$rawRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.cpu_busy_mean_pct 'N2'))</td>")
        [void]$rawRowsHtml.AppendLine("<td>$(Escape-Html $errorText)</td>")
        [void]$rawRowsHtml.AppendLine("</tr>")
    }

    $label = Escape-Html $Meta.label
    $createdAt = Escape-Html $Meta.created_at
    $hostName = Escape-Html $Meta.hostname
    $os = Escape-Html $Meta.os
    $kernel = Escape-Html $Meta.kernel
    $targetPath = Escape-Html $Meta.target_path

    $html = @"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Storage Perf Dashboard - $label</title>
  <style>
    :root {
      --bg: #f4f7f5;
      --panel: #ffffff;
      --ink: #1f2a24;
      --muted: #5d6a63;
      --line: #d7e0db;
      --accent: #1f9d84;
      --accent-2: #f0a530;
      --good: #13795b;
      --bad: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Avenir Next", "Helvetica Neue", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 5% 10%, #d6efe6 0, transparent 35%),
        radial-gradient(circle at 95% 5%, #ffe9c9 0, transparent 35%),
        var(--bg);
    }
    .container {
      max-width: 1280px;
      margin: 24px auto 40px;
      padding: 0 16px;
    }
    .hero {
      background: linear-gradient(135deg, #0e7d69 0%, #1f9d84 45%, #f0a530 100%);
      color: white;
      border-radius: 18px;
      padding: 20px 24px;
      box-shadow: 0 14px 34px rgba(13, 49, 41, 0.25);
    }
    .hero h1 { margin: 0; font-size: 28px; letter-spacing: 0.3px; }
    .hero .sub { margin-top: 6px; opacity: 0.95; font-size: 14px; }
    .meta {
      margin-top: 12px;
      font-size: 13px;
      line-height: 1.55;
    }
    .cards {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      margin: 16px 0 22px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 14px;
      box-shadow: 0 4px 12px rgba(24, 40, 32, 0.06);
    }
    .card .k { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
    .card .v { margin-top: 4px; font-size: 24px; font-weight: 700; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      margin: 0 0 14px;
      overflow-x: auto;
    }
    .panel h2 {
      margin: 0 0 10px;
      font-size: 16px;
      letter-spacing: 0.2px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 940px;
    }
    thead th {
      text-align: left;
      background: #eef4f1;
      border-bottom: 1px solid var(--line);
      padding: 9px 10px;
      font-size: 12px;
      text-transform: uppercase;
      color: #33423a;
      letter-spacing: 0.4px;
    }
    tbody td {
      border-bottom: 1px solid #edf2ef;
      padding: 8px 10px;
      font-size: 13px;
      white-space: nowrap;
    }
    tbody tr:nth-child(even) { background: #fafcfb; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .note { color: var(--muted); font-size: 12px; margin-top: 8px; }
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <h1>Storage Perf Dashboard</h1>
      <div class="sub">Run label: <strong>$label</strong></div>
      <div class="meta">
        <div><strong>Created:</strong> $createdAt</div>
        <div><strong>Host:</strong> $hostName</div>
        <div><strong>OS:</strong> $os ($kernel)</div>
        <div><strong>Target:</strong> $targetPath</div>
      </div>
    </section>

    <section class="cards">
      <div class="card"><div class="k">Total samples</div><div class="v">$(Escape-Html (Format-NullableNumber $Rows.Count "N0"))</div></div>
      <div class="card"><div class="k">Avg total IOPS</div><div class="v">$(Escape-Html (Format-NullableNumber $overallIops "N0"))</div></div>
      <div class="card"><div class="k">Avg throughput (MiB/s)</div><div class="v">$(Escape-Html (Format-NullableNumber $overallBw "N1"))</div></div>
      <div class="card"><div class="k">Avg await (ms)</div><div class="v">$(Escape-Html (Format-NullableNumber $overallAwait "N2"))</div></div>
      <div class="card"><div class="k">Avg util (%)</div><div class="v">$(Escape-Html (Format-NullableNumber $overallUtil "N2"))</div></div>
      <div class="card"><div class="k">Avg CPU busy (%)</div><div class="v">$(Escape-Html (Format-NullableNumber $overallCpuBusy "N2"))</div></div>
    </section>

    <section class="panel">
      <h2>Workload Means (across iterations)</h2>
      <table>
        <thead>
          <tr>
            <th>Workload</th>
            <th class="num">Runs</th>
            <th class="num">Total IOPS</th>
            <th class="num">Total MiB/s</th>
            <th class="num">Read Lat (us)</th>
            <th class="num">Write Lat (us)</th>
            <th class="num">Await (ms)</th>
            <th class="num">Util (%)</th>
            <th class="num">Queue</th>
            <th class="num">CPU Busy (%)</th>
          </tr>
        </thead>
        <tbody>
$($groupRowsHtml.ToString())
        </tbody>
      </table>
    </section>

    <section class="panel">
      <h2>Raw Iteration Results</h2>
      <table>
        <thead>
          <tr>
            <th>Workload</th>
            <th class="num">Iter</th>
            <th class="num">Total IOPS</th>
            <th class="num">Total MiB/s</th>
            <th class="num">Read Lat (us)</th>
            <th class="num">Write Lat (us)</th>
            <th class="num">Await (ms)</th>
            <th class="num">Util (%)</th>
            <th class="num">Queue</th>
            <th class="num">CPU Busy (%)</th>
            <th>Error</th>
          </tr>
        </thead>
        <tbody>
$($rawRowsHtml.ToString())
        </tbody>
      </table>
      <div class="note">`summary.csv` and `results.csv` contain the same raw rows for scripting/automation. Open this file directly in any browser.</div>
    </section>
  </div>
</body>
</html>
"@
    Set-Content -LiteralPath $Path -Value $html -Encoding UTF8
}

function Write-CompareDashboard {
    param(
        [object[]]$CompareRows,
        [string]$LabelA,
        [string]$LabelB,
        [string]$Path,
        [string]$SourceA,
        [string]$SourceB
    )
    if ($null -eq $CompareRows -or $CompareRows.Count -eq 0) { return }

    $metricRows = @()
    $metricGroups = $CompareRows | Group-Object metric | Sort-Object Name
    foreach ($g in $metricGroups) {
        $avgDelta = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.delta }) | Where-Object { $null -ne $_ })
        $avgDeltaPct = Get-Mean (($g.Group | ForEach-Object { ConvertTo-Number $_.delta_pct }) | Where-Object { $null -ne $_ })
        $metricRows += [pscustomobject]@{
            metric = $g.Name
            sample_count = $g.Count
            avg_delta = $avgDelta
            avg_delta_pct = $avgDeltaPct
        }
    }

    $best = $CompareRows |
        Where-Object { $null -ne (ConvertTo-Number $_.delta_pct) } |
        Sort-Object { [Math]::Abs((ConvertTo-Number $_.delta_pct)) } -Descending |
        Select-Object -First 1

    $detailRowsHtml = New-Object System.Text.StringBuilder
    foreach ($row in ($CompareRows | Sort-Object metric, workload)) {
        $delta = ConvertTo-Number $row.delta
        $deltaPct = ConvertTo-Number $row.delta_pct
        $class = ""
        if ($null -ne $delta) {
            if ($delta -gt 0) { $class = "pos" }
            elseif ($delta -lt 0) { $class = "neg" }
        }
        [void]$detailRowsHtml.AppendLine("<tr>")
        [void]$detailRowsHtml.AppendLine("<td>$(Escape-Html $row.workload)</td>")
        [void]$detailRowsHtml.AppendLine("<td>$(Escape-Html $row.metric)</td>")
        [void]$detailRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.value_a 'N2'))</td>")
        [void]$detailRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.value_b 'N2'))</td>")
        [void]$detailRowsHtml.AppendLine("<td class='num $class'>$(Escape-Html (Format-NullableNumber $row.delta 'N2'))</td>")
        [void]$detailRowsHtml.AppendLine("<td class='num $class'>$(Escape-Html (Format-NullableNumber $row.delta_pct 'N2'))</td>")
        [void]$detailRowsHtml.AppendLine("</tr>")
    }

    $metricRowsHtml = New-Object System.Text.StringBuilder
    foreach ($row in $metricRows) {
        $class = ""
        $avgDelta = ConvertTo-Number $row.avg_delta
        if ($null -ne $avgDelta) {
            if ($avgDelta -gt 0) { $class = "pos" }
            elseif ($avgDelta -lt 0) { $class = "neg" }
        }
        [void]$metricRowsHtml.AppendLine("<tr>")
        [void]$metricRowsHtml.AppendLine("<td>$(Escape-Html $row.metric)</td>")
        [void]$metricRowsHtml.AppendLine("<td class='num'>$(Escape-Html (Format-NullableNumber $row.sample_count 'N0'))</td>")
        [void]$metricRowsHtml.AppendLine("<td class='num $class'>$(Escape-Html (Format-NullableNumber $row.avg_delta 'N2'))</td>")
        [void]$metricRowsHtml.AppendLine("<td class='num $class'>$(Escape-Html (Format-NullableNumber $row.avg_delta_pct 'N2'))</td>")
        [void]$metricRowsHtml.AppendLine("</tr>")
    }

    $bestText = if ($null -eq $best) {
        "-"
    } else {
        "{0} / {1}: {2:N2}%" -f $best.workload, $best.metric, (ConvertTo-Number $best.delta_pct)
    }

    $html = @"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Storage Perf Compare - $(Escape-Html $LabelA) vs $(Escape-Html $LabelB)</title>
  <style>
    :root {
      --bg: #f6f8fb;
      --panel: #ffffff;
      --ink: #202c3c;
      --muted: #5f6c80;
      --line: #d9e2ef;
      --accent: #2f6fed;
      --accent-2: #0aa57b;
      --pos: #117a59;
      --neg: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Avenir Next", "Helvetica Neue", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 95% 10%, #e2f5ff 0, transparent 35%),
        radial-gradient(circle at 5% 0%, #e5ffe9 0, transparent 35%),
        var(--bg);
    }
    .container { max-width: 1320px; margin: 24px auto 40px; padding: 0 16px; }
    .hero {
      background: linear-gradient(135deg, #1f4db8 0%, #2f6fed 42%, #0aa57b 100%);
      color: #fff; border-radius: 18px; padding: 20px 24px;
      box-shadow: 0 14px 32px rgba(20, 49, 106, 0.25);
    }
    .hero h1 { margin: 0; font-size: 27px; }
    .hero .sub { margin-top: 6px; font-size: 14px; opacity: 0.96; }
    .cards {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin: 16px 0 22px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 14px;
      box-shadow: 0 4px 12px rgba(24, 40, 32, 0.05);
    }
    .card .k { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
    .card .v { margin-top: 4px; font-size: 22px; font-weight: 700; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      margin-bottom: 14px;
      overflow-x: auto;
    }
    .panel h2 { margin: 0 0 10px; font-size: 16px; }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 900px;
    }
    thead th {
      text-align: left;
      background: #eef3fb;
      border-bottom: 1px solid var(--line);
      padding: 9px 10px;
      font-size: 12px;
      color: #33445d;
      text-transform: uppercase;
      letter-spacing: 0.4px;
    }
    tbody td {
      border-bottom: 1px solid #edf2f9;
      padding: 8px 10px;
      font-size: 13px;
      white-space: nowrap;
    }
    tbody tr:nth-child(even) { background: #fafcff; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .pos { color: var(--pos); font-weight: 600; }
    .neg { color: var(--neg); font-weight: 600; }
    .note { color: var(--muted); font-size: 12px; margin-top: 8px; }
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <h1>Storage Compare Dashboard</h1>
      <div class="sub"><strong>$(Escape-Html $LabelA)</strong> vs <strong>$(Escape-Html $LabelB)</strong></div>
      <div class="sub">Source A: $(Escape-Html $SourceA)</div>
      <div class="sub">Source B: $(Escape-Html $SourceB)</div>
    </section>

    <section class="cards">
      <div class="card"><div class="k">Comparison Rows</div><div class="v">$(Escape-Html (Format-NullableNumber $CompareRows.Count 'N0'))</div></div>
      <div class="card"><div class="k">Metrics Compared</div><div class="v">$(Escape-Html (Format-NullableNumber $metricRows.Count 'N0'))</div></div>
      <div class="card"><div class="k">Largest Absolute Shift</div><div class="v">$(Escape-Html $bestText)</div></div>
    </section>

    <section class="panel">
      <h2>Average Delta by Metric (B - A)</h2>
      <table>
        <thead>
          <tr>
            <th>Metric</th>
            <th class="num">Samples</th>
            <th class="num">Avg Delta</th>
            <th class="num">Avg Delta %</th>
          </tr>
        </thead>
        <tbody>
$($metricRowsHtml.ToString())
        </tbody>
      </table>
    </section>

    <section class="panel">
      <h2>Detailed Workload Delta</h2>
      <table>
        <thead>
          <tr>
            <th>Workload</th>
            <th>Metric</th>
            <th class="num">$(Escape-Html $LabelA)</th>
            <th class="num">$(Escape-Html $LabelB)</th>
            <th class="num">Delta</th>
            <th class="num">Delta %</th>
          </tr>
        </thead>
        <tbody>
$($detailRowsHtml.ToString())
        </tbody>
      </table>
      <div class="note">Positive/negative coloring indicates sign only. For latency metrics, negative deltas are typically better.</div>
    </section>
  </div>
</body>
</html>
"@
    Set-Content -LiteralPath $Path -Value $html -Encoding UTF8
}

function Run-Benchmark {
    $diskspdExe = Resolve-DiskSpd -Binary $DiskSpdPath -AutoInstall $AutoInstallDiskSpd -InstallDir $DiskSpdInstallDir
    $workloads = Load-Workloads -Path $WorkloadsJson

    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $resultDir = Join-Path $OutputDir "$Label-$timestamp"
    $rawDir = Join-Path $resultDir "raw"
    New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

    $meta = [ordered]@{
        label = $Label
        created_at = (Get-Date).ToString("o")
        hostname = $env:COMPUTERNAME
        os = (Get-CimInstance Win32_OperatingSystem).Caption
        kernel = (Get-CimInstance Win32_OperatingSystem).Version
        target_path = $TargetPath
        diskspd_path = $diskspdExe
        args = @{
            size = $Size
            duration_seconds = $DurationSeconds
            warmup_seconds = $WarmupSeconds
            repeat = $Repeat
            disk_instance = $DiskInstance
            extra_diskspd_arg = $ExtraDiskSpdArg
            auto_install_diskspd = $AutoInstallDiskSpd
            diskspd_install_dir = $DiskSpdInstallDir
            generate_pdf = $GeneratePdf
            pdf_browser_path = $PdfBrowserPath
        }
        workloads = $workloads
    }
    $meta | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $resultDir "metadata.json") -Encoding UTF8
    $workloads | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $resultDir "workloads.json") -Encoding UTF8

    $rows = @()
    $totalRuns = $workloads.Count * $Repeat
    $idx = 0
    foreach ($wl in $workloads) {
        for ($iter = 1; $iter -le $Repeat; $iter++) {
            $idx++
            Write-Host "[$idx/$totalRuns] $($wl.name) (iter $iter/$Repeat) ..."
            $row = Run-Workload -DiskSpdExe $diskspdExe -Workload $wl -Iteration $iter -TargetPath $TargetPath -Size $Size -DurationSeconds $DurationSeconds -WarmupSeconds $WarmupSeconds -RequestedDisk $DiskInstance -ExtraDiskSpdArg $ExtraDiskSpdArg -RawDir $rawDir
            $rows += [pscustomobject]$row
            if (-not [string]::IsNullOrWhiteSpace($row.fio_error)) {
                Write-Warning $row.fio_error
            }
        }
    }

    $summaryPath = Join-Path $resultDir "summary.csv"
    $resultsPath = Join-Path $resultDir "results.csv"
    Write-SummaryCsv -Rows $rows -Path $summaryPath
    Write-SummaryCsv -Rows $rows -Path $resultsPath
    $dashboardPath = Join-Path $resultDir "dashboard.html"
    Write-RunDashboard -Rows $rows -Meta $meta -Path $dashboardPath
    $pdfPath = Join-Path $resultDir "dashboard.pdf"
    if ($GeneratePdf) {
        [void](Convert-HtmlToPdf -HtmlPath $dashboardPath -PdfPath $pdfPath -BrowserPath $PdfBrowserPath)
    }

    Write-Host ""
    Write-Host "Run summary (mean across iterations):"
    "{0,-28} {1,10} {2,10} {3,14} {4,14} {5,12} {6,10}" -f "workload", "IOPS", "MiB/s", "read_lat(us)", "write_lat(us)", "await(ms)", "util(%)" | Write-Host
    $byWorkload = $rows | Group-Object workload | Sort-Object Name
    foreach ($group in $byWorkload) {
        $iops = Get-Mean (($group.Group | ForEach-Object { ConvertTo-Number $_.total_iops }) | Where-Object { $null -ne $_ })
        $bw = Get-Mean (($group.Group | ForEach-Object { ConvertTo-Number $_.total_bw_mib_s }) | Where-Object { $null -ne $_ })
        $readLat = Get-Mean (($group.Group | ForEach-Object { ConvertTo-Number $_.read_lat_mean_us }) | Where-Object { $null -ne $_ })
        $writeLat = Get-Mean (($group.Group | ForEach-Object { ConvertTo-Number $_.write_lat_mean_us }) | Where-Object { $null -ne $_ })
        $await = Get-Mean (($group.Group | ForEach-Object { ConvertTo-Number $_.io_await_mean_ms }) | Where-Object { $null -ne $_ })
        $util = Get-Mean (($group.Group | ForEach-Object { ConvertTo-Number $_.io_util_mean_pct }) | Where-Object { $null -ne $_ })
        "{0,-28} {1,10:N1} {2,10:N1} {3,14:N1} {4,14:N1} {5,12:N2} {6,10:N2}" -f $group.Name, $iops, $bw, $readLat, $writeLat, $await, $util | Write-Host
    }
    Write-Host ""
    Write-Host "Results written to: $resultDir"
    Write-Host "Dashboard: $dashboardPath"
    if ($GeneratePdf -and (Test-Path -LiteralPath $pdfPath)) {
        Write-Host "PDF: $pdfPath"
    }
    Write-Host "CSV: $resultsPath"
}

function Get-ResultLabel {
    param([string]$Dir, [string]$Fallback)
    $meta = Join-Path $Dir "metadata.json"
    if (-not (Test-Path -LiteralPath $meta)) { return $Fallback }
    try {
        $payload = Get-Content -LiteralPath $meta -Raw | ConvertFrom-Json
        if ($null -ne $payload.label -and $payload.label.ToString().Trim() -ne "") {
            return $payload.label.ToString().Trim()
        }
    } catch {}
    return $Fallback
}

function Group-MeanByWorkload {
    param(
        [object[]]$Rows,
        [string]$Metric
    )
    $map = @{}
    $groups = $Rows | Group-Object workload
    foreach ($g in $groups) {
        $vals = @()
        foreach ($r in $g.Group) {
            $n = ConvertTo-Number $r.$Metric
            if ($null -ne $n) { $vals += $n }
        }
        if ($vals.Count -gt 0) {
            $map[$g.Name] = (Get-Mean $vals)
        }
    }
    return $map
}

function Run-Compare {
    $dirA = (Resolve-Path -LiteralPath $A).Path
    $dirB = (Resolve-Path -LiteralPath $B).Path
    $sumA = Resolve-RunCsvPath -Dir $dirA
    $sumB = Resolve-RunCsvPath -Dir $dirB
    if ($null -eq $sumA) { throw "Missing results.csv/summary.csv in: $dirA" }
    if ($null -eq $sumB) { throw "Missing results.csv/summary.csv in: $dirB" }

    $rowsA = Import-Csv -LiteralPath $sumA
    $rowsB = Import-Csv -LiteralPath $sumB
    $labelA = Get-ResultLabel -Dir $dirA -Fallback "A"
    $labelB = Get-ResultLabel -Dir $dirB -Fallback "B"

    $metrics = @(
        "total_iops",
        "total_bw_mib_s",
        "read_lat_mean_us",
        "write_lat_mean_us",
        "io_await_mean_ms",
        "io_util_mean_pct",
        "io_queue_mean",
        "cpu_iowait_mean_pct",
        "cpu_busy_mean_pct"
    )

    $workloads = @{}
    foreach ($r in $rowsA) { if ($r.workload) { $workloads[$r.workload] = $true } }
    foreach ($r in $rowsB) { if ($r.workload) { $workloads[$r.workload] = $true } }
    $wlSorted = $workloads.Keys | Sort-Object

    "{0,-28} {1,-20} {2,12} {3,12} {4,12} {5,10}" -f "workload", "metric", $labelA, $labelB, "delta", "delta_%" | Write-Host
    "-" * 100 | Write-Host

    $outRows = @()
    foreach ($metric in $metrics) {
        $meansA = Group-MeanByWorkload -Rows $rowsA -Metric $metric
        $meansB = Group-MeanByWorkload -Rows $rowsB -Metric $metric
        foreach ($wl in $wlSorted) {
            if (-not $meansA.ContainsKey($wl)) { continue }
            if (-not $meansB.ContainsKey($wl)) { continue }
            $aVal = [double]$meansA[$wl]
            $bVal = [double]$meansB[$wl]
            $delta = $bVal - $aVal
            $deltaPct = if ($aVal -ne 0.0) { ($delta / $aVal) * 100.0 } else { [double]::NaN }
            "{0,-28} {1,-20} {2,12:N2} {3,12:N2} {4,12:N2} {5,10:N2}" -f $wl, $metric, $aVal, $bVal, $delta, $deltaPct | Write-Host
            $outRows += [pscustomobject]@{
                workload = $wl
                metric = $metric
                label_a = $labelA
                value_a = $aVal
                label_b = $labelB
                value_b = $bVal
                delta = $delta
                delta_pct = $deltaPct
            }
        }
    }

    $outPath = if ([string]::IsNullOrWhiteSpace($CompareOutput)) {
        Join-Path (Get-Location).Path ("compare-{0}-vs-{1}.csv" -f (Split-Path $dirA -Leaf), (Split-Path $dirB -Leaf))
    } else {
        $CompareOutput
    }
    $outRows | Export-Csv -LiteralPath $outPath -NoTypeInformation -Encoding UTF8
    $compareDash = [System.IO.Path]::ChangeExtension($outPath, ".html")
    Write-CompareDashboard -CompareRows $outRows -LabelA $labelA -LabelB $labelB -Path $compareDash -SourceA $sumA -SourceB $sumB
    $comparePdf = [System.IO.Path]::ChangeExtension($outPath, ".pdf")
    if ($GeneratePdf) {
        [void](Convert-HtmlToPdf -HtmlPath $compareDash -PdfPath $comparePdf -BrowserPath $PdfBrowserPath)
    }
    Write-Host ""
    Write-Host "Comparison CSV written to: $outPath"
    Write-Host "Comparison dashboard: $compareDash"
    if ($GeneratePdf -and (Test-Path -LiteralPath $comparePdf)) {
        Write-Host "Comparison PDF: $comparePdf"
    }
}

if ($PSCmdlet.ParameterSetName -eq "Compare") {
    Run-Compare
} else {
    Run-Benchmark
}
