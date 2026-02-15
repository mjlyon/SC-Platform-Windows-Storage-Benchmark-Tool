# SC Windows Storage Benchmarking

This repo now contains two benchmark harnesses:

- `storage_perf_windows.ps1`: Windows guest benchmark runner using `diskspd` + Windows perf counters.
- `storage_perf.py`: Linux guest benchmark runner using `fio` + `iostat` + `/proc/stat`.

If your target is Windows guest behavior, use the PowerShell harness first.

## What It Measures

Per workload + iteration:

- Throughput: `read_bw_mib_s`, `write_bw_mib_s`, `total_bw_mib_s`
- IOPS: `read_iops`, `write_iops`, `total_iops`
- Latency: `read_lat_mean_us`, `write_lat_mean_us`, plus `io_await_*` from disk counters
- Queue depth pressure: `io_queue_mean`, `io_queue_p95`
- Utilization proxy: `io_util_mean_pct`, `io_util_p95_pct`, `io_util_max_pct`
- CPU iowait:
  - Linux: populated from `/proc/stat`
  - Windows: not available natively, left blank (`cpu_iowait_*`), with `cpu_busy_*` included

Outputs per run:

- `metadata.json`
- `workloads.json`
- `summary.csv`
- `results.csv` (same raw rows as `summary.csv`, for easier downstream naming)
- `dashboard.html` (single-run web dashboard)
- `dashboard.pdf` (single-run PDF report, best-effort via headless Edge/Chrome)
- `raw/` logs and counter snapshots

## Windows Guest (DiskSpd)

## Prereqs

- Windows guest with admin shell
- `diskspd.exe` on `PATH`, or let the harness auto-install it (default behavior)
- PowerShell 5.1+ or 7+

If `diskspd` is missing, the harness now tries:
1. Existing known folders (`.\tools\diskspd`, local app data install dir, common Program Files paths)
2. `winget` package install
3. Official ZIP download/install (`https://aka.ms/diskspd`) into local install dir

## Run baseline (example: `virtio-scsi`)

```powershell
Set-Location C:\bench\storage-perf
.\storage_perf_windows.ps1 `
  -Label virtio-scsi `
  -TargetPath C:\bench\testfile.dat `
  -Size 20G `
  -DurationSeconds 60 `
  -WarmupSeconds 10 `
  -Repeat 3 `
  -OutputDir .\results
```

Then switch VM disk bus to `virtio-blk` and run:

```powershell
.\storage_perf_windows.ps1 `
  -Label virtio-blk `
  -TargetPath C:\bench\testfile.dat `
  -Size 20G `
  -DurationSeconds 60 `
  -WarmupSeconds 10 `
  -Repeat 3 `
  -OutputDir .\results
```

## Compare two runs

```powershell
.\storage_perf_windows.ps1 -Compare `
  -A .\results\virtio-scsi-20260211-120000 `
  -B .\results\virtio-blk-20260211-123000 `
  -CompareOutput .\compare-scsi-vs-blk.csv
```

Compare mode also writes a matching HTML dashboard next to the CSV (same name, `.html` extension).
If browser-based PDF export is available, compare mode also writes a matching `.pdf`.

## Optional knobs

- `-DiskInstance "0 C:"` to pin which `PhysicalDisk(*)` instance gets summarized
- `-WorkloadsJson .\my_workloads.json` to override workload matrix
- `-ExtraDiskSpdArg "-h"` (or repeated) to pass extra `diskspd` flags
- `-AutoInstallDiskSpd $false` to disable auto-install attempts
- `-DiskSpdInstallDir "$env:LOCALAPPDATA\diskspd"` to control local auto-install location
- `-GeneratePdf $false` to skip PDF generation
- `-PdfBrowserPath "C:\Program Files\Microsoft\Edge\Application\msedge.exe"` to force a specific browser binary for PDF export

## Workloads JSON format (Windows)

```json
[
  { "name": "seq_read_1m_qd32", "rw": "read", "bs": "1M", "o": 32, "t": 1 },
  { "name": "rand_rw_4k_70r_qd32", "rw": "randrw", "bs": "4K", "o": 32, "t": 1, "rwmixread": 70 }
]
```

Supported `rw`: `read`, `write`, `randread`, `randwrite`, `randrw`.
Use `t` (threads) and `o` (outstanding I/O per thread) to explore low-vs-high concurrency regimes where backend differences usually appear.

## Linux Guest (`fio`) Optional

If you also want Linux-side confirmation:

```bash
python3 storage_perf.py run --label virtio-scsi --filename /mnt/bench/testfile --size 20G
python3 storage_perf.py run --label virtio-blk --filename /mnt/bench/testfile --size 20G
python3 storage_perf.py compare --a results/virtio-scsi-... --b results/virtio-blk-...
```

## Experiment Hygiene (Important for Fair A/B)

- Keep VM CPU/memory topology identical between `virtio-scsi` and `virtio-blk`.
- Keep backing storage identical (same host disk, cache mode, qcow/raw format).
- Reboot between variants, or at least clear caches and let background IO settle.
- Use same workload file size and test path.
- Keep test duration long enough for tail latency to stabilize (60s+ is a good start).
- Run at least 3 iterations and compare means + variability.
