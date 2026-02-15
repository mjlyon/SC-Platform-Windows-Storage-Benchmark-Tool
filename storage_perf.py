#!/usr/bin/env python3
"""Storage performance benchmark harness for virtio backend comparisons.

Runs a workload matrix via fio and captures:
- fio throughput/IOPS/latency metrics
- iostat extended disk metrics (await, queue depth, util, etc.)
- CPU iowait from /proc/stat

Typical usage:
  python3 storage_perf.py run --label virtio-scsi --filename /mnt/bench/testfile --size 20G
  python3 storage_perf.py run --label virtio-blk  --filename /mnt/bench/testfile --size 20G
  python3 storage_perf.py compare --a results/virtio-scsi-... --b results/virtio-blk-...
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import pathlib
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_WORKLOADS = [
    {"name": "seq_read_1m_qd32", "rw": "read", "bs": "1m", "iodepth": 32, "numjobs": 1},
    {"name": "seq_write_1m_qd32", "rw": "write", "bs": "1m", "iodepth": 32, "numjobs": 1},
    {"name": "rand_read_4k_qd1", "rw": "randread", "bs": "4k", "iodepth": 1, "numjobs": 1},
    {"name": "rand_read_4k_qd32", "rw": "randread", "bs": "4k", "iodepth": 32, "numjobs": 1},
    {"name": "rand_write_4k_qd1", "rw": "randwrite", "bs": "4k", "iodepth": 1, "numjobs": 1},
    {"name": "rand_write_4k_qd32", "rw": "randwrite", "bs": "4k", "iodepth": 32, "numjobs": 1},
    {
        "name": "rand_rw_4k_70r_qd32",
        "rw": "randrw",
        "rwmixread": 70,
        "bs": "4k",
        "iodepth": 32,
        "numjobs": 1,
    },
    {
        "name": "rand_rw_16k_70r_qd32",
        "rw": "randrw",
        "rwmixread": 70,
        "bs": "16k",
        "iodepth": 32,
        "numjobs": 1,
    },
    {
        "name": "rand_read_4k_qd32_nj4",
        "rw": "randread",
        "bs": "4k",
        "iodepth": 32,
        "numjobs": 4,
    },
    {
        "name": "rand_write_4k_qd32_nj4",
        "rw": "randwrite",
        "bs": "4k",
        "iodepth": 32,
        "numjobs": 4,
    },
    {
        "name": "rand_rw_4k_70r_qd64_nj4",
        "rw": "randrw",
        "rwmixread": 70,
        "bs": "4k",
        "iodepth": 64,
        "numjobs": 4,
    },
]


SUMMARY_COLUMNS = [
    "label",
    "workload",
    "iteration",
    "rw",
    "bs",
    "iodepth",
    "numjobs",
    "rwmixread",
    "read_iops",
    "write_iops",
    "total_iops",
    "read_bw_mib_s",
    "write_bw_mib_s",
    "total_bw_mib_s",
    "read_lat_mean_us",
    "write_lat_mean_us",
    "read_p99_us",
    "write_p99_us",
    "selected_device",
    "io_await_mean_ms",
    "io_await_p95_ms",
    "io_await_max_ms",
    "io_util_mean_pct",
    "io_util_p95_pct",
    "io_util_max_pct",
    "io_queue_mean",
    "io_queue_p95",
    "cpu_iowait_mean_pct",
    "cpu_iowait_p95_pct",
    "cpu_iowait_max_pct",
    "fio_exit_code",
    "fio_error",
]


COMPARE_METRICS = [
    "total_iops",
    "total_bw_mib_s",
    "read_lat_mean_us",
    "write_lat_mean_us",
    "io_await_mean_ms",
    "io_util_mean_pct",
    "io_queue_mean",
    "cpu_iowait_mean_pct",
    "cpu_busy_mean_pct",
]


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    if pct <= 0:
        return min(values)
    if pct >= 100:
        return max(values)
    seq = sorted(values)
    if len(seq) == 1:
        return seq[0]
    rank = (len(seq) - 1) * (pct / 100.0)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return seq[lo]
    return seq[lo] + (seq[hi] - seq[lo]) * (rank - lo)


def mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return statistics.fmean(values)


def ns_to_us(ns: Optional[float]) -> Optional[float]:
    if ns is None:
        return None
    return ns / 1000.0


def basename_device(device: str) -> str:
    device = device.strip()
    if device.startswith("/dev/"):
        return pathlib.Path(device).name
    return device


def is_candidate_blockdev(name: str) -> bool:
    # Drop pseudo devices that often add noise in iostat output.
    prefixes = ("loop", "ram", "sr", "md")
    return not name.startswith(prefixes)


class CpuIowaitSampler:
    """Samples CPU iowait percentage from /proc/stat at fixed interval."""

    def __init__(self, interval_s: float = 1.0) -> None:
        self.interval_s = interval_s
        self.samples: List[float] = []
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._enabled = pathlib.Path("/proc/stat").exists()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def start(self) -> None:
        if not self._enabled:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._enabled:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _read_cpu_times(self) -> Optional[List[int]]:
        try:
            with open("/proc/stat", "r", encoding="utf-8") as fh:
                first = fh.readline().strip()
        except OSError:
            return None
        if not first.startswith("cpu "):
            return None
        parts = first.split()[1:]
        try:
            return [int(p) for p in parts]
        except ValueError:
            return None

    def _run(self) -> None:
        prev = self._read_cpu_times()
        if prev is None:
            return
        while not self._stop.wait(self.interval_s):
            current = self._read_cpu_times()
            if current is None or len(current) < 5:
                continue
            total_delta = sum(current) - sum(prev)
            iowait_delta = current[4] - prev[4]
            prev = current
            if total_delta <= 0:
                continue
            self.samples.append((iowait_delta / total_delta) * 100.0)


def get_fio_percentile_us(section: Dict[str, Any], target: float) -> Optional[float]:
    clat_ns = section.get("clat_ns", {})
    pcts = clat_ns.get("percentile", {}) if isinstance(clat_ns, dict) else {}
    if not isinstance(pcts, dict) or not pcts:
        return None
    best_key = None
    best_dist = float("inf")
    for key in pcts.keys():
        try:
            numeric = float(key)
        except (TypeError, ValueError):
            continue
        dist = abs(numeric - target)
        if dist < best_dist:
            best_dist = dist
            best_key = key
    if best_key is None:
        return None
    return ns_to_us(to_float(pcts.get(best_key)))


def get_fio_latency_mean_us(section: Dict[str, Any]) -> Optional[float]:
    clat_ns = section.get("clat_ns", {})
    if isinstance(clat_ns, dict) and "mean" in clat_ns:
        return ns_to_us(to_float(clat_ns.get("mean")))
    clat_us = section.get("clat_us", {})
    if isinstance(clat_us, dict) and "mean" in clat_us:
        return to_float(clat_us.get("mean"))
    return None


def parse_fio_result(path: pathlib.Path) -> Dict[str, Optional[float]]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list) or not jobs:
        raise ValueError(f"Invalid fio JSON: {path}")

    read_iops = 0.0
    write_iops = 0.0
    read_bw_kib = 0.0
    write_bw_kib = 0.0

    # Latency is typically meaningful from the group-reported combined job. If not,
    # we fallback to the first job and then to None.
    first = jobs[0]
    first_read = first.get("read", {}) if isinstance(first, dict) else {}
    first_write = first.get("write", {}) if isinstance(first, dict) else {}
    read_lat_mean_us = (
        get_fio_latency_mean_us(first_read) if isinstance(first_read, dict) else None
    )
    write_lat_mean_us = (
        get_fio_latency_mean_us(first_write) if isinstance(first_write, dict) else None
    )
    read_p99_us = (
        get_fio_percentile_us(first_read, 99.0) if isinstance(first_read, dict) else None
    )
    write_p99_us = (
        get_fio_percentile_us(first_write, 99.0)
        if isinstance(first_write, dict)
        else None
    )

    for job in jobs:
        if not isinstance(job, dict):
            continue
        read = job.get("read", {})
        write = job.get("write", {})
        if isinstance(read, dict):
            read_iops += to_float(read.get("iops")) or 0.0
            read_bw_kib += to_float(read.get("bw")) or 0.0
        if isinstance(write, dict):
            write_iops += to_float(write.get("iops")) or 0.0
            write_bw_kib += to_float(write.get("bw")) or 0.0

    read_bw_mib = read_bw_kib / 1024.0
    write_bw_mib = write_bw_kib / 1024.0
    return {
        "read_iops": read_iops,
        "write_iops": write_iops,
        "total_iops": read_iops + write_iops,
        "read_bw_mib_s": read_bw_mib,
        "write_bw_mib_s": write_bw_mib,
        "total_bw_mib_s": read_bw_mib + write_bw_mib,
        "read_lat_mean_us": read_lat_mean_us,
        "write_lat_mean_us": write_lat_mean_us,
        "read_p99_us": read_p99_us,
        "write_p99_us": write_p99_us,
    }


def iostat_supports_y(iostat_bin: str) -> bool:
    try:
        proc = subprocess.run(
            [iostat_bin, "-y", "1", "1"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
        return proc.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def launch_iostat(iostat_bin: str, output_path: pathlib.Path) -> subprocess.Popen:
    supports_y = iostat_supports_y(iostat_bin)
    cmd = [iostat_bin, "-x", "-d"]
    if supports_y:
        cmd.append("-y")
    cmd.append("1")

    out_fh = output_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        stdout=out_fh,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,  # allow clean termination of the process group
    )
    return proc


def stop_iostat(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except OSError:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        proc.wait(timeout=2)


def parse_iostat_samples(path: pathlib.Path) -> Dict[str, List[Dict[str, float]]]:
    """Parses iostat -x output into per-device samples keyed by metric column."""
    by_device: Dict[str, List[Dict[str, float]]] = {}
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or not line.startswith("Device"):
            i += 1
            continue

        headers = lines[i].split()
        i += 1
        while i < len(lines):
            row = lines[i].strip()
            if not row:
                i += 1
                break
            if row.startswith("Device") or row.startswith("avg-cpu"):
                break
            fields = lines[i].split()
            if len(fields) < 2:
                i += 1
                continue
            device = fields[0]
            sample: Dict[str, float] = {}
            for idx, col in enumerate(headers[1:], start=1):
                if idx >= len(fields):
                    break
                val = to_float(fields[idx])
                if val is not None:
                    sample[col] = val
            if sample:
                by_device.setdefault(device, []).append(sample)
            i += 1

    return by_device


def pick_device(
    by_device: Dict[str, List[Dict[str, float]]], requested: Optional[str]
) -> Optional[str]:
    if not by_device:
        return None

    if requested:
        key = basename_device(requested)
        if key in by_device:
            return key

    best_name = None
    best_util = float("-inf")
    for dev, samples in by_device.items():
        if not is_candidate_blockdev(dev):
            continue
        util_vals = [s.get("%util") for s in samples if "%util" in s]
        util = mean([v for v in util_vals if v is not None]) or 0.0
        if util > best_util:
            best_util = util
            best_name = dev

    if best_name:
        return best_name
    return next(iter(by_device.keys()))


def collect_metric(
    samples: List[Dict[str, float]], aliases: Tuple[str, ...]
) -> List[float]:
    values: List[float] = []
    for sample in samples:
        for name in aliases:
            if name in sample:
                values.append(sample[name])
                break
    return values


def summarize_iostat(
    by_device: Dict[str, List[Dict[str, float]]], device: Optional[str]
) -> Dict[str, Optional[float]]:
    if not by_device or not device or device not in by_device:
        return {
            "selected_device": device,
            "io_await_mean_ms": None,
            "io_await_p95_ms": None,
            "io_await_max_ms": None,
            "io_util_mean_pct": None,
            "io_util_p95_pct": None,
            "io_util_max_pct": None,
            "io_queue_mean": None,
            "io_queue_p95": None,
        }

    samples = by_device[device]
    await_vals = collect_metric(samples, ("await",))
    util_vals = collect_metric(samples, ("%util", "util"))
    queue_vals = collect_metric(samples, ("aqu-sz", "avgqu-sz"))

    return {
        "selected_device": device,
        "io_await_mean_ms": mean(await_vals),
        "io_await_p95_ms": percentile(await_vals, 95),
        "io_await_max_ms": max(await_vals) if await_vals else None,
        "io_util_mean_pct": mean(util_vals),
        "io_util_p95_pct": percentile(util_vals, 95),
        "io_util_max_pct": max(util_vals) if util_vals else None,
        "io_queue_mean": mean(queue_vals),
        "io_queue_p95": percentile(queue_vals, 95),
    }


def summarize_cpu_iowait(samples: List[float]) -> Dict[str, Optional[float]]:
    return {
        "cpu_iowait_mean_pct": mean(samples),
        "cpu_iowait_p95_pct": percentile(samples, 95),
        "cpu_iowait_max_pct": max(samples) if samples else None,
    }


def load_workloads(path: Optional[pathlib.Path]) -> List[Dict[str, Any]]:
    if path is None:
        return list(DEFAULT_WORKLOADS)
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, list):
        raise ValueError("Workload file must be a JSON array")
    parsed: List[Dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            raise ValueError("Each workload entry must be an object")
        required = ("name", "rw", "bs", "iodepth", "numjobs")
        missing = [k for k in required if k not in row]
        if missing:
            raise ValueError(f"Workload {row!r} missing required fields: {missing}")
        parsed.append(row)
    return parsed


def run_single_workload(
    args: argparse.Namespace,
    workload: Dict[str, Any],
    iteration: int,
    raw_dir: pathlib.Path,
) -> Dict[str, Any]:
    slug = workload["name"]
    run_prefix = f"{slug}.run{iteration:02d}"
    fio_json = raw_dir / f"{run_prefix}.fio.json"
    fio_log = raw_dir / f"{run_prefix}.fio.log"
    iostat_log = raw_dir / f"{run_prefix}.iostat.log"

    fio_cmd = [
        args.fio_bin,
        f"--name={slug}",
        f"--filename={args.filename}",
        f"--size={args.size}",
        f"--rw={workload['rw']}",
        f"--bs={workload['bs']}",
        f"--iodepth={int(workload['iodepth'])}",
        f"--numjobs={int(workload['numjobs'])}",
        f"--runtime={args.runtime}",
        f"--ramp_time={args.ramp_time}",
        "--time_based=1",
        "--group_reporting=1",
        "--randrepeat=0",
        "--norandommap=1",
        f"--direct={int(args.direct)}",
        f"--ioengine={args.ioengine}",
        "--thread=1",
        "--eta=never",
        "--output-format=json",
        f"--output={fio_json}",
    ]
    if workload.get("rwmixread") is not None:
        fio_cmd.append(f"--rwmixread={int(workload['rwmixread'])}")
    for item in args.extra_fio_arg:
        fio_cmd.append(item)

    cpu_sampler = CpuIowaitSampler(interval_s=1.0)
    iostat_proc = launch_iostat(args.iostat_bin, iostat_log)
    cpu_sampler.start()

    start = time.time()
    with fio_log.open("w", encoding="utf-8") as log_fh:
        proc = subprocess.run(
            fio_cmd,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            check=False,
        )
    elapsed = time.time() - start

    cpu_sampler.stop()
    stop_iostat(iostat_proc)

    fio_error = ""
    fio_metrics: Dict[str, Optional[float]] = {}
    if proc.returncode == 0:
        try:
            fio_metrics = parse_fio_result(fio_json)
        except Exception as exc:  # pragma: no cover - defensive path
            fio_error = f"Failed to parse fio output: {exc}"
    else:
        fio_error = f"fio exited with code {proc.returncode}"

    iostat_metrics: Dict[str, Optional[float]] = {}
    try:
        by_device = parse_iostat_samples(iostat_log)
        selected_device = pick_device(by_device, args.device)
        iostat_metrics = summarize_iostat(by_device, selected_device)
    except Exception as exc:  # pragma: no cover - defensive path
        iostat_metrics = summarize_iostat({}, None)
        fio_error = (fio_error + "; " if fio_error else "") + f"iostat parse error: {exc}"

    cpu_metrics = summarize_cpu_iowait(cpu_sampler.samples)

    row: Dict[str, Any] = {
        "label": args.label,
        "workload": workload["name"],
        "iteration": iteration,
        "rw": workload["rw"],
        "bs": workload["bs"],
        "iodepth": int(workload["iodepth"]),
        "numjobs": int(workload["numjobs"]),
        "rwmixread": workload.get("rwmixread", ""),
        "fio_exit_code": proc.returncode,
        "fio_error": fio_error,
        "elapsed_s": elapsed,
    }
    row.update(fio_metrics)
    row.update(iostat_metrics)
    row.update(cpu_metrics)
    return row


def write_csv(path: pathlib.Path, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def print_run_summary(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    print("\nRun summary (mean across iterations):")
    print(
        f"{'workload':28} {'IOPS':>10} {'MiB/s':>10} {'await(ms)':>12} "
        f"{'util(%)':>10} {'cpu_iowait(%)':>14}"
    )
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["workload"]), []).append(row)

    for wl in sorted(grouped.keys()):
        group = grouped[wl]
        total_iops = mean([to_float(r.get("total_iops")) or 0.0 for r in group]) or 0.0
        bw = mean([to_float(r.get("total_bw_mib_s")) or 0.0 for r in group]) or 0.0
        await_ms = mean(
            [v for v in (to_float(r.get("io_await_mean_ms")) for r in group) if v is not None]
        )
        util = mean(
            [v for v in (to_float(r.get("io_util_mean_pct")) for r in group) if v is not None]
        )
        cpu_iowait = mean(
            [
                v
                for v in (to_float(r.get("cpu_iowait_mean_pct")) for r in group)
                if v is not None
            ]
        )
        print(
            f"{wl:28} {total_iops:10.1f} {bw:10.1f} "
            f"{(await_ms if await_ms is not None else float('nan')):12.2f} "
            f"{(util if util is not None else float('nan')):10.2f} "
            f"{(cpu_iowait if cpu_iowait is not None else float('nan')):14.2f}"
        )


def cmd_run(args: argparse.Namespace) -> int:
    if shutil.which(args.fio_bin) is None:
        eprint(f"fio binary not found: {args.fio_bin}")
        return 2
    if shutil.which(args.iostat_bin) is None:
        eprint(f"iostat binary not found: {args.iostat_bin}")
        return 2
    if not pathlib.Path("/proc/stat").exists():
        eprint(
            "Warning: /proc/stat not available; cpu_iowait metrics will be empty "
            "(run inside Linux guest for full telemetry)."
        )

    workloads = load_workloads(pathlib.Path(args.workloads) if args.workloads else None)

    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    outdir = pathlib.Path(args.output_dir) / f"{args.label}-{stamp}"
    raw_dir = outdir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=False)

    metadata = {
        "label": args.label,
        "created_at": dt.datetime.now().isoformat(),
        "hostname": os.uname().nodename if hasattr(os, "uname") else "",
        "kernel": " ".join(os.uname()) if hasattr(os, "uname") else "",
        "args": vars(args),
        "workloads": workloads,
    }
    (outdir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (outdir / "workloads.json").write_text(json.dumps(workloads, indent=2), encoding="utf-8")

    rows: List[Dict[str, Any]] = []
    total_runs = len(workloads) * int(args.repeat)
    run_idx = 0
    for workload in workloads:
        for iteration in range(1, int(args.repeat) + 1):
            run_idx += 1
            print(
                f"[{run_idx}/{total_runs}] {workload['name']} "
                f"(iter {iteration}/{args.repeat}) ..."
            )
            row = run_single_workload(args, workload, iteration, raw_dir)
            rows.append(row)
            if row.get("fio_error"):
                print(f"  warning: {row['fio_error']}")

    write_csv(outdir / "summary.csv", rows, SUMMARY_COLUMNS)
    print_run_summary(rows)
    print(f"\nResults written to: {outdir}")
    return 0


def load_summary_csv(path: pathlib.Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            rows.append(dict(row))
    return rows


def group_means(rows: List[Dict[str, Any]], metric: str) -> Dict[str, float]:
    by_wl: Dict[str, List[float]] = {}
    for row in rows:
        wl = row.get("workload", "")
        val = to_float(row.get(metric))
        if wl and val is not None:
            by_wl.setdefault(wl, []).append(val)
    out: Dict[str, float] = {}
    for wl, vals in by_wl.items():
        out[wl] = statistics.fmean(vals)
    return out


def label_from_metadata(result_dir: pathlib.Path, fallback: str) -> str:
    meta = result_dir / "metadata.json"
    if not meta.exists():
        return fallback
    try:
        payload = json.loads(meta.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback
    label = payload.get("label")
    if isinstance(label, str) and label.strip():
        return label.strip()
    return fallback


def cmd_compare(args: argparse.Namespace) -> int:
    dir_a = pathlib.Path(args.a).resolve()
    dir_b = pathlib.Path(args.b).resolve()
    summary_a = dir_a / "summary.csv"
    summary_b = dir_b / "summary.csv"
    if not summary_a.exists():
        eprint(f"Missing summary file: {summary_a}")
        return 2
    if not summary_b.exists():
        eprint(f"Missing summary file: {summary_b}")
        return 2

    rows_a = load_summary_csv(summary_a)
    rows_b = load_summary_csv(summary_b)
    label_a = label_from_metadata(dir_a, "A")
    label_b = label_from_metadata(dir_b, "B")

    workloads = sorted(
        set(r.get("workload", "") for r in rows_a).union(
            set(r.get("workload", "") for r in rows_b)
        )
    )

    output_rows: List[Dict[str, Any]] = []
    print(
        f"{'workload':28} {'metric':20} {label_a:>12} {label_b:>12} "
        f"{'delta':>12} {'delta_%':>10}"
    )
    print("-" * 100)
    for metric in COMPARE_METRICS:
        means_a = group_means(rows_a, metric)
        means_b = group_means(rows_b, metric)
        for wl in workloads:
            a_val = means_a.get(wl)
            b_val = means_b.get(wl)
            if a_val is None or b_val is None:
                continue
            delta = b_val - a_val
            delta_pct = (delta / a_val * 100.0) if a_val != 0 else float("nan")
            output_rows.append(
                {
                    "workload": wl,
                    "metric": metric,
                    "label_a": label_a,
                    "value_a": a_val,
                    "label_b": label_b,
                    "value_b": b_val,
                    "delta": delta,
                    "delta_pct": delta_pct,
                }
            )
            print(
                f"{wl:28} {metric:20} {a_val:12.2f} {b_val:12.2f} "
                f"{delta:12.2f} {delta_pct:10.2f}"
            )

    out_path = pathlib.Path(args.output) if args.output else pathlib.Path(
        f"compare-{dir_a.name}-vs-{dir_b.name}.csv"
    )
    out_cols = [
        "workload",
        "metric",
        "label_a",
        "value_a",
        "label_b",
        "value_b",
        "delta",
        "delta_pct",
    ]
    write_csv(out_path, output_rows, out_cols)
    print(f"\nComparison CSV written to: {out_path.resolve()}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Storage benchmark harness for virtio backend comparison"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run benchmark workloads and collect telemetry")
    run.add_argument("--label", required=True, help="Result label (e.g., virtio-scsi)")
    run.add_argument(
        "--filename",
        required=True,
        help="fio target (file path or block device, e.g. /mnt/bench/testfile)",
    )
    run.add_argument("--size", default="20G", help="fio --size (default: 20G)")
    run.add_argument("--runtime", type=int, default=60, help="fio runtime seconds")
    run.add_argument("--ramp-time", type=int, default=10, help="fio ramp time seconds")
    run.add_argument("--repeat", type=int, default=3, help="Iterations per workload")
    run.add_argument(
        "--workloads",
        help="Optional JSON file overriding default workload matrix",
    )
    run.add_argument(
        "--device",
        help="Target device name for iostat filtering (e.g. /dev/vdb). "
        "If omitted, chooses busiest device by %util.",
    )
    run.add_argument("--ioengine", default="libaio", help="fio ioengine (default: libaio)")
    run.add_argument(
        "--direct",
        type=int,
        choices=[0, 1],
        default=1,
        help="fio direct I/O mode (default: 1)",
    )
    run.add_argument(
        "--extra-fio-arg",
        action="append",
        default=[],
        help="Pass-through fio argument, can be repeated (example: --extra-fio-arg=--iodepth_batch=16)",
    )
    run.add_argument("--fio-bin", default="fio", help="fio binary path/name")
    run.add_argument("--iostat-bin", default="iostat", help="iostat binary path/name")
    run.add_argument("--output-dir", default="results", help="Output base directory")
    run.set_defaults(func=cmd_run)

    compare = sub.add_parser(
        "compare",
        help="Compare two benchmark result directories (summary.csv + metadata.json)",
    )
    compare.add_argument("--a", required=True, help="First result directory")
    compare.add_argument("--b", required=True, help="Second result directory")
    compare.add_argument("--output", help="Output CSV path for comparison table")
    compare.set_defaults(func=cmd_compare)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
