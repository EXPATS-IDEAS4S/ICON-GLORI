#!/usr/bin/env python3
"""
Validate ICON/MSG crop files in a time range.

Checks performed:
- file can be opened (corruption check)
- lat/lon size is 100x100
- required variable exists (default: clm, case-insensitive)
- for MSG, all theoretical hourly timestamps are present in [start, end]
- for ICON, each daily init (00Z, 12Z) has a complete forecast horizon

Default folder is aligned with create_icon_msg_crops_from_bucket.py output.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import xarray as xr


ICON_RE = re.compile(
    r"^ICON500m_[^_]+_[^_]+_\d{8}_\d{2}_(?P<date>\d{4}-\d{2}-\d{2})_(?P<hour>\d{2})_(?P<domain>central|west|east)\.nc$"
)
ICON_RE_WITH_INIT = re.compile(
    r"^ICON500m_[^_]+_[^_]+_(?P<init_date>\d{8})_(?P<init_hour>\d{2})_(?P<date>\d{4}-\d{2}-\d{2})_(?P<hour>\d{2})_(?P<domain>central|west|east)\.nc$"
)
MSG_RE = re.compile(
    r"^MSG_[^_]+_[^_]+_(?P<date>\d{4}-\d{2}-\d{2})_(?P<hour>\d{2})_(?P<domain>central|west|east)\.nc$"
)

MSG_COMPONENT = "MSG"
ICON_COMPONENTS = ("ICON00", "ICON12")
ICON_INIT_HOURS = ("00", "12")


# ==========================
# User-configurable settings
# ==========================
START = "20250401_00"
END = "20250930_00"
HOURLY_RES_HOURS = 1
FOLDER = "/data1/crops/teamx_Apr-Sep_2025_icon_msg_new/nc/1"
EXPECTED_SIZE = 100
ICON_FORECAST_HOURS = 48
REPORT_JSON = "./logs/crop_integrity_report.json"

# Required data variable names can differ by source.
# Matching is case-insensitive.
REQUIRED_VAR_BY_SOURCE = {
    "ICON": 'SYNMSG_BT_CL_IR10.8',
    "ICON00": 'IR_108',
    "ICON12": 'IR_108',
    "MSG": 'IR_108',
}


@dataclass
class ParsedFile:
    path: Path
    source: str
    timestamp: datetime
    domain: str
    init_datetime: Optional[datetime] = None


def parse_dt_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d_%H")


def build_expected_timestamps(start_dt: datetime, end_dt: datetime, step_hours: int) -> List[datetime]:
    if end_dt < start_dt:
        raise ValueError("end date must be >= start date")
    if step_hours <= 0:
        raise ValueError("hourly step must be > 0")

    out = []
    cur = start_dt
    while cur <= end_dt:
        out.append(cur)
        cur += timedelta(hours=step_hours)
    return out


def parse_crop_filename(path: Path) -> Optional[ParsedFile]:
    name = path.name

    m_icon = ICON_RE_WITH_INIT.match(name)
    if m_icon:
        init_dt = datetime.strptime(
            f"{m_icon.group('init_date')}_{m_icon.group('init_hour')}", "%Y%m%d_%H"
        )
        ts = datetime.strptime(
            f"{m_icon.group('date')}_{m_icon.group('hour')}", "%Y-%m-%d_%H"
        )
        init_hour = m_icon.group("init_hour")
        return ParsedFile(
            path=path,
            source=f"ICON{init_hour}",
            timestamp=ts,
            domain=m_icon.group("domain"),
            init_datetime=init_dt,
        )

    m_icon_legacy = ICON_RE.match(name)
    if m_icon_legacy:
        ts = datetime.strptime(
            f"{m_icon_legacy.group('date')}_{m_icon_legacy.group('hour')}", "%Y-%m-%d_%H"
        )
        return ParsedFile(path=path, source="ICON", timestamp=ts, domain=m_icon_legacy.group("domain"))

    m_msg = MSG_RE.match(name)
    if m_msg:
        ts = datetime.strptime(
            f"{m_msg.group('date')}_{m_msg.group('hour')}", "%Y-%m-%d_%H"
        )
        return ParsedFile(path=path, source="MSG", timestamp=ts, domain=m_msg.group("domain"))

    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate MSG hourly coverage and ICON init forecast completeness.")
    parser.add_argument("--start", default=START, help="Start timestamp, format YYYYMMDD_HH")
    parser.add_argument("--end", default=END, help="End timestamp, format YYYYMMDD_HH")
    parser.add_argument(
        "--step-hours",
        type=int,
        default=HOURLY_RES_HOURS,
        help="Expected hourly resolution in hours (default: 1)",
    )
    parser.add_argument("--folder", default=FOLDER, help="Folder containing cropped .nc files")
    parser.add_argument("--expected-size", type=int, default=EXPECTED_SIZE, help="Expected lat/lon size")
    parser.add_argument(
        "--icon-forecast-hours",
        type=int,
        default=ICON_FORECAST_HOURS,
        help="Expected number of hourly forecasts per ICON init (default: 48)",
    )
    parser.add_argument("--report-json", default=REPORT_JSON, help="Path to output report JSON")
    return parser.parse_args()


def build_expected_icon_init_runs(start_dt: datetime, end_dt: datetime) -> List[datetime]:
    out: List[datetime] = []
    cur_day = start_dt.date()
    end_day = end_dt.date()
    while cur_day <= end_day:
        for init_hour in ICON_INIT_HOURS:
            out.append(datetime.strptime(f"{cur_day.strftime('%Y%m%d')}_{init_hour}", "%Y%m%d_%H"))
        cur_day += timedelta(days=1)
    return out


def detect_lat_lon_dims(ds: xr.Dataset) -> Tuple[Optional[str], Optional[str]]:
    lat_name = "lat" if "lat" in ds.dims else None
    lon_name = "lon" if "lon" in ds.dims else None

    if lat_name is None:
        for cand in ("latitude", "y"):
            if cand in ds.dims:
                lat_name = cand
                break

    if lon_name is None:
        for cand in ("longitude", "x"):
            if cand in ds.dims:
                lon_name = cand
                break

    return lat_name, lon_name


def has_required_var(ds: xr.Dataset, required_var: str) -> bool:
    required = required_var.lower()
    data_var_names = [v.lower() for v in ds.data_vars]
    return required in data_var_names


def check_file(path: Path, required_var: str, expected_size: int) -> Dict[str, object]:
    result = {
        "path": str(path),
        "ok": True,
        "errors": [],
    }

    try:
        # Open and touch metadata/variables to detect broken files early.
        with xr.open_dataset(path, engine="h5netcdf") as ds:
            _ = list(ds.data_vars)

            if not has_required_var(ds, required_var):
                result["ok"] = False
                result["errors"].append(f"missing_required_var:{required_var}")

            lat_name, lon_name = detect_lat_lon_dims(ds)
            if lat_name is None or lon_name is None:
                result["ok"] = False
                result["errors"].append("missing_lat_lon_dims")
            else:
                lat_size = int(ds.sizes.get(lat_name, -1))
                lon_size = int(ds.sizes.get(lon_name, -1))
                if lat_size != expected_size or lon_size != expected_size:
                    result["ok"] = False
                    result["errors"].append(
                        f"wrong_size:{lat_name}={lat_size},{lon_name}={lon_size},expected={expected_size}x{expected_size}"
                    )

            # Force a small read to catch lazy IO issues.
            first_var = next(iter(ds.data_vars), None)
            if first_var is not None:
                _ = ds[first_var].isel({d: 0 for d in ds[first_var].dims if ds.sizes[d] > 0}).values

    except Exception as exc:
        result["ok"] = False
        result["errors"].append(f"corrupted_or_unreadable:{type(exc).__name__}:{exc}")

    return result


def main() -> int:
    args = parse_args()

    start_dt = parse_dt_hour(args.start)
    end_dt = parse_dt_hour(args.end)
    expected_timestamps = build_expected_timestamps(start_dt, end_dt, args.step_hours)
    if args.icon_forecast_hours <= 0:
        raise ValueError("icon-forecast-hours must be > 0")

    folder = Path(args.folder)
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"Folder not found or not a directory: {folder}")

    parsed_files: List[ParsedFile] = []
    ignored_files: List[str] = []

    for p in sorted(folder.glob("*.nc")):
        parsed = parse_crop_filename(p)
        if parsed is None:
            ignored_files.append(str(p))
            continue
        parsed_files.append(parsed)

    expected_set: Set[datetime] = set(expected_timestamps)
    expected_icon_init_runs = build_expected_icon_init_runs(start_dt, end_dt)
    expected_icon_init_run_set = set(expected_icon_init_runs)

    # MSG theoretical hourly timestamp coverage in [start, end].
    present_timestamps_msg: Set[datetime] = set()

    # ICON forecast coverage by init run (source, init_datetime), independent of domain.
    icon_forecasts_by_init: Dict[Tuple[str, datetime], Set[datetime]] = {}

    for pf in parsed_files:
        if pf.source == MSG_COMPONENT:
            if pf.timestamp in expected_set:
                present_timestamps_msg.add(pf.timestamp)
        elif pf.source in ICON_COMPONENTS and pf.init_datetime is not None:
            if pf.init_datetime not in expected_icon_init_run_set:
                continue
            key = (pf.source, pf.init_datetime)
            icon_forecasts_by_init.setdefault(key, set()).add(pf.timestamp)

    expected_ts_str = [ts.strftime("%Y-%m-%d_%H") for ts in expected_timestamps]
    present_msg_str = {ts.strftime("%Y-%m-%d_%H") for ts in present_timestamps_msg}
    missing_timestamps_msg = [ts for ts in expected_ts_str if ts not in present_msg_str]
    msg_coverage_summary = {
        "expected": len(expected_timestamps),
        "present": len(present_timestamps_msg),
        "missing": len(missing_timestamps_msg),
    }

    icon_init_forecast_checks: List[Dict[str, object]] = []
    incomplete_icon_init_runs = 0

    for init_dt in expected_icon_init_runs:
        init_hour = init_dt.strftime("%H")
        source = f"ICON{init_hour}"
        present_forecasts = icon_forecasts_by_init.get((source, init_dt), set())

        expected_forecasts = [init_dt + timedelta(hours=h) for h in range(args.icon_forecast_hours)]
        expected_forecast_str = [ts.strftime("%Y-%m-%d_%H") for ts in expected_forecasts]
        present_forecast_str = {ts.strftime("%Y-%m-%d_%H") for ts in present_forecasts}
        missing_forecasts = [ts for ts in expected_forecast_str if ts not in present_forecast_str]

        is_complete = len(missing_forecasts) == 0
        if not is_complete:
            incomplete_icon_init_runs += 1

        icon_init_forecast_checks.append(
            {
                "source": source,
                "init": init_dt.strftime("%Y-%m-%d_%H"),
                "expected_forecasts": args.icon_forecast_hours,
                "present_forecasts": len(present_forecasts),
                "missing_forecasts": len(missing_forecasts),
                "is_complete": is_complete,
                "missing_timestamps": missing_forecasts,
            }
        )

    # Validate all matched files.
    file_checks = []
    bad_files = []
    for pf in parsed_files:
        if pf.timestamp not in expected_set:
            continue
        required_var = REQUIRED_VAR_BY_SOURCE.get(pf.source, REQUIRED_VAR_BY_SOURCE.get("ICON"))
        if not required_var:
            continue
        check = check_file(
            path=pf.path,
            required_var=required_var,
            expected_size=args.expected_size,
        )
        file_checks.append(check)
        if not check["ok"]:
            bad_files.append(check)

    missing_timestamps_global: Set[str] = set(missing_timestamps_msg)

    summary = {
        "start": args.start,
        "end": args.end,
        "step_hours": args.step_hours,
        "folder": str(folder),
        "required_var_by_source": REQUIRED_VAR_BY_SOURCE,
        "expected_size": args.expected_size,
        "expected_timestamps": len(expected_timestamps),
        "msg_timestamp_coverage": msg_coverage_summary,
        "icon_expected_inits": len(expected_icon_init_runs),
        "icon_forecast_hours_per_init": args.icon_forecast_hours,
        "icon_complete_inits": len(expected_icon_init_runs) - incomplete_icon_init_runs,
        "icon_incomplete_inits": incomplete_icon_init_runs,
        "missing_timestamps_unique": len(missing_timestamps_global),
        "files_scanned_total": len(list(folder.glob("*.nc"))),
        "files_matched_pattern": len(parsed_files),
        "files_ignored_pattern": len(ignored_files),
        "files_validated_in_range": len(file_checks),
        "bad_files": len(bad_files),
        "all_checks_passed": len(bad_files) == 0 and len(missing_timestamps_msg) == 0 and incomplete_icon_init_runs == 0,
    }

    report = {
        "summary": summary,
        "bad_files": bad_files,
        "missing_timestamps": {
            "global_unique": sorted(missing_timestamps_global),
        },
        "missing_timestamps_by_component": {
            MSG_COMPONENT: missing_timestamps_msg,
        },
        "icon_init_forecast_checks": icon_init_forecast_checks,
        "ignored_files": ignored_files,
    }

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("=== Crop Integrity Report ===")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print(f"json_report: {report_path.resolve()}")

    if bad_files:
        print("\nFirst 10 bad files:")
        for bf in bad_files[:10]:
            print(f"- {bf['path']} :: {', '.join(bf['errors'])}")

    if missing_timestamps_msg:
        print("\nFirst 20 MSG missing theoretical timestamps:")
        for ts in missing_timestamps_msg[:20]:
            print(f"- {ts}")

    print("\nMSG hourly timestamp coverage:")
    print(
        f"- MSG: present={msg_coverage_summary['present']}/"
        f"{msg_coverage_summary['expected']}, missing={msg_coverage_summary['missing']}"
    )

    print("\nICON init forecast completeness:")
    print(
        f"- complete_inits={summary['icon_complete_inits']}/"
        f"{summary['icon_expected_inits']} (forecast_hours={args.icon_forecast_hours})"
    )
    for row in [r for r in icon_init_forecast_checks if not r['is_complete']][:10]:
        print(f"  - {row['source']} init={row['init']} missing={row['missing_forecasts']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#nohup 634688