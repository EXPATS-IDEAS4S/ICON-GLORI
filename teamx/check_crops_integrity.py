#!/usr/bin/env python3
"""
Validate ICON/MSG crop files in a time range.

Checks performed:
- file can be opened (corruption check)
- lat/lon size is 100x100
- required variable exists (default: clm, case-insensitive)
- for each timestamp in [start, end], ICON and MSG are present
- for each timestamp, central/west/east domains are present for both sources

Default folder is aligned with create_icon_msg_crops_from_bucket.py output.
"""

from __future__ import annotations

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
MSG_RE = re.compile(
    r"^MSG_[^_]+_[^_]+_(?P<date>\d{4}-\d{2}-\d{2})_(?P<hour>\d{2})_(?P<domain>central|west|east)\.nc$"
)

DOMAINS = ("central", "west", "east")
SOURCES = ("ICON", "MSG")


# ==========================
# User-configurable settings
# ==========================
START = "20250401_00"
END = "20250930_00"
FOLDER = "/data1/crops/teamx_Apr-Sep_2025_icon_msg/nc/1"
EXPECTED_SIZE = 100
REPORT_JSON = "./logs/crop_integrity_report.json"

# Required data variable names can differ by source.
# Matching is case-insensitive.
REQUIRED_VAR_BY_SOURCE = {
    "ICON": 'SYNMSG_BT_CL_IR10.8',
    "MSG": 'IR_108',
}


@dataclass
class ParsedFile:
    path: Path
    source: str
    timestamp: datetime
    domain: str


def parse_dt_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d_%H")


def build_expected_timestamps(start_dt: datetime, end_dt: datetime) -> List[datetime]:
    if end_dt < start_dt:
        raise ValueError("end date must be >= start date")

    out = []
    cur = start_dt
    while cur <= end_dt:
        out.append(cur)
        cur += timedelta(hours=1)
    return out


def parse_crop_filename(path: Path) -> Optional[ParsedFile]:
    name = path.name

    m_icon = ICON_RE.match(name)
    if m_icon:
        ts = datetime.strptime(
            f"{m_icon.group('date')}_{m_icon.group('hour')}", "%Y-%m-%d_%H"
        )
        return ParsedFile(path=path, source="ICON", timestamp=ts, domain=m_icon.group("domain"))

    m_msg = MSG_RE.match(name)
    if m_msg:
        ts = datetime.strptime(
            f"{m_msg.group('date')}_{m_msg.group('hour')}", "%Y-%m-%d_%H"
        )
        return ParsedFile(path=path, source="MSG", timestamp=ts, domain=m_msg.group("domain"))

    return None


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
    start_dt = parse_dt_hour(START)
    end_dt = parse_dt_hour(END)
    expected_timestamps = build_expected_timestamps(start_dt, end_dt)

    folder = Path(FOLDER)
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

    # Track availability per (timestamp, source, domain)
    available: Dict[Tuple[datetime, str, str], List[Path]] = {}
    for pf in parsed_files:
        if pf.timestamp in expected_set:
            key = (pf.timestamp, pf.source, pf.domain)
            available.setdefault(key, []).append(pf.path)

    # Validate all matched files.
    file_checks = []
    bad_files = []
    for pf in parsed_files:
        if pf.timestamp not in expected_set:
            continue
        required_var = REQUIRED_VAR_BY_SOURCE.get(pf.source)
        if not required_var:
            continue
        check = check_file(
            path=pf.path,
            required_var=required_var,
            expected_size=EXPECTED_SIZE,
        )
        file_checks.append(check)
        if not check["ok"]:
            bad_files.append(check)

    # Missing coverage by timestamp/source/domain.
    missing: List[Dict[str, str]] = []
    duplicates: List[Dict[str, object]] = []
    for ts in expected_timestamps:
        for source in SOURCES:
            for domain in DOMAINS:
                key = (ts, source, domain)
                files_here = available.get(key, [])
                if len(files_here) == 0:
                    missing.append(
                        {
                            "timestamp": ts.strftime("%Y-%m-%d_%H"),
                            "source": source,
                            "domain": domain,
                        }
                    )
                elif len(files_here) > 1:
                    duplicates.append(
                        {
                            "timestamp": ts.strftime("%Y-%m-%d_%H"),
                            "source": source,
                            "domain": domain,
                            "files": [str(f) for f in files_here],
                        }
                    )

    summary = {
        "start": START,
        "end": END,
        "folder": str(folder),
        "required_var_by_source": REQUIRED_VAR_BY_SOURCE,
        "expected_size": EXPECTED_SIZE,
        "expected_timestamps": len(expected_timestamps),
        "expected_combinations": len(expected_timestamps) * len(SOURCES) * len(DOMAINS),
        "files_scanned_total": len(list(folder.glob("*.nc"))),
        "files_matched_pattern": len(parsed_files),
        "files_ignored_pattern": len(ignored_files),
        "files_validated_in_range": len(file_checks),
        "bad_files": len(bad_files),
        "missing_combinations": len(missing),
        "duplicate_combinations": len(duplicates),
        "all_checks_passed": len(bad_files) == 0 and len(missing) == 0,
    }

    report = {
        "summary": summary,
        "bad_files": bad_files,
        "missing": missing,
        "duplicates": duplicates,
        "ignored_files": ignored_files,
    }

    report_path = Path(REPORT_JSON)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("=== Crop Integrity Report ===")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print(f"json_report: {report_path.resolve()}")

    if bad_files:
        print("\nFirst 10 bad files:")
        for bf in bad_files[:10]:
            print(f"- {bf['path']} :: {', '.join(bf['errors'])}")

    if missing:
        print("\nFirst 20 missing combinations:")
        for m in missing[:20]:
            print(f"- {m['timestamp']} {m['source']} {m['domain']}")

    if duplicates:
        print("\nFirst 10 duplicate combinations:")
        for d in duplicates[:10]:
            print(f"- {d['timestamp']} {d['source']} {d['domain']} ({len(d['files'])} files)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
