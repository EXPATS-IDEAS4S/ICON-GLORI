#!/usr/bin/env python3
"""
Scan crop .nc files and rename variables according to RENAME_MAP.
Uses h5py in-place rename (no full file rewrite needed).

Settings are aligned with check_crops_integrity.py.
"""

from __future__ import annotations

from pathlib import Path

import h5py

# ==========================
# User-configurable settings
# ==========================
FOLDER = "/data1/crops/teamx_Apr-Sep_2025_icon_msg/nc/1"

# Any variable whose name is a key will be renamed to the corresponding value.
RENAME_MAP = {
    "SYNMSG_BT_CL_IR10.8": "IR_108",
}

DRY_RUN = False  # Set to True to only print what would be renamed without modifying files.

# ==========================

def process_file(path: Path) -> dict:
    result = {"path": str(path), "renamed": [], "skipped": [], "error": None}
    mode = "r" if DRY_RUN else "r+"
    try:
        with h5py.File(path, mode) as f:
            top_keys = list(f.keys())
            for old_name, new_name in RENAME_MAP.items():
                if old_name in top_keys:
                    if new_name in top_keys:
                        result["skipped"].append(
                            f"{old_name} -> {new_name} (target already exists)"
                        )
                    else:
                        if not DRY_RUN:
                            f.move(old_name, new_name)
                        result["renamed"].append(f"{old_name} -> {new_name}")
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def main() -> None:
    folder = Path(FOLDER)
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"Folder not found: {folder}")

    nc_files = sorted(folder.glob("*.nc"))
    print(f"Found {len(nc_files)} .nc files in {folder}")
    if DRY_RUN:
        print("DRY RUN — no files will be modified.\n")

    renamed_count = 0
    error_count = 0

    for path in nc_files:
        result = process_file(path)
        if result["error"]:
            print(f"[ERROR] {path.name}: {result['error']}")
            error_count += 1
        elif result["renamed"]:
            for r in result["renamed"]:
                print(f"[{'DRY' if DRY_RUN else 'OK'}] {path.name}: {r}")
            renamed_count += len(result["renamed"])
        elif result["skipped"]:
            for s in result["skipped"]:
                print(f"[SKIP] {path.name}: {s}")

    print(f"\nDone. Renames: {renamed_count} | Errors: {error_count}")


if __name__ == "__main__":
    main()
