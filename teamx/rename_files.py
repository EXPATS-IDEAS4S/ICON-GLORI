import os
import re
import shutil
from glob import glob
from datetime import datetime

# === CONFIGURATION ===
extension = "png"  # tif or .png
folders = {
    f"/data/trade_pc/ICON/icon_teamx/20250630_00/img_ml/{extension}_greyscale": "icon",
    f"/data/trade_pc/ICON/icon_teamx/20250630_00/img_ml_cropped/{extension}_greyscale": "icon-cropped",
    f"/data/trade_pc/ICON/icon_teamx/20250630_00/msg/img/hourly/CMA/closing/{extension}_200K-300K_greyscale": "msg",
    f"/data/trade_pc/ICON/icon_teamx/20250630_00/msg/img_cropped/hourly/CMA/closing/{extension}_200K-300K_greyscale": "msg-cropped",
}

# root output folder
out_root = "/data/trade_pc/ICON/icon_teamx/20250630_00/data_plot"

# regex patterns for datetime extraction
patterns = [
    (re.compile(r"(\d{8}_\d{2}:\d{2})"), "%Y%m%d_%H:%M"),      # e.g. 20250701_23:00
    (re.compile(r"ilf3f(\d{10})"), "%y%m%d%H%M%S"),            # e.g. ilf3f02000000
    (re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"), "%Y-%m-%dT%H:%M:%S"),  # e.g. 2025-06-30T01:00:00
]

def extract_datetime(filename):
    """Extract datetime string (standardized as YYYYmmdd_HHMM) from filename."""
    for regex, dt_format in patterns:
        match = regex.search(filename)
        if match:
            date_str = match.group(1)
            try:
                dt = datetime.strptime(date_str, dt_format)
                return dt.strftime("%Y%m%d_%H%M")
            except Exception as e:
                print(f"⚠️ Failed parsing datetime from {date_str} with {dt_format}: {e}")
    return None

# === PROCESS FILES ===
for folder, data_type in folders.items():
    file_list = glob(os.path.join(folder, f"*.{extension}")) #+ glob(os.path.join(folder, "*.tiff"))
    print(f"Found {len(file_list)} files in {folder} ({data_type})")

    # create subfolder for each data type
    out_dir = os.path.join(out_root, data_type)
    os.makedirs(out_dir, exist_ok=True)

    for f in file_list:
        base = os.path.basename(f)
        ext = os.path.splitext(base)[1].lower()  # keep .tif or .tiff
        dt_str = extract_datetime(base)

        if not dt_str:
            print(f"⚠️ Could not extract datetime from {base}, skipping.")
            continue

        new_name = f"{dt_str}_{data_type}{ext}"
        out_path = os.path.join(out_dir, new_name)

        # copy without overwriting
        if not os.path.exists(out_path):
            shutil.copy2(f, out_path)
            print(f"✅ Copied {base} → {new_name} in {data_type}/")
        else:
            print(f"⏩ Skipping {new_name}, already exists in {data_type}/")
