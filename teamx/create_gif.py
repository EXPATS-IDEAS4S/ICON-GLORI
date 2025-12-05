import imageio
import os
from glob import glob

# === USER SETTINGS ===
date = "20250630_00"
data_source = "msg"  # "icon" or "msg" or 'mtg
cropped = ""  # "" or "cropped"
img_folder = f"/data/trade_pc/ICON/icon_teamx/20250630_00/msg/img/hourly/CMA/closing/png_200K-300K_greyscale"
output_gif = f"/{img_folder}/movie_png_{data_source}_{date}{cropped}.gif"
fps = 1   # frames per second (lower = slower animation)

# --- Collect images ---
img_files = sorted(glob(os.path.join(img_folder, "*.png")))

if not img_files:
    raise FileNotFoundError(f"No .png files found in {img_folder}")

# --- Create GIF ---
frames = []
for f in img_files:
    frames.append(imageio.imread(f))

imageio.mimsave(output_gif, frames, fps=fps)

print(f"GIF saved to {output_gif}")
