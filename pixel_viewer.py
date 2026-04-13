#!/usr/bin/env python3
"""
AMG8833 Live Pixel Viewer
==========================
Shows the raw 8×8 thermal image, the background-subtracted delta,
and blob centroids — all updating in real time.

Usage:
    pip install pyserial matplotlib scipy
    python3 pixel_viewer.py --port /dev/ttyUSB0   # Linux/Mac
    python3 pixel_viewer.py --port COM3            # Windows
    python3 pixel_viewer.py --demo                 # No sensor needed

The ESP32 sketch must have  #define PIXEL_STREAM  true
"""

import argparse
import re
import sys
import threading
import queue
import time
import random
import math
from datetime import datetime

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.animation import FuncAnimation
from matplotlib.patches import Circle
from scipy.ndimage import zoom          # for smooth upscaling

# ── Serial protocol parser ────────────────────────────────────────────────────
# Line format:  PIXELS:f,f,...(x64)|BG:f,f,...(x64)|BLOBS:cx,cy;cx,cy;...

RE_LINE = re.compile(r"PIXELS:([^|]+)\|BG:([^|]+)\|BLOBS:(.*)")

data_queue = queue.Queue(maxsize=4)   # drop stale frames rather than back-log
running    = True

# ── Data source threads ───────────────────────────────────────────────────────

def serial_thread(port, baud):
    global running
    try:
        import serial
    except ImportError:
        print("ERROR: pyserial not installed.  pip install pyserial")
        running = False
        return
    try:
        ser = serial.Serial(port, baud, timeout=1)
        print(f"Connected to {port} @ {baud} baud.")
    except Exception as e:
        print(f"ERROR: {e}")
        running = False
        return

    while running:
        try:
            line = ser.readline().decode("utf-8", errors="replace").strip()
            m = RE_LINE.match(line)
            if not m:
                continue
            pixels = np.array([float(x) for x in m.group(1).split(",")]).reshape(8, 8)
            bg     = np.array([float(x) for x in m.group(2).split(",")]).reshape(8, 8)
            blobs  = []
            if m.group(3).strip():
                for pair in m.group(3).split(";"):
                    cx, cy = pair.split(",")
                    blobs.append((float(cx), float(cy)))   # (col, row)
            try:
                data_queue.put_nowait((pixels, bg, blobs))
            except queue.Full:
                pass   # drop frame — viewer is keeping up fine
        except Exception:
            pass
    ser.close()


def demo_thread():
    """Generates synthetic pixel frames so you can test without a sensor."""
    global running
    t = 0
    while running:
        # Ambient background ~24 °C
        bg = np.full((8, 8), 24.0) + np.random.randn(8, 8) * 0.3

        # Simulate 1 or 2 moving blobs (people crossing)
        pixels = bg.copy()
        blobs  = []
        n_people = random.choices([1, 2], weights=[0.7, 0.3])[0]
        for p in range(n_people):
            # Column sweeps 0→7 over ~3 seconds, offset per person
            col = ((t * 0.8 + p * 4.0) % 9) - 0.5
            row = 3.5 + math.sin(t * 0.5 + p) * 1.5
            for r in range(8):
                for c in range(8):
                    dist = math.sqrt((r - row) ** 2 + (c - col) ** 2)
                    pixels[r, c] += max(0, 10 * math.exp(-dist ** 2 / 1.5))
            if 0 <= col <= 7:
                blobs.append((col, row))

        pixels += np.random.randn(8, 8) * 0.2
        try:
            data_queue.put_nowait((pixels, bg, blobs))
        except queue.Full:
            pass
        t += 0.1
        time.sleep(0.1)


# ── Smooth upscale ────────────────────────────────────────────────────────────

SCALE = 32   # each 8×8 pixel → 32×32 display pixels  (= 256×256 image)

def upscale(arr):
    return zoom(arr, SCALE, order=1)   # bilinear


# ── Build figure ──────────────────────────────────────────────────────────────

C_BG    = "#0d1117"
C_PANEL = "#161b22"
C_TEXT  = "#e6edf3"
C_BLOB  = "#ff6b6b"
C_DIV   = "#f5a623"

def build():
    plt.style.use("dark_background")
    fig = plt.figure(figsize=(12, 5), facecolor=C_BG)
    fig.canvas.manager.set_window_title("AMG8833 Live Pixel Viewer")

    gs = gridspec.GridSpec(1, 3, figure=fig,
                           left=0.04, right=0.96,
                           top=0.88,  bottom=0.08,
                           wspace=0.1)

    ax_raw   = fig.add_subplot(gs[0])
    ax_delta = fig.add_subplot(gs[1])
    ax_blob  = fig.add_subplot(gs[2])

    titles = ["Raw Temperature (°C)", "Delta from Background (°C)", "Blob Detection"]
    axes   = [ax_raw, ax_delta, ax_blob]
    for ax, title in zip(axes, titles):
        ax.set_facecolor(C_PANEL)
        ax.set_title(title, color=C_TEXT, fontsize=10, pad=6)
        ax.set_xticks([]); ax.set_yticks([])
        ax.set_xlabel("← Hallway  |  Room →", color=C_TEXT, fontsize=8)

    # Add column divider label
    for ax in axes:
        ax.axvline(x=SCALE * 4 - 0.5, color=C_DIV, linewidth=1.2, linestyle="--", alpha=0.6)

    fig.suptitle("AMG8833 Live Thermal View", color=C_TEXT, fontsize=12,
                 fontweight="bold", y=0.97)

    # Colourbar axes
    cax_raw   = fig.add_axes([0.04,  0.04, 0.28, 0.025])
    cax_delta = fig.add_axes([0.365, 0.04, 0.28, 0.025])

    return fig, ax_raw, ax_delta, ax_blob, cax_raw, cax_delta


# ── Animation update ──────────────────────────────────────────────────────────

def make_updater(fig, ax_raw, ax_delta, ax_blob, cax_raw, cax_delta):
    im_raw   = [None]
    im_delta = [None]
    im_blob  = [None]
    cb_raw   = [None]
    cb_delta = [None]
    blob_artists = []

    def update(_frame):
        nonlocal blob_artists

        # Grab the latest frame (skip stale ones)
        frame = None
        while not data_queue.empty():
            try:
                frame = data_queue.get_nowait()
            except queue.Empty:
                break
        if frame is None:
            return

        pixels, bg, blobs = frame
        delta = pixels - bg

        up_raw   = upscale(pixels)
        up_delta = upscale(delta)
        up_blob  = upscale(np.clip(delta, 0, None))

        display_size = SCALE * 8  # 256

        # ── Raw panel ──────────────────────────────────────────────────────
        if im_raw[0] is None:
            im_raw[0] = ax_raw.imshow(up_raw, cmap="inferno",
                                      origin="upper",
                                      extent=[0, display_size, display_size, 0])
            cb_raw[0] = fig.colorbar(im_raw[0], cax=cax_raw, orientation="horizontal")
            cb_raw[0].ax.tick_params(colors=C_TEXT, labelsize=7)
        else:
            im_raw[0].set_data(up_raw)
            im_raw[0].set_clim(vmin=pixels.min(), vmax=pixels.max())

        # ── Delta panel ────────────────────────────────────────────────────
        d_max = max(abs(delta).max(), 1.0)
        if im_delta[0] is None:
            im_delta[0] = ax_delta.imshow(up_delta, cmap="RdYlGn_r",
                                          origin="upper",
                                          extent=[0, display_size, display_size, 0],
                                          vmin=-d_max, vmax=d_max)
            cb_delta[0] = fig.colorbar(im_delta[0], cax=cax_delta, orientation="horizontal")
            cb_delta[0].ax.tick_params(colors=C_TEXT, labelsize=7)
        else:
            im_delta[0].set_data(up_delta)
            im_delta[0].set_clim(vmin=-d_max, vmax=d_max)

        # ── Blob panel ─────────────────────────────────────────────────────
        if im_blob[0] is None:
            im_blob[0] = ax_blob.imshow(up_blob, cmap="hot",
                                        origin="upper",
                                        extent=[0, display_size, display_size, 0],
                                        vmin=0)
        else:
            im_blob[0].set_data(up_blob)
            im_blob[0].set_clim(vmin=0, vmax=max(up_blob.max(), 1))

        # Remove old blob markers
        for artist in blob_artists:
            artist.remove()
        blob_artists.clear()

        for (col, row) in blobs:
            # col/row are in sensor pixel space (0-7); scale to display space
            cx_px = (col + 0.5) * SCALE
            cy_px = (row + 0.5) * SCALE
            for ax in (ax_raw, ax_delta, ax_blob):
                circle = Circle((cx_px, cy_px), radius=SCALE * 0.9,
                                edgecolor=C_BLOB, facecolor="none",
                                linewidth=2, zorder=5)
                ax.add_patch(circle)
                blob_artists.append(circle)

                dot = Circle((cx_px, cy_px), radius=SCALE * 0.15,
                             facecolor=C_BLOB, zorder=6)
                ax.add_patch(dot)
                blob_artists.append(dot)

        # Min/max annotation on raw panel
        for txt in ax_raw.texts:
            txt.remove()
        ax_raw.text(4, 12, f"min {pixels.min():.1f}°  max {pixels.max():.1f}°",
                    color=C_TEXT, fontsize=7, alpha=0.8)

        fig.canvas.draw_idle()

    return update


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AMG8833 Live Pixel Viewer")
    parser.add_argument("--port",     default="/dev/ttyUSB0")
    parser.add_argument("--baud",     default=115200, type=int)
    parser.add_argument("--demo",     action="store_true",
                        help="Run with synthetic data (no sensor needed)")
    parser.add_argument("--interval", default=120, type=int,
                        help="Refresh interval ms (default 120 ≈ 8 fps)")
    args = parser.parse_args()

    global running

    if args.demo:
        t = threading.Thread(target=demo_thread, daemon=True)
    else:
        t = threading.Thread(target=serial_thread, args=(args.port, args.baud), daemon=True)
    t.start()

    fig, ax_raw, ax_delta, ax_blob, cax_raw, cax_delta = build()
    updater = make_updater(fig, ax_raw, ax_delta, ax_blob, cax_raw, cax_delta)

    ani = FuncAnimation(fig, updater, interval=args.interval, cache_frame_data=False)

    def on_close(_):
        global running
        running = False

    fig.canvas.mpl_connect("close_event", on_close)

    try:
        plt.show()
    except KeyboardInterrupt:
        pass
    finally:
        running = False


if __name__ == "__main__":
    main()
