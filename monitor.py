#!/usr/bin/env python3
"""
AMG8833 People Counter — Live Matplotlib Dashboard
====================================================
Run on your PC while the ESP32 is connected via USB.

Dashboard panels:
  1. Occupancy over time  (line chart, live scrolling)
  2. Cumulative IN / OUT  (line chart)
  3. Event rate           (bar chart — events per minute, rolling window)
  4. Big stats panel      (current occupancy, totals, warnings)
  5. Live event log       (last 12 events as scrolling text)

Usage:
    pip install pyserial matplotlib
    python3 monitor.py --port /dev/ttyUSB0      # Linux/Mac
    python3 monitor.py --port COM3              # Windows
    python3 monitor.py --port COM3 --demo       # Run with fake data (no sensor needed)

Press Ctrl+C or close the window to exit.
"""

import argparse
import re
import sys
import time
import threading
import queue
from collections import deque
from datetime import datetime

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.animation import FuncAnimation
import matplotlib.patches as mpatches

# ── Shared state (written by serial thread, read by plot thread) ──────────────

data_queue   = queue.Queue()   # raw parsed events pushed from serial thread

timestamps   = deque(maxlen=500)   # datetime of each event
occupancy_ts = deque(maxlen=500)   # occupancy value at each event
enter_ts     = deque(maxlen=500)
exit_ts      = deque(maxlen=500)

event_log    = deque(maxlen=12)    # human-readable strings for the log panel

total_enter  = 0
total_exit   = 0
errors       = 0
running      = True

# ── Colour scheme ─────────────────────────────────────────────────────────────
C_BG       = "#1a1a2e"
C_PANEL    = "#16213e"
C_ENTER    = "#00d4aa"   # teal-green
C_EXIT     = "#ff6b6b"   # coral-red
C_OCC      = "#f5a623"   # amber
C_TEXT     = "#e0e0e0"
C_WARN     = "#ff4444"
C_GRID     = "#2a2a4a"

# ── Serial / demo reader thread ───────────────────────────────────────────────

RE_ENTER = re.compile(r"\[COUNT\].*ENTER.*In:\s*(\d+)\s*Out:\s*(\d+)")
RE_EXIT  = re.compile(r"\[COUNT\].*EXIT.*In:\s*(\d+)\s*Out:\s*(\d+)")
RE_WARN  = re.compile(r"ERROR|WARNING", re.IGNORECASE)


def serial_thread(port, baud, logfile_path):
    """Reads from the ESP32 serial port and pushes events onto data_queue."""
    global running
    try:
        import serial
    except ImportError:
        print("ERROR: pyserial not installed. Run:  pip install pyserial")
        running = False
        return

    try:
        ser = serial.Serial(port, baud, timeout=1)
        print(f"Connected to {port} @ {baud} baud.")
    except Exception as e:
        print(f"ERROR opening serial port: {e}")
        running = False
        return

    logfile = open(logfile_path, "a")
    logfile.write(f"\n=== Session started {datetime.now()} ===\n")

    while running:
        try:
            raw = ser.readline()
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace").strip()
            logfile.write(line + "\n")
            logfile.flush()

            m = RE_ENTER.search(line)
            if m:
                data_queue.put(("ENTER", int(m.group(1)), int(m.group(2)), datetime.now()))
                continue
            m = RE_EXIT.search(line)
            if m:
                data_queue.put(("EXIT", int(m.group(1)), int(m.group(2)), datetime.now()))
                continue
            if RE_WARN.search(line):
                data_queue.put(("WARN", 0, 0, datetime.now(), line))
        except Exception:
            pass

    logfile.write(f"=== Session ended {datetime.now()} ===\n")
    logfile.close()
    ser.close()


def demo_thread():
    """Generates fake events so you can see the dashboard without a sensor."""
    import random
    global running
    time.sleep(1.5)
    enter_count = 0
    exit_count  = 0
    print("Demo mode: generating synthetic events...")
    while running:
        delay = random.uniform(1.5, 5.0)
        time.sleep(delay)
        if not running:
            break
        # Bias slightly toward enter when occupancy is low, exit when high
        occ = enter_count - exit_count
        p_enter = 0.7 if occ < 3 else 0.3
        if random.random() < p_enter:
            enter_count += 1
            data_queue.put(("ENTER", enter_count, exit_count, datetime.now()))
        else:
            if occ > 0:
                exit_count += 1
            data_queue.put(("EXIT", enter_count, exit_count, datetime.now()))


# ── Plot setup ────────────────────────────────────────────────────────────────

def build_figure():
    plt.style.use("dark_background")
    fig = plt.figure(figsize=(14, 8), facecolor=C_BG)
    fig.canvas.manager.set_window_title("AMG8833 People Counter — Live Dashboard")

    gs = gridspec.GridSpec(
        3, 3,
        figure=fig,
        hspace=0.45,
        wspace=0.35,
        left=0.07, right=0.97,
        top=0.92,  bottom=0.07,
    )

    ax_occ   = fig.add_subplot(gs[0, :2])   # top-left wide: occupancy over time
    ax_cum   = fig.add_subplot(gs[1, :2])   # mid-left wide: cumulative in/out
    ax_rate  = fig.add_subplot(gs[2, :2])   # bottom-left wide: events per minute
    ax_stats = fig.add_subplot(gs[0, 2])    # top-right: big numbers
    ax_log   = fig.add_subplot(gs[1:, 2])   # right tall: event log

    for ax in (ax_occ, ax_cum, ax_rate, ax_stats, ax_log):
        ax.set_facecolor(C_PANEL)
        for spine in ax.spines.values():
            spine.set_edgecolor(C_GRID)

    fig.suptitle("AMG8833 Door Counter  —  Live Dashboard",
                 color=C_TEXT, fontsize=13, fontweight="bold", y=0.97)

    return fig, ax_occ, ax_cum, ax_rate, ax_stats, ax_log


# ── Per-frame update ──────────────────────────────────────────────────────────

WINDOW_SECS = 120   # x-axis rolling window in seconds

def make_updater(fig, ax_occ, ax_cum, ax_rate, ax_stats, ax_log):
    global total_enter, total_exit, errors

    def update(_frame):
        global total_enter, total_exit, errors

        # Drain all pending events from the queue
        changed = False
        while not data_queue.empty():
            try:
                item = data_queue.get_nowait()
            except queue.Empty:
                break

            kind = item[0]
            if kind in ("ENTER", "EXIT"):
                _, te, tx, ts = item
                total_enter = te
                total_exit  = tx
                occ = te - tx
                timestamps.append(ts)
                occupancy_ts.append(occ)
                enter_ts.append(te)
                exit_ts.append(tx)

                arrow = "➜ ENTER" if kind == "ENTER" else "← EXIT "
                color = C_ENTER if kind == "ENTER" else C_EXIT
                log_str = f"{ts.strftime('%H:%M:%S')}  {arrow}  occ={occ}"
                event_log.append((log_str, color))

                if occ < 0:
                    errors += 1
                    event_log.append(("  ⚠ occupancy negative!", C_WARN))
                changed = True

            elif kind == "WARN":
                _, _, _, ts, msg = item
                event_log.append((f"{ts.strftime('%H:%M:%S')}  !! {msg[:40]}", C_WARN))
                changed = True

        if not changed and timestamps:
            pass  # still redraw to scroll the time axis

        now = time.time()
        occ_now = (total_enter - total_exit)

        # ── Convert timestamps to seconds-ago ──────────────────────────────
        if timestamps:
            t0 = timestamps[0].timestamp()
            xs = [t.timestamp() - t0 for t in timestamps]
            x_now = datetime.now().timestamp() - t0
        else:
            xs = []
            x_now = 0

        # ── 1. Occupancy chart ──────────────────────────────────────────────
        ax_occ.cla()
        ax_occ.set_facecolor(C_PANEL)
        ax_occ.set_title("Occupancy Over Time", color=C_TEXT, fontsize=9, pad=4)
        ax_occ.set_ylabel("People in room", color=C_TEXT, fontsize=8)
        ax_occ.tick_params(colors=C_TEXT, labelsize=7)
        ax_occ.yaxis.label.set_color(C_TEXT)
        ax_occ.grid(True, color=C_GRID, linewidth=0.5)
        for spine in ax_occ.spines.values():
            spine.set_edgecolor(C_GRID)

        if len(xs) >= 2:
            ax_occ.step(xs, list(occupancy_ts), where="post",
                        color=C_OCC, linewidth=2)
            ax_occ.fill_between(xs, list(occupancy_ts), step="post",
                                alpha=0.15, color=C_OCC)
            ax_occ.set_xlim(max(0, x_now - WINDOW_SECS), x_now + 5)
            ax_occ.set_ylim(bottom=0,
                            top=max(max(occupancy_ts) + 2, 5))
        else:
            ax_occ.set_xlim(0, WINDOW_SECS)
            ax_occ.set_ylim(0, 5)
            ax_occ.text(WINDOW_SECS / 2, 2.5, "Waiting for events…",
                        ha="center", va="center", color=C_TEXT, fontsize=9, alpha=0.5)

        ax_occ.set_xlabel("Elapsed seconds", color=C_TEXT, fontsize=8)

        # ── 2. Cumulative chart ─────────────────────────────────────────────
        ax_cum.cla()
        ax_cum.set_facecolor(C_PANEL)
        ax_cum.set_title("Cumulative Entries & Exits", color=C_TEXT, fontsize=9, pad=4)
        ax_cum.set_ylabel("Count", color=C_TEXT, fontsize=8)
        ax_cum.tick_params(colors=C_TEXT, labelsize=7)
        ax_cum.grid(True, color=C_GRID, linewidth=0.5)
        for spine in ax_cum.spines.values():
            spine.set_edgecolor(C_GRID)

        if len(xs) >= 1:
            ax_cum.step(xs, list(enter_ts), where="post",
                        color=C_ENTER, linewidth=2, label="Entries")
            ax_cum.step(xs, list(exit_ts), where="post",
                        color=C_EXIT,  linewidth=2, label="Exits",
                        linestyle="--")
            ax_cum.set_xlim(max(0, x_now - WINDOW_SECS), x_now + 5)
            ax_cum.set_ylim(bottom=0,
                            top=max(total_enter + 2, 5))
            ax_cum.legend(loc="upper left", fontsize=7,
                          facecolor=C_PANEL, edgecolor=C_GRID,
                          labelcolor=C_TEXT)
        else:
            ax_cum.set_xlim(0, WINDOW_SECS)
            ax_cum.set_ylim(0, 5)

        ax_cum.set_xlabel("Elapsed seconds", color=C_TEXT, fontsize=8)

        # ── 3. Events-per-minute bar chart (5-second buckets) ───────────────
        ax_rate.cla()
        ax_rate.set_facecolor(C_PANEL)
        ax_rate.set_title("Event Rate  (5-second buckets)", color=C_TEXT, fontsize=9, pad=4)
        ax_rate.set_ylabel("Events", color=C_TEXT, fontsize=8)
        ax_rate.tick_params(colors=C_TEXT, labelsize=7)
        ax_rate.grid(True, color=C_GRID, linewidth=0.5, axis="y")
        for spine in ax_rate.spines.values():
            spine.set_edgecolor(C_GRID)

        BUCKET = 5   # seconds per bar
        N_BUCKETS = 24  # show last 24 buckets = 2 minutes
        if timestamps:
            t_end = datetime.now().timestamp()
            t_start = t_end - BUCKET * N_BUCKETS
            bucket_enters = [0] * N_BUCKETS
            bucket_exits  = [0] * N_BUCKETS
            for i, t in enumerate(timestamps):
                ts_f = t.timestamp()
                if ts_f < t_start:
                    continue
                b = int((ts_f - t_start) / BUCKET)
                b = min(b, N_BUCKETS - 1)
                # Determine event type from occupancy delta
                if i > 0:
                    delta = list(occupancy_ts)[i] - list(occupancy_ts)[i - 1]
                else:
                    delta = list(occupancy_ts)[i]
                if delta >= 0:
                    bucket_enters[b] += 1
                else:
                    bucket_exits[b] += 1

            bx = list(range(N_BUCKETS))
            ax_rate.bar(bx, bucket_enters, color=C_ENTER, alpha=0.8,
                        label="Enter", width=0.85)
            ax_rate.bar(bx, [-v for v in bucket_exits], color=C_EXIT, alpha=0.8,
                        label="Exit", width=0.85)
            ax_rate.axhline(0, color=C_TEXT, linewidth=0.5)
            ax_rate.set_xticks([0, N_BUCKETS // 2, N_BUCKETS - 1])
            ax_rate.set_xticklabels(
                [f"-{N_BUCKETS * BUCKET}s", f"-{N_BUCKETS * BUCKET // 2}s", "now"],
                color=C_TEXT, fontsize=7)
            ax_rate.legend(loc="upper left", fontsize=7,
                           facecolor=C_PANEL, edgecolor=C_GRID,
                           labelcolor=C_TEXT)
            peak = max(max(bucket_enters), 1)
            ax_rate.set_ylim(-peak - 0.5, peak + 0.5)
        else:
            ax_rate.set_xlim(0, N_BUCKETS)
            ax_rate.set_ylim(-3, 3)
            ax_rate.text(N_BUCKETS / 2, 0, "No data yet",
                         ha="center", va="center", color=C_TEXT, fontsize=9, alpha=0.5)

        # ── 4. Stats panel ──────────────────────────────────────────────────
        ax_stats.cla()
        ax_stats.set_facecolor(C_PANEL)
        ax_stats.set_xlim(0, 1)
        ax_stats.set_ylim(0, 1)
        ax_stats.axis("off")
        for spine in ax_stats.spines.values():
            spine.set_edgecolor(C_GRID)

        occ_color = C_OCC if occ_now >= 0 else C_WARN
        ax_stats.text(0.5, 0.92, "OCCUPANCY", ha="center", va="top",
                      color=C_TEXT, fontsize=9, fontweight="bold", transform=ax_stats.transAxes)
        ax_stats.text(0.5, 0.68, str(max(occ_now, 0)), ha="center", va="top",
                      color=occ_color, fontsize=52, fontweight="bold",
                      transform=ax_stats.transAxes)
        ax_stats.text(0.5, 0.44, f"IN   {total_enter:4d}", ha="center", va="top",
                      color=C_ENTER, fontsize=12, fontfamily="monospace",
                      transform=ax_stats.transAxes)
        ax_stats.text(0.5, 0.32, f"OUT  {total_exit:4d}", ha="center", va="top",
                      color=C_EXIT,  fontsize=12, fontfamily="monospace",
                      transform=ax_stats.transAxes)

        warn_str = f"⚠ {errors} warning{'s' if errors != 1 else ''}" if errors else "✓ No warnings"
        warn_col = C_WARN if errors else C_ENTER
        ax_stats.text(0.5, 0.12, warn_str, ha="center", va="top",
                      color=warn_col, fontsize=8, transform=ax_stats.transAxes)

        # ── 5. Event log ────────────────────────────────────────────────────
        ax_log.cla()
        ax_log.set_facecolor(C_PANEL)
        ax_log.set_xlim(0, 1)
        ax_log.set_ylim(0, 1)
        ax_log.axis("off")
        ax_log.set_title("Event Log", color=C_TEXT, fontsize=9, pad=4)
        for spine in ax_log.spines.values():
            spine.set_edgecolor(C_GRID)

        log_list = list(event_log)
        n = len(log_list)
        for i, (msg, col) in enumerate(log_list):
            y = 0.97 - i * (0.97 / max(event_log.maxlen or 12, 1))
            ax_log.text(0.04, y, msg, ha="left", va="top",
                        color=col, fontsize=7, fontfamily="monospace",
                        transform=ax_log.transAxes,
                        clip_on=True)

        fig.canvas.draw_idle()

    return update


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AMG8833 People Counter — Live Dashboard")
    parser.add_argument("--port",    default="/dev/ttyUSB0", help="Serial port")
    parser.add_argument("--baud",    default=115200, type=int)
    parser.add_argument("--log",     default="counter_log.txt", help="Log file path")
    parser.add_argument("--demo",    action="store_true",
                        help="Run with synthetic data (no sensor needed)")
    parser.add_argument("--interval", default=500, type=int,
                        help="Plot refresh interval in ms (default 500)")
    args = parser.parse_args()

    global running

    # Start data source thread
    if args.demo:
        t = threading.Thread(target=demo_thread, daemon=True)
    else:
        t = threading.Thread(target=serial_thread,
                             args=(args.port, args.baud, args.log),
                             daemon=True)
    t.start()

    # Build and animate the figure
    fig, ax_occ, ax_cum, ax_rate, ax_stats, ax_log = build_figure()
    updater = make_updater(fig, ax_occ, ax_cum, ax_rate, ax_stats, ax_log)

    ani = FuncAnimation(fig, updater, interval=args.interval, cache_frame_data=False)

    def on_close(_event):
        global running
        running = False

    fig.canvas.mpl_connect("close_event", on_close)

    try:
        plt.show()
    except KeyboardInterrupt:
        pass
    finally:
        running = False

    # Session summary
    print("\n" + "─" * 60)
    print("Session summary:")
    print(f"  Total ENTER    : {total_enter}")
    print(f"  Total EXIT     : {total_exit}")
    print(f"  Final occupancy: {total_enter - total_exit}")
    print(f"  Warnings       : {errors}")
    if errors == 0:
        print("  ✓ No sanity errors!")
    else:
        print("  ✗ Check INVERT_DIRECTION or TEMP_THRESHOLD_ABOVE_BG in the .ino")


if __name__ == "__main__":
    main()
