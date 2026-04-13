#!/usr/bin/env python3
"""
AMG8833 People Counter — Central Server
=========================================
Receives HTTP POST events from all ESP32 sensors and stores them in SQLite.

Usage:
    pip install flask
    python server.py

The server listens on  http://0.0.0.0:5000
Set SERVER_IP in the Arduino sketch to this machine's local IP address.

Database file:  occupancy.db  (created automatically next to this script)
"""

import sqlite3
import os
from datetime import datetime, timezone
from flask import Flask, request, jsonify

# ── Config ────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "occupancy.db")
PORT    = 5000

app = Flask(__name__)

# ── Database setup ────────────────────────────────────────────

def get_db():
    """Open a database connection for the current request."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row   # rows behave like dicts
    return conn


def init_db():
    """Create tables if they don't exist yet."""
    with get_db() as conn:
        conn.executescript("""
            -- Every individual enter/exit event from every sensor
            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,           -- ISO-8601 UTC
                room        TEXT    NOT NULL,           -- e.g. "living_room"
                direction   TEXT    NOT NULL,           -- "enter" or "exit"
                total_in    INTEGER NOT NULL DEFAULT 0, -- running total from that sensor
                total_out   INTEGER NOT NULL DEFAULT 0,
                occupancy   INTEGER NOT NULL DEFAULT 0
            );

            -- Latest known state per room (upserted on every event)
            CREATE TABLE IF NOT EXISTS room_state (
                room        TEXT    PRIMARY KEY,
                occupancy   INTEGER NOT NULL DEFAULT 0,
                total_in    INTEGER NOT NULL DEFAULT 0,
                total_out   INTEGER NOT NULL DEFAULT 0,
                last_update TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_room      ON events(room);
            CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
        """)
    print(f"Database ready: {DB_PATH}")


# ── Routes ────────────────────────────────────────────────────

@app.route("/event", methods=["POST"])
def receive_event():
    """
    Expected JSON body:
        {
          "room":      "living_room",
          "direction": "enter",
          "total_in":  5,
          "total_out": 3,
          "occupancy": 2
        }
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid or missing JSON"}), 400

    room      = data.get("room",      "").strip()
    direction = data.get("direction", "").strip().lower()
    total_in  = int(data.get("total_in",  0))
    total_out = int(data.get("total_out", 0))
    occupancy = int(data.get("occupancy", total_in - total_out))

    if not room:
        return jsonify({"error": "Missing 'room' field"}), 400
    if direction not in ("enter", "exit"):
        return jsonify({"error": "direction must be 'enter' or 'exit'"}), 400

    ts = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        # Insert the raw event
        conn.execute("""
            INSERT INTO events (timestamp, room, direction, total_in, total_out, occupancy)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ts, room, direction, total_in, total_out, occupancy))

        # Upsert the room summary (INSERT or UPDATE)
        conn.execute("""
            INSERT INTO room_state (room, occupancy, total_in, total_out, last_update)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(room) DO UPDATE SET
                occupancy   = excluded.occupancy,
                total_in    = excluded.total_in,
                total_out   = excluded.total_out,
                last_update = excluded.last_update
        """, (room, occupancy, total_in, total_out, ts))

    print(f"[{ts}]  {room:20s}  {direction:5s}  "
          f"in={total_in}  out={total_out}  occupancy={occupancy}")

    return jsonify({"status": "ok", "timestamp": ts}), 201


@app.route("/status", methods=["GET"])
def status():
    """Returns current occupancy for all rooms."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT room, occupancy, total_in, total_out, last_update "
            "FROM room_state ORDER BY room"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/history", methods=["GET"])
def history():
    """
    Returns recent events.
    Optional query params:
        ?room=living_room    filter by room
        ?limit=100           max rows (default 100)
    """
    room  = request.args.get("room",  None)
    limit = int(request.args.get("limit", 100))

    with get_db() as conn:
        if room:
            rows = conn.execute(
                "SELECT * FROM events WHERE room=? ORDER BY id DESC LIMIT ?",
                (room, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/reset/<room>", methods=["POST"])
def reset_room(room):
    """
    Reset the counters for one room back to zero.
    Useful if the ESP32 rebooted and its local counts reset.
    POST /reset/living_room
    """
    ts = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute("""
            INSERT INTO room_state (room, occupancy, total_in, total_out, last_update)
            VALUES (?, 0, 0, 0, ?)
            ON CONFLICT(room) DO UPDATE SET
                occupancy=0, total_in=0, total_out=0, last_update=excluded.last_update
        """, (room, ts))
    return jsonify({"status": "reset", "room": room})


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    print(f"\nServer running on http://0.0.0.0:{PORT}")
    print("Endpoints:")
    print(f"  POST http://<your-ip>:{PORT}/event          <- ESP32 posts here")
    print(f"  GET  http://<your-ip>:{PORT}/status         <- current occupancy all rooms")
    print(f"  GET  http://<your-ip>:{PORT}/history        <- recent events")
    print(f"  GET  http://<your-ip>:{PORT}/history?room=X <- events for one room")
    print(f"  POST http://<your-ip>:{PORT}/reset/<room>   <- reset a room's counters\n")
    app.run(host="0.0.0.0", port=PORT, debug=False)
