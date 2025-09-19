#!/usr/bin/env python3
"""
Generate a simple 24-hour IIoT dataset + an MQTT replayer script in the current folder.

Outputs:
  - simple_day.csv
  - simple_mqtt_replayer.py

Defaults:
  - Asia/Kolkata timezone (UTC+05:30)
  - 24 hours of data for 2025-09-17
  - One machine: symbiotic/blr/line1/SF-01
  - Sampling every 10 seconds
  - Minimal signals: state, counters, run_minutes_today, motor_current_a
"""

import argparse
import csv
import json
import os
from datetime import datetime, timedelta, timezone
import random

def build_args():
    p = argparse.ArgumentParser(description="Generate simple IIoT dataset + replayer.")
    p.add_argument("--site", default="blr")
    p.add_argument("--line", default="line1")
    p.add_argument("--machine", default="SF-01")
    p.add_argument("--date", default="2025-09-17", help="YYYY-MM-DD (local Asia/Kolkata)")
    p.add_argument("--interval-sec", type=int, default=10, help="Sampling interval (seconds)")
    p.add_argument("--ideal-ct-s", type=float, default=12.0, help="Ideal cycle time per part (seconds)")
    p.add_argument("--reject-rate", type=float, default=0.02, help="Reject probability (0..1)")
    p.add_argument("--outfile", default="simple_day.csv")
    p.add_argument("--replayer", default="simple_mqtt_replayer.py")
    return p.parse_args()

def frand(mu=0.0, sigma=1.0):
    # simple normal-ish noise using random.gauss
    return random.gauss(mu, sigma)

def main():
    args = build_args()
    # Fixed IST timezone
    tz = timezone(timedelta(hours=5, minutes=30))

    # Parse date and build 24h window
    y, m, d = map(int, args.date.split("-"))
    t0 = datetime(y, m, d, 0, 0, 0, tzinfo=tz)
    t1 = datetime(y, m, d, 23, 59, 50, tzinfo=tz)  # ensure multiple of 10s fits nicely

    site = args.site
    line = args.line
    machine = args.machine
    interval = args.interval_sec
    ideal_ct_s = float(args.ideal_ct_s)
    reject_rate = float(args.reject_rate)

    # Topic root
    root = f"symbiotic/{site}/{line}/{machine}"
    def topic(object_, metric):
        return f"{root}/{object_}/{metric}"

    # Build timestamps
    ts_list = []
    t = t0
    while t <= t1:
        ts_list.append(t)
        t = t + timedelta(seconds=interval)

    n = len(ts_list)

    # State codes: 0 STOPPED, 2 RUN, 3 IDLE, 4 FAULT (rare)
    # Day profile:
    # 00:00–06:00 mostly STOPPED, 06:00–22:00 mostly RUN w/ small IDLE, 22:00–24:00 mixed
    random.seed(7)
    state_code = []
    for t in ts_list:
        mm = t.hour * 60 + t.minute
        if 0 <= mm < 360:           # 00:00-06:00
            s = 0 if random.random() < 0.9 else 3
        elif 360 <= mm < 1320:      # 06:00-22:00
            s = 2 if random.random() < 0.9 else 3
        else:                        # 22:00-24:00
            s = 2 if random.random() < 0.5 else 3
        state_code.append(s)

    # Planned idle windows (IST)
    planned_windows = [(13*60, 13*60+30), (20*60, 20*60+15)]  # 13:00-13:30, 20:00-20:15
    for i, t in enumerate(ts_list):
        mm = t.hour * 60 + t.minute
        for (a, b) in planned_windows:
            if a <= mm < b:
                state_code[i] = 3

    # Short rare faults
    rare_faults = [(10*60+15, 3), (19*60+40, 4)]  # (start_min, dur_min)
    for i, t in enumerate(ts_list):
        mm = t.hour * 60 + t.minute
        for (start_m, dur_m) in rare_faults:
            if start_m <= mm < start_m + dur_m:
                state_code[i] = 4

    running = [1 if s == 2 else 0 for s in state_code]

    # Counters: simple part production while running; one part per ideal_ct_s
    parts_total = []
    parts_good  = []
    run_minutes_today = []

    pt = 0
    pg = 0
    run_sec_accum = 0.0
    accum_cycle = 0.0

    for i in range(n):
        if running[i]:
            run_sec_accum += interval
            accum_cycle += interval
            while accum_cycle >= ideal_ct_s:
                accum_cycle -= ideal_ct_s
                pt += 1
                if random.random() >= reject_rate:
                    pg += 1
        # if not running: no increment
        parts_total.append(pt)
        parts_good.append(pg)
        run_minutes_today.append(run_sec_accum / 60.0)

    # Simple motor current
    motor_current = []
    for i in range(n):
        s = state_code[i]
        if s == 2:        # RUN
            motor_current.append(10.0 + frand(0, 0.6))
        elif s == 3:      # IDLE
            motor_current.append(1.2 + frand(0, 0.2))
        elif s == 4:      # FAULT
            motor_current.append(0.3 + frand(0, 0.1))
        else:             # STOPPED
            motor_current.append(0.2 + frand(0, 0.1))

    # Write CSV (one MQTT message per row)
    outfile = os.path.abspath(args.outfile)
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["ts_iso","topic","payload_json","site","line","machine","object","metric"]
        )
        w.writeheader()

        def add_row(t, object_, metric, value, unit=None):
            payload = {"ts": t.isoformat(), "value": value, "q": "good"}
            if unit is not None:
                payload["unit"] = unit
            w.writerow({
                "ts_iso": t.isoformat(),
                "topic": topic(object_, metric),
                "payload_json": json.dumps(payload, separators=(",",":")),
                "site": site, "line": line, "machine": machine,
                "object": object_, "metric": metric
            })

        # One-time config at start
        add_row(ts_list[0], "config", "ideal_ct_s", ideal_ct_s, "s")

        # Time-series rows
        for i, t in enumerate(ts_list):
            add_row(t, "state",   "state_code", int(state_code[i]))
            add_row(t, "state",   "running", bool(running[i]))
            add_row(t, "counter", "parts_total", int(parts_total[i]), "count")
            add_row(t, "counter", "parts_good",  int(parts_good[i]),  "count")
            add_row(t, "kpi",     "run_minutes_today", float(run_minutes_today[i]), "min")
            add_row(t, "sensor",  "motor_current_a", float(motor_current[i]), "A")

    # Create replayer script
    replayer_path = os.path.abspath(args.replayer)
    replayer_code = f'''#!/usr/bin/env python3
import argparse, csv, json, time, ssl
from datetime import datetime
import paho.mqtt.client as mqtt

def main():
    ap = argparse.ArgumentParser(description="Simple MQTT CSV replayer")
    ap.add_argument("--csv", required=True, help="Path to CSV (ts_iso,topic,payload_json)")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=8883)
    ap.add_argument("--username")
    ap.add_argument("--password")
    ap.add_argument("--client-id", default="simple-replayer")
    ap.add_argument("--qos", type=int, default=0)
    ap.add_argument("--retain", action="store_true")
    ap.add_argument("--speed", type=float, default=1.0, help="Time accel (1.0=real-time)")
    ap.add_argument("--loop", action="store_true")
    args = ap.parse_args()

    client = mqtt.Client(client_id=args.client_id, protocol=mqtt.MQTTv311)
    if args.username:
        client.username_pw_set(args.username, args.password)

    if args.port == 8883:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(False)

    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    def publish_once():
        with open(args.csv, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            prev = None
            for row in r:
                topic = row["topic"]
                payload = row["payload_json"]
                try:
                    t = datetime.fromisoformat(row["ts_iso"])
                except Exception:
                    t = None
                if prev and t:
                    dt = (t - prev).total_seconds()
                    if dt > 0:
                        time.sleep(dt / max(args.speed, 0.001))
                prev = t
                client.publish(topic, payload, qos=args.qos, retain=args.retain).wait_for_publish()

    try:
        if args.loop:
            while True:
                publish_once()
        else:
            publish_once()
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
'''
    with open(replayer_path, "w", encoding="utf-8") as f:
        f.write(replayer_code)

    # Make scripts executable on Unix
    try:
        os.chmod(replayer_path, 0o755)
    except Exception:
        pass

    print("Generated files:")
    print(" -", outfile)
    print(" -", replayer_path)
    print("\nNext steps:")
    print("  pip3 install paho-mqtt")
    print("  python3", os.path.basename(replayer_path),
          "--csv", os.path.basename(outfile),
          "--host 71d9da11243841fead13bdff5af31f1e.s1.eu.hivemq.cloud",
          "--port 8883",
          "--username symbiotic --password '123abC$$' --speed 1")

if __name__ == "__main__":
    main()
