#!/usr/bin/env python3
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
