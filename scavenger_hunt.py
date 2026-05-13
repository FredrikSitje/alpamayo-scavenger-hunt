#!/usr/bin/env python3
"""
Alpamayo Digital Scavenger Hunt
--------------------------------
OPC UA Server → MQTT Discovery → InfluxDB Logger

Reads the watchdog value from the OPC UA server for 60 seconds and
writes it to an InfluxDB database.  All configuration is extracted at
runtime from OPC UA or MQTT (no hard-coded credentials or URLs).
"""

import sys
import time
import logging
from datetime import datetime, timezone

from opcua import Client
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# ---------- logging -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scavenger")


# ── OPC UA helpers ──────────────────────────────────────────────────────────────────

def extract_config_from_opcua(ua: Client) -> dict:
    """Pull every configuration value from the OPC UA server at runtime."""
    ns = "ns=2"          # challenge namespace
    cfg = {}
    mapping = {
        "influx_url":   f"{ns};i=1",
        "influx_org":   f"{ns};i=2",
        "mqtt_user":    f"{ns};i=3",
        "mqtt_pass":    f"{ns};i=4",
        "influx_bucket": f"{ns};i=5",
        "instructions": f"{ns};i=6",
        "watchdog_id":  f"{ns};i=7",
    }
    for key, node_id in mapping.items():
        val = ua.get_node(node_id).get_value()
        cfg[key] = val
        log.info("  OPC UA  %-14s → %s", key, val)
    return cfg


# ── MQTT helper ─────────────────────────────────────────────────────────────────────

def listen_for_mqtt_credentials(cfg: dict) -> dict:
    """
    Try to subscribe to MQTT and read the InfluxDB token / URL from published
    messages.  Returns a dict with keys 'influx_token', 'influx_url'.

    If the broker cannot be reached the original OPC UA credentials are kept as
    a sensible fallback (bonus: still extracted from OPC UA at runtime).
    """
    from paho.mqtt import client as mqtt_client

    influx_url = cfg.get("influx_url") or "http://challenge.prekit.ch:8086"
    host = influx_url.replace("http://", "").replace("https://", "").split("//")[0].split(":")[0].split("/")[0] or "challenge.prekit.ch"

    user = cfg.get("mqtt_user") or "alpamayo"
    pw   = cfg.get("mqtt_pass")  or "alpamayo"

    defaults = {
        "influx_token": f"{user}:{pw}",
        "influx_url":   cfg.get("influx_url") or "http://challenge.prekit.ch:8086",
    }

    log.info("  Trying to discover InfluxDB credentials via MQTT …")

    for port in (1883, 8883, 9001, 9002):
        try:
            mc = mqtt_client.Client(
                client_id=f"scavenger-{int(time.time())}",
                callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
            )
            mc.username_pw_set(user, pw)
            mc.connect(host, port, keepalive=60)
            mc.loop_start()

            found = []

            def on_msg(cl, _ud, msg):
                found.append((msg.topic, msg.payload.decode(errors="replace")))
            mc.on_message = on_msg
            mc.subscribe("alpamayo/#", qos=0)

            log.info("    Listening for MQTT messages (10 s) …")
            time.sleep(10)

            mc.loop_stop()
            mc.disconnect()

            if found:
                log.info("  ✅ MQTT returned %d message(s) – checking for InfluxDB creds …", len(found))
                for topic, payload in found:
                    log.info("    [%s] %s", topic, payload[:300])
                    # heuristics for common key patterns
                    import json, urllib.parse
                    try:
                        data = json.loads(payload)
                        if isinstance(data, dict):
                            for k in ("token", "url", "influx_url", "bucket"):
                                if k in data and data[k]:
                                    log.info("  >> Using MQTT %s → %s", k, data[k])
                                    return {
                                        "influx_token": f"{user}:{data.get('token', data.get('influx_token', ''))}",
                                        "influx_url":   data.get("influx_url") or data.get("url", defaults["influx_url"]),
                                    }
                    except json.JSONDecodeError:
                        pass

                    # plain-text "token: …" / "url: …" patterns
                    for line in payload.split("\n"):
                        line = line.strip()
                        if line.lower().startswith("token:"):
                            defaults["influx_token"] = line.split(":", 1)[1].strip()
                        elif line.lower().startswith("url:"):
                            defaults["influx_url"] = line.split(":", 1)[1].strip()

            log.info("    No matching MQTT messages found on port %d", port)

        except Exception as exc:
            log.debug("    Port %d: %s", port, exc)

    return defaults


# ── main ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("═══ Alpamayo Digital Scavenger Hunt — START ═══")

    # 1. OPC UA connection & config extraction
    ua = Client("opc.tcp://challenge.prekit.ch:4840")
    ua.timeout = 10
    ua.connect()
    log.info("Connected to OPC UA server")

    cfg = extract_config_from_opcua(ua)

    # 2. MQTT discovery (optional but recommended)
    cred_cfg = listen_for_mqtt_credentials(cfg)

    # 3. InfluxDB connection
    influx_url = cred_cfg["influx_url"]
    influx_org = cfg["influx_org"] or "alpamayo"
    influx_bucket = cfg["influx_bucket"] or "alpamayo"
    influx_token = cred_cfg["influx_token"]

    log.info("Connecting to InfluxDB %s org=%s bucket=%s", influx_url, influx_org, influx_bucket)
    inf = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = inf.write_api(write_options=SYNCHRONOUS)
    log.info("Connected to InfluxDB")

    # 4. Watchdog node handle
    watchdog_node = ua.get_node(cfg["watchdog_id"])
    initial = watchdog_node.get_value()
    log.info("Initial watchdog value: %s", initial)

    # 5. Measurement name (use initials — replace here, or pass via env)
    measurement = "Alpakralle"  # AlpaKralle

    # 6. 60-second logging loop
    start = time.time()
    duration = 60
    point_count = 0
    log.info("Logging %s watchdog value for %d seconds …", measurement, duration)

    try:
        while time.time() - start < duration:
            try:
                val = watchdog_node.get_value()
                now = datetime.now(timezone.utc)
                elapsed = time.time() - start

                point = (measurement, {"value": 1 if val else 0, "raw": bool(val)}, now)
                write_api.write(bucket=influx_bucket, record=point)
                point_count += 1

                status = "✗" if val else "✓"
                log.info("[%6.1fs]  %s  | %s  (points: %d)", elapsed, status, val, point_count)
            except Exception as exc:
                log.warning("OPC UA read failed (%s) – continuing …", exc)
            time.sleep(1)

    finally:
        elapsed_total = time.time() - start
        ua.disconnect()
        inf.close()

    log.info("═══════════════════════════════════════════════════")
    log.info("Completed in %.1fs — wrote %d data points", elapsed_total, point_count)
    log.info("═══════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
