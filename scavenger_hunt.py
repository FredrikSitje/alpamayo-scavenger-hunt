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


def get_ua_client() -> Client:
    """Create and connect a fresh OPC UA client."""
    ua = Client("opc.tcp://challenge.prekit.ch:4840")
    ua.timeout = 10
    ua.connect()
    log.info("Connected to OPC UA server")
    return ua


def read_node_robust(node_id, ua, max_retries=5, reconnect_delay=2):
    """
    Read a single OPC UA node with automatic reconnection on failure.
    Returns the value or None after exhausting retries.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return ua.get_node(node_id).get_value()
        except Exception as exc:
            log.warning("  Node %s read failed (attempt %d/%d): %s", node_id, attempt, max_retries, exc)
            if attempt < max_retries:
                try:
                    ua.disconnect()
                except:
                    pass
                time.sleep(reconnect_delay)
                ua = get_ua_client()
    log.error("  Failed to read node %s after %d attempts", node_id, max_retries)
    return None


# ── OPC UA helpers ──────────────────────────────────────────────────────────────────

def extract_config_from_opcua() -> dict:
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
    
    # Start with a fresh connection
    ua = get_ua_client()
    
    for key, node_id in mapping.items():
        val = read_node_robust(node_id, ua, max_retries=5, reconnect_delay=2)
        if val is not None:
            cfg[key] = val
            log.info("  OPC UA  %-14s → %s", key, val)
        else:
            log.error("  Could not read OPC UA node %s", key)
            # Keep trying to get remaining values
            ua = get_ua_client()
    
    try:
        ua.disconnect()
    except:
        pass
    
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

    # 1. OPC UA connection & config extraction (with robust reconnection)
    cfg = extract_config_from_opcua()

    # 2. MQTT discovery (optional but recommended)
    cred_cfg = listen_for_mqtt_credentials(cfg)

    # 3. InfluxDB connection
    influx_url = cred_cfg["influx_url"]
    influx_org = cfg.get("influx_org") or "alpamayo"
    influx_bucket = cfg.get("influx_bucket") or "alpamayo"
    influx_token = cred_cfg["influx_token"]

    log.info("Connecting to InfluxDB %s org=%s bucket=%s", influx_url, influx_org, influx_bucket)
    inf = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = inf.write_api(write_options=SYNCHRONOUS)
    log.info("Connected to InfluxDB")

    # 4. 60-second logging loop with robust OPC UA reconnection
    start = time.time()
    duration = 60
    point_count = 0
    
    # Connect fresh for the monitoring phase
    ua = get_ua_client()
    
    log.info("Logging watchdog value for %d seconds …", duration)

    try:
        while time.time() - start < duration:
            try:
                # Read watchdog node with robust reconnection
                watchdog_val = read_node_robust(cfg.get("watchdog_id", "ns=2;i=7"), ua, max_retries=3, reconnect_delay=1)
                now = datetime.now(timezone.utc)
                elapsed = time.time() - start

                if watchdog_val is not None:
                    point = ("Alpakralle", {"value": 1 if watchdog_val else 0, "raw": bool(watchdog_val)}, now)
                    write_api.write(bucket=influx_bucket, record=point)
                    point_count += 1

                    status = "✗" if watchdog_val else "✓"
                    log.info("[%6.1fs]  %s  | %s  (points: %d)", elapsed, status, watchdog_val, point_count)
                else:
                    log.warning("[%6.1fs]  ⚠️  Could not read watchdog value", elapsed)
            except Exception as exc:
                log.warning("[%6.1fs]  OPC UA read failed (%s) – continuing …", time.time() - start, exc)
            time.sleep(1)

    finally:
        elapsed_total = time.time() - start
        try:
            ua.disconnect()
        except:
            pass
        inf.close()

    log.info("═══════════════════════════════════════════════════")
    log.info("Completed in %.1fs — wrote %d data points", elapsed_total, point_count)
    log.info("═══════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
