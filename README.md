# Alpamayo Digital Scavenger Hunt

## Challenge

Solve Till's digital scavenger hunt and claim your interview with us.

### Steps completed

1. ✅ **Port 4840 auf `challenge.prekit.ch` geöffnet** – OPC UA Server gefunden
2. ✅ **MQTT Discovery** – InfluxDB Credentials automatisch extrahiert
3. ✅ **OPC UA Configuration** – Alle Config-Werte (`INFLUX_URL`, `INFLUX_ORG`, `INFLUX_BUCKET`, `MQTT_USER`, `MQTT_PASS`, `watchdog_id`) wurden zur Laufzeit vom OPC UA Server gelesen (keine hardcoded Werte)
4. ✅ **60-Sekunden-Logger** – Python-Script liest den `watchdog` Node (60 Sek.) und schreibt in die InfluxDB
5. ✅ **Docker-Image** – Containerized für reproduzierbare Ausführung
6. ✅ **GitHub Actions** – Workflow läuft mit den geloggten Daten durch

### Starten

```bash
# Lokal
python scavenger_hunt.py

# Docker
docker build -t alpamayo-scavenger .
docker run alpamayo-scavenger
```

### Messungsname

Ändere `TP` in `scavenger_hunt.py` Zeile mit deinem Messungs-Namen (Initialen).

### Credits

Erster Clue in der Browser Console versteckt 🌀
