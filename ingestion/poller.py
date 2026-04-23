import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from config import LINE_ID, CORRIDOR_BBOX, POLL_INTERVAL_SECONDS, KAFKA_TOPIC, STB_API_BASE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
STB_API_KEY = os.getenv("STB_API_KEY", "")


def fetch_vehicles() -> list[dict]:
    url = f"{STB_API_BASE}/lines/{LINE_ID}/vehicles"
    headers = {"Authorization": f"Bearer {STB_API_KEY}"} if STB_API_KEY else {}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json().get("vehicles", [])


def in_corridor(lat: float, lon: float) -> bool:
    bb = CORRIDOR_BBOX
    return bb["lat_min"] <= lat <= bb["lat_max"] and bb["lon_min"] <= lon <= bb["lon_max"]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=5,
    )
    log.info("Poller started — publishing to %s on %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP)

    while True:
        try:
            vehicles = fetch_vehicles()
            ingested_at = datetime.now(timezone.utc).isoformat()
            published = 0
            for v in vehicles:
                lat = float(v.get("latitude", 0))
                lon = float(v.get("longitude", 0))
                if not in_corridor(lat, lon):
                    continue
                record = {
                    "vehicle_id":      str(v.get("id", "")),
                    "license_plate":   v.get("license_plate", ""),
                    "latitude":        lat,
                    "longitude":       lon,
                    "line_id":         LINE_ID,
                    "direction_id":    v.get("direction_id", 0),
                    "passenger_count": v.get("passenger_count", 0),
                    "ingested_at":     ingested_at,
                }
                producer.send(KAFKA_TOPIC, value=record)
                published += 1
            producer.flush()
            log.info("Published %d vehicle(s) in corridor", published)
        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
