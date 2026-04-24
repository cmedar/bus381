import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from config import (
    LINE_ID, ROUTE_ID, TARGET_DIRECTION,
    CORRIDOR_BBOX, POLL_INTERVAL_SECONDS,
    KAFKA_TOPIC, MOBI_BUS_DATA_URL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def fetch_vehicles() -> list[dict]:
    resp = requests.get(MOBI_BUS_DATA_URL, timeout=15)
    resp.raise_for_status()
    return resp.json()


def in_corridor(lat: float, lon: float) -> bool:
    bb = CORRIDOR_BBOX
    return bb["lat_min"] <= lat <= bb["lat_max"] and bb["lon_min"] <= lon <= bb["lon_max"]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=5,
    )
    log.info("Poller started — route %s (id=%s) → topic=%s on %s",
             LINE_ID, ROUTE_ID, KAFKA_TOPIC, KAFKA_BOOTSTRAP)

    while True:
        try:
            raw = fetch_vehicles()
            ingested_at = datetime.now(timezone.utc).isoformat()
            published = 0

            for entry in raw:
                veh  = entry.get("vehicle", {})
                trip = veh.get("trip", {})

                if trip.get("routeId") != ROUTE_ID:
                    continue
                if trip.get("directionId") != TARGET_DIRECTION:
                    continue

                pos = veh.get("position", {})
                lat = float(pos.get("latitude", 0))
                lon = float(pos.get("longitude", 0))
                if not in_corridor(lat, lon):
                    continue

                inner_veh = veh.get("vehicle", {})
                source_ts = veh.get("timestamp")
                event_time = (
                    datetime.fromtimestamp(source_ts, tz=timezone.utc).isoformat()
                    if source_ts else ingested_at
                )

                record = {
                    "vehicle_id":    str(inner_veh.get("id", "")),
                    "license_plate": inner_veh.get("licensePlate", ""),
                    "latitude":      lat,
                    "longitude":     lon,
                    "line_id":       LINE_ID,
                    "direction_id":  int(trip.get("directionId", 0)),
                    "passenger_count": None,
                    "ingested_at":   ingested_at,
                    "event_time":    event_time,
                }
                producer.send(KAFKA_TOPIC, value=record)
                published += 1

            producer.flush()
            log.info("Published %d vehicle(s) in corridor", published)

        except requests.HTTPError as e:
            log.error("HTTP error from mo-bi.ro (may be rate-limited): %s", e)
        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
