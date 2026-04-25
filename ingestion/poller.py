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
from stops_381 import nearest_stop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Referer": "https://maps.mo-bi.ro/",
    "Origin": "https://maps.mo-bi.ro",
}


def fetch_vehicles() -> list[dict]:
    resp = requests.get(MOBI_BUS_DATA_URL, headers=_HEADERS, timeout=15)
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

            route_buses = [e for e in raw if e.get("vehicle", {}).get("trip", {}).get("routeId") == ROUTE_ID]
            log.info("Route %s active buses: %d", LINE_ID, len(route_buses))
            for entry in route_buses:
                veh  = entry.get("vehicle", {})
                trip = veh.get("trip", {})
                pos  = veh.get("position", {})
                lat  = float(pos.get("latitude", 0))
                lon  = float(pos.get("longitude", 0))
                _, _, sname = nearest_stop(lat, lon)
                plate = veh.get("vehicle", {}).get("licensePlate", "?")
                log.info("  [dir%s] %s  near %s", trip.get("directionId", "?"), plate, sname)

            for entry in route_buses:
                veh  = entry.get("vehicle", {})
                trip = veh.get("trip", {})

                if trip.get("directionId") != TARGET_DIRECTION:
                    continue

                pos  = veh.get("position", {})
                lat  = float(pos.get("latitude", 0))
                lon  = float(pos.get("longitude", 0))
                if not in_corridor(lat, lon):
                    continue

                inner_veh = veh.get("vehicle", {})
                source_ts = veh.get("timestamp")
                event_time = (
                    datetime.fromtimestamp(source_ts, tz=timezone.utc).isoformat()
                    if source_ts else ingested_at
                )

                stop_seq, stop_id, stop_name = nearest_stop(lat, lon)

                record = {
                    "vehicle_id":      str(inner_veh.get("id", "")),
                    "license_plate":   inner_veh.get("licensePlate", ""),
                    "latitude":        lat,
                    "longitude":       lon,
                    "line_id":         LINE_ID,
                    "direction_id":    int(trip.get("directionId", 0)),
                    "passenger_count": None,
                    "nearest_stop_seq":  stop_seq,
                    "nearest_stop_id":   stop_id,
                    "nearest_stop_name": stop_name,
                    "ingested_at":     ingested_at,
                    "event_time":      event_time,
                }
                producer.send(KAFKA_TOPIC, value=record)
                log.info("  %s  near %-35s  (seq %d)  %.5f,%.5f",
                         inner_veh.get("licensePlate", "?"), stop_name, stop_seq, lat, lon)
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
