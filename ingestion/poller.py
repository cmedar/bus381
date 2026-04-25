import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from config import (
    LINE_ID, ROUTE_ID,
    POLL_INTERVAL_SECONDS, KAFKA_TOPIC,
    MOBI_NEXT_ARR_URL, STOP_PIATA_ROMANA,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Referer": "https://maps.mo-bi.ro/",
    "Origin": "https://maps.mo-bi.ro",
}


def fetch_next_arrivals() -> dict:
    url = f"{MOBI_NEXT_ARR_URL}/{STOP_PIATA_ROMANA}?_t={int(time.time())}"
    resp = requests.get(url, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()


def find_381(data: dict) -> dict | None:
    for line in data.get("lines", []):
        if line.get("id") == ROUTE_ID:
            return line
    return None


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=5,
    )
    log.info("Poller started — next arrivals for route %s at stop %s (Piata Romana)",
             LINE_ID, STOP_PIATA_ROMANA)

    while True:
        try:
            data = fetch_next_arrivals()
            ingested_at = datetime.now(timezone.utc).isoformat()

            line = find_381(data)
            if line:
                arriving_s   = int(line.get("arrivingTime", 0))
                is_timetable = line.get("isTimetable", True)
                direction    = line.get("directionName", "")
                source       = "timetable" if is_timetable else "live"
                mins, secs   = divmod(arriving_s, 60)

                log.info("Next 381 at Piata Romana: %dm%02ds (%s) → %s",
                         mins, secs, source, direction)

                record = {
                    "stop_id":             str(STOP_PIATA_ROMANA),
                    "stop_name":           data.get("name", "Piata Romana"),
                    "line_id":             LINE_ID,
                    "route_id":            ROUTE_ID,
                    "arriving_in_seconds": arriving_s,
                    "is_timetable":        is_timetable,
                    "direction_name":      direction,
                    "ingested_at":         ingested_at,
                }
                producer.send(KAFKA_TOPIC, value=record)
                producer.flush()
            else:
                log.info("No 381 arrival data at Piata Romana")

        except requests.HTTPError as e:
            log.error("HTTP error from mo-bi.ro: %s", e)
        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
