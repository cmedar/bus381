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
    MOBI_NEXT_ARR_URL, STOP_GH_SINCAI_DIR0, STOP_GH_SINCAI_DIR1,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

STOPS = [
    (STOP_GH_SINCAI_DIR0, "→ Clabucet"),
    (STOP_GH_SINCAI_DIR1, "→ Piata Resita"),
]

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Referer": "https://maps.mo-bi.ro/",
    "Origin": "https://maps.mo-bi.ro",
}


def fetch_next_arrivals(stop_id: int) -> dict:
    url = f"{MOBI_NEXT_ARR_URL}/{stop_id}?_t={int(time.time())}"
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
    log.info("Poller started — route %s at Gh. Sincai (both directions)", LINE_ID)

    while True:
        ingested_at = datetime.now(timezone.utc).isoformat()

        for stop_id, label in STOPS:
            try:
                data  = fetch_next_arrivals(stop_id)
                line  = find_381(data)
                if line:
                    arriving_s   = int(line.get("arrivingTime", 0))
                    is_timetable = line.get("isTimetable", True)
                    source       = "timetable" if is_timetable else "live"
                    mins, secs   = divmod(arriving_s, 60)
                    log.info("Next 381 Gh. Sincai %-18s %dm%02ds (%s)",
                             label, mins, secs, source)
                    record = {
                        "stop_id":             str(stop_id),
                        "stop_name":           "Colegiul Gh. Sincai",
                        "direction_label":     label,
                        "line_id":             LINE_ID,
                        "route_id":            ROUTE_ID,
                        "arriving_in_seconds": arriving_s,
                        "is_timetable":        is_timetable,
                        "ingested_at":         ingested_at,
                    }
                    producer.send(KAFKA_TOPIC, value=record)
                else:
                    log.info("Next 381 Gh. Sincai %-18s no data", label)
            except requests.HTTPError as e:
                log.error("HTTP error stop %s: %s", stop_id, e)
            except Exception as e:
                log.error("Poll error stop %s: %s", stop_id, e)

        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
