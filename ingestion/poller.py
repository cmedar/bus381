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
    MOBI_NEXT_ARR_URL, CORRIDOR_STOPS,
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


def poll_stop(producer, stop_id: int, stop_name: str, corridor_seq: int,
              direction: int, ingested_at: str):
    try:
        data = fetch_next_arrivals(stop_id)
        line = find_381(data)
        if line:
            arriving_s   = int(line.get("arrivingTime", 0))
            is_timetable = line.get("isTimetable", True)
            mins, secs   = divmod(arriving_s, 60)
            log.info("  [dir%d] seq%d %-26s %dm%02ds (%s)",
                     direction, corridor_seq, stop_name, mins, secs,
                     "sched" if is_timetable else "live")
            producer.send(KAFKA_TOPIC, value={
                "stop_id":             str(stop_id),
                "stop_name":           stop_name,
                "corridor_seq":        corridor_seq,
                "direction":           direction,
                "line_id":             LINE_ID,
                "route_id":            ROUTE_ID,
                "arriving_in_seconds": arriving_s,
                "is_timetable":        is_timetable,
                "ingested_at":         ingested_at,
            })
        else:
            log.info("  [dir%d] seq%d %-26s no data", direction, corridor_seq, stop_name)
    except requests.HTTPError as e:
        log.error("HTTP error stop %s: %s", stop_id, e)
    except Exception as e:
        log.error("Poll error stop %s: %s", stop_id, e)


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=5,
    )
    log.info("Poller started — %d corridor stops every %ds",
             len(CORRIDOR_STOPS), POLL_INTERVAL_SECONDS)

    while True:
        ingested_at = datetime.now(timezone.utc).isoformat()
        log.info("--- poll %s ---", ingested_at[11:19])

        for stop_id, stop_name, seq in CORRIDOR_STOPS:
            poll_stop(producer, stop_id, stop_name, seq, 0, ingested_at)

        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
