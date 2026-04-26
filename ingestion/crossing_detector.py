import os
import csv
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_TOPIC, CROSSINGS_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATA_DIR           = Path(os.getenv("DATA_DIR", "/data"))
CROSSINGS_CSV      = DATA_DIR / "crossings.csv"
CROSSINGS_FIELDS   = [
    "crossed_at", "stop_id", "stop_name", "direction",
    "corridor_seq", "eta_before", "eta_after",
]
CROSSING_THRESHOLD = 60  # ETA must jump by more than this (seconds) to count as a crossing


def append_crossing(row: dict):
    is_new = not CROSSINGS_CSV.exists()
    with open(CROSSINGS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CROSSINGS_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="latest",
        group_id="crossing-detector",
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=5,
    )
    log.info("Crossing detector started — watching %s → publishing to %s, CSV: %s",
             KAFKA_TOPIC, CROSSINGS_TOPIC, CROSSINGS_CSV)

    last_eta: dict[str, int] = {}  # stop_id → last arriving_in_seconds

    for msg in consumer:
        rec       = msg.value
        stop_id   = rec["stop_id"]
        new_eta   = rec["arriving_in_seconds"]
        stop_name = rec.get("stop_name", stop_id)
        direction = rec.get("direction", 0)

        if stop_id in last_eta:
            old_eta = last_eta[stop_id]
            if new_eta > old_eta + CROSSING_THRESHOLD:
                crossed_at = datetime.now(timezone.utc).isoformat()
                log.info("BUS CROSSED  %-26s  dir=%d  eta %ds→%ds  at %s",
                         stop_name, direction, old_eta, new_eta, crossed_at[11:19])
                producer.send(CROSSINGS_TOPIC, value={
                    "stop_id":      stop_id,
                    "stop_name":    stop_name,
                    "corridor_seq": rec.get("corridor_seq", -1),
                    "direction":    direction,
                    "crossed_at":   crossed_at,
                    "eta_before":   old_eta,
                    "eta_after":    new_eta,
                })
                producer.flush()
                append_crossing({
                    "crossed_at":   crossed_at,
                    "stop_id":      stop_id,
                    "stop_name":    stop_name,
                    "direction":    direction,
                    "corridor_seq": rec.get("corridor_seq", -1),
                    "eta_before":   old_eta,
                    "eta_after":    new_eta,
                })

        last_eta[stop_id] = new_eta


if __name__ == "__main__":
    main()
