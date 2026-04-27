import os
import csv
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaConsumer
from config import CROSSINGS_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATA_DIR        = Path(os.getenv("DATA_DIR", "/data"))
SESSIONS_JSON   = DATA_DIR / "sessions.json"
JOURNEYS_CSV    = DATA_DIR / "journeys.csv"

STALE_MINUTES        = 60   # drop sessions inactive longer than this
MAX_SEGMENT_MINUTES  = 15   # reject crossing assignment if gap since last stop > this
START_SEQ     = 1   # Gh. Sincai
END_SEQ       = 7   # Piata Romana
CORRIDOR_DIR  = 0

STOP_NAMES = {
    1: "Gh. Sincai",
    2: "Bd. Marasesti",
    3: "Piata Sf. Gheorghe",
    4: "Universitate",
    5: "Bd. Nicolae Balcescu",
    6: "Arthur Verona",
    7: "Piata Romana",
}

STOP_KEYS = {
    1: "sincai",
    2: "marasesti",
    3: "sf_gheorghe",
    4: "universitate",
    5: "nicolae_balcescu",
    6: "arthur_verona",
    7: "romana",
}

# one _at + _eta_before + _eta_after per stop, plus session metadata
JOURNEYS_FIELDS = (
    ["session_id"] +
    [f"{k}_{col}"
     for k in STOP_KEYS.values()
     for col in ("at", "eta_before", "eta_after")] +
    ["total_seconds"]
)


class Session:
    def __init__(self, crossing: dict):
        self.session_id = f"bus_{crossing['crossed_at'][11:19].replace(':', '')}"
        self.started_at = crossing["crossed_at"]
        self.last_seq   = START_SEQ
        self.status     = "in_progress"
        self.crossings  = {START_SEQ: crossing}   # seq → full crossing dict
        self.total_s    = None

    def add_crossing(self, crossing: dict):
        seq = crossing["corridor_seq"]
        self.crossings[seq] = crossing
        self.last_seq = seq
        if seq == END_SEQ:
            self.status  = "complete"
            start        = datetime.fromisoformat(self.crossings[START_SEQ]["crossed_at"])
            end          = datetime.fromisoformat(crossing["crossed_at"])
            self.total_s = int((end - start).total_seconds())
            m, s         = divmod(self.total_s, 60)
            log.info("Journey complete: %s → %s  total %dm%02ds",
                     self.crossings[START_SEQ]["crossed_at"][11:19],
                     crossing["crossed_at"][11:19], m, s)

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "status":     self.status,
            "last_seq":   self.last_seq,
            "crossings":  {str(k): v["crossed_at"] for k, v in self.crossings.items()},
            "total_s":    self.total_s,
        }


def save_sessions(sessions: list):
    with open(SESSIONS_JSON, "w") as f:
        json.dump([s.to_dict() for s in sessions], f, indent=2)


def append_journey(session: Session):
    is_new = not JOURNEYS_CSV.exists()
    row = {"session_id": session.session_id, "total_seconds": session.total_s}
    for seq, key in STOP_KEYS.items():
        c = session.crossings.get(seq, {})
        row[f"{key}_at"]         = c.get("crossed_at", "")
        row[f"{key}_eta_before"] = c.get("eta_before", "")
        row[f"{key}_eta_after"]  = c.get("eta_after", "")
    with open(JOURNEYS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=JOURNEYS_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def _within_plausible_time(session: Session, crossed_at: str) -> bool:
    last_cross = session.crossings[session.last_seq]["crossed_at"]
    elapsed_s  = (datetime.fromisoformat(crossed_at) - datetime.fromisoformat(last_cross)).total_seconds()
    return elapsed_s <= MAX_SEGMENT_MINUTES * 60


def purge_stale(sessions: list) -> list:
    """Remove sessions with no update in STALE_MINUTES."""
    cutoff = datetime.now(timezone.utc).timestamp() - STALE_MINUTES * 60
    active = []
    for s in sessions:
        last_crossing = s.crossings[s.last_seq]["crossed_at"]
        ts = datetime.fromisoformat(last_crossing).timestamp()
        if ts >= cutoff:
            active.append(s)
        else:
            log.info("Purged stale session %s (last seen at seq=%d)", s.session_id, s.last_seq)
    return active


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    consumer = KafkaConsumer(
        CROSSINGS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="latest",
        group_id="session-tracker",
    )
    log.info("Session tracker started — dir0 Gh.Sincai→Romana, dynamic slots")

    sessions: list[Session] = []

    for msg in consumer:
        rec = msg.value
        if rec.get("direction") != CORRIDOR_DIR:
            continue
        seq = rec.get("corridor_seq", -1)
        if seq < START_SEQ or seq > END_SEQ:
            continue

        sessions = purge_stale(sessions)

        if seq == START_SEQ:
            sessions.append(Session(rec))
            log.info("Bus entered corridor at Gh.Sincai %s — %d active",
                     rec["crossed_at"][11:19], len(sessions))

        else:
            candidates = [
                s for s in sessions
                if s.status == "in_progress"
                and s.last_seq == seq - 1
                and _within_plausible_time(s, rec["crossed_at"])
            ]
            if not candidates:
                log.warning("No plausible session for seq=%d (%s) at %s — skipping "
                            "(FIFO mismatch or no bus in corridor)",
                            seq, STOP_NAMES.get(seq, seq), rec["crossed_at"][11:19])
                continue
            target = min(candidates, key=lambda s: s.started_at)
            target.add_crossing(rec)
            if target.status == "complete":
                append_journey(target)

        save_sessions(sessions)
        log.info("Sessions: %s", [
            f"{s.session_id}({STOP_NAMES.get(s.last_seq, '?')}{'✓' if s.status == 'complete' else ''})"
            for s in sessions
        ])


if __name__ == "__main__":
    main()
