import os
import csv
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaConsumer
from config import CROSSINGS_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATA_DIR         = Path(os.getenv("DATA_DIR", "/data"))
STALE_MINUTES    = 60
MAX_SEGMENT_MINUTES = 15


@dataclass
class Corridor:
    direction:    int
    stop_names:   dict   # seq → display name
    stop_keys:    dict   # seq → CSV column stem
    sessions_json: Path
    journeys_csv:  Path

    @property
    def start_seq(self): return min(self.stop_names)
    @property
    def end_seq(self):   return max(self.stop_names)

    @property
    def journeys_fields(self):
        return (
            ["session_id"] +
            [f"{k}_{col}"
             for k in self.stop_keys.values()
             for col in ("at", "eta_before", "eta_after")] +
            ["total_seconds"]
        )


DIR0 = Corridor(
    direction=0,
    stop_names={
        1: "Gh. Sincai", 2: "Bd. Marasesti", 3: "Piata Sf. Gheorghe",
        4: "Universitate", 5: "Bd. Nicolae Balcescu", 6: "Arthur Verona",
        7: "Piata Romana",
    },
    stop_keys={
        1: "sincai", 2: "marasesti", 3: "sf_gheorghe",
        4: "universitate", 5: "nicolae_balcescu", 6: "arthur_verona",
        7: "romana",
    },
    sessions_json=DATA_DIR / "sessions.json",
    journeys_csv=DATA_DIR  / "journeys.csv",
)

DIR1 = Corridor(
    direction=1,
    stop_names={
        1: "Piata Romana", 2: "George Enescu", 3: "Bd. Nicolae Balcescu",
        4: "Piata 21 Dec 1989", 5: "Piata Sf. Gheorghe",
        6: "Bd. Marasesti", 7: "Gh. Sincai",
    },
    stop_keys={
        1: "romana", 2: "enescu", 3: "balcescu",
        4: "piata21", 5: "sf_gheorghe", 6: "marasesti",
        7: "sincai",
    },
    sessions_json=DATA_DIR / "sessions_dir1.json",
    journeys_csv=DATA_DIR  / "journeys_dir1.csv",
)

CORRIDORS = {0: DIR0, 1: DIR1}


class Session:
    def __init__(self, crossing: dict, corridor: Corridor):
        self.corridor   = corridor
        self.session_id = f"bus_{crossing['crossed_at'][11:19].replace(':', '')}"
        self.started_at = crossing["crossed_at"]
        self.last_seq   = corridor.start_seq
        self.status     = "in_progress"
        self.crossings  = {corridor.start_seq: crossing}
        self.total_s    = None

    def add_crossing(self, crossing: dict):
        seq = crossing["corridor_seq"]
        self.crossings[seq] = crossing
        self.last_seq = seq
        if seq == self.corridor.end_seq:
            self.status = "complete"
            start        = datetime.fromisoformat(self.crossings[self.corridor.start_seq]["crossed_at"])
            end          = datetime.fromisoformat(crossing["crossed_at"])
            self.total_s = int((end - start).total_seconds())
            m, s         = divmod(self.total_s, 60)
            log.info("Journey complete [dir%d]: %s → %s  total %dm%02ds",
                     self.corridor.direction,
                     self.crossings[self.corridor.start_seq]["crossed_at"][11:19],
                     crossing["crossed_at"][11:19], m, s)

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "status":     self.status,
            "last_seq":   self.last_seq,
            "crossings":  {str(k): v["crossed_at"] for k, v in self.crossings.items()},
            "total_s":    self.total_s,
        }


def save_sessions(sessions: list, corridor: Corridor):
    with open(corridor.sessions_json, "w") as f:
        json.dump([s.to_dict() for s in sessions], f, indent=2)


def append_journey(session: Session):
    corridor = session.corridor
    is_new   = not corridor.journeys_csv.exists()
    row = {"session_id": session.session_id, "total_seconds": session.total_s}
    for seq, key in corridor.stop_keys.items():
        c = session.crossings.get(seq, {})
        row[f"{key}_at"]         = c.get("crossed_at", "")
        row[f"{key}_eta_before"] = c.get("eta_before", "")
        row[f"{key}_eta_after"]  = c.get("eta_after", "")
    with open(corridor.journeys_csv, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=corridor.journeys_fields)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def purge_stale(sessions: list) -> list:
    cutoff = datetime.now(timezone.utc).timestamp() - STALE_MINUTES * 60
    active = []
    for s in sessions:
        last_crossing = s.crossings[s.last_seq]["crossed_at"]
        ts = datetime.fromisoformat(last_crossing).timestamp()
        if ts >= cutoff:
            active.append(s)
        else:
            log.info("Purged stale session %s (dir%d, last seen seq=%d)",
                     s.session_id, s.corridor.direction, s.last_seq)
    return active


def _within_plausible_time(session: Session, crossed_at: str) -> bool:
    last_cross = session.crossings[session.last_seq]["crossed_at"]
    elapsed_s  = (datetime.fromisoformat(crossed_at) - datetime.fromisoformat(last_cross)).total_seconds()
    return elapsed_s <= MAX_SEGMENT_MINUTES * 60


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    consumer = KafkaConsumer(
        CROSSINGS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="latest",
        group_id="session-tracker",
    )
    log.info("Session tracker started — dir0 (Sincai→Romana) + dir1 (Romana→Sincai)")

    all_sessions: dict[int, list[Session]] = {0: [], 1: []}

    for msg in consumer:
        rec       = msg.value
        direction = rec.get("direction")
        corridor  = CORRIDORS.get(direction)
        if corridor is None:
            continue

        seq = rec.get("corridor_seq", -1)
        if seq < corridor.start_seq or seq > corridor.end_seq:
            continue

        sessions = purge_stale(all_sessions[direction])
        all_sessions[direction] = sessions

        if seq == corridor.start_seq:
            sessions.append(Session(rec, corridor))
            log.info("Bus entered corridor dir%d at %s %s — %d active",
                     direction, corridor.stop_names[corridor.start_seq],
                     rec["crossed_at"][11:19], len(sessions))
        else:
            candidates = [
                s for s in sessions
                if s.status == "in_progress"
                and s.last_seq == seq - 1
                and _within_plausible_time(s, rec["crossed_at"])
            ]
            if not candidates:
                log.warning("No plausible session for dir%d seq=%d (%s) at %s — skipping",
                            direction, seq, corridor.stop_names.get(seq, seq),
                            rec["crossed_at"][11:19])
                continue
            target = min(candidates, key=lambda s: s.started_at)
            target.add_crossing(rec)
            if target.status == "complete":
                append_journey(target)

        save_sessions(sessions, corridor)
        log.info("Dir%d sessions: %s", direction, [
            f"{s.session_id}({corridor.stop_names.get(s.last_seq, '?')}{'✓' if s.status == 'complete' else ''})"
            for s in sessions
        ])


if __name__ == "__main__":
    main()
