LINE_ID  = 381    # human-facing route number
ROUTE_ID = "184"  # mo-bi.ro internal route_id (from GTFS routes.txt)

POLL_INTERVAL_SECONDS = 20
KAFKA_TOPIC           = "stb-arrivals"
CROSSINGS_TOPIC       = "bus-crossings"

_MOBI_PROXY       = "https://crimson-river-eb3a.ciprian-medar.workers.dev"
MOBI_BUS_DATA_URL = f"{_MOBI_PROXY}/api/busData"
MOBI_NEXT_ARR_URL = f"{_MOBI_PROXY}/api/nextArrivals"

# Direction 0 corridor stops (Tineretului → Piata Romana), in order
CORRIDOR_STOPS = [
    (3688,  "Visana",                 0),
    (3782,  "Gh. Sincai",             1),
    (3678,  "Bd. Marasesti",          2),
    (7257,  "Piata Sf. Gheorghe",     3),
    (7256,  "Universitate",           4),
    (12353, "Bd. Nicolae Balcescu",   5),
    (12354, "Arthur Verona",          6),
    (6588,  "Piata Romana",           7),
]  # (stop_id, stop_name, corridor_sequence)

