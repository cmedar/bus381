LINE_ID  = 381    # human-facing route number
ROUTE_ID = "184"  # mo-bi.ro internal route_id (from GTFS routes.txt)

POLL_INTERVAL_SECONDS = 90
KAFKA_TOPIC           = "stb-arrivals"
CROSSINGS_TOPIC       = "bus-crossings"

_MOBI_PROXY       = "https://crimson-river-eb3a.ciprian-medar.workers.dev"
MOBI_BUS_DATA_URL = f"{_MOBI_PROXY}/api/busData"
MOBI_NEXT_ARR_URL = f"{_MOBI_PROXY}/api/nextArrivals"

# Direction 0 corridor stops (Tineretului → Piata Romana), in order
CORRIDOR_STOPS_DIR0 = [
    (3688,  "Visana",                 0),
    (3782,  "Gh. Sincai",             1),
    (3678,  "Bd. Marasesti",          2),
    (7257,  "Piata Sf. Gheorghe",     3),
    (7256,  "Universitate",           4),
    (12353, "Bd. Nicolae Balcescu",   5),
    (12354, "Arthur Verona",          6),
    (6588,  "Piata Romana",           7),
]  # (stop_id, stop_name, corridor_sequence)

# Direction 1 corridor stops (Piata Romana → Tineretului), in order
CORRIDOR_STOPS_DIR1 = [
    (5972,  "Orlando",                0),
    (3826,  "Piata Romana",           1),
    (12514, "George Enescu",          2),
    (7411,  "Bd. Nicolae Balcescu",   3),
    (7462,  "Piata 21 Dec 1989",      4),
    (6611,  "Piata Sf. Gheorghe",     5),
    (3667,  "Bd. Marasesti",          6),
    (3784,  "Gh. Sincai",             7),
]  # (stop_id, stop_name, corridor_sequence)

CORRIDOR_STOPS = CORRIDOR_STOPS_DIR0  # backwards-compat alias

