LINE_ID  = 381    # human-facing route number
ROUTE_ID = "184"  # mo-bi.ro internal route_id (from GTFS routes.txt)

# direction_id == 0 → toward Clabucet (passes through Piata Romana)
# direction_id == 1 → toward Piata Resita (return leg, not tracked)
TARGET_DIRECTION = 0

# Narrow bbox covering the final approach to Piata Romana
CORRIDOR_BBOX = {
    "lat_min": 44.4270,
    "lat_max": 44.4450,
    "lon_min": 26.0950,
    "lon_max": 26.1000,
}

POLL_INTERVAL_SECONDS = 30
KAFKA_TOPIC = "stb-arrivals"

_MOBI_PROXY         = "https://crimson-river-eb3a.ciprian-medar.workers.dev"
MOBI_BUS_DATA_URL   = f"{_MOBI_PROXY}/api/busData"
MOBI_NEXT_ARR_URL   = f"{_MOBI_PROXY}/api/nextArrivals"
STOP_PIATA_ROMANA   = 6424  # GTFS stop_id for Piata Romana (direction 0 arrival stop)
