LINE_ID  = 381    # human-facing route number
ROUTE_ID = "184"  # mo-bi.ro internal route_id (from GTFS routes.txt)

POLL_INTERVAL_SECONDS = 40
KAFKA_TOPIC = "stb-arrivals"

_MOBI_PROXY          = "https://crimson-river-eb3a.ciprian-medar.workers.dev"
MOBI_BUS_DATA_URL    = f"{_MOBI_PROXY}/api/busData"
MOBI_NEXT_ARR_URL    = f"{_MOBI_PROXY}/api/nextArrivals"
STOP_GH_SINCAI_DIR0  = 3782  # Colegiul Gh. Sincai — toward Clabucet / Piata Romana
STOP_GH_SINCAI_DIR1  = 3784  # Colegiul Gh. Sincai — toward Piata Resita
