import os
from pathlib import Path

# Raw CSV source — set DATA_DIR to wherever you copied the EC2 data
DATA_DIR = Path(os.getenv("DATA_DIR", "../../data"))

JOURNEYS_CSV  = DATA_DIR / "journeys.csv"
CROSSINGS_CSV = DATA_DIR / "crossings.csv"
ARRIVALS_CSV  = DATA_DIR / "arrivals.csv"

# Processed outputs (written by silver/gold notebooks)
SILVER_DIR = Path(__file__).parent.parent / "data"
SILVER_JOURNEYS = SILVER_DIR / "silver_journeys.parquet"
