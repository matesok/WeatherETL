import json
import glob
import logging
from pathlib import Path
import os

if os.getenv('AIRFLOW_HOME'):
    PROJECT_ROOT = Path('/opt/airflow')
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / 'data'
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'
HELPER_DIR = DATA_DIR / 'helper'
COORDS_PATH = HELPER_DIR / 'coords.json'
DB_PATH = PROJECT_ROOT / 'db/weather.db'

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def save_json_file(data, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    logging.info(f"Saved data to {path}")


def append_to_city_coords(city_id, coords):
    COORDS_PATH.parent.mkdir(parents=True, exist_ok=True)

    if COORDS_PATH.exists():
        data = json.loads(COORDS_PATH.read_text())
    else:
        data = {}

    data.setdefault(city_id, {
        'lat': coords[0],
        'lon': coords[1]
    })

    COORDS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def check_for_existing_coords(city):
    if COORDS_PATH.exists():
        json_data = json.loads(COORDS_PATH.read_text())
        return json_data.get(city)
    return None


def find_files_paths(path):
    return [x for x in glob.glob(path, recursive=True)]
