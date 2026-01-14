import os
import datetime
import logging

from src.utils.requests_utils import try_get_request
from src.utils.files_utils import save_json_file, append_to_city_coords, RAW_DIR
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("API_KEY")
base_api_url = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
base_coord_api = "http://api.openweathermap.org/geo/1.0/direct?q={city},{country_code}&limit=1&appid={API_KEY}"


def get_weather_data(city):
    logging.info(f"Getting weather data for {city}")
    lat, lon = get_city_coordinates(city)
    response = try_get_request(base_api_url.format(lat=lat, lon=lon, API_KEY=api_key), 30)
    raw_data = _enhance_raw_data(response, 'openweathermap')
    save_json_file(raw_data,
                   RAW_DIR / f"weather_{datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.json")

    logging.info(f"Weather data for {city} retrieved")

def get_city_coordinates(city):
    logging.info(f"Getting city coordinates for {city}")
    response = try_get_request(base_coord_api.format(city=city, country_code='PL', API_KEY=api_key), 30)
    first_coord = response.json()[0]
    lat, lon = first_coord["lat"], first_coord["lon"]
    append_to_city_coords(city, (lat, lon))

    logging.info(f"City coordinates for {city} retrieved")
    return lat, lon


def _enhance_raw_data(response, source):
    return {
        'metadata': {
            'source': source,
            'status_code': response.status_code,
            'fetched_at': datetime.datetime.now(datetime.timezone.utc).isoformat()
        },
        'data': response.json()
    }
