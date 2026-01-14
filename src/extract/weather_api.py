import os
from hashlib import sha1
from datetime import datetime, timezone
import logging

from src.utils.requests_utils import try_get_request
from src.utils.files_utils import save_json_file, check_for_existing_coords, append_to_city_coords, RAW_DIR
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("API_KEY")
base_api_url = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={API_KEY}"
base_coord_api = "http://api.openweathermap.org/geo/1.0/direct?q={city},{country_code}&limit=1&appid={API_KEY}"


def get_weather_data(cities):
    logging.info(f"Getting weather data for {cities}")
    extraction_time = datetime.now(timezone.utc)
    extraction_hour = extraction_time.strftime("%Y%m%dT%HZ")

    for city in cities:
        lat, lon = get_city_coordinates(city)
        request_ts = datetime.now(timezone.utc)
        response = try_get_request(base_api_url.format(lat=lat, lon=lon, API_KEY=api_key), 30)
        json_response = response.json()
        city_id = _get_openweather_city_id(json_response)
        run_id = sha1(str(str(city_id) + str(request_ts)).encode()).hexdigest()[:8]
        raw_data = _enhance_raw_data(json_response, response.status_code, city_id, extraction_time, extraction_hour)
        # already_exists = check_if_weather_already_extracted(city_id, extraction_hour)
        #
        # if already_exists:
        #     return None

        save_json_file(raw_data,
                       RAW_DIR / f"{extraction_hour}/weather_{city_id}_{run_id}.json")

        logging.info(f"Weather data for {city} retrieved")

    return extraction_hour


def get_city_coordinates(city):
    logging.info(f"Getting city coordinates for {city}")
    coords = check_for_existing_coords(city)
    if coords:
        logging.info(f"City coordinates for {city} were already retrieved")
        return coords['lat'], coords['lon']

    response = try_get_request(base_coord_api.format(city=city, country_code='PL', API_KEY=api_key), 30)
    first_coord = response.json()[0]
    lat, lon = first_coord["lat"], first_coord["lon"]
    append_to_city_coords(city, (lat, lon))

    logging.info(f"City coordinates for {city} retrieved")
    return lat, lon


def _enhance_raw_data(response, status_code, city_id, extraction_time, extraction_hour, source='openweathermap'):
    return {
        'metadata': {
            'city_id': city_id,
            'source': source,
            'status_code': status_code,
            'fetched_at': extraction_time.isoformat(),
            'request_hour': extraction_hour
        },
        'data': response
    }


def _get_openweather_city_id(json_response):
    if json_response:
        return int(json_response['id'])
    return None
