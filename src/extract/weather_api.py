import json
import os
from hashlib import sha1
from datetime import datetime, timezone
import logging
from io import BytesIO

from src.utils.requests_utils import try_get_request
from src.utils.files_utils import RAW_DIR
from src.aws.aws_s3 import upload_file
from dotenv import load_dotenv

load_dotenv()

base_api_url = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={API_KEY}"
base_coord_api = "http://api.openweathermap.org/geo/1.0/direct?q={city},{country_code}&limit=1&appid={API_KEY}"


def get_weather_data(cities):
    try:
        logging.info(f"Getting weather data for {cities}")
        extraction_time = datetime.now(timezone.utc)
        extraction_hour = extraction_time.strftime("%Y/%m/%d/%H")

        for city in cities:
            lat, lon = get_city_coordinates(city)
            request_ts = datetime.now(timezone.utc)
            api_key = os.getenv('API_KEY')
            if not api_key:
                raise ValueError("API_KEY is not set. Please check your .env file.")

            response = try_get_request(base_api_url.format(lat=lat, lon=lon, API_KEY=api_key), 30)

            json_response = response.json()
            city_id = _get_openweather_city_id(json_response)
            run_id = sha1(str(str(city_id) + str(request_ts)).encode()).hexdigest()[:8]
            raw_data = _enhance_raw_data(json_response, response.status_code, run_id, extraction_hour, request_ts)

            upload_file(BytesIO(json.dumps(raw_data).encode('utf-8')),
                        RAW_DIR, extraction_hour, f'{run_id}.json')
            logging.info(f"Weather data for {city} retrieved")

        return f'{RAW_DIR}/{extraction_hour}'

    except Exception as e:
        logging.error(f"An error occurred while extracting weather data: {e}")
        raise


def get_city_coordinates(city):
    logging.info(f"Getting city coordinates for {city}")
    api_key = os.getenv('API_KEY')
    response = try_get_request(base_coord_api.format(city=city, country_code='PL', API_KEY=api_key), 30)
    first_coord = response.json()[0]
    lat, lon = first_coord["lat"], first_coord["lon"]
    logging.info(f"City coordinates for {city} retrieved")
    return lat, lon


def _enhance_raw_data(response, status_code, fetch_id, extraction_hour, request_ts, source='openweathermap'):
    return {
        'metadata': {
            'fetch_id': fetch_id,
            'source': source,
            'status_code': status_code,
            'fetched_ts': int(request_ts.timestamp()),
            'request_hour': extraction_hour
        },
        'data': response
    }


def _get_openweather_city_id(json_response):
    if json_response:
        return int(json_response['id'])
    return None
