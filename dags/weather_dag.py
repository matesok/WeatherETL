import logging
import os
import sys

from dotenv import load_dotenv

from src.extract import weather_api
from src.transform import transform_weather
from src.load import load_weather
from airflow.decorators import dag, task
from pendulum import datetime, duration


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


@dag(
    schedule='@hourly',
    start_date=datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather"],
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(minutes=30),
    },
)
def weather_dag():
    configure_logging()
    cities_str = os.getenv("CITIES_TO_FETCH", [])
    cities = cities_str.split(',') if cities_str else []
    logging.info("cities: {}".format(cities))

    @task
    def extract_weather():
        return weather_api.get_weather_data(cities)

    @task
    def transform_weather_task(s3_directory):
        return transform_weather.transform_batch(s3_directory)

    @task
    def load_weather_task(processed_s3_directory):
        load_weather.load_weather_data(processed_s3_directory)

    s3_directory = extract_weather()
    processed_s3_directory = transform_weather_task(s3_directory)
    load_weather_task(processed_s3_directory)


weather_dag()
