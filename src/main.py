from extract import weather_api
from transform.transform_weather import transform_batch
from load.load_weather import load_weather_data
import logging
import sys


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


if __name__ == "__main__":
    configure_logging()
    s3_dir = weather_api.get_weather_data(['Kuźnia Raciborska', 'Racibórz', 'Warszawa', 'Toruń'])
    processed_s3_dir = transform_batch(s3_dir)
    load_weather_data(processed_s3_dir)
