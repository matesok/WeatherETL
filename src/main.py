from extract import weather_api
from transform.transform_weather import transform_data
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
    extraction_dir = weather_api.get_weather_data(['Kuźnia Raciborska', 'Racibórz', 'Warszawa', 'Toruń'])
    print(extraction_dir)
    transform_data(extraction_dir)