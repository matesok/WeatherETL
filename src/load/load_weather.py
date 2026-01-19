import pandas as pd
import psycopg2
import os

from src.load.sql_queries import LOCATION_LOAD, METADATA_LOAD, SELECT_LOCATION_IDS, SELECT_METADATA_IDS, FACT_LOAD
from src.utils.files_utils import PROCESSED_DIR, DB_PATH


def load_weather_data(path):
    path = path + ".parquet"
    df = pd.read_parquet(PROCESSED_DIR / path, engine='pyarrow')
    location = df[['city_id', 'city_name']].drop_duplicates()
    metadata = df[['fetch_id', 'fetch_ts', 'source']]
    weather = df.drop(columns=['city_name', 'fetch_ts', 'source'])
    with psycopg2.connect(database=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
                          host=os.getenv('DB_HOST'),
                          port=os.getenv('DB_PORT')) as conn:
        with conn.cursor() as cur:
            load_location(location_df=location, cur=cur)
            load_metadata(metadata_df=metadata, cur=cur)
            load_facts(weather_df=weather, cur=cur)
            conn.commit()


def load_location(location_df, cur):
    for _, row in location_df.iterrows():
        cur.execute(LOCATION_LOAD, list(row))


def load_metadata(metadata_df, cur):
    for _, row in metadata_df.iterrows():
        cur.execute(METADATA_LOAD, list(row))


def load_facts(weather_df, cur):
    cur.execute(SELECT_LOCATION_IDS)
    location_ids_lookup = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute(SELECT_METADATA_IDS)
    metadata_ids_lookup = {row[1]: row[0] for row in cur.fetchall()}

    df_fact = weather_df.copy()
    df_fact['location_id'] = df_fact['city_id'].map(location_ids_lookup)
    df_fact['metadata_id'] = df_fact['fetch_id'].map(metadata_ids_lookup)

    for row in df_fact.itertuples():
        cur.execute(FACT_LOAD, (
            row.location_id,
            row.metadata_id,
            row.observation_ts,
            row.main_weather,
            row.weather_description,
            row.temperature_c,
            row.temperature_feels_like_c,
            row.pressure,
            row.humidity,
            row.wind_speed,
            row.clouds_percent,
            row.rain_1h,
            row.snow_1h
        ))
