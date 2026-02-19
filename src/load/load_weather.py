import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import os
from psycopg2.extensions import register_adapter, AsIs

from src.load.sql_queries import DATE_LOAD, TIME_LOAD, LOCATION_LOAD, METADATA_LOAD, SELECT_DATE_IDS, SELECT_TIME_IDS, \
    SELECT_LOCATION_IDS, SELECT_METADATA_IDS, CONDITION_LOAD, SELECT_CONDITION_IDS, FACT_LOAD_NEW
from src.utils.files_utils import PROCESSED_DIR

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)


def load_weather_data(path):
    path = path + ".parquet"
    df = pd.read_parquet(PROCESSED_DIR / path, engine='pyarrow')
    location = df[['city_id', 'city_name', 'lat', 'lon']].drop_duplicates()
    metadata = df[['fetch_id', 'fetch_ts', 'source']]
    condition = df[['condition_id', 'main_weather', 'weather_description']].drop_duplicates()
    date = df[['date', 'day_of_week', 'month', 'quarter', 'year']].drop_duplicates()
    time = df[['time', 'hour', 'minute', 'second']].drop_duplicates()

    with psycopg2.connect(database=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
                          host=os.getenv('DB_HOST'),
                          port=os.getenv('DB_PORT')) as conn:
        with conn.cursor() as cur:
            insert_df_to_db(df=location, insert_query=LOCATION_LOAD, cur=cur)
            insert_df_to_db(df=metadata, insert_query=METADATA_LOAD, cur=cur)
            insert_df_to_db(df=condition, insert_query=CONDITION_LOAD, cur=cur)
            insert_df_to_db(df=date, insert_query=DATE_LOAD, cur=cur)
            insert_df_to_db(df=time, insert_query=TIME_LOAD, cur=cur)
            load_facts(weather_df=df, cur=cur)
            conn.commit()


def insert_df_to_db(df, insert_query, cur):
    cur.executemany(insert_query, df.values.tolist())


def load_facts(weather_df, cur):
    cur.execute(SELECT_LOCATION_IDS, (weather_df['city_id'].unique().tolist(),))
    loc_df = pd.DataFrame(cur.fetchall(), columns=['location_id', 'city_id'])

    cur.execute(SELECT_METADATA_IDS, (weather_df['fetch_id'].unique().tolist(),))
    met_df = pd.DataFrame(cur.fetchall(), columns=['metadata_id', 'fetch_id'])

    cur.execute(SELECT_DATE_IDS, (weather_df['date'].unique().tolist(),))
    dat_df = pd.DataFrame(cur.fetchall(), columns=['date_id', 'date'])

    cur.execute(SELECT_TIME_IDS, (weather_df['time'].unique().tolist(),))
    tim_df = pd.DataFrame(cur.fetchall(), columns=['time_id', 'time'])

    cur.execute(SELECT_CONDITION_IDS, (weather_df['condition_id'].unique().tolist(),))
    con_df = pd.DataFrame(cur.fetchall(), columns=['condition_sur_id', 'condition_id'])

    df_fact = weather_df.copy()

    df_fact = df_fact.merge(
        loc_df, how='left', on='city_id'
    )

    df_fact = df_fact.merge(
        met_df, how='left', on='fetch_id'
    )

    df_fact = df_fact.merge(
        dat_df, how='left', on='date'
    )

    df_fact = df_fact.merge(
        tim_df, how='left', on='time'
    )

    df_fact = df_fact.merge(
        con_df, how='left', on='condition_id'
    )

    # Select and rename columns in one operation to avoid SettingWithCopyWarning
    to_load = df_fact[['location_id', 'metadata_id', 'date_id', 'time_id', 'condition_sur_id', 'temperature_c',
                       'temperature_feels_like_c', 'pressure', 'humidity', 'wind_speed', 'clouds_percent',
                       'rain_1h', 'snow_1h']].rename(columns={
        'condition_sur_id': 'weather_condition_id',
        'pressure': 'pressure_hpa',
        'humidity': 'humidity_percent',
        'wind_speed': 'wind_speed_mps',
        'rain_1h': 'rain_volume_last_1h',
        'snow_1h': 'snow_volume_last_1h',
        'temperature_c': 'temperature_celsius',
        'temperature_feels_like_c': 'temperature_feels_like_celsius'
    })

    columns = ','.join(to_load.columns)
    sql = FACT_LOAD_NEW % columns
    tuples = [tuple(x) for x in to_load.to_numpy()]

    execute_values(cur, sql, tuples)

