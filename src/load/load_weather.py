from io import BytesIO
import pandas as pd
from psycopg2.extras import execute_values
from src.load.sql_queries import *
from src.utils.db_utils import get_psycopg
from src.aws.aws_s3 import fetch_file


def load_weather_data(processed_s3_key):
    bf = fetch_file(processed_s3_key)
    parquet_bytes = BytesIO(bf)

    df = pd.read_parquet(parquet_bytes, engine='pyarrow')
    with get_psycopg() as cur:
        load_staging_data(df, cur)
        load_dimensions(cur)
        load_facts(cur)


def load_staging_data(df, cur):
    columns = ','.join(df.columns)
    sql = STAGING_LOAD % columns
    tuples = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, sql, tuples)


def load_dimensions(cur):
    cur.execute(DATE_LOAD)
    cur.execute(TIME_LOAD)
    cur.execute(CONDITION_LOAD)
    cur.execute(METADATA_LOAD)
    cur.execute(LOCATION_LOAD)


def load_facts(cur):
    cur.execute(FACT_LOAD)
