from contextlib import contextmanager

import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter, AsIs

import os


def get_db_params():
    return {
        "dbname": os.getenv('DB_NAME'),
        "user": os.getenv('DB_USER'),
        "password": os.getenv('DB_PASSWORD'),
        "host": os.getenv('DB_HOST'),
        "port": os.getenv('DB_PORT')
    }


register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)


@contextmanager
def get_psycopg():
    conn = psycopg2.connect(**get_db_params())
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            yield cur
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
