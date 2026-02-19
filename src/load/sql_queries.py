LOCATION_LOAD = ("""
                 INSERT INTO weather_data.dim_location(city_id, city_name, lat, lon)
                 VALUES (%s, %s, %s, %s)
                 ON CONFLICT (city_id) DO NOTHING;
                 """)

METADATA_LOAD = ("""
                 INSERT INTO weather_data.ingest_metadata(fetch_id, fetch_ts, source)
                 VALUES (%s, to_timestamp((%s)::double precision), %s)
                 ON CONFLICT (fetch_id) DO NOTHING;
                 """)

DATE_LOAD = ("""
                 INSERT INTO weather_data.dim_date(date, day_of_week, month, quarter, year)
                 VALUES (%s, %s, %s, %s, %s)
                 ON CONFLICT (date) DO NOTHING;
                 """)

TIME_LOAD = ("""
             INSERT INTO weather_data.dim_time(time, hour, minute, second)
             VALUES (%s, %s, %s, %s)
             ON CONFLICT (time) DO NOTHING;
             """)


CONDITION_LOAD = ("""
             INSERT INTO weather_data.dim_weather_condition(condition_id, main, description)
             VALUES (%s, %s, %s)
             ON CONFLICT (condition_id) DO NOTHING;
             """)

SELECT_DATE_IDS = ("""
                   SELECT id, "date"
                   FROM weather_data.dim_date
                   where date = ANY(%s);
                   """)

SELECT_TIME_IDS = ("""
                   SELECT id, time
                   FROM weather_data.dim_time
                   where time = ANY(%s);
                   """)

SELECT_LOCATION_IDS = ("""
                       SELECT id, city_id
                       FROM weather_data.dim_location
                       where city_id = ANY(%s);
                       """)

SELECT_METADATA_IDS = ("""
                       SELECT id, fetch_id
                       FROM weather_data.ingest_metadata
                       where fetch_id = ANY(%s);
                       """)


SELECT_CONDITION_IDS = ("""
                        SELECT id, condition_id
                        FROM weather_data.dim_weather_condition
                        where condition_id = ANY(%s);
                        """)

FACT_LOAD = """
            INSERT INTO weather_data.fact_weather(location_id,
                                                  metadata_id,
                                                  date_id,
                                                  time_id,
                                                  weather_condition_id,
                                                  temperature_celsius,
                                                  temperature_feels_like_celsius,
                                                  pressure_hpa,
                                                  humidity_percent,
                                                  wind_speed_mps,
                                                  clouds_percent,
                                                  rain_volume_last_1h,
                                                  snow_volume_last_1h)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT do NOTHING;
            """

FACT_LOAD_NEW = """
            INSERT INTO weather_data.fact_weather(%s)
            VALUES %%s
            ON CONFLICT do NOTHING;
            """
