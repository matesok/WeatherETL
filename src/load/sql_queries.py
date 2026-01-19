LOCATION_LOAD = ("""
                 INSERT INTO weather_data.dim_location(city_id, city_name)
                 VALUES (%s, %s)
                 ON CONFLICT (city_id,city_name ) DO NOTHING;
                 """)

METADATA_LOAD = ("""
                 INSERT INTO weather_data.ingest_metadata(fetch_id, fetch_ts, source)
                 VALUES (%s, to_timestamp((%s)::double precision), %s)
                 ON CONFLICT (fetch_id) DO NOTHING;
                 """)

SELECT_LOCATION_IDS = ("""
                       SELECT id, city_id
                       FROM weather_data.dim_location;
                       """)

SELECT_METADATA_IDS = ("""
                       SELECT id, fetch_id
                       FROM weather_data.ingest_metadata;
                       """)

FACT_LOAD = """
            INSERT INTO weather_data.fact_weather(location_id,
                                                  metadata_id,
                                                  observation_ts,
                                                  main_weather,
                                                  weather_description,
                                                  temperature_celsius,
                                                  temperature_feels_like_celsius,
                                                  pressure_hpa,
                                                  humidity_percent,
                                                  wind_speed_mps,
                                                  clouds_percent,
                                                  rain_volume_last_1h,
                                                  snow_volume_last_1h)
            VALUES (%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT
                DO NOTHING; 
            """
