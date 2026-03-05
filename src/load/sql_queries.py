STAGING_LOAD = ("""
                INSERT INTO weather_data.staging_weather(%s)
                VALUES %%s
                ON CONFLICT DO NOTHING;
                """
                )

LOCATION_LOAD = ("""
                 INSERT INTO weather_data.dim_location(city_id, city_name, lat, lon)
                 SELECT city_id, city_name, lat, lon
                 FROM weather_data.staging_weather
                 ON CONFLICT (city_id) DO NOTHING;
                 """)

METADATA_LOAD = ("""
                 INSERT INTO weather_data.ingest_metadata(fetch_id, fetch_ts, source)
                 SELECT fetch_id, to_timestamp((fetch_ts)::double precision), source
                 FROM weather_data.staging_weather
                 ON CONFLICT (fetch_id) DO NOTHING;
                 """)

DATE_LOAD = ("""
             INSERT INTO weather_data.dim_date(date, day, day_of_week, month, quarter, year)
             SELECT s.date,
                    extract(day from s.date),
                    extract(isodow from s.date),
                    extract(month from s.date),
                    extract(quarter from s.date),
                    extract(year from s.date)
             FROM weather_data.staging_weather s
             ON CONFLICT (date) DO NOTHING;
             """)

TIME_LOAD = ("""
             INSERT INTO weather_data.dim_time(time, hour, minute, second)
             SELECT s.time, extract(hour from s.time), extract(minute from s.time), extract(second from s.time)
             FROM weather_data.staging_weather s
             ON CONFLICT (time) DO NOTHING;
             """)

CONDITION_LOAD = ("""
                  INSERT INTO weather_data.dim_weather_condition(condition_id, main, description)
                  SELECT condition_id, main_weather, weather_description
                  FROM weather_data.staging_weather
                  ON CONFLICT (condition_id) DO NOTHING;
                  """)

FACT_LOAD = ("""
             INSERT INTO weather_data.fact_weather (location_id, metadata_id, date_id, time_id, weather_condition_id,
                                                    temperature_celsius, temperature_feels_like_celsius, pressure_hpa,
                                                    humidity_percent,
                                                    wind_speed_mps, clouds_percent, rain_volume_last_1h,
                                                    snow_volume_last_1h)
             SELECT l.id,
                    m.id,
                    d.id,
                    t.id,
                    c.id,
                    s.temperature_c,
                    s.temperature_feels_like_c,
                    s.pressure,
                    s.humidity,
                    s.wind_speed,
                    s.clouds_percent,
                    s.rain_1h,
                    s.snow_1h
             FROM weather_data.staging_weather s
                      JOIN weather_data.dim_location l ON s.city_id = l.city_id
                      JOIN weather_data.ingest_metadata m ON s.fetch_id = m.fetch_id
                      JOIN weather_data.dim_date d ON s.date = d.date
                      JOIN weather_data.dim_time t ON s.time = t.time
                      JOIN weather_data.dim_weather_condition c ON s.condition_id = c.condition_id
             ON CONFLICT DO NOTHING;
             """)
