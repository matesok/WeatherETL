create schema if not exists weather_data;

create table if not exists weather_data.ingest_metadata
(
    id       serial primary key,
    fetch_id varchar(255) not null,
    fetch_ts timestamp    not null,
    source   varchar(255) not null,
    unique (fetch_id)
);

create table if not exists weather_data.dim_location
(
    id        serial primary key,
    city_id   integer      not null,
    city_name varchar(255) not null,
    unique (city_id, city_name)
);

create table if not exists weather_data.fact_weather
(
    id                             serial primary key,
    location_id                    integer      not null,
    metadata_id                    integer      not null,
    observation_ts                 timestamp    not null,
    main_weather                   varchar(255) not null,
    weather_description            varchar(255) not null,
    temperature_celsius            float        not null,
    temperature_feels_like_celsius float        not null,
    pressure_hpa                   int          not null,
    humidity_percent               int          not null,
    wind_speed_mps                 float        not null,
    clouds_percent                 int          not null,
    rain_volume_last_1h            float,
    snow_volume_last_1h            float,
    foreign key (location_id) references weather_data.dim_location (id),
    foreign key (metadata_id) references weather_data.ingest_metadata (id),
    unique (location_id, observation_ts)
)
