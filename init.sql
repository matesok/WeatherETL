create schema if not exists weather_data;

create table if not exists weather_data.staging_weather
(
    id                       serial primary key,
    fetch_id                 varchar(255) not null,
    fetch_ts                 int          not null,
    source                   varchar(255) not null,
    city_id                  integer      not null, --natural key from API
    city_name                varchar(255) not null,
    lat                      float        not null,
    lon                      float        not null,

    "date"                   date         not null,
    "time"                   time         not null,

    condition_id             integer      not null,
    main_weather             varchar(255) not null,
    weather_description      varchar(255) not null,

    temperature_c            float        not null,
    temperature_feels_like_c float        not null,
    pressure                 int          not null,
    humidity                 int          not null,
    wind_speed               float        not null,
    clouds_percent           int          not null,
    rain_1h                  float,
    snow_1h                  float
);

create table if not exists weather_data.dim_date
(
    id          serial primary key,
    "date"      date        not null,
    day_of_week varchar(20) not null,
    day         int         not null,
    month       int         not null,
    quarter     int         not null,
    year        int         not null,
    unique ("date")
);

CREATE INDEX idx_dim_date_date ON weather_data.dim_date ("date");

create table if not exists weather_data.dim_time
(
    id     serial primary key,
    "time" time not null,
    hour   int  not null,
    minute int  not null,
    second int  not null,
    unique ("time")
);

CREATE INDEX idx_dim_date_time ON weather_data.dim_time ("time");

create table if not exists weather_data.ingest_metadata
(
    id       serial primary key,
    fetch_id varchar(255) not null,
    fetch_ts timestamp    not null,
    source   varchar(255) not null,
    unique (fetch_id)
);

CREATE INDEX idx_ingest_metadata_fetch_id ON weather_data.ingest_metadata ("fetch_id");

create table if not exists weather_data.dim_location
(
    id        serial primary key,
    city_id   integer      not null, --natural key from API
    city_name varchar(255) not null,
    lat       float        not null,
    lon       float        not null,
    unique (city_id)
);

CREATE INDEX idx_dim_location_city_id ON weather_data.dim_location ("city_id");

create table if not exists weather_data.dim_weather_condition
(
    id           serial primary key,
    condition_id integer      not null, --natural key from API
    main         varchar(255) not null,
    description  varchar(255) not null,
    unique (condition_id)
);

CREATE INDEX idx_dim_weather_condition ON weather_data.dim_weather_condition ("condition_id");

create table if not exists weather_data.fact_weather
(
    id                             serial primary key,
    location_id                    integer not null,
    metadata_id                    integer not null,
    date_id                        integer not null,
    time_id                        integer not null,
    weather_condition_id           integer not null,
    temperature_celsius            float   not null,
    temperature_feels_like_celsius float   not null,
    pressure_hpa                   int     not null,
    humidity_percent               int     not null,
    wind_speed_mps                 float   not null,
    clouds_percent                 int     not null,
    rain_volume_last_1h            float,
    snow_volume_last_1h            float,
    foreign key (location_id) references weather_data.dim_location (id),
    foreign key (metadata_id) references weather_data.ingest_metadata (id),
    foreign key (date_id) references weather_data.dim_date (id),
    foreign key (time_id) references weather_data.dim_time (id),
    foreign key (weather_condition_id) references weather_data.dim_weather_condition (id),
    unique (location_id, date_id, time_id)
)
