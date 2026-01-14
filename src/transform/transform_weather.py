import glob
import json

from src.utils.files_utils import RAW_DIR, find_files_paths
import os


def transform_data(directory_name):
    path = str(RAW_DIR / directory_name / "*.json")
    files_to_process = find_files_paths(path)
    transformed_data = []
    for file in files_to_process:
        with open(file) as json_file:
            raw_data = json.load(json_file)
            transformed_data.append(transform_single_weather_data(raw_data))


def transform_single_weather_data(raw_data):
    data_dic = raw_data['data']
    weather_dic = data_dic['weather']
    main_dic = data_dic['main']
    wind_dic = data_dic['wind']
    clouds_dic = data_dic['clouds']
    rain_dic = data_dic.get('rain', {})
    snow_dic = data_dic.get('snow', {})

    print(weather_dic)
    return {
        'city_id': data_dic['id'],
        'city_name': data_dic['name'],
        'observation_date': data_dic['dt'],
        'main_weather': weather_dic['main'],
        'weather_description': weather_dic['description'],
        'temperature_c': main_dic['temp'],
        'temperature_feels_like_c': main_dic['feels_like'],
        'pressure': main_dic['pressure'],
        'humidity': main_dic['humidity'],
        'wind_speed': wind_dic['speed'],
        'clouds_percent': clouds_dic['all'],
        'rain_1h': rain_dic.get('1h', 0),
        'snow_1h': snow_dic.get('1h', 0)
    }
