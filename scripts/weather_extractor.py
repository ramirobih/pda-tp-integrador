import os
import requests
import pandas as pd
from dotenv import load_dotenv

def get_weather(city, api_key):
    url = 'http://api.openweathermap.org/data/2.5/weather'
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',  # Temperatura en grados Celsius
        'lang': 'es'        # Descripciones en español
    }
    response = requests.get(url, params=params)
    data = response.json()
    if response.status_code != 200:
        raise Exception(f"Error al obtener datos para {city}: {data.get('message', '')}")
    return data

def process_weather_data(data):
    df = pd.json_normalize(data)
    # seleccionar columnas relevantes
    df = df[[
        'name',
        'weather[0].description',
        'main.temp',
        'main.feels_like',
        'main.temp_min',
        'main.temp_max',
        'main.pressure',
        'main.humidity',
        'wind.speed',
        'wind.deg',
        'clouds.all',
        'dt'
    ]]
    df.columns = [
        'city',
        'weather_description',
        'temperature',
        'feels_like',
        'temp_min',
        'temp_max',
        'pressure',
        'humidity',
        'wind_speed',
        'wind_deg',
        'cloudiness',
        'timestamp'
    ]
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df

def get_weather_data():
    # cargar variables de entorno
    load_dotenv()
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
    if not OPENWEATHER_API_KEY:
        raise Exception("La API Key de OpenWeatherMap no está configurada en las variables de entorno.")
    
    cities = ['Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'La Plata']
    df_list = []

    for city in cities:
        try:
            data = get_weather(city, OPENWEATHER_API_KEY)
            df = process_weather_data(data)
            df_list.append(df)
        except Exception as e:
            print(e)
    
    if df_list:
        df_weather = pd.concat(df_list, ignore_index=True)
        # crea carpeta de staging si no existe
        os.makedirs('staging', exist_ok=True)
        # guarda en formato Parquet
        df_weather.to_parquet('staging/weather_data.parquet', index=False)
    else:
        print("No se obtuvieron datos de clima para ninguna ciudad.")
