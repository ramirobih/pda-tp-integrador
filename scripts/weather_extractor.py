import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from dotenv import load_dotenv

def get_weather_data(cities):
    api_key = os.getenv('OPENWEATHER_API_KEY')
    if not api_key:
        raise Exception("La API Key no está configurada en las variables de entorno.")

    url = 'http://api.openweathermap.org/data/2.5/weather'
    weather_data = []

    for city in cities:
        params = {
            'q': city + ',AR',
            'appid': api_key,
            'units': 'metric',
            'lang': 'es'
        }
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if response.status_code == 200:
                # extraer los datos relevantes
                weather_info = {
                    'city': data.get('name'),
                    'country': data.get('sys', {}).get('country'),
                    'weather': data.get('weather', [{}])[0].get('description'),
                    'temperature': data.get('main', {}).get('temp'),
                    'feels_like': data.get('main', {}).get('feels_like'),
                    'temp_min': data.get('main', {}).get('temp_min'),
                    'temp_max': data.get('main', {}).get('temp_max'),
                    'pressure': data.get('main', {}).get('pressure'),
                    'humidity': data.get('main', {}).get('humidity'),
                    'wind_speed': data.get('wind', {}).get('speed'),
                    'wind_deg': data.get('wind', {}).get('deg'),
                    'cloudiness': data.get('clouds', {}).get('all'),
                    'timestamp': pd.to_datetime(data.get('dt'), unit='s').floor('S')
                }
                weather_data.append(weather_info)
            else:
                print(f"No se pudo obtener datos para {city}: {data.get('message')}")
        except Exception as e:
            print(f"Error al procesar {city}: {e}")

    df_weather = pd.DataFrame(weather_data)
    return df_weather

def create_redshift_engine():
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
    REDSHIFT_PORT = os.getenv('REDSHIFT_PORT', 5439)
    REDSHIFT_USER = os.getenv('REDSHIFT_USER')
    REDSHIFT_PASS = os.getenv('REDSHIFT_PASS')
    REDSHIFT_DB   = os.getenv('REDSHIFT_DB')

    if not all([REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASS, REDSHIFT_DB]):
        raise Exception("Las credenciales de Redshift no están completamente configuradas en las variables de entorno.")

    # URL-encodear el usuario y la contraseña
    REDSHIFT_USER_ENCODED = quote_plus(REDSHIFT_USER)
    REDSHIFT_PASS_ENCODED = quote_plus(REDSHIFT_PASS)

    conn_str = f"postgresql+psycopg2://{REDSHIFT_USER_ENCODED}:{REDSHIFT_PASS_ENCODED}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
    engine = create_engine(conn_str)
    return engine

def create_table_if_not_exists(engine, schema_name, table_name):
    # Especificar el esquema en el nombre de la tabla
    full_table_name = f'"{schema_name}"."{table_name}"'

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        city VARCHAR(50),
        country VARCHAR(5),
        weather VARCHAR(100),
        temperature FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        pressure INT,
        humidity INT,
        wind_speed FLOAT,
        wind_deg INT,
        cloudiness INT,
        timestamp TIMESTAMP
    );
    """
    try:
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
        print(f"Tabla '{full_table_name}' verificada/creada exitosamente.")
    except SQLAlchemyError as e:
        print(f"Error al crear la tabla: {e}")
        raise

def insert_data_to_redshift(engine, df, schema_name, table_name):
    # schema y nombre de la tabla
    full_table_name = f'{schema_name}.{table_name}'
    try:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False, method='multi')
        print(f"Datos insertados correctamente en {full_table_name} en Redshift.")
    except SQLAlchemyError as e:
        print(f"Error al insertar datos: {e}")
        raise

if __name__ == '__main__':
    load_dotenv()

    cities = [
        'Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'La Plata',
        'San Miguel de Tucumán', 'Mar del Plata', 'Salta', 'Santa Fe',
        'San Juan', 'Resistencia', 'Neuquén', 'Santiago del Estero',
        'Corrientes', 'Posadas', 'Bahía Blanca', 'Paraná', 'Formosa',
        'San Luis', 'La Rioja', 'Catamarca', 'Comodoro Rivadavia',
        'San Salvador de Jujuy', 'Río Cuarto', 'Concordia', 'Bariloche',
        'Trelew', 'Río Gallegos', 'Ushuaia', 'Santa Rosa', 'Rawson',
        'Viedma', 'San Fernando del Valle de Catamarca', 'San Luis',
        'San Rafael', 'Villa María', 'Olavarría', 'Pergamino', 'Zárate',
        'Junín', 'Venado Tuerto', 'Goya', 'San Nicolás de los Arroyos',
        'Tandil', 'Necochea', 'Villa Carlos Paz', 'Alta Gracia',
        'Villa Mercedes', 'San Francisco', 'Chivilcoy', 'General Roca',
        'San Martín', 'Rafaela', 'Campana', 'La Banda', 'Tres Arroyos',
        'San Pedro', 'Puerto Madryn', 'Río Grande', 'Esquel', 'El Calafate'
    ]

    df = get_weather_data(cities)

    if not df.empty:
        engine = create_redshift_engine()
        schema_name = '2024_ramiro_bihurriet_schema'
        table_name = 'weather_data'

        create_table_if_not_exists(engine, schema_name, table_name)
        insert_data_to_redshift(engine, df, schema_name, table_name)
    else:
        print("No hay datos para insertar en Redshift.")