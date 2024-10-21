import pytest
from unittest.mock import patch
import pandas as pd
from weather_extractor import get_weather_data

@patch('weather_extractor.requests.get')
def test_get_weather_data(mock_get):
    # Configurar el mock para simular la respuesta de la API
    mock_response = {
        'name': 'Buenos Aires',
        'sys': {'country': 'AR'},
        'weather': [{'description': 'cielo claro'}],
        'main': {
            'temp': 25,
            'feels_like': 26,
            'temp_min': 24,
            'temp_max': 26,
            'pressure': 1013,
            'humidity': 60
        },
        'wind': {'speed': 5, 'deg': 180},
        'clouds': {'all': 0},
        'dt': 1609459200  # 2021-01-01 00:00:00 UTC
    }
    # Configurar el objeto de respuesta de requests.get
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    cities = ['Buenos Aires']
    df = get_weather_data(cities)

    # Verificar que el DataFrame no está vacío
    assert not df.empty
    # Verificar que contiene las columnas esperadas
    expected_columns = [
        'city', 'country', 'weather', 'temperature', 'feels_like',
        'temp_min', 'temp_max', 'pressure', 'humidity', 'wind_speed',
        'wind_deg', 'cloudiness', 'timestamp'
    ]
    assert list(df.columns) == expected_columns
    # Verificar que los datos son correctos
    assert df.iloc[0]['city'] == 'Buenos Aires'
    assert df.iloc[0]['temperature'] == 25
    assert df.iloc[0]['timestamp'] == pd.to_datetime(1609459200, unit='s').floor('S')
