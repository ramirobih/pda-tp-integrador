import os
from unittest.mock import patch
from sqlalchemy.engine import Engine
from weather_extractor import create_redshift_engine

@patch.dict(os.environ, {
    'REDSHIFT_HOST': 'test-host',
    'REDSHIFT_PORT': '5439',
    'REDSHIFT_USER': 'test-user',
    'REDSHIFT_PASS': 'test-pass',
    'REDSHIFT_DB': 'test-db'
})
def test_create_redshift_engine():
    engine = create_redshift_engine()
    assert isinstance(engine, Engine)
    # Verificar que la cadena de conexión es correcta
    expected_url = 'postgresql+psycopg2://test-user:test-pass@test-host:5439/test-db'
    assert str(engine.url) == expected_url
