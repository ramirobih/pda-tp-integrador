#!/bin/bash

# Inicializar la base de datos si es la primera vez
if [ ! -f /app/airflow/airflow.db ]; then
    echo "Inicializando la base de datos de Airflow..."
    airflow db init
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
fi

# Iniciar el scheduler en segundo plano
echo "Iniciando el scheduler de Airflow..."
airflow scheduler &

# Iniciar el webserver
echo "Iniciando el webserver de Airflow..."
exec airflow webserver
