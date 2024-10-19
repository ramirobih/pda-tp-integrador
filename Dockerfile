# imagen base de Python
FROM python:3.8-slim-buster

# variables de entorno necesarias para Airflow
ENV AIRFLOW_HOME=/app/airflow

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# directorio de trabajo
WORKDIR /app

COPY requirements.txt .

# instala las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt

# Instalar Airflow y los proveedores necesarios
RUN pip install --no-cache-dir apache-airflow[postgres]==2.10.2

COPY . .

# exponer el puerto de Airflow
EXPOSE 8080

# Luego iniciar el servidor web de Airflow
CMD ["bash", "-c", "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com && airflow webserver"]
