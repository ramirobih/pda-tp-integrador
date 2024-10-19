# imagen base de Python
FROM python:3.8-slim-buster

# directorio de trabajo dentro del contenedor
WORKDIR /app

COPY requirements.txt .

# instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt