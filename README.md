# Proyecto de Integración de Datos Climáticos

## Descripción

Este proyecto extrae datos climáticos de las principales ciudades de Argentina utilizando la API de OpenWeatherMap y los carga en una base de datos Redshift. El proceso se automatiza mediante un DAG en Airflow y se ejecuta dentro de contenedores Docker.

## Estructura del Proyecto

- `dags/`: Contiene el archivo del DAG de Airflow.
- `scripts/`: Scripts Python para extraer y procesar los datos.
- `tests/`: Pruebas unitarias para las funciones del proyecto.
- `Dockerfile`: Archivo para construir la imagen Docker.
- `docker-compose.yml`: Configuración para levantar los servicios necesarios.
- `.github/workflows/ci.yml`: Workflow de GitHub Actions para ejecutar los tests.
- `env.example`: Archivo de ejemplo para configurar las variables de entorno.
- `entrypoint.sh`: Script bash que maneja la inicialización de Airflow y el inicio de los servicios.

## Requisitos Previos

- Docker y Docker Compose instalados en el sistema.
- Cuenta en OpenWeatherMap para obtener una API Key.
- Acceso a un clúster de Redshift y las credenciales correspondientes.

## Instalación y Configuración

1. **Clonar el repositorio:**

   ```bash
   git clone https://github.com/tu_usuario/tu_repositorio.git
   cd tu_repositorio

## Pruebas Unitarias

Este proyecto incluye pruebas unitarias para asegurar el correcto funcionamiento de las funciones principales.

### Ejecutar las pruebas localmente

Para ejecutar las pruebas en tu entorno local:

1. Asegúrate de tener las dependencias instaladas:

   ```bash
   pip install -r requirements.txt