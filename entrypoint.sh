#!/bin/bash

# 1. Ejecutar las migraciones de la base de datos
echo "Ejecutando migraciones de Airflow..."
airflow db migrate

# 2. Crear el usuario administrador (el '|| true' evita que falle si el usuario ya fue creado antes)
echo "Verificando/creando usuario administrador..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# 3. Ejecutar el comando original del contenedor (webserver o scheduler)
exec airflow "$@" 