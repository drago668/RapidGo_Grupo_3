FROM apache/airflow:2.7.2

# Copiar el script de entrada al contenedor
COPY entrypoint.sh /entrypoint.sh

# Dar permisos de ejecución al script
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Definir el entrypoint por defecto
ENTRYPOINT ["/entrypoint.sh"]