# Checkpoint Semana 2 – Pipeline Funcional y CI 
---
### 1. Qué quedó funcionando esta semana
* **Generador de eventos el producer/producer.py`:** Emisión continua de pedidos hacia el topic `rapidgo_demanda_zonas` en Kafka, utilizando `zona` como clave de partición (ADR-001) e inyección programada de ráfagas para disparar alertas.
* **Procesamiento en tiempo real el spark/spark_stream.py:** Spark Structured Streaming consumiendo de Kafka con 4 flujos activos de escritura:
  * `query_eventos`: Persistencia de eventos limpios en `/datalake/processed/eventos`.
  * `query_kpi_demanda`: Agregación de demanda en ventana deslizante de 5 min (paso 1 min) y evaluación de la regla `ALERTA_DEMANDA_SUPERIOR_UMBRAL` (> 50 ped/min = CRITICAL).
  * `query_alerta_capacidad`: Acumulación de órdenes en estado `CREADO` en ventana de 10 min evaluando `ALERTA_ZONA_SIN_CAPACIDAD` (> 80 pedidos en espera = CRITICAL).
  * `query_kpi_franja`: Conteo horario de demanda segmentado por categoría de comercio.
* **Persistencia y Resiliencia en Data Lake:** Creación exitosa de archivos Parquet particionados y directorios de control en `./data/checkpoints/` para tolerancia a fallos.
* **Integración Continua (CI):** Workflow activo en GitHub Actions (`ci.yml`) que valida automáticamente sintaxis (`compileall`), pruebas unitarias (`pytest`) y consistencia de `docker compose config`.
---
### 2. Valor o evidencia real observada en la ejecución
* **Disparo real de alerta en streaming:** Al ejecutar el Producer, la inyección controlada de una ráfaga de **55 pedidos consecutivos en menos de 2 segundos** sobre `ZONA_CENTRO` superó el umbral nominal de 40 pedidos/min, provocando que Spark generara en tiempo real la bandera `"CRITICAL"` en la columna `ALERTA_DEMANDA_SUPERIOR_UMBRAL`.
<img width="1243" height="811" alt="image" src="https://github.com/user-attachments/assets/a417b621-7b33-4375-be47-43e918319f3e" />
---
### 3. Decisión técnica más importante y por qué 
* **Decisión:** Creación del Producer de eventos con clave zona y automatización de la integración continua mediante GitHub Actions.
* **Justificación técnica:** El Producer asegura la generación de tráfico continuo respetando el orden por zona en Kafka, mientras que los Actions permiten validar la sintaxis y los umbrales de alerta de forma inmediata en cada commit, protegiendo la rama principal sin requerir clústeres pesados en la nube.
---
### 4. Problema encontrado y cómo se resolvió
* **Problema real:** Para lograr una ejecución a tiempo real sincronizada entre los componentes, la ejecución manual y externa del script presentaba dificultades de conectividad de red con el broker interno de Kafka (`kafka:19092`), afectando la fluidez del pipeline.
* **Cómo se resolvió:** Se resolvió contenerizando el Producer mediante un `Dockerfile` propio e integrándolo como servicio dentro de `docker-compose.yml`, asegurando que los eventos se transmitan de manera continua y en tiempo real dentro de la misma red compartida de Docker.
---
### 5. Enlaces a Issues, Pull Requests y Actions
* **Integrante 1 (Producer & Docker):**
  * PR: [https://github.com/drago668/RapidGo_Grupo_3/pull/17#issue-6071757962]
  * Issue: [https://github.com/drago668/RapidGo_Grupo_3/issues/4]
* **Integrante 2 (Spark Streaming & Data Lake):**
  * PR: [https://github.com/drago668/RapidGo_Grupo_3/pull/16#issue-6071735287]
  * Issue: [https://github.com/drago668/RapidGo_Grupo_3/issues/5]
* **Integrante 3 (Reglas de Negocio, Tests y CI):**
  * PR: [https://github.com/drago668/RapidGo_Grupo_3/pull/19#issue-6091904626]
  * Issue: [https://github.com/drago668/RapidGo_Grupo_3/issues/7]


