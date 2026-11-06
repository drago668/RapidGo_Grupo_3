# Checkpoint Semana 3 – Orquestación con Airflow

> **Fecha:** 2026-11-04 · **Release esperado:** `v0.3.0-orchestration`

### 1. Qué quedó funcionando esta semana

- DAG `rapid_go` con 6 etapas lógicas y 4 ramas paralelas en ingesta/procesamiento (11 task instances), visible y ejecutado desde la UI de Airflow.
- Docker Compose con healthchecks y `depends_on: condition: service_healthy` para Kafka, PostgreSQL y Spark Master (PR #28), eliminando las condiciones de carrera.
- Inicialización manual de Airflow (`airflow db migrate` + `airflow users create`) con `docker compose run`, tras descartar el entrypoint automático.
- El pipeline de la Semana 2 (producer → Kafka → Spark → Parquet) sigue operativo.
- PR #29 abierto: agrega `retries` y `xcom_push` explícito; aún no está en `develop`.

### 2. Valor o evidencia real observada en la ejecución

- Run `manual__2026-11-04T13:39:06`: estado `success`, duración **10,06 s**, 6 task instances en `try_number=1`.
- Salidas de los logs:
  - `Kafka ingesta: 4 pedidos`
  - `Control de calidad OK`
  - `Pedidos en estado crítico (CREADO): 4`
  - `Pedidos críticos por zona: {'ZONA_CENTRO': 2, 'ZONA_NORTE': 1, 'ZONA_SUR': 1}`
- UI de Airflow accesible en `http://localhost:8085`; se registraron accesos a `/home` y `/xcom/list/`.
- La ejecución corresponde a la versión de 6 tareas; el commit `f66ccc2` (4 ramas) es posterior y todavía no tiene evidencia de ejecución.

### 3. Decisión técnica tomada y por qué

Se eligió **inicialización manual de Airflow** en lugar del entrypoint automático. El script fallaba por CRLF/permisos, ejecutaba migraciones duplicadas entre webserver y scheduler y ocultaba los errores en el ciclo de vida del contenedor (commits `b19309e`, `58b2bf5`, `c31765b`, `dbf7e9e`). Los comandos manuales son visibles, repetibles y equivalentes en macOS y WSL 2.
> Adicionalmente para cumplir con la tarea de provocar fallos controlados, se crea el directorio [`errors`](https://github.com/drago668/RapidGo_Grupo_3/tree/main/docs/errors)  
### 4. Problema encontrado y cómo se resolvió

- **Problema:** `Connection refused` intermitentes entre Kafka, Spark y Airflow por condiciones de carrera.
  **Solución:** healthchecks nativos (`nc`, `pg_isready`) y `condition: service_healthy` en el compose (PR #28).
- **Problema:** el DAG no importaba después de un cambio.
  **Mensaje real:** `airflow.exceptions.AirflowException: DAG is missing the start_date parameter` (13:46).
  **Tareas afectadas:** todas; el scheduler desactivó el DAG a las 13:48.
  **Solución:** se corrigió el `start_date`; el `DagFileProcessor` volvió a reportar `0 errors`.
- **Pendiente:** el healthcheck de Spark usa `nc`, que no existe en la imagen (`/bin/sh: 1: nc: not found`), por lo que el master queda `unhealthy`. El fallo controlado en `quality_check` (Issues #25 y #26) aún no se ha provocado.

- **Validación del datalake:** se agregó una revisión al inicio del pipeline que verifica si hay archivos Parquet en la carpeta del datalake. Si la carpeta está vacía, la tarea `validar_datalake` falla y las siguientes se detienen, mostrando un error en la interfaz de Airflow. Esto ayuda a detectar temprano si no se generaron los datos esperados.
- **Validación de calidad:** la tarea `quality_check` revisa que pedido_id y zona no estén vacíos. Para probarlo se envió un pedido con zona vacía: la tarea falló y las siguientes se detuvieron. Al corregir el dato y usar Clear task, el DAG terminó en success. Ver docs/errors/error3.md.

### 5. Enlaces de trazabilidad

- **PR #28:** https://github.com/drago668/RapidGo_Grupo_3/pull/28
- **PR #29:** https://github.com/drago668/RapidGo_Grupo_3/pull/29
- **Issues:** [#21](https://github.com/drago668/RapidGo_Grupo_3/issues/21), [#22](https://github.com/drago668/RapidGo_Grupo_3/issues/22), [#23](https://github.com/drago668/RapidGo_Grupo_3/issues/23), [#24](https://github.com/drago668/RapidGo_Grupo_3/issues/24), [#25](https://github.com/drago668/RapidGo_Grupo_3/issues/25), [#26](https://github.com/drago668/RapidGo_Grupo_3/issues/26)
- **Commits:** `9bc19f1` (DAG de 6 tareas), `f66ccc2` (4 ramas), `62f6ae8` (healthchecks), `dbf7e9e` (entrypoint descartado)
