# Checkpoint Semana 1 – Arquitectura y Fundamentos
---

### 1. Qué quedó funcionando esta semana
* **Estructura modular del repositorio:** Directorios organizados para todo el ciclo de vida (`producer/`, `spark/`, `airflow/`, `datalake/`, `docs/`, `samples/`, `tests/`).
* **Contratos JSON de eventos:** Definición formal y validada de `samples/pedido_evento.json` y del evento de alerta `samples/alerta_evento.json`.
* **Especificación de métricas de negocio:** Definición de 3 KPIs operacionales (`pedidos_por_minuto`, `share_demanda_zona` y `demanda_franja_categoria`) y 2 reglas de alerta operacionales con umbrales iniciales justificados.
* **Gobierno y gestión del proyecto:** Tablero Kanban activo en GitHub Projects con el backlog de las 4 semanas y asignación de responsables.
* **Decisión de Arquitectura:** Redacción y aprobación del documento `docs/decisions/ADR-001.md`.

---

### 2. Valor o evidencia real observada en la ejecución
* **Consistencia del esquema:** Se validó que el contrato de evento en `samples/` contiene de forma estricta los campos requeridos para las funciones de ventana en Spark (`zona_id`, `timestamp`, `categoria`, `estado`, `valor_total`).
* **Topología de Kafka:** Se definió el topic principal `rapidgo.pedidos` configurado con 4 particiones para balancear la carga de eventos.
* **Trazabilidad de equipo:** Repositorio público configurado con los 3 integrantes como colaboradores y flujo de trabajo basado en ramas e Issues.

---

### 3. Decisión técnica más importante y por qué (ADR-001)
* **Decisión:** Usar `zona_id` como clave (*message key*) de particionamiento en Kafka con 4 particiones físicas.
* **Justificación técnica:** Dado que la Variante 5 analiza la demanda zonal, el uso de `zona_id` como key asegura que todos los pedidos de un mismo cuadrante caigan siempre en la misma partición y mantengan orden temporal estricto. Esto optimiza el procesamiento en Spark Structured Streaming, eliminando el costo de operaciones de *shuffle* masivo entre nodos al calcular las ventanas de 5 minutos por zona.

---

### 4. Problema encontrado y riesgo principal
* **Problema/Riesgo:** Definir un umbral estático de alerta (> 50 ped/min) puede provocar falsas alarmas en zonas comerciales de alta densidad o pasar por alto saturaciones en zonas residenciales pequeñas donde 20 pedidos/min ya colapsan la flota local.
* **Cómo se mitiga:** Se adoptan los umbrales de 35 (Warning) y 50 (Critical) pedidos/min como valores base para el arranque de la Semana 1. En la Semana 2 se calibrarán observando los datos simulados reales del *producer* en Spark.

---

### 5. Plan de la Semana 2 y Enlaces de trazabilidad
#### Plan para la Semana 2 (`v0.2.0-streaming`):
1. Construir el generador en Python (`producer/`) emitiendo eventos continuos a Kafka.
2. Desarrollar el job de PySpark consumiendo de Kafka, aplicando ventanas deslizantes de 5 minutos y persistiendo en `datalake/` en formato Parquet.
3. Implementar las pruebas unitarias con `pytest` en `tests/` y habilitar el workflow de CI en GitHub Actions.

---

#### Trazabilidad en GitHub:
* **Release:** `v0.1.0-foundation`
* **Issue #1:** [Configuración Inicial](https://github.com/drago668/RapidGo_Grupo_3/issues/1#issue-5972061054)
* **Issue #2:** [Definición de contrato JSON](https://github.com/drago668/RapidGo_Grupo_3/issues/2#issue-5972080211)
* **Issue #3:** [Redacción documento ADR-001](https://github.com/drago668/RapidGo_Grupo_3/issues/3#issue-5972128454)
