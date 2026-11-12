 

# RapidGo Grupo 3

## Índice

- [Miembros del grupo](#miembros-del-grupo)
- [Problema de negocio](#problema-de-negocio)
- [Arquitectura de la solución](#arquitectura-de-la-solucion)
- [Tecnologías usadas](#tecnologias-usadas)
- [Prerrequisitos](#prerequisitos)
- [Cómo ejecutar localmente](#como-ejecutar-localmente)
- [KPIs y reglas de alerta](#indicadores-clave-de-rendimiento-kpis-y-reglas-de-alerta)
- [Topics y contrato de eventos](#topics-y-contrato-de-eventos-kafka)
- [Pruebas y GitHub Actions](#cómo-ejecutar-pruebas-y-github-actions)
- [Airflow y DAG](#cómo-acceder-a-airflow-y-ejecutar-el-dag)

### *Variante 5* **demanda por zonas**

> **Eventos:** zona, timestamp, categoria, estado
> **KPIs:** pedidos/minuto, zona caliente, franja
> **Alertas:** demanda alta / capacidad insuficiente

## Miembros del grupo

- Daniel Esteban Rivera Gomez
- Lusia Valentina Rodriguez Perez
- Shirley Dayana Perez Esquivel

## Problema de negocio

RapidGo es una plataforma de entregas que necesita una plataforma de datos capaz de capturar eventos operacionales, procesarlos, almacenarlos, generar indicadores y automatizar procesos de control. Todos los equipos trabajarán sobre la misma organización, pero resolverán problemas diferentes.

### Valor de la solución

Mmmmm

---

## Arquitectura de la solucion

![arquitectura](image/README/arquitectura.svg)

## Tecnologias usadas


| Capa                 | Tecnología                                                     | Versión                    | Uso en el proyecto                                                                                                            |
| :--------------------- | :---------------------------------------------------------------- | :---------------------------- | :------------------------------------------------------------------------------------------------------------------------------ |
| Contenedores         | Docker + Docker Compose                                         | Compose v2                  | Orquesta los 7 servicios, la red `pipeline-net`, los volúmenes y los healthchecks.                                           |
| Mensajería          | Apache Kafka (KRaft)                                            | 4.3.1                       | Bus de eventos. Topic `rapidgo_demanda_zonas`, key `zona`, listeners `9092` (host), `19092` (interno) y `29093` (controller). |
| Procesamiento        | Apache Spark + Structured Streaming                             | 4.2.0                       | Master + 1 worker (1 core, 768 MB). Ventanas deslizantes/fijas, watermarks y escritura en Parquet.                            |
| Orquestación        | Apache Airflow                                                  | 2.7.2                       | `LocalExecutor`; webserver en `8085`; DAG `rapid_go` con 4 ramas paralelas, reintentos y XCom.                                |
| Metadatos            | PostgreSQL                                                      | 13                          | Base de datos de Airflow (`airflow`), con healthcheck `pg_isready`.                                                           |
| Lenguaje             | Python                                                          | 3.12 (producer) / 3.13 (CI) | Generación de eventos y ejecución de pruebas.                                                                               |
| Cliente Kafka        | kafka-python-ng (contenedor) / kafka-python 3.0.12 (local y CI) | —                          | Publicación de eventos y pruebas del producer.                                                                               |
| API de Spark         | PySpark                                                         | 4.2.0                       | Dependencia local alineada con la versión del clúster.                                                                      |
| Almacenamiento       | Apache Parquet                                                  | —                          | Data Lake en `processed/` (eventos) y `curated/` (KPIs y alertas).                                                            |
| Pruebas              | pytest                                                          | —                          | 33 pruebas del contrato, reglas de alerta y producer.                                                                         |
| CI                   | GitHub Actions                                                  | —                          | `compileall`, `pytest` y `docker compose config` en `main` y `Develop`.                                                       |
| Diagramas            | PlantUML + Mermaid                                              | —                          | Diagramas de infraestructura y flujo de datos.                                                                                |
| Control de versiones | Git + GitHub                                                    | —                          | Ramas, PR, issues, releases y ADR.                                                                                            |

### Roles

## Prerequisitos

Para poder ejecutar esta solucion de forma local debes contar con las siguietenes herramientas para poder ejecutar el proyecto

- **Docker:** es una herramienta que permite empaquetar una aplicación junto con todo lo que necesita para funcionar dentro de una caja virtual llamada "contenedor"  Gracias a esto, un programa corre exactamente igual en cualquier computadora, sin importar si es Windows, Mac o Linux.  Si no lo tienes instalado pues obtenerlo en [docker](https://www.docker.com/)

  *Para comprobar si lo tienes instalado dirigete a la terminal de tu equipo y ejecuta*:

  ```shell
    docker --version
  ```

  esto mostrara por terminal algun mensaje como: `Docker version 27.x.x, build....`
- **Git:** es un sistema de control de versiones que permite descargar el código fuente del proyecto y gestionar todos sus cambios e historial de forma ordenada. Si no lo tienes instalado pues obtenerlo en [Git](https://git-scm.com)

  *Para comprobar si lo tienes instalado dirígete a la terminal de tu equipo y ejecuta*:

  ```shell
    git --version
  ```

  esto mostrará por terminal algún mensaje como: `git version 2.x.x...`
- Puertos libres: 9092, 7077, 8081, 8082, 8085.
- Recomendado: 8 GB de RAM para Docker.
- Opcional para pruebas: Python 3.12 o 3.13.

## Como ejecutar localmente

Para poder acceder y correr de forma local este flujo de trabajo puedes seguir los siguientes pasos de configuracion:

1. Dirigete a la carpeta donde deseas trabajar este proyecto `cd  /Users/user/documentos/`
2. Crea una carpeta para contener este proyecto `mkdir rapid_go`
3. Muevete a esta carpeta que haz creado `cd rapid_go`
4. Trae el proyecto a tu equipo local con el comando  `git clone https://github.com/drago668/RapidGo_Grupo_3.git` lo cual traera todo el contenido de este repositorio para que lo ejecutes
5. Una vez termine este proceso se creara la carpeta `RapidGo_Grupo_3` donde queda contenido todo el codigo, ahora entra a dicha carpeta con `cd  RapidGo_Grupo_3`
6. la estructura del protecto debe quedar tal que asi:

```bash
├── airflow
│   └── dags
│       └── rapid_go_dag.py
├── conftest.py
├── data
│   ├── curated
│   ├── processed
│   └── raw
├── datalake
│   └── README.md
├── docker-compose.yml
├── Dockerfile
├── docs
│   ├── checkpoints
│   │   ├── semana1.md
│   │   ├── semana2.md
│   │   └── semana3.md
│   ├── decisions
│   │   ├── ADR-001.md
│   │   ├── ADR-002.md
│   │   └── ADR-003.md
│   └── errors
│       ├── error1.md
│       ├── error2.md
│       ├── error3.md
│       └── image
│           └── error1
├── entrypoint.sh
├── producer
│   ├── __init__.py
│   ├── contrato.py
│   ├── Dockerfile
│   └── producer.py
├── README.md
├── requirements-dev.txt
├── requirements.txt
├── samples
│   └── example_event.json
├── spark
│   ├── __init__.py
│   ├── reglas.py
│   └── spark_stream.py
└── tests
    ├── test_contrato.py
    ├── test_producer.py
    └── test_reglas.py
```

7. docker compose up -d (con lo cual construiras toda la infraestructura del proyecto)
8. 

## Indicadores Clave de Rendimiento (KPIs) y Reglas de Alerta

* **Riesgo Principal:** Saturación y Déficit de Flota
* **Problema:** Picos masivos de solicitudes por zona que superan la capacidad de preparación en cocinas y la disponibilidad de repartidores, generando pedidos en estado `CREADO` sin asignar, incumplimiento de SLA y cancelaciones.

---

### 1. Definición de KPIs Mínimos


| KPI                                        | Métrica Técnica              | Ventana de Cálculo              | Unidad        | Objetivo Operacional                                                                                                                     |
| :------------------------------------------- | :------------------------------- | :--------------------------------- | :-------------- | :----------------------------------------------------------------------------------------------------------------------------------------- |
| **KPI 1: Tasa de Demanda por Zona**        | `kpi_pedidos_por_minuto`       | Deslizante de 5 min (paso 1 min) | `pedidos/min` | Monitorear el ritmo de entrada de órdenes en tiempo real para anticipar congestión en comercios y escasez de repartidores.             |
| **KPI 2: Concentración de Zona Caliente** | `kpi_share_demanda_zona`       | Fija (*tumbling*) de 15 min      | `% del total` | Detectar cuadrantes que absorben una porción desproporcionada de la demanda metropolitana para redistribuir la flota.                   |
| **KPI 3: Demanda por Franja y Categoría** | `kpi_demanda_franja_categoria` | Fija de 1 hora                   | `conteo`      | Segmentar el volumen de órdenes por horario (pico almuerzo, pico cena, valle) y tipo de comercio (Restaurante, Supermercado, Farmacia). |

#### Fórmulas de Cálculo (Procesamiento en Spark)

* **KPI 1 (Rendimiento):**

  $$
  \text{Throughput}(z, t) = \frac{\text{Conteo de pedidos en zona } z \text{ en la ventana } [t - 5\text{ min}, t]}{5\text{ minutos}}

  $$
* **KPI 2 (% Participación):**

  $$
  \ \text{ Demanda}(z) = \left( \frac{\text{Pedidos en Zona } z}{\sum_{k \in \text{Zonas}} \text{Pedidos en Zona } k} \right) \times 100

  $$
* **KPI 3 (Segmentación horaria):**

  $$
  \text{Volumen}(f, c) = \text{COUNT}(\text{pedidos}) \quad \forall \text{ timestamp} \in \text{Franja } f \land \text{categoría} = c

  $$

---

### 2. Reglas de Alerta Operacionales y Umbrales Iniciales


| Alerta                             | Evento Disparador                  | Umbral Inicial                                               | Nivel      | Acción Operativa Automatizada                                                                                                                            |
| :----------------------------------- | :----------------------------------- | :------------------------------------------------------------- | :----------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ALERTA_DEMANDA_SUPERIOR_UMBRAL** | Picos de solicitudes por zona      | **Warning:** $> 35$ ped/min**Critical:** $> 50$ ped/min      | `CRITICAL` | 1. Activar tarifa dinámica (*surge pricing*) para regular la entrada de órdenes.2. Notificar a repartidores en zonas aledañas con incentivo por viaje. |
| **ALERTA_ZONA_SIN_CAPACIDAD**      | Acumulación de órdenes en espera | $> 80$ pedidos en estado `CREADO` sin asignar por $> 10$ min | `CRITICAL` | 1. Pausa preventiva de admisión de nuevos pedidos en la app para esa zona (*pause orders*).2. Rebalanceo forzado de la flota.                            |

#### Justificación Técnica y de Negocio de los Umbrales

* **Justificación Alerta 1 (Sobredemanda > 50 ped/min):**
  La capacidad promedio estimada de preparación de los comercios aliados en un radio de cobertura zonal es de 25 a 30 pedidos por minuto. Superar los 35 pedidos/minuto satura los tiempos de preparación en cocinas, y superar los 50 pedidos/minuto agota los repartidores disponibles en el cuadrante, elevando exponencialmente el tiempo de entrega.
* **Justificación Alerta 2 (Capacidad Insuficiente > 80 pedidos sin asignar por > 10 min):**
  Tener más de 80 pedidos sin chofer asignado durante más de 10 minutos garantiza que el tiempo total de entrega superará los 45 minutos (violando el SLA prometido de RapidGo). Esta alerta protege la reputación del servicio evitando cancelaciones masivas.

---
