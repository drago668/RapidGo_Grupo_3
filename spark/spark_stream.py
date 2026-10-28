from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Configuración base
TOPIC = "rapidgo_demanda_zonas"
KAFKA_SERVER = "kafka:19092"
BASE_PATH = "/datalake"

# Inicializar SparkSession
spark = (SparkSession.builder
    .appName("RapidGo_DemandaZonas")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. ESQUEMA DEL CONTRATO DE EVENTO
schema = T.StructType([
    T.StructField("pedido_id", T.StringType()),
    T.StructField("zona", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("categoria", T.StringType()),
    T.StructField("estado", T.StringType())
])

# 2. CONSUMO DESDE KAFKA
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load())

# 3. PARSEO BASICO 
eventos_procesados = (raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .withColumn("data", F.from_json("json_value", schema))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
)

# 4. CÁLCULO DE KPIs Y ALERTAS SEGÚN REGLAS DE NEGOCIO
# KPI 1 y Alerta 1: Tasa de Demanda por Zona
kpi_demanda = (eventos_procesados
    .withWatermark("event_time", "5 minutes")
    # Ventana deslizante de 5 min, paso de 1 min[cite: 17]
    .groupBy(F.window("event_time", "5 minutes", "1 minute"), "zona")
    .agg(F.count("*").alias("kpi_pedidos_por_minuto"))
    # Alerta 1: Umbrales >35 Warning, >50 Critical[cite: 18]
    .withColumn("ALERTA_DEMANDA_SUPERIOR_UMBRAL", 
        F.when(F.col("kpi_pedidos_por_minuto") > 50, "CRITICAL")
         .when(F.col("kpi_pedidos_por_minuto") > 35, "WARNING")
         .otherwise("NORMAL")
    )
)

# Alerta 2: Zona sin Capacidad 
alerta_capacidad = (eventos_procesados
    # Filtrar solo estado CREADO[cite: 18]
    .filter(F.col("estado") == "CREADO")
    .withWatermark("event_time", "10 minutes")
    # Ventana fija de 10 min para evaluar acumulación[cite: 18]
    .groupBy(F.window("event_time", "10 minutes"), "zona")
    .agg(F.count("*").alias("pedidos_creados_espera"))
    # Disparador: > 80 pedidos[cite: 18]
    .withColumn("ALERTA_ZONA_SIN_CAPACIDAD",
        F.when(F.col("pedidos_creados_espera") > 80, "CRITICAL")
         .otherwise("NORMAL")
    )
)

# KPI 3: Demanda por Franja y Categoría
kpi_franja_categoria = (eventos_procesados
    .withWatermark("event_time", "1 hour")
    # Ventana fija de 1 hora agrupada por categoría[cite: 17]
    .groupBy(F.window("event_time", "1 hour"), "categoria")
    .agg(F.count("*").alias("kpi_demanda_franja_categoria"))
)

# 5. PERSISTENCIA EN PARQUET Y CHECKPOINTS

# Flujo 1: Escritura de los eventos individuales (Processed)
query_eventos = (eventos_procesados.writeStream
    .format("parquet")
    .option("path", f"{BASE_PATH}/processed/eventos")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/eventos")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())

# Flujo 2: Escritura de KPI 1 y Alerta 1 (Demanda por zona)
query_kpi_demanda = (kpi_demanda.writeStream
    .format("parquet")
    .option("path", f"{BASE_PATH}/curated/kpi_demanda")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/kpi_demanda")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())

# Flujo 3: Escritura de Alerta 2 (Zona sin capacidad)
query_alerta_capacidad = (alerta_capacidad.writeStream
    .format("parquet")
    .option("path", f"{BASE_PATH}/curated/alerta_capacidad")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/alerta_capacidad")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())

# Flujo 4: Escritura de KPI 3 (Demanda por franja y categoría)
query_kpi_franja = (kpi_franja_categoria.writeStream
    .format("parquet")
    .option("path", f"{BASE_PATH}/curated/kpi_franja_categoria")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/kpi_franja_categoria")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())

# Mantener la ejecución activa para todos los flujos
spark.streams.awaitAnyTermination()