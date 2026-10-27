from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Configuración base
TOPIC = "rapidgo_demanda_zonas"
KAFKA_SERVER = "kafka:19092"
BASE_PATH = "/opt/spark/work-dir/datalake"

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

# 3. PARSEO Y 3 TRANSFORMACIONES MÍNIMAS
eventos_procesados = (raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    # Transformación 1: Parsear JSON a columnas estructuradas
    .withColumn("data", F.from_json("json_value", schema))
    .select("data.*")
    # Transformación 2: Convertir string a TimestampType real para ventanas temporales
    .withColumn("event_time", F.to_timestamp("timestamp"))
    # Transformación 3: Regla de Alerta 1 (Capacidad) a nivel de evento individual
    .withColumn("alerta_estado", F.when(F.col("estado").isin("cancelado", "rechazado"), "CRITICA_CAPACIDAD").otherwise("NORMAL"))
)

# 4. AGREGACIÓN POR VENTANA Y REGLA DE ALERTA 2 (Picos de demanda)
metricas_demanda = (eventos_procesados
    .withWatermark("event_time", "2 minutes")
    # Agregación por ventana de 1 minuto agrupada por zona
    .groupBy(F.window("event_time", "1 minute"), "zona")
    .agg(F.count("*").alias("pedidos_por_minuto"))
    # Regla de Alerta 2: Demanda superior al umbral (ej. 40 pedidos/min)
    .withColumn("alerta_demanda", F.when(F.col("pedidos_por_minuto") > 40, "PICO_DEMANDA").otherwise("NORMAL"))
)

# 5. PERSISTENCIA EN PARQUET Y CHECKPOINTS
# Escritura de los eventos individuales (Processed)
query_eventos = (eventos_procesados.writeStream
    .format("parquet")
    .option("path", f"{BASE_PATH}/processed/eventos")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/eventos")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())

# Escritura de los KPIs y Alertas de demanda por ventana (Curated)
query_metricas = (metricas_demanda.writeStream
    .format("parquet")
    .option("path", f"{BASE_PATH}/curated/metricas_demanda")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoints/metricas")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())

query_eventos.awaitTermination()
query_metricas.awaitTermination()