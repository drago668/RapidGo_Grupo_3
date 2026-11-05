from collections import Counter
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance

KAFKA_TOPIC = "rapidgo_demanda_zonas"


@dag(
    dag_id="rapid_go",
    schedule=None,
    start_date=datetime(2026, 10, 21, tzinfo=timezone.utc),
    catchup=False,
    # Configuración base de reintentos para todo el DAG
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=20),
    },
)
def pipeline():

    @task
    def trigger_source():
        print("Iniciando pipeline rapid_go")
        # XCom implícito: comparte metadato de inicio
        return {"estado": "INICIADO"}

    # RETRY CONFIGURADO: Justificado por ser ingesta de red/broker (fallo transitorio)
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def kafka_ingest(estado_previo: dict):
        print(f"Kafka topic={KAFKA_TOPIC} | estado previo={estado_previo['estado']}")
        ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pedidos = [
            {"pedido_id": "PED-001", "zona": "ZONA_CENTRO", "timestamp": ahora,
             "categoria": "restaurante", "estado": "CREADO"},
            {"pedido_id": "PED-002", "zona": "ZONA_CENTRO", "timestamp": ahora,
             "categoria": "farmacia", "estado": "CREADO"},
            {"pedido_id": "PED-003", "zona": "ZONA_NORTE", "timestamp": ahora,
             "categoria": "supermercado", "estado": "CREADO"},
            {"pedido_id": "PED-004", "zona": "ZONA_SUR", "timestamp": ahora,
             "categoria": "express", "estado": "CREADO"},
        ]
        print(f"Kafka ingesta: {len(pedidos)} pedidos")
        # XCom: pasa metadatos de estado y lista de pedidos a la siguiente tarea
        return {"estado": "CREADO", "pedidos": pedidos}

    @task
    def submit_spark_job(ingesta: dict):
        print(f"Spark procesa {len(ingesta['pedidos'])} pedidos")
        # El estado del proceso pasa a PROCESADO
        return {**ingesta, "estado": "PROCESADO"}

    @task
    def quality_check(datos_multiples: list):
        pedidos_unificados = []

        for datos in datos_multiples:
            if datos["estado"] != "PROCESADO":
                raise AirflowException(f"Calidad fallida: estado {datos['estado']}, se esperaba PROCESADO")
            for pedido in datos["pedidos"]:
                if not pedido["pedido_id"] or not pedido["zona"]:
                    raise AirflowException(f"Pedido inválido: {pedido}")
                
            pedidos_unificados.extend(datos["pedidos"])
        print("Control de calidad OK para las 4 ramas")
        return {"estado": "PROCESADO", "pedidos": pedidos_unificados}

    @task
    def analizar_alertas_demanda(datos: dict, ti: TaskInstance = None):
        criticos = sum(
            1 for pedido in datos["pedidos"] if pedido["estado"] == "CREADO"
        )
        print(f"Pedidos en estado crítico (CREADO): {criticos}")
        
        # XCom Explícito: guarda métrica clave consultable en la UI de Airflow
        if ti:
            ti.xcom_push(key="total_pedidos_criticos", value=criticos)
            
        return {**datos, "pedidos_criticos": criticos}

    @task
    def analizar_zonas_saturadas(datos: dict, ti: TaskInstance = None):
        conteo = Counter(
            pedido["zona"]
            for pedido in datos["pedidos"]
            if pedido["estado"] == "CREADO"
        )
        print(f"Pedidos críticos por zona: {dict(conteo)}")
        print("Ciclo rapid_go finalizado")
        
        # XCom Explícito: guarda el diccionario de zonas calientes
        if ti:
            ti.xcom_push(key="resumen_zonas_calientes", value=dict(conteo))
            
        return {"zonas": dict(conteo), "estado_final": "FINALIZADO"}

    inicio = trigger_source()
    ramas_procesadas = []

    for i in range(1, 5):
        ingesta = kafka_ingest.override(task_id=f"kafka_ingest_{i}")(inicio)
        procesado = submit_spark_job.override(task_id=f"submit_spark_job_{i}")(ingesta)
        ramas_procesadas.append(procesado)

    validado = quality_check(ramas_procesadas)
    alertas = analizar_alertas_demanda(validado)
    zonas = analizar_zonas_saturadas(validado)

dag_instance = pipeline()