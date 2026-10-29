import os
import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# 1. Configuración de conexión automática:
# En Docker usa 'kafka:19092', en consola local usa 'localhost:9092'
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "rapidgo_demanda_zonas")
ID_EQUIPO = "grupo_03"

# Zonas y categorías del modelo de negocio
ZONAS = ["ZONA_NORTE", "ZONA_CENTRO", "ZONA_SUR", "ZONA_OCCIDENTE"]
CATEGORIAS = ["restaurante", "supermercado", "farmacia", "express"]
ESTADOS = ["CREADO", "EN_CAMINO", "ENTREGADO", "CANCELADO"]


def conectar_kafka() -> KafkaProducer:
    """Intenta conectarse a Kafka con reintentos hasta que el broker esté listo."""
    print(f" Conectando a Kafka en: {KAFKA_SERVER}...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3
            )
            print(f" Conectado a Kafka. Transmitiendo en topic: '{TOPIC}'")
            return producer
        except Exception as error:
            print(f" Esperando a que Kafka inicie... ({error}). Reintentando en 3s...")
            time.sleep(3)


def generar_pedido(zona_forzada: str = None, forzar_creado: bool = False) -> dict:
    """Genera un evento idéntico al contrato StructType de tu Spark."""
    zona = zona_forzada if zona_forzada else random.choices(
        ZONAS, weights=[0.2, 0.5, 0.15, 0.15]
    )[0]

    estado = "CREADO" if forzar_creado else random.choices(
        ESTADOS, weights=[0.6, 0.2, 0.15, 0.05]
    )[0]

    return {
        "pedido_id": f"PED-{uuid.uuid4().hex[:6].upper()}",
        "zona": zona,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "categoria": random.choice(CATEGORIAS),
        "estado": estado
    }


def iniciar_produccion():
    producer = conectar_kafka()
    print("Generador de eventos de RapidGo iniciado.")
    print("Presiona Ctrl + C para detener.\n")

    contador = 0
    try:
        while True:
            contador += 1

            # SIMULACIÓN 1: Cada 35 pedidos inyecta 55 pedidos rápidos a ZONA_CENTRO
            # Esto activará la ALERTA 1 en Spark: ALERTA_DEMANDA_SUPERIOR_UMBRAL = CRITICAL (> 50 ped/min)
            if contador % 35 == 0:
                print("\n[SIMULACIÓN] Disparando PICO DE DEMANDA (>50 ped/min) en ZONA_CENTRO...")
                for _ in range(55):
                    pedido = generar_pedido(zona_forzada="ZONA_CENTRO")
                    producer.send(topic=TOPIC, key=pedido["zona"], value=pedido)
                    time.sleep(0.01)
                producer.flush()
                print("55 pedidos enviados. Spark marcará 'CRITICAL' en kpi_demanda.\n")

            # SIMULACIÓN 2: Cada 70 pedidos inyecta 85 pedidos en estado CREADO
            # Esto activará la ALERTA 2 en Spark: ALERTA_ZONA_SIN_CAPACIDAD = CRITICAL (> 80 en espera)
            elif contador % 70 == 0:
                print("\n[SIMULACIÓN] Disparando SATURACIÓN DE CAPACIDAD (>80 CREADO) en ZONA_NORTE...")
                for _ in range(85):
                    pedido = generar_pedido(zona_forzada="ZONA_NORTE", forzar_creado=True)
                    producer.send(topic=TOPIC, key=pedido["zona"], value=pedido)
                    time.sleep(0.01)
                producer.flush()
                print("85 pedidos CREADO enviados. Spark marcará 'CRITICAL' en alerta_capacidad.\n")

            # Flujo regular de operación (1 pedido cada segundo aprox.)
            else:
                pedido = generar_pedido()
                producer.send(topic=TOPIC, key=pedido["zona"], value=pedido)
                print(f"[{pedido['timestamp']}] {pedido['pedido_id']} | {pedido['zona']} | {pedido['categoria']} | {pedido['estado']}")
                time.sleep(random.uniform(0.6, 1.2))

    except KeyboardInterrupt:
        print("\n Productor detenido.")
    finally:
        producer.close()


if __name__ == "__main__":
    iniciar_produccion()