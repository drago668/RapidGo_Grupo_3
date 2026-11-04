import random
from datetime import datetime

from producer import producer as prod
from producer.contrato import FORMATO_TIMESTAMP, validar_evento


def test_generar_pedido_cumple_el_contrato():
    pedido = prod.generar_pedido()

    assert validar_evento(pedido) == pedido
    assert pedido["pedido_id"].startswith("PED-")
    datetime.strptime(pedido["timestamp"], FORMATO_TIMESTAMP)


def test_generar_pedido_forzado():
    pedido = prod.generar_pedido(zona_forzada="ZONA_CENTRO", forzar_creado=True)

    assert pedido["zona"] == "ZONA_CENTRO"
    assert pedido["estado"] == "CREADO"


def test_generar_pedido_solo_usa_zonas_del_contrato():
    random.seed(42)
    zonas = {prod.generar_pedido()["zona"] for _ in range(200)}

    assert zonas <= set(prod.ZONAS)


def test_conectar_kafka_reintenta_hasta_lograr_conexion(monkeypatch):
    intentos = {"total": 0}

    class KafkaProducerFalso:
        pass

    def kafka_producer_falso(*args, **kwargs):
        intentos["total"] += 1
        if intentos["total"] < 3:
            raise ConnectionError("broker no disponible")
        return KafkaProducerFalso()

    monkeypatch.setattr(prod, "KafkaProducer", kafka_producer_falso)
    monkeypatch.setattr(prod.time, "sleep", lambda segundos: None)

    producer = prod.conectar_kafka()

    assert isinstance(producer, KafkaProducerFalso)
    assert intentos["total"] == 3