import json
from pathlib import Path
import pytest

from producer.contrato import (
    CAMPOS,
    ESTADOS,
    ZONAS,
    EventoInvalido,
    validar_evento,
)

SAMPLE = Path(__file__).resolve().parents[1] / "samples" / "example_event.json"


def cargar_sample() -> dict:
    return json.loads(SAMPLE.read_text(encoding="utf-8"))


def test_sample_cumple_el_contrato():
    evento = cargar_sample()

    assert validar_evento(evento) == evento
    assert set(evento) == set(CAMPOS)
    assert evento["estado"] in ESTADOS
    assert evento["zona"] in ZONAS


@pytest.mark.parametrize("campo", CAMPOS)
def test_campo_faltante_es_invalido(campo):
    evento = cargar_sample()
    del evento[campo]

    with pytest.raises(EventoInvalido):
        validar_evento(evento)


@pytest.mark.parametrize(
    ("campo", "valor_invalido"),
    [
        ("zona", "ZONA_FANTASMA"),
        ("categoria", "ferreteria"),
        ("estado", "creado"),
        ("timestamp", "21/10/2026 22:30"),
        ("pedido_id", ""),
        ("pedido_id", None),
    ],
)
def test_valores_invalidos(campo, valor_invalido):
    evento = cargar_sample()
    evento[campo] = valor_invalido

    with pytest.raises(EventoInvalido):
        validar_evento(evento)


def test_evento_que_no_es_diccionario():
    with pytest.raises(EventoInvalido):
        validar_evento(["pedido_id"])