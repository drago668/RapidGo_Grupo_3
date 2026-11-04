import pytest

from spark.reglas import (
    NIVEL_CRITICAL,
    NIVEL_NORMAL,
    NIVEL_WARNING,
    clasificar_capacidad,
    clasificar_demanda,
)


@pytest.mark.parametrize(
    ("pedidos_por_minuto", "esperado"),
    [
        (0, NIVEL_NORMAL),      # caso normal
        (20, NIVEL_NORMAL),     # caso normal
        (35, NIVEL_NORMAL),     # caso limite: 35 no supera el umbral
        (36, NIVEL_WARNING),    # caso limite: valor en WARNING
        (50, NIVEL_WARNING),    # caso limite: 50  que sigue siendo  WARNING
        (51, NIVEL_CRITICAL),   # caso de alerta
        (120, NIVEL_CRITICAL),  # caso de alerta
    ],
)
def test_clasificar_demanda(pedidos_por_minuto, esperado):
    assert clasificar_demanda(pedidos_por_minuto) == esperado


@pytest.mark.parametrize(
    ("pedidos_creados", "esperado"),
    [
        (0, NIVEL_NORMAL),       # caso normal
        (80, NIVEL_NORMAL),      # caso limite: 80 no supera el umbral
        (81, NIVEL_CRITICAL),    # caso limite: valor superior a 80 -- CRITICAL
        (500, NIVEL_CRITICAL),   # caso de alerta
    ],
)
def test_clasificar_capacidad(pedidos_creados, esperado):
    assert clasificar_capacidad(pedidos_creados) == esperado


@pytest.mark.parametrize("valor_invalido", [-1, -50.5])
def test_valores_negativos_son_invalidos(valor_invalido):
    with pytest.raises(ValueError):
        clasificar_demanda(valor_invalido)
    with pytest.raises(ValueError):
        clasificar_capacidad(valor_invalido)


@pytest.mark.parametrize("valor_invalido", [None, "alto", True])
def test_tipos_no_numericos_son_invalidos(valor_invalido):
    with pytest.raises(ValueError):
        clasificar_demanda(valor_invalido)
    with pytest.raises(ValueError):
        clasificar_capacidad(valor_invalido)