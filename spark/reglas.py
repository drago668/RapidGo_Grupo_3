"""Script de prueba que no levanta la infraestructura a traves de Docker

Umbrales aprobados:
    - ALERTA_DEMANDA_SUPERIOR_UMBRAL: WARNING > 35 ped/min, CRITICAL > 50 ped/min
    - ALERTA_ZONA_SIN_CAPACIDAD:       CRITICAL > 80 pedidos CREADO sin asignar
"""

UMBRAL_DEMANDA_WARNING = 35
UMBRAL_DEMANDA_CRITICAL = 50
UMBRAL_CAPACIDAD_CRITICAL = 80

NIVEL_NORMAL = "NORMAL"
NIVEL_WARNING = "WARNING"
NIVEL_CRITICAL = "CRITICAL"


def _validar_conteo(valor, nombre: str) -> float:
    """Rechaza valores que no representan un conteo valido de pedidos."""
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        raise ValueError(
            f"'{nombre}' debe ser un numero, se recibio {type(valor).__name__}"
        )
    if valor < 0:
        raise ValueError(f"'{nombre}' no puede ser negativo")
    return valor


def clasificar_demanda(pedidos_por_minuto: float) -> str:
    """Clasifica la demanda de una zona segun los umbrales de la Variante 5.

    >>> clasificar_demanda(20)
    'NORMAL'
    >>> clasificar_demanda(50)
    'WARNING'
    >>> clasificar_demanda(51)
    'CRITICAL'
    """
    valor = _validar_conteo(pedidos_por_minuto, "pedidos_por_minuto")

    if valor > UMBRAL_DEMANDA_CRITICAL:
        return NIVEL_CRITICAL
    if valor > UMBRAL_DEMANDA_WARNING:
        return NIVEL_WARNING
    return NIVEL_NORMAL


def clasificar_capacidad(pedidos_creados: float) -> str:
    """Clasifica la acumulacion de pedidos CREADO sin asignar en una zona.

    >>> clasificar_capacidad(80)
    'NORMAL'
    >>> clasificar_capacidad(81)
    'CRITICAL'
    """
    valor = _validar_conteo(pedidos_creados, "pedidos_creados")

    if valor > UMBRAL_CAPACIDAD_CRITICAL:
        return NIVEL_CRITICAL
    return NIVEL_NORMAL