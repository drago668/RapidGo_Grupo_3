from datetime import datetime

CAMPOS = ("pedido_id", "zona", "timestamp", "categoria", "estado")

ZONAS = ("ZONA_NORTE", "ZONA_CENTRO", "ZONA_SUR", "ZONA_OCCIDENTE")
CATEGORIAS = ("restaurante", "supermercado", "farmacia", "express")
ESTADOS = ("CREADO", "EN_CAMINO", "ENTREGADO", "CANCELADO")

FORMATO_TIMESTAMP = "%Y-%m-%d %H:%M:%S"


class EventoInvalido(ValueError):
    """Se lanza cuando un evento no cumple el contrato del equipo."""


def validar_evento(evento: dict) -> dict:
    """Valida un evento contra el contrato y lo devuelve sin modificar."""
    if not isinstance(evento, dict):
        raise EventoInvalido("El evento debe ser un diccionario")

    faltantes = [campo for campo in CAMPOS if campo not in evento]
    if faltantes:
        raise EventoInvalido(
            "Faltan campos obligatorios: " + ", ".join(faltantes)
        )

    for campo in CAMPOS:
        valor = evento[campo]
        if not isinstance(valor, str) or not valor.strip():
            raise EventoInvalido(f"El campo '{campo}' debe ser texto no vacio")

    if evento["zona"] not in ZONAS:
        raise EventoInvalido(f"Zona desconocida: {evento['zona']}")
    if evento["categoria"] not in CATEGORIAS:
        raise EventoInvalido(f"Categoria desconocida: {evento['categoria']}")
    if evento["estado"] not in ESTADOS:
        raise EventoInvalido(f"Estado desconocido: {evento['estado']}")

    try:
        datetime.strptime(evento["timestamp"], FORMATO_TIMESTAMP)
    except ValueError as error:
        raise EventoInvalido(
            f"timestamp invalido: {evento['timestamp']} "
            f"(formato esperado {FORMATO_TIMESTAMP})"
        ) from error

    return evento