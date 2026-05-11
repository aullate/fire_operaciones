from typing import Literal
from pydantic import BaseModel


class Operacion(BaseModel):
    es_operacion: bool
    ticker: str | None = None
    nombre_empresa: str | None = None
    tipo_activo: Literal["accion", "opcion_call", "opcion_put", "opcion", "etf", "fondo", "cripto", "otro"] | None = None
    accion: Literal["compra", "venta", "ampliacion", "reduccion"] | None = None
    precio: float | None = None
    divisa: str | None = None
    notas: str | None = None


class RespuestaLLM(BaseModel):
    operaciones: list[Operacion]


class RespuestaMensaje(BaseModel):
    """Resultado del LLM para un único mensaje dentro de un batch."""
    operaciones: list[Operacion]


class RespuestaBatch(BaseModel):
    """Resultado del LLM para un batch de N mensajes, en el mismo orden."""
    mensajes: list[RespuestaMensaje]
