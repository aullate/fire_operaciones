import json
import os
import time
import uuid
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
import anthropic
import instructor
from dotenv import load_dotenv
from src.schema import Operacion, RespuestaBatch, RespuestaMensaje

load_dotenv()

_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "25"))

_client = instructor.from_anthropic(anthropic.Anthropic())

_SYSTEM_PROMPT = """
Eres un extractor de operaciones de inversión de mensajes de WhatsApp de un grupo de inversión FIRE (Financial Independence, Retire Early) en español.

Recibirás un batch de mensajes numerados. Para CADA mensaje debes devolver sus operaciones en el mismo orden.

REGLAS por mensaje:
1. Devuelve SIEMPRE un array de operaciones para ese mensaje, incluso si solo hay una.
2. Si el mensaje NO es una operación financiera clara, devuelve un único elemento con es_operacion=false.
3. Si hay MÚLTIPLES activos en un mensaje, devuelve UNA operación por activo.
4. El campo "accion" indica el tipo de movimiento (compra/venta/ampliacion/reduccion), NO el tipo de activo.
5. "ampliacion" = añadir más a una posición ya existente ("compro más", "añado", "amplío").
6. "reduccion" = vender parte de una posición existente.
7. tipo_activo: usa "accion" para acciones y SOCIMIs/REITs que cotizan individualmente, "etf" para ETFs (incluidos ETFs de REITs), "fondo" para fondos de inversión, "opcion_call"/"opcion_put" si se especifica el tipo, "opcion" si es una opción sin especificar, "cripto" para criptomonedas, "otro" para el resto. Compras de inmuebles físicos (pisos, estudios) NO son operaciones financieras → es_operacion: false.
8. Divisa: usa "EUR" si el precio tiene € o es un mercado europeo sin símbolo explícito, "USD" si tiene $ o es mercado americano, "GBP" si tiene £.
9. Limpia el ticker: elimina el prefijo $ (ej. "$VICI" → "VICI").
10. Si solo hay nombre de empresa sin ticker (ej. "Novo Nordisk"), deja ticker=null y rellena nombre_empresa.

EJEMPLOS (mensaje individual):

"Compro Novo Nordisk a 37€"
→ operaciones: [{es_operacion: true, ticker: "NVO", nombre_empresa: "Novo Nordisk", tipo_activo: "accion", accion: "compra", precio: 37.0, divisa: "EUR"}]

"Compro 11 acciones más de Nvo a 38,38 euros y cierro posición espero jeje"
→ operaciones: [{es_operacion: true, ticker: "NVO", nombre_empresa: "Novo Nordisk", tipo_activo: "accion", accion: "ampliacion", precio: 38.38, divisa: "EUR"}]

"Vendidas 200 acciones de VERALLIA (VRLA) a 23,80€"
→ operaciones: [{es_operacion: true, ticker: "VRLA", nombre_empresa: "Verallia", tipo_activo: "accion", accion: "venta", precio: 23.80, divisa: "EUR"}]

"Compras de hoy en UK\nLGEN 1450 a 2,4130\nDGEO 116 a 17.3650"
→ operaciones: [{es_operacion: true, ticker: "LGEN", tipo_activo: "accion", accion: "compra", precio: 2.413, divisa: "GBP"}, {es_operacion: true, ticker: "DGEO", tipo_activo: "accion", accion: "compra", precio: 17.365, divisa: "GBP"}]

"Compras del mes: $PRU + $O + $PFE"
→ operaciones: [{es_operacion: true, ticker: "PRU", tipo_activo: "accion", accion: "compra", precio: null, divisa: "USD"}, ...]

"Idem 😂" → operaciones: [{es_operacion: false}]
"Bajó un 9 %" → operaciones: [{es_operacion: false}]

Usa punto como separador decimal en los precios (convierte comas a puntos).
El array "mensajes" del response debe tener exactamente el mismo número de elementos que mensajes recibas.
"""


@dataclass
class BatchResult:
    batch_id: str
    batch: list[dict]
    response: RespuestaBatch | None
    started_at: datetime
    duration_s: float
    error: str | None = None
    fallback: bool = False
    # set after alignment check
    pairs: list[tuple[dict, list[Operacion]]] = field(default_factory=list)


def _parse_batch(batch: list[dict]) -> BatchResult:
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    numbered = "\n\n".join(
        f"[{i+1}] {msg['mensaje']}" for i, msg in enumerate(batch)
    )
    try:
        response: RespuestaBatch = _client.chat.completions.create(
            model=_MODEL,
            max_tokens=4096,
            system=[{"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": numbered}],
            response_model=RespuestaBatch,
        )
        duration_s = time.perf_counter() - t0

        # Alineación: si el LLM devuelve distinto número de respuestas, fallback 1-a-1
        if len(response.mensajes) != len(batch):
            result = BatchResult(
                batch_id=batch_id, batch=batch, response=None,
                started_at=started_at, duration_s=duration_s,
                error=f"desalineación: {len(response.mensajes)} respuestas para {len(batch)} mensajes",
                fallback=True,
            )
            result.pairs = _fallback_one_by_one(batch, batch_id)
            return result

        result = BatchResult(
            batch_id=batch_id, batch=batch, response=response,
            started_at=started_at, duration_s=duration_s,
        )
        result.pairs = list(zip(batch, [r.operaciones for r in response.mensajes]))
        return result

    except Exception as e:
        duration_s = time.perf_counter() - t0
        result = BatchResult(
            batch_id=batch_id, batch=batch, response=None,
            started_at=started_at, duration_s=duration_s,
            error=str(e), fallback=True,
        )
        result.pairs = _fallback_one_by_one(batch, batch_id)
        return result


def _fallback_one_by_one(batch: list[dict], batch_id: str) -> list[tuple[dict, list[Operacion]]]:
    """Procesa cada mensaje individualmente cuando falla la alineación del batch."""
    pairs = []
    for msg in batch:
        try:
            response: RespuestaBatch = _client.chat.completions.create(
                model=_MODEL,
                max_tokens=1024,
                system=[{"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": f"[1] {msg['mensaje']}"}],
                response_model=RespuestaBatch,
            )
            ops = response.mensajes[0].operaciones if response.mensajes else []
        except Exception:
            ops = []
        pairs.append((msg, ops))
    return pairs


def parse_messages(messages: list[dict]) -> Generator[BatchResult, None, None]:
    batches = [messages[i:i + BATCH_SIZE] for i in range(0, len(messages), BATCH_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_parse_batch, batch): batch for batch in batches}
        for future in as_completed(futures):
            yield future.result()


# ── Anthropic Messages Batch API (async, 50% cheaper) ────────────────────────

_raw_client = anthropic.Anthropic()

BATCH_API_POLL_S = int(os.getenv("BATCH_API_POLL_S", "300"))


_JSON_SUFFIX = """

Responde ÚNICAMENTE con un JSON válido con esta estructura exacta (sin markdown, sin explicación):
{"operaciones": [{"es_operacion": true/false, "ticker": "...", "nombre_empresa": "...", "tipo_activo": "...", "accion": "...", "precio": 0.0, "divisa": "...", "notas": "..."}]}

Todos los campos opcionales pueden ser null."""


def submit_batch_job(messages: list[dict]) -> str:
    """Submits all messages to the Anthropic Batch API. Returns the batch job ID."""
    requests = [
        {
            "custom_id": str(i),
            "params": {
                "model": _MODEL,
                "max_tokens": 1024,
                "system": [{"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                "messages": [{"role": "user", "content": f"[1] {msg['mensaje']}{_JSON_SUFFIX}"}],
            },
        }
        for i, msg in enumerate(messages)
    ]

    batch = _raw_client.messages.batches.create(requests=requests)
    return batch.id


def wait_for_batch_job(
    batch_id: str,
    log: object = print,
    poll_s: int = BATCH_API_POLL_S,
) -> None:
    """Polls until the batch job is complete. Raises RuntimeError if cancelled/expired."""
    import time as _time
    while True:
        batch = _raw_client.messages.batches.retrieve(batch_id)
        status = batch.processing_status
        counts = batch.request_counts
        ts = datetime.now().strftime("%H:%M:%S")
        log(f"  [{ts}] {batch_id[:12]}  status={status}  processing={counts.processing}  "
            f"succeeded={counts.succeeded}  errored={counts.errored}")
        if status == "canceling":
            log(f"  [{ts}] Batch cancelado externamente, esperando confirmación...")
        if status == "ended":
            if counts.succeeded == 0:
                raise RuntimeError(
                    f"Batch {batch_id[:12]} terminó sin resultados "
                    f"(canceled={counts.canceled}  errored={counts.errored})"
                )
            break
        _time.sleep(poll_s)


def collect_batch_results(
    batch_id: str,
    messages: list[dict],
) -> list[BatchResult]:
    """Reads results from a completed batch job and returns one BatchResult per message."""
    started_at = datetime.now(timezone.utc)

    # Index succeeded message objects by custom_id
    result_map: dict[str, object] = {}
    for item in _raw_client.messages.batches.results(batch_id):
        if item.result.type == "succeeded":
            result_map[item.custom_id] = item.result.message

    batch_results: list[BatchResult] = []
    for i, msg in enumerate(messages):
        uid = str(uuid.uuid4())
        raw_msg = result_map.get(str(i))

        if raw_msg is None:
            br = BatchResult(
                batch_id=uid, batch=[msg], response=None,
                started_at=started_at, duration_s=0.0,
                error="no result from batch API", fallback=True,
            )
            br.pairs = [(msg, [])]
            batch_results.append(br)
            continue

        try:
            text = next(b.text for b in raw_msg.content if b.type == "text")
            # Strip markdown code fences if model wraps JSON
            text = text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            data = json.loads(text)
            # LLM sometimes returns RespuestaBatch format {"mensajes": [...]} instead of
            # RespuestaMensaje format {"operaciones": [...]} — normalize it
            if "mensajes" in data and "operaciones" not in data:
                msgs_data = data["mensajes"]
                ops = []
                for m in (msgs_data if isinstance(msgs_data, list) else [msgs_data]):
                    if isinstance(m, dict) and "operaciones" in m:
                        ops.extend(m["operaciones"])
                    elif isinstance(m, list):
                        ops.extend(m)
                data = {"operaciones": ops}
            parsed: RespuestaMensaje = RespuestaMensaje.model_validate(data)
            br = BatchResult(
                batch_id=uid, batch=[msg], response=None,
                started_at=started_at, duration_s=0.0,
            )
            br.pairs = [(msg, parsed.operaciones)]
        except Exception as e:
            br = BatchResult(
                batch_id=uid, batch=[msg], response=None,
                started_at=started_at, duration_s=0.0,
                error=str(e), fallback=True,
            )
            br.pairs = [(msg, [])]

        batch_results.append(br)

    return batch_results
