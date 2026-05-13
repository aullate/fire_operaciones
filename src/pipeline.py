from collections.abc import Callable
from datetime import datetime
from pathlib import Path
import duckdb

from src.extractor import extract_messages, count_raw_messages
from src.parser import (
    parse_messages, _MODEL, BATCH_SIZE, MAX_WORKERS,
    submit_batch_job, wait_for_batch_job, collect_batch_results,
)

DB_PATH = Path("data/operaciones.duckdb")
TXT_PATH = Path("data/operaciones.txt")


def _ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SEQUENCE IF NOT EXISTS llm_mensajes_id_seq START 1")
    con.execute("""
        CREATE TABLE IF NOT EXISTS llm_batches (
            batch_id    VARCHAR      PRIMARY KEY,
            started_at  TIMESTAMPTZ  NOT NULL,
            model       VARCHAR      NOT NULL,
            n_mensajes  INTEGER      NOT NULL,
            n_ops       INTEGER      NOT NULL,
            fallback    BOOLEAN      NOT NULL DEFAULT false,
            error       VARCHAR,
            inserted_at TIMESTAMPTZ  NOT NULL DEFAULT now()
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS llm_mensajes (
            id          INTEGER      PRIMARY KEY,
            batch_id    VARCHAR      REFERENCES llm_batches(batch_id),
            fecha       TIMESTAMP    NOT NULL,
            usuario     VARCHAR      NOT NULL,
            mensaje     VARCHAR      NOT NULL,
            inserted_at TIMESTAMPTZ  NOT NULL DEFAULT now()
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS llm_operaciones (
            mensaje_id     INTEGER      NOT NULL REFERENCES llm_mensajes(id),
            ticker         VARCHAR,
            nombre_empresa VARCHAR,
            tipo_activo    VARCHAR,
            accion         VARCHAR,
            precio         DOUBLE,
            divisa         VARCHAR,
            inserted_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
        )
    """)
    con.execute("COMMENT ON COLUMN llm_batches.batch_id    IS 'UUID del batch de procesamiento LLM'")
    con.execute("COMMENT ON COLUMN llm_batches.started_at  IS 'Timestamp de inicio de la llamada al LLM'")
    con.execute("COMMENT ON COLUMN llm_batches.model       IS 'Modelo Anthropic usado (ej. claude-haiku-4-5-20251001)'")
    con.execute("COMMENT ON COLUMN llm_batches.n_mensajes  IS 'Número de mensajes enviados en este batch'")
    con.execute("COMMENT ON COLUMN llm_batches.n_ops       IS 'Número de operaciones financieras detectadas'")
    con.execute("COMMENT ON COLUMN llm_batches.fallback    IS 'True si el LLM desalineó y se reprocesó mensaje a mensaje'")
    con.execute("COMMENT ON COLUMN llm_batches.error       IS 'Texto del error si lo hubo, null si ok'")
    con.execute("COMMENT ON COLUMN llm_batches.inserted_at IS 'Timestamp de inserción del registro en DuckDB'")
    con.execute("COMMENT ON COLUMN llm_mensajes.id          IS 'PK autoincremental'")
    con.execute("COMMENT ON COLUMN llm_mensajes.batch_id    IS 'Batch al que pertenece este mensaje'")
    con.execute("COMMENT ON COLUMN llm_mensajes.fecha       IS 'Timestamp del mensaje en WhatsApp'")
    con.execute("COMMENT ON COLUMN llm_mensajes.usuario     IS 'Nombre o teléfono normalizado del emisor'")
    con.execute("COMMENT ON COLUMN llm_mensajes.mensaje     IS 'Texto completo del mensaje (sin sufijo de edición)'")
    con.execute("COMMENT ON COLUMN llm_mensajes.inserted_at IS 'Timestamp de inserción del registro en DuckDB'")
    con.execute("COMMENT ON COLUMN llm_operaciones.mensaje_id     IS 'Mensaje origen de esta operación'")
    con.execute("COMMENT ON COLUMN llm_operaciones.ticker         IS 'Símbolo bursátil normalizado (sin prefijo $)'")
    con.execute("COMMENT ON COLUMN llm_operaciones.nombre_empresa IS 'Nombre de la empresa, generado por el LLM'")
    con.execute("COMMENT ON COLUMN llm_operaciones.tipo_activo    IS 'accion|opcion_call|opcion_put|etf|cripto|otro'")
    con.execute("COMMENT ON COLUMN llm_operaciones.accion         IS 'compra|venta|ampliacion|reduccion'")
    con.execute("COMMENT ON COLUMN llm_operaciones.precio         IS 'Precio unitario del activo'")
    con.execute("COMMENT ON COLUMN llm_operaciones.divisa         IS 'EUR|USD|GBP'")
    con.execute("COMMENT ON COLUMN llm_operaciones.inserted_at    IS 'Timestamp de inserción del registro en DuckDB'")
    con.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            ticker         VARCHAR      PRIMARY KEY,
            ticker_yf      VARCHAR,
            long_name      VARCHAR,
            sector         VARCHAR,
            industry       VARCHAR,
            country        VARCHAR,
            exchange       VARCHAR,
            current_price  DOUBLE,
            currency       VARCHAR,
            updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
        )
    """)
    con.execute("COMMENT ON COLUMN market_data.ticker         IS 'Ticker tal como aparece en llm_operaciones'")
    con.execute("COMMENT ON COLUMN market_data.ticker_yf      IS 'Ticker resuelto con sufijo de exchange (ej. AENA.MC)'")
    con.execute("COMMENT ON COLUMN market_data.long_name      IS 'Nombre oficial largo de la empresa (yfinance)'")
    con.execute("COMMENT ON COLUMN market_data.sector         IS 'Sector de actividad (yfinance)'")
    con.execute("COMMENT ON COLUMN market_data.industry       IS 'Industria específica (yfinance)'")
    con.execute("COMMENT ON COLUMN market_data.country        IS 'País de cotización (yfinance)'")
    con.execute("COMMENT ON COLUMN market_data.exchange       IS 'Bolsa donde cotiza (yfinance)'")
    con.execute("COMMENT ON COLUMN market_data.current_price  IS 'Precio de mercado en el momento del enriquecimiento'")
    con.execute("COMMENT ON COLUMN market_data.currency       IS 'Divisa del precio (yfinance)'")
    con.execute("COMMENT ON COLUMN market_data.updated_at     IS 'Última vez que se actualizaron estos datos'")


def _get_last_fecha(con: duckdb.DuckDBPyConnection) -> datetime:
    try:
        result = con.execute("SELECT MAX(fecha) FROM llm_mensajes").fetchone()
        if result and result[0] is not None:
            return result[0]
    except duckdb.CatalogException:
        pass
    return datetime.min


def get_status() -> dict:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        r_msg = con.execute("SELECT COUNT(*), MAX(fecha) FROM llm_mensajes").fetchone()
        total_msgs = r_msg[0] if r_msg else 0
        last_fecha = r_msg[1] if r_msg else None
        r_ops = con.execute("SELECT COUNT(*) FROM llm_operaciones").fetchone()
        total_ops = r_ops[0] if r_ops else 0
    except duckdb.CatalogException:
        total_msgs = 0
        total_ops = 0
        last_fecha = None
    con.close()

    total_raw = count_raw_messages(str(TXT_PATH)) if TXT_PATH.exists() else 0
    all_messages = extract_messages(str(TXT_PATH)) if TXT_PATH.exists() else []
    cutoff = last_fecha if last_fecha else datetime.min
    pending = sum(1 for m in all_messages if m["fecha"] > cutoff)

    return {
        "total_msgs": total_msgs,
        "total_ops": total_ops,
        "last_fecha": last_fecha,
        "pending_messages": pending,
        "total_raw": total_raw,
    }


def run(full: bool = False, log: Callable[[str], None] = print) -> tuple[int, int]:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    if full:
        con.execute("DROP TABLE IF EXISTS llm_operaciones")
        con.execute("DROP TABLE IF EXISTS llm_mensajes")
        con.execute("DROP TABLE IF EXISTS llm_batches")
        con.execute("DROP SEQUENCE IF EXISTS llm_mensajes_id_seq")

    _ensure_tables(con)

    # --- Extracción (regex, sin LLM) ---
    last_fecha = _get_last_fecha(con)
    all_messages = extract_messages(str(TXT_PATH))
    total_raw = len(all_messages)
    new_messages = [m for m in all_messages if m["fecha"] > last_fecha]
    skipped = total_raw - len(new_messages)

    n = len(new_messages)
    n_batches = -(-n // BATCH_SIZE)
    log(f"  Extracción  : {total_raw} mensajes en .txt")
    log(f"               {skipped} ya en DuckDB (saltados)")
    log(f"               {n} nuevos → LLM")
    log(f"  LLM         : {_MODEL}  |  batch={BATCH_SIZE}  |  workers={MAX_WORKERS}  |  {n_batches} batches")
    log("")

    if not new_messages:
        con.close()
        return 0, 0

    # --- Llamadas LLM por batch ---
    total_ops_added = 0
    msgs_processed = 0

    for batch_result in parse_messages(new_messages):
        batch_ops = sum(
            1 for _, ops in batch_result.pairs for op in ops if op.es_operacion
        )

        # Insertar auditoría del batch PRIMERO (llm_batches es padre de llm_mensajes)
        con.execute(
            "INSERT INTO llm_batches (batch_id, started_at, model, n_mensajes, n_ops, fallback, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                batch_result.batch_id,
                batch_result.started_at,
                _MODEL,
                len(batch_result.batch),
                batch_ops,
                batch_result.fallback,
                batch_result.error,
            ],
        )

        # Insertar cada mensaje del batch y sus operaciones
        for msg_dict, operaciones_list in batch_result.pairs:
            row = con.execute(
                "INSERT INTO llm_mensajes (id, batch_id, fecha, usuario, mensaje) VALUES (nextval('llm_mensajes_id_seq'), ?, ?, ?, ?) RETURNING id",
                [batch_result.batch_id, msg_dict["fecha"], msg_dict["usuario"], msg_dict["mensaje"]],
            ).fetchone()
            mensaje_id = row[0]

            ops_this_msg = 0
            for op in operaciones_list:
                if not op.es_operacion:
                    continue
                con.execute(
                    "INSERT INTO llm_operaciones (mensaje_id, ticker, nombre_empresa, tipo_activo, accion, precio, divisa) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [mensaje_id, op.ticker, op.nombre_empresa, op.tipo_activo,
                     op.accion, op.precio, op.divisa],
                )
                total_ops_added += 1
                ops_this_msg += 1

            msgs_processed += 1
            fecha_str = msg_dict["fecha"].strftime("%d/%m/%y %H:%M")
            op_str = f"+{ops_this_msg} op" if ops_this_msg else "·"
            log(f"  [{msgs_processed:>4}/{n}] {fecha_str}  {msg_dict['usuario'][:20]:<20}  {op_str}")

        status = "FALLBACK" if batch_result.fallback else "ok"
        error_short = batch_result.error.splitlines()[0] if batch_result.error else ""
        error_str = f"  ⚠ {error_short}" if error_short else ""
        log(f"  ── batch {batch_result.batch_id[:8]}  {len(batch_result.batch)} msgs  "
            f"{batch_ops} ops  [{status}]{error_str}")
        log("")

    con.close()
    return msgs_processed, total_ops_added


def run_async(
    full: bool = False,
    log: Callable[[str], None] = print,
    poll_s: int = 60,
) -> tuple[int, int]:
    """Like run(), but uses the Anthropic Messages Batch API (async, 50% cheaper).

    Submits all messages, polls until complete, then writes results to DuckDB.
    Can take up to 24 hours — progress is logged while polling.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    if full:
        con.execute("DROP TABLE IF EXISTS llm_operaciones")
        con.execute("DROP TABLE IF EXISTS llm_mensajes")
        con.execute("DROP TABLE IF EXISTS llm_batches")
        con.execute("DROP SEQUENCE IF EXISTS llm_mensajes_id_seq")

    _ensure_tables(con)

    last_fecha = _get_last_fecha(con)
    all_messages = extract_messages(str(TXT_PATH))
    new_messages = [m for m in all_messages if m["fecha"] > last_fecha]
    skipped = len(all_messages) - len(new_messages)
    n = len(new_messages)

    log(f"  Extracción  : {len(all_messages)} mensajes en .txt")
    log(f"               {skipped} ya en DuckDB (saltados)")
    log(f"               {n} nuevos → Anthropic Batch API")
    log(f"  Modelo      : {_MODEL}")
    log("")

    if not new_messages:
        con.close()
        return 0, 0, 0, 0

    # Submit all messages in one Batch API job
    log("  Enviando al Batch API...")
    batch_job_id = submit_batch_job(new_messages)
    log(f"  Job ID: {batch_job_id}")
    log(f"  Esperando resultados (poll cada {poll_s}s, puede tardar hasta 24h)...")
    log("")

    wait_for_batch_job(batch_job_id, log=log, poll_s=poll_s)

    log("")
    log("  Procesando resultados...")
    batch_results = collect_batch_results(batch_job_id, new_messages)

    total_ops_added = 0
    msgs_processed = 0

    for batch_result in batch_results:
        batch_ops = sum(
            1 for _, ops in batch_result.pairs for op in ops if op.es_operacion
        )

        con.execute(
            "INSERT INTO llm_batches (batch_id, started_at, model, n_mensajes, n_ops, fallback, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                batch_result.batch_id,
                batch_result.started_at,
                _MODEL,
                len(batch_result.batch),
                batch_ops,
                batch_result.fallback,
                batch_result.error,
            ],
        )

        for msg_dict, operaciones_list in batch_result.pairs:
            row = con.execute(
                "INSERT INTO llm_mensajes (id, batch_id, fecha, usuario, mensaje) VALUES (nextval('llm_mensajes_id_seq'), ?, ?, ?, ?) RETURNING id",
                [batch_result.batch_id, msg_dict["fecha"], msg_dict["usuario"], msg_dict["mensaje"]],
            ).fetchone()
            mensaje_id = row[0]

            for op in operaciones_list:
                if not op.es_operacion:
                    continue
                con.execute(
                    "INSERT INTO llm_operaciones (mensaje_id, ticker, nombre_empresa, tipo_activo, accion, precio, divisa) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [mensaje_id, op.ticker, op.nombre_empresa, op.tipo_activo,
                     op.accion, op.precio, op.divisa],
                )
                total_ops_added += 1

            msgs_processed += 1

    fallbacks = sum(1 for br in batch_results if br.fallback)
    errors = sum(1 for br in batch_results if br.error)

    con.close()
    return msgs_processed, total_ops_added, fallbacks, errors
