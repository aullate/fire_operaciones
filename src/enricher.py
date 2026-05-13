from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
import logging
from pathlib import Path
import duckdb
import yfinance as yf

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

from src.pipeline import DB_PATH, _ensure_tables

EXCHANGE_SUFFIXES = [
    "",      # US — NYSE, NASDAQ
    ".MC",   # España — BME
    ".L",    # UK — LSE
    ".DE",   # Alemania — XETRA
    ".PA",   # Francia — Euronext Paris
    ".AS",   # Países Bajos — Euronext Amsterdam
    ".MI",   # Italia — Borsa Italiana
    ".SW",   # Suiza — SIX
    ".BR",   # Bélgica — Euronext Brussels
    ".VI",   # Austria — Wiener Börse
    ".ST",   # Suecia — Nasdaq Stockholm
    ".OL",   # Noruega — Oslo Børs
    ".CO",   # Dinamarca — Nasdaq Copenhagen
    ".HK",   # Hong Kong — HKEX
]

MAX_WORKERS = 3


def _is_valid(info: dict) -> bool:
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    return price is not None and price > 0


def resolve_ticker(ticker: str) -> tuple[str, dict] | None:
    """Try exchange suffixes until a valid yfinance ticker is found.
    Returns (resolved_ticker, info) or None."""
    for suffix in EXCHANGE_SUFFIXES:
        candidate = ticker + suffix
        try:
            info = yf.Ticker(candidate).info
            if _is_valid(info):
                return candidate, info
        except Exception:
            continue
    return None


def _fetch_one_history(ticker: str, ticker_yf: str, trade_date, currency: str | None) -> tuple:
    """Fetch close price for a single (ticker_yf, trade_date). Returns (ticker, ticker_yf, trade_date, close_price, currency)."""
    try:
        df = yf.Ticker(ticker_yf).history(
            start=trade_date,
            end=trade_date + timedelta(days=1),
            auto_adjust=True,
        )
        close_price = float(df["Close"].iloc[0]) if not df.empty else None
    except Exception:
        close_price = None
    return (ticker, ticker_yf, trade_date, close_price, currency)


def fetch_historical_prices(con: duckdb.DuckDBPyConnection, log=print) -> tuple[int, int]:
    """Fetch close prices for unique (ticker, trade_date) pairs not yet in historical_prices."""
    pairs = con.execute("""
        SELECT DISTINCT
            o.ticker,
            md.ticker_yf,
            CAST(m.fecha AS DATE) AS trade_date,
            md.currency
        FROM llm_operaciones o
        JOIN llm_mensajes m ON o.mensaje_id = m.id
        JOIN market_data md ON o.ticker = md.ticker
        WHERE o.ticker IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM historical_prices hp
              WHERE hp.ticker = o.ticker
                AND hp.trade_date = CAST(m.fecha AS DATE)
          )
        ORDER BY o.ticker, trade_date
    """).fetchall()

    total = len(pairs)
    log(f"  Precios históricos: {total} pares únicos (ticker, fecha) pendientes")
    if total == 0:
        return 0, 0

    results: list[tuple] = [None] * total
    futures_map = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for i, (ticker, ticker_yf, trade_date, currency) in enumerate(pairs):
            fut = pool.submit(_fetch_one_history, ticker, ticker_yf, trade_date, currency)
            futures_map[fut] = i

        for fut in as_completed(futures_map):
            results[futures_map[fut]] = fut.result()

    fetched = 0
    for i, (ticker, ticker_yf, trade_date, close_price, currency) in enumerate(results, 1):
        con.execute(
            """
            INSERT INTO historical_prices (ticker, ticker_yf, trade_date, close_price, currency)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (ticker, trade_date) DO UPDATE SET
                ticker_yf   = excluded.ticker_yf,
                close_price = excluded.close_price,
                currency    = excluded.currency,
                updated_at  = now()
            """,
            [ticker, ticker_yf, trade_date, close_price, currency],
        )
        status = f"{close_price:.4f} {currency}" if close_price is not None else "sin datos (festivo/finde)"
        log(f"  [{i:>3}/{total}] {ticker:<12} {str(trade_date):<12}  {status}")
        fetched += 1

    return total, fetched


def run_enrich(log=print) -> tuple[int, int]:
    con = duckdb.connect(str(DB_PATH))
    _ensure_tables(con)

    tickers: list[str] = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT ticker FROM llm_operaciones WHERE ticker IS NOT NULL ORDER BY ticker"
        ).fetchall()
    ]
    total = len(tickers)
    log(f"  Tickers únicos en llm_operaciones : {total}")
    log("")

    futures_map = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for ticker in tickers:
            fut = pool.submit(resolve_ticker, ticker)
            futures_map[fut] = ticker

        enriched = 0
        done = 0
        for fut in as_completed(futures_map):
            ticker = futures_map[fut]
            result = fut.result()
            done += 1

            if result is None:
                log(f"  [{done:>3}/{total}] {ticker:<12}  ✗ no encontrado")
                continue

            ticker_yf, info = result
            price = info.get("currentPrice") or info.get("regularMarketPrice")
            con.execute(
                """
                INSERT INTO market_data (ticker, ticker_yf, long_name, sector, industry, country, exchange, current_price, currency)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (ticker) DO UPDATE SET
                    ticker_yf     = excluded.ticker_yf,
                    long_name     = excluded.long_name,
                    sector        = excluded.sector,
                    industry      = excluded.industry,
                    country       = excluded.country,
                    exchange      = excluded.exchange,
                    current_price = excluded.current_price,
                    currency      = excluded.currency,
                    updated_at    = now()
                """,
                [
                    ticker,
                    ticker_yf,
                    info.get("longName"),
                    info.get("sector"),
                    info.get("industry"),
                    info.get("country"),
                    info.get("exchange"),
                    price,
                    info.get("currency"),
                ],
            )
            suffix_str = f" ({ticker_yf})" if ticker_yf != ticker else ""
            log(f"  [{done:>3}/{total}] {ticker:<12}{suffix_str:<12}  {info.get('longName', '')[:40]}")
            enriched += 1

    log("")
    fetch_historical_prices(con, log)

    con.close()
    return total, enriched
