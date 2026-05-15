from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import timedelta
import logging
import time
import duckdb
import requests
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

# Stooq usa el mismo sufijo que yfinance para la mayoría de mercados europeos
STOOQ_SUFFIXES = [
    ".us",   # US
    ".mc",   # España
    ".uk",   # UK
    ".de",   # Alemania
    ".f",    # Frankfurt
    ".pa",   # Francia
    ".nl",   # Países Bajos
    ".it",   # Italia
    ".sw",   # Suiza
    ".be",   # Bélgica
    ".se",   # Suecia
    ".no",   # Noruega
    ".dk",   # Dinamarca
]

_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF = [1, 3, 7]  # segundos entre reintentos


def _is_valid(info: dict) -> bool:
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    return price is not None and price > 0


def _fetch_info_with_retry(candidate: str) -> dict:
    """Fetch yfinance info, retrying only on network exceptions (not on empty responses)."""
    for attempt, wait_s in enumerate(_RETRY_BACKOFF):
        try:
            info = yf.Ticker(candidate).info
            return info  # empty or not — caller decides validity
        except Exception:
            if attempt < _RETRY_ATTEMPTS - 1:
                time.sleep(wait_s)
    return {}


def _resolve_yfinance(ticker: str) -> tuple[str, dict] | None:
    """Try yfinance across exchange suffixes."""
    for suffix in EXCHANGE_SUFFIXES:
        candidate = ticker + suffix
        info = _fetch_info_with_retry(candidate)
        if _is_valid(info):
            return candidate, info
    return None


def _resolve_stooq(ticker: str) -> tuple[str, dict] | None:
    """Try Stooq across exchange suffixes via direct HTTP CSV API."""
    from datetime import date, timedelta as td
    import io
    import pandas as pd
    end = date.today()
    start = end - td(days=7)
    start_s, end_s = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

    for suffix in STOOQ_SUFFIXES:
        candidate = (ticker + suffix).lower()
        try:
            url = f"https://stooq.com/q/d/l/?s={candidate}&d1={start_s}&d2={end_s}&i=d"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200 or "No data" in resp.text or len(resp.text.strip()) < 30:
                continue
            df = pd.read_csv(io.StringIO(resp.text))
            if df.empty or "Close" not in df.columns:
                continue
            price = float(df["Close"].iloc[-1])
            if price <= 0:
                continue
            currency = "USD" if suffix == ".us" else None
            return candidate, {
                "currentPrice": price,
                "currency": currency,
                "longName": ticker,
                "source": "stooq",
            }
        except Exception:
            continue
    return None


def resolve_ticker(ticker: str) -> tuple[str, dict] | None:
    """Query yfinance and Stooq in parallel, return the first valid result."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(_resolve_yfinance, ticker): "yfinance",
            pool.submit(_resolve_stooq, ticker): "stooq",
        }
        done, pending = wait(futures, return_when=FIRST_COMPLETED)

        # Check completed futures first
        for fut in done:
            result = fut.result()
            if result is not None:
                # Cancel pending (best-effort)
                for p in pending:
                    p.cancel()
                return result

        # First completed returned None — wait for the other
        for fut in as_completed(pending):
            result = fut.result()
            if result is not None:
                return result

    return None


def _fetch_one_history(ticker: str, ticker_yf: str, trade_date, currency: str | None) -> tuple:
    """Fetch close price for a single (ticker_yf, trade_date). Returns (ticker, ticker_yf, trade_date, close_price, currency)."""
    for attempt, wait_s in enumerate(_RETRY_BACKOFF):
        try:
            df = yf.Ticker(ticker_yf).history(
                start=trade_date,
                end=trade_date + timedelta(days=1),
                auto_adjust=True,
            )
            if not df.empty:
                return (ticker, ticker_yf, trade_date, float(df["Close"].iloc[0]), currency)
            break  # Empty = festivo/finde, no retry
        except Exception:
            if attempt < _RETRY_ATTEMPTS - 1:
                time.sleep(wait_s)
    return (ticker, ticker_yf, trade_date, None, currency)


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
                AND hp.close_price IS NOT NULL
          )
        ORDER BY o.ticker, trade_date
    """).fetchall()

    total = len(pairs)
    log(f"  Precios históricos: {total} pares únicos (ticker, fecha) pendientes")
    if total == 0:
        return 0, 0

    fetched = 0
    for i, (ticker, ticker_yf, trade_date, currency) in enumerate(pairs, 1):
        _, _, _, close_price, currency = _fetch_one_history(ticker, ticker_yf, trade_date, currency)
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


def run_market(log=print) -> tuple[int, int]:
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

    enriched = 0
    done = 0
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(resolve_ticker, ticker): ticker for ticker in tickers}
        for fut in as_completed(futures):
            ticker = futures[fut]
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
