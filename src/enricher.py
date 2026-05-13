from pathlib import Path
import duckdb
import yfinance as yf

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
]


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

    enriched = 0
    for i, ticker in enumerate(tickers, 1):
        result = resolve_ticker(ticker)
        if result is None:
            log(f"  [{i:>3}/{total}] {ticker:<12}  ✗ no encontrado")
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
        log(f"  [{i:>3}/{total}] {ticker:<12}{suffix_str:<12}  {info.get('longName', '')[:40]}")
        enriched += 1

    con.close()
    return total, enriched
