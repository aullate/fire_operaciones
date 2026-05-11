import subprocess
import sys
import typer
from src.pipeline import run as pipeline_run, run_async as pipeline_run_async, get_status

app = typer.Typer(help="fire_operaciones — WhatsApp investment parser CLI")


@app.command()
def run(
    full: bool = typer.Option(False, "--full", help="Borra el histórico DuckDB y reprocesa todo desde cero"),
    use_batch_api: bool = typer.Option(False, "--async", help="Usa Anthropic Batch API (50% más barato, puede tardar hasta 24h)"),
    poll: int = typer.Option(300, "--poll", help="Segundos entre comprobaciones del estado del batch (solo con --async)"),
):
    """Procesa operaciones.txt (incremental o completo) y lanza el dashboard."""
    mode = "completo" if full else "incremental"
    api_mode = "batch-api" if use_batch_api else "sync"
    typer.echo(f"\n── fire run [{mode}] [{api_mode}] ─────────────────────────")

    if use_batch_api:
        msgs_processed, ops_added = pipeline_run_async(full=full, log=typer.echo, poll_s=poll)
    else:
        msgs_processed, ops_added = pipeline_run(full=full, log=typer.echo)

    if msgs_processed == 0:
        typer.echo("  No hay mensajes nuevos.")
    else:
        typer.echo(f"── Resumen ────────────────────────────────────────────")
        typer.echo(f"  Mensajes procesados      : {msgs_processed}")
        typer.echo(f"  Operaciones guardadas    : {ops_added}")

    typer.echo("\nArrancando dashboard...")
    subprocess.run([sys.executable, "-m", "streamlit", "run", "src/app/main.py"], check=True)


@app.command()
def status():
    """Muestra el estado local (DuckDB, mensajes) y verifica la API key."""
    import os
    from dotenv import load_dotenv
    import anthropic

    # --- Local state ---
    s = get_status()
    last = s["last_fecha"].strftime("%d/%m/%Y %H:%M") if s["last_fecha"] else "—"
    typer.echo(f"Mensajes en .txt      : {s['total_raw']}")
    typer.echo(f"Mensajes pendientes   : {s['pending_messages']}  (filtrados, listos para LLM)")
    typer.echo(f"Operaciones en DuckDB : {s['total_ops']}")
    typer.echo(f"Ultimo registro       : {last}")

    # --- API key check ---
    typer.echo("")
    load_dotenv()
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        typer.echo("API key            : NO encontrada en .env")
        return

    try:
        client = anthropic.Anthropic(api_key=key)
        client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{"role": "user", "content": "Di solo: OK"}],
        )
        typer.echo(f"API key            : OK ({key[:8]}...{key[-4:]})")
    except anthropic.AuthenticationError:
        typer.echo(f"API key            : INVALIDA ({key[:8]}...{key[-4:]})")
    except anthropic.BadRequestError as e:
        if "credit balance" in str(e).lower():
            typer.echo(f"API key            : SIN CREDITOS — ve a console.anthropic.com/billing")
        else:
            typer.echo(f"API key            : ERROR — {e}")
    except Exception as e:
        typer.echo(f"API key            : ERROR — {e}")


@app.command(name="app")
def app_cmd():
    """Lanza el dashboard Streamlit sin reprocesar."""
    typer.echo("Arrancando dashboard...")
    subprocess.run([sys.executable, "-m", "streamlit", "run", "src/app/main.py"], check=True)


if __name__ == "__main__":
    app()
