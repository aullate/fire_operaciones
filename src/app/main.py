import duckdb
import streamlit as st

DB_PATH = "data/operaciones.duckdb"

st.set_page_config(page_title="FIRE Operaciones", layout="wide")
st.title("FIRE — Operaciones de inversión")

try:
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT
            m.fecha,
            m.usuario,
            m.mensaje,
            o.ticker,
            o.nombre_empresa,
            o.tipo_activo,
            o.accion,
            o.precio,
            o.divisa
        FROM llm_operaciones o
        JOIN llm_mensajes m ON o.mensaje_id = m.id
        ORDER BY m.fecha DESC
    """).df()
    con.close()
except Exception:
    st.warning("No hay datos aún. Ejecuta `uv run fire run` primero.")
    st.stop()

st.dataframe(df, use_container_width=True)
