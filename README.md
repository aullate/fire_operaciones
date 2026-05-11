# fire_operaciones

Pipeline ETL que extrae y estructura operaciones de inversión a partir de exportaciones de chats de WhatsApp. Proyecto personal para la comunidad FIRE.

## Qué hace

1. Lee `operaciones.txt` (exportación de WhatsApp) desde la raíz del repo
2. Extrae metadata (fecha, usuario) con Regex
3. Usa un LLM (Anthropic + Instructor) para identificar y estructurar operaciones financieras
4. Almacena el resultado en `data/operaciones.duckdb`
5. Lanza un dashboard Streamlit para consultar las operaciones

El procesamiento es **incremental**: solo se envían al LLM los mensajes nuevos (posteriores al último registro en DuckDB), ahorrando tokens en cada ejecución.

## Stack

- **Python 3.11+**
- **Regex** — extracción de metadata WhatsApp
- **Instructor + Pydantic** — structured output del LLM
- **Anthropic API** — modelo de lenguaje para parsear texto libre
- **Polars** — transformación de datos
- **DuckDB** — almacenamiento local analítico
- **Streamlit** — dashboard de consulta
- **uv** — gestión de dependencias

## Instalación

```bash
uv sync
cp .env.example .env
# Editar .env y añadir ANTHROPIC_API_KEY
```

## Uso

| Comando | Descripción |
|---|---|
| `uv run fire run` | Procesa mensajes nuevos (incremental) y lanza el dashboard |
| `uv run fire run --full` | Borra el histórico y reprocesa todo desde cero |
| `uv run fire run --async` | Usa Anthropic Batch API (50% más barato, puede tardar hasta 24h) |
| `uv run fire run --async --full` | Batch API + reprocesar todo desde cero |
| `uv run fire run --async --poll 60` | Batch API con check de estado cada 60s (default: 300s) |
| `uv run fire status` | Estado local (DuckDB, mensajes pendientes) + verificación de API key |
| `uv run fire app` | Lanza el dashboard sin reprocesar |

### Monitorizar un batch en curso

El progreso se imprime en consola cada 5 minutos con el batch ID y contadores. También puedes seguirlo en el panel de Anthropic:

```
https://platform.claude.com/workspaces/default/batches?batch=<BATCH_ID>
```

## Estructura

```
fire_operaciones/
├── src/
│   ├── schema.py        # Modelos Pydantic (contratos de datos)
│   ├── extractor.py     # Regex: WhatsApp .txt → mensajes estructurados
│   ├── parser.py        # LLM: texto libre → operaciones Pydantic
│   ├── pipeline.py      # Orquestación + escritura DuckDB
│   └── app/
│       ├── __init__.py
│       └── main.py      # Dashboard Streamlit (placeholder)
├── data/                    # Carpeta completa en .gitignore
│   ├── operaciones.txt      # Input WhatsApp (datos confidenciales)
│   └── operaciones.duckdb   # Base de datos local
├── tests/
│   └── dummy_data.txt   # Datos de prueba sintéticos
├── cli.py               # CLI: fire run [--full] | fire status | fire app
└── pyproject.toml
```

## Formato de entrada

Exportación estándar de WhatsApp:
```
DD/MM/YY, HH:MM - Usuario: Mensaje
```

Ejemplos válidos:
```
24/11/25, 12:37 - +34 600 00 00 02: Compro Novo Nordisk a 37€
27/11/25, 13:35 - Chus (FIRE): Compradas 42 acciones de Verallia × 23,74€ y 41 acciones de FDJU
```

## Reglas de negocio

- Un mensaje puede contener **N operaciones** — siempre se devuelve un array
- Mensajes no financieros ("Idem 😂", "Bajó un 9%") se descartan (`es_operacion: False`)
- Mensajes de sistema (cifrado, "añadió a", "creó el grupo") se filtran en el extractor
- Teléfonos normalizados: `+34 600 00 00 00` → `+34600000000`
