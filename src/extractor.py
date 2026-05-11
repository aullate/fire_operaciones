import re
from datetime import datetime

# Matches: "DD/MM/YY, HH:MM - "
_TIMESTAMP_RE = re.compile(r"^(\d{1,2}/\d{1,2}/\d{2,4}),\s(\d{1,2}:\d{2})\s-\s")

# Matches the user portion: either a phone (+34 ...) or a name, followed by ": "
_USER_RE = re.compile(r"^(\+[\d\s]+|[^:]+?):\s(.+)$", re.DOTALL)

# System message patterns to discard (checked against full line and message body)
_SYSTEM_PATTERNS = re.compile(
    r"(cifrados de extremo a extremo|"
    r"añadió a|"
    r"te añadió|"
    r"creó el grupo|"
    r"eliminó a|"
    r"salió del grupo|"
    r"cambió el (icono|asunto|descripción)|"
    r"Cambió tu código de seguridad|"
    r"mensajes y las llamadas están cifrados)",
    re.IGNORECASE,
)

# WhatsApp invisible char prefix on system-style messages
_LTR_MARK = "‎"


def _normalize_user(user: str) -> str:
    if user.startswith("+"):
        return re.sub(r"\s+", "", user)
    return user.strip()


def _parse_date(date_str: str, time_str: str) -> datetime:
    for fmt in ("%d/%m/%y %H:%M", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(f"{date_str} {time_str}", fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str} {time_str}")


def _flush(raw_messages: list, date_str: str, time_str: str, user: str, lines: list[str]) -> None:
    mensaje = "\n".join(lines).strip()
    if not mensaje:
        return
    if not user:
        return
    if _LTR_MARK in user:
        return
    if _SYSTEM_PATTERNS.search(mensaje):
        return
    try:
        fecha = _parse_date(date_str, time_str)
    except ValueError:
        return
    raw_messages.append({
        "fecha": fecha,
        "usuario": _normalize_user(user),
        "mensaje": mensaje,
    })


def count_raw_messages(txt_path: str) -> int:
    count = 0
    with open(txt_path, encoding="utf-8") as f:
        for line in f:
            if _TIMESTAMP_RE.match(line) and ":" in line[20:]:
                count += 1
    return count


def extract_messages(txt_path: str) -> list[dict]:
    raw_messages: list[dict] = []
    current: dict | None = None
    current_lines: list[str] = []

    with open(txt_path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            ts_match = _TIMESTAMP_RE.match(line)
            if ts_match:
                # Flush previous message
                if current:
                    _flush(raw_messages, current["date"], current["time"], current["user"], current_lines)
                after_ts = line[ts_match.end():]
                user_match = _USER_RE.match(after_ts)
                if user_match:
                    current = {"date": ts_match.group(1), "time": ts_match.group(2), "user": user_match.group(1)}
                    current_lines = [user_match.group(2)]
                else:
                    # System message — mark to discard
                    current = {"date": ts_match.group(1), "time": ts_match.group(2), "user": ""}
                    current_lines = [after_ts]
            else:
                # Continuation of previous message
                if current is not None:
                    current_lines.append(line)

    if current:
        _flush(raw_messages, current["date"], current["time"], current["user"], current_lines)

    return raw_messages
