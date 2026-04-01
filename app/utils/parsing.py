from datetime import datetime


def _norm_shift(s: str) -> str:
    s = str(s or "").strip().lower()
    if "mañ" in s or "man" in s or s == "m":
        return "morning"
    if "tard" in s or s == "t":
        return "afternoon"
    return ""


def _parse_date_cell(v):
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None