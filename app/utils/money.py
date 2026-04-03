import re

def safe_float(v) -> float:
    """Convierte strings numéricos estilo AR a float."""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()
    if not s:
        return 0.0

    # limpiar símbolos
    s = s.replace("$", "").replace(" ", "")
    s = re.sub(r"[^0-9,\.\-]", "", s)

    if not s or s in ("-", ",", "."):
        return 0.0

    # Caso: tiene coma y punto
    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            # 1,234.56 (USA)
            s = s.replace(",", "")
        else:
            # 1.234,56 (AR)
            s = s.replace(".", "").replace(",", ".")
        return float(s)

    # Solo coma
    if "," in s:
        if re.match(r"^-?\d{1,3}(,\d{3})+$", s):
            # miles con coma
            return float(s.replace(",", ""))
        return float(s.replace(",", "."))

    # Solo punto
    if "." in s:
        if re.match(r"^-?\d{1,3}(\.\d{3})+$", s):
            # miles con punto
            return float(s.replace(".", ""))
        return float(s)

    return float(s)


def ars(v) -> str:
    """Formatea número a estilo AR ($ 1.234,56)"""
    try:
        n = float(v or 0)
    except:
        n = 0.0

    s = f"{n:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"$ {s}"