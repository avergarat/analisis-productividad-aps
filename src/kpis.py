"""
Cálculo de 10 KPIs de productividad APS.
Basado en el modelo de análisis SSMC - Servicio de Salud Metropolitano Central.
"""
import pandas as pd
import numpy as np

try:
    import streamlit as st
    _cache = st.cache_data
except Exception:
    # Permite importar kpis.py fuera de un contexto Streamlit (tests, scripts)
    def _cache(fn=None, **_kw):
        return fn if fn is not None else (lambda f: f)

# ──────────────────────────────────────────────
# Definición de KPIs: umbrales y alertas
# ──────────────────────────────────────────────
KPI_DEFINITIONS = {
    "ocupacion": {
        "nombre": "Tasa de Ocupación",
        "unidad": "%",
        "umbral_ok": 65,
        "umbral_alerta": 50,
        "direccion": "mayor_es_mejor",
        "descripcion": "Cupos citados / (citados + disponibles) × 100",
    },
    "no_show": {
        "nombre": "Tasa de No-Show",
        "unidad": "%",
        "umbral_ok": 10,
        "umbral_alerta": 15,
        "direccion": "menor_es_mejor",
        "descripcion": "(Citados - Completados) / Citados × 100",
    },
    "bloqueo": {
        "nombre": "Tasa de Bloqueo",
        "unidad": "%",
        "umbral_ok": 10,
        "umbral_alerta": 15,
        "direccion": "menor_es_mejor",
        "descripcion": "Bloqueados / Total × 100",
    },
    "efectividad": {
        "nombre": "Efectividad de Cita",
        "unidad": "%",
        "umbral_ok": 88,
        "umbral_alerta": 80,
        "direccion": "mayor_es_mejor",
        "descripcion": "Completados / Citados × 100",
    },
    "rendimiento": {
        "nombre": "Rendimiento Promedio",
        "unidad": "min",
        "umbral_ok": None,
        "umbral_alerta": 30,   # % desviación permitida
        "direccion": "referencia",
        "descripcion": "Promedio de minutos por atención",
    },
    "sobrecupo": {
        "nombre": "Cupos Sobrecupo",
        "unidad": "%",
        "umbral_ok": 5,
        "umbral_alerta": 10,
        "direccion": "menor_es_mejor",
        "descripcion": "Sobrecupos / Total × 100",
    },
    "cobertura_sectorial": {
        "nombre": "Cobertura Sectorial",
        "unidad": "%",
        "umbral_ok": 80,
        "umbral_alerta": 60,
        "direccion": "mayor_es_mejor",
        "descripcion": "Registros con sector informado / Total × 100",
    },
    "agendamiento_remoto": {
        "nombre": "Agendamiento Remoto",
        "unidad": "%",
        "umbral_ok": 20,
        "umbral_alerta": 5,
        "direccion": "mayor_es_mejor",
        "descripcion": "(Telefónico + Telesalud) / Total × 100",
    },
    "variacion_mensual": {
        "nombre": "Variación Mensual Ocupación",
        "unidad": "pp",
        "umbral_ok": 5,
        "umbral_alerta": 10,
        "direccion": "menor_es_mejor",
        "descripcion": "Cambio mes a mes en tasa de ocupación (puntos porcentuales)",
    },
    "ocupacion_extendida": {
        "nombre": "Ocupación Horario Extendido",
        "unidad": "%",
        "umbral_ok": 50,
        "umbral_alerta": 30,
        "direccion": "mayor_es_mejor",
        "descripcion": "Citados en hora ≥18:00 / (Citados + Disponibles) en horario extendido × 100",
    },
}


def _safe_pct(numerator: float, denominator: float) -> float:
    """División segura que retorna 0.0 si denominador es 0."""
    if denominator == 0 or pd.isna(denominator):
        return 0.0
    return round((numerator / denominator) * 100, 2)


def semaforo(valor: float, kpi_key: str) -> str:
    """
    Retorna 'verde', 'amarillo' o 'rojo' según el valor y los umbrales del KPI.
    """
    defn = KPI_DEFINITIONS.get(kpi_key, {})
    umbral_ok = defn.get("umbral_ok")
    umbral_alerta = defn.get("umbral_alerta")
    direccion = defn.get("direccion", "mayor_es_mejor")

    if umbral_ok is None:
        return "gris"

    if direccion == "mayor_es_mejor":
        if valor >= umbral_ok:
            return "verde"
        elif valor >= umbral_alerta:
            return "amarillo"
        else:
            return "rojo"
    elif direccion == "menor_es_mejor":
        if valor <= umbral_ok:
            return "verde"
        elif valor <= umbral_alerta:
            return "amarillo"
        else:
            return "rojo"
    return "gris"


# ──────────────────────────────────────────────
# Funciones de cálculo individuales
# ──────────────────────────────────────────────

def calc_ocupacion(df: pd.DataFrame) -> float:
    citados = (df["ESTADO CUPO"] == "CITADO").sum()
    disponibles = (df["ESTADO CUPO"] == "DISPONIBLE").sum()
    return _safe_pct(citados, citados + disponibles)


def calc_no_show(df: pd.DataFrame) -> float:
    mask_citados = df["ESTADO CUPO"] == "CITADO"
    citados = mask_citados.sum()
    completados = (mask_citados & (df["ESTADO CITA"] == "Completado")).sum()
    return _safe_pct(citados - completados, citados)


def calc_bloqueo(df: pd.DataFrame) -> float:
    bloqueados = (df["ESTADO CUPO"] == "BLOQUEADO").sum()
    total = len(df)
    return _safe_pct(bloqueados, total)


def calc_efectividad(df: pd.DataFrame) -> float:
    mask_citados = df["ESTADO CUPO"] == "CITADO"
    citados = mask_citados.sum()
    completados = (mask_citados & (df["ESTADO CITA"] == "Completado")).sum()
    return _safe_pct(completados, citados)


def calc_rendimiento(df: pd.DataFrame) -> float:
    if "RENDIMIENTO" not in df.columns:
        return 0.0
    vals = pd.to_numeric(df["RENDIMIENTO"], errors="coerce").dropna()
    return round(vals.mean(), 1) if len(vals) > 0 else 0.0


def calc_sobrecupo(df: pd.DataFrame) -> float:
    sobrecupos = (df["TIPO CUPO"] == "Sobrecupo").sum()
    return _safe_pct(sobrecupos, len(df))


def calc_cobertura_sectorial(df: pd.DataFrame) -> float:
    if "SECTOR" not in df.columns:
        return 0.0
    informado = (df["SECTOR"] != "NO INFORMADO").sum()
    return _safe_pct(informado, len(df))


def calc_agendamiento_remoto(df: pd.DataFrame) -> float:
    if "TIPO DE AGENDAMIENTO" not in df.columns:
        return 0.0
    remoto = df["TIPO DE AGENDAMIENTO"].isin(
        ["Telefónicamente", "Telefonicamente", "Telesalud"]
    ).sum()
    return _safe_pct(remoto, len(df))


def calc_ocupacion_extendida(df: pd.DataFrame) -> float:
    if "HORA_NUM" not in df.columns:
        return 0.0
    df_ext = df[df["HORA_NUM"] >= 18]
    if df_ext.empty:
        return 0.0
    citados = (df_ext["ESTADO CUPO"] == "CITADO").sum()
    disponibles = (df_ext["ESTADO CUPO"] == "DISPONIBLE").sum()
    return _safe_pct(citados, citados + disponibles)


# ──────────────────────────────────────────────
# Función principal: calcular todos los KPIs
# ──────────────────────────────────────────────

@_cache
def calculate_all_kpis(df: pd.DataFrame) -> dict:
    """
    Calcula los 10 KPIs sobre el DataFrame filtrado.
    Returns dict con valores y semáforos.
    """
    if df.empty:
        return {}

    kpis = {}

    calcs = {
        "ocupacion": calc_ocupacion,
        "no_show": calc_no_show,
        "bloqueo": calc_bloqueo,
        "efectividad": calc_efectividad,
        "rendimiento": calc_rendimiento,
        "sobrecupo": calc_sobrecupo,
        "cobertura_sectorial": calc_cobertura_sectorial,
        "agendamiento_remoto": calc_agendamiento_remoto,
        "ocupacion_extendida": calc_ocupacion_extendida,
    }

    for key, fn in calcs.items():
        try:
            valor = fn(df)
        except Exception:
            valor = 0.0
        kpis[key] = {
            "valor": valor,
            "semaforo": semaforo(valor, key),
            **KPI_DEFINITIONS[key],
        }

    # Variación mensual (requiere datos agrupados por mes)
    kpis["variacion_mensual"] = _calc_variacion_mensual(df)

    return kpis


def _calc_variacion_mensual(df: pd.DataFrame) -> dict:
    """Calcula variación mes a mes en tasa de ocupación."""
    defn = KPI_DEFINITIONS["variacion_mensual"]
    if "MES_NUM" not in df.columns or df.empty:
        return {"valor": 0.0, "semaforo": "gris", **defn}

    monthly = (
        df.groupby("MES_NUM")
        .apply(lambda x: calc_ocupacion(x))
        .reset_index(name="ocupacion")
    )
    if len(monthly) < 2:
        return {"valor": 0.0, "semaforo": "gris", **defn}

    monthly = monthly.sort_values("MES_NUM")
    monthly["variacion"] = monthly["ocupacion"].diff().abs()
    max_var = monthly["variacion"].dropna().max()
    valor = round(max_var, 2) if pd.notna(max_var) else 0.0

    return {"valor": valor, "semaforo": semaforo(valor, "variacion_mensual"), **defn}


# ──────────────────────────────────────────────
# KPIs agrupados (para gráficos de evolución)
# ──────────────────────────────────────────────

@_cache
def kpis_por_mes(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna DataFrame con KPIs calculados por mes."""
    if "MES_NUM" not in df.columns or df.empty:
        return pd.DataFrame()

    MESES_ES = {
        1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
    }

    rows = []
    for mes, grp in df.groupby("MES_NUM"):
        row = {"mes": mes, "mes_nombre": MESES_ES.get(int(mes), str(mes))}
        row["ocupacion"] = calc_ocupacion(grp)
        row["no_show"] = calc_no_show(grp)
        row["bloqueo"] = calc_bloqueo(grp)
        row["efectividad"] = calc_efectividad(grp)
        row["rendimiento"] = calc_rendimiento(grp)
        row["agendamiento_remoto"] = calc_agendamiento_remoto(grp)
        row["sobrecupo"] = calc_sobrecupo(grp)
        row["cobertura_sectorial"] = calc_cobertura_sectorial(grp)
        row["total_registros"] = len(grp)
        row["citados"] = (grp["ESTADO CUPO"] == "CITADO").sum()
        row["disponibles"] = (grp["ESTADO CUPO"] == "DISPONIBLE").sum()
        row["bloqueados"] = (grp["ESTADO CUPO"] == "BLOQUEADO").sum()
        rows.append(row)

    return pd.DataFrame(rows).sort_values("mes")


@_cache
def kpis_por_instrumento(df: pd.DataFrame) -> pd.DataFrame:
    """KPIs agrupados por instrumento/profesional."""
    if "INSTRUMENTO" not in df.columns or df.empty:
        return pd.DataFrame()

    rows = []
    for instrumento, grp in df.groupby("INSTRUMENTO"):
        row = {"instrumento": instrumento}
        row["ocupacion"] = calc_ocupacion(grp)
        row["no_show"] = calc_no_show(grp)
        row["efectividad"] = calc_efectividad(grp)
        row["rendimiento"] = calc_rendimiento(grp)
        row["total"] = len(grp)
        row["citados"] = (grp["ESTADO CUPO"] == "CITADO").sum()
        rows.append(row)

    return pd.DataFrame(rows).sort_values("ocupacion", ascending=False)


@_cache
def kpis_por_centro(df: pd.DataFrame) -> pd.DataFrame:
    """KPIs agrupados por establecimiento."""
    if "ESTABLECIMIENTO" not in df.columns or df.empty:
        return pd.DataFrame()

    rows = []
    for centro, grp in df.groupby("ESTABLECIMIENTO"):
        row = {"centro": centro}
        row["ocupacion"] = calc_ocupacion(grp)
        row["no_show"] = calc_no_show(grp)
        row["bloqueo"] = calc_bloqueo(grp)
        row["efectividad"] = calc_efectividad(grp)
        row["rendimiento"] = calc_rendimiento(grp)
        row["total"] = len(grp)
        rows.append(row)

    return pd.DataFrame(rows).sort_values("ocupacion", ascending=False)


def detectar_alertas(df: pd.DataFrame) -> list:
    """
    Detecta brechas críticas según el modelo APS.
    Returns: lista de dicts con tipo, descripcion, valor, nivel
    """
    if df.empty:
        return []

    alertas = []
    kpis = calculate_all_kpis(df)

    checks = [
        ("ocupacion", "Subutilización de Cupos", "rojo"),
        ("no_show", "No-Show Elevado", "rojo"),
        ("bloqueo", "Bloqueos Elevados", "rojo"),
        ("agendamiento_remoto", "Agendamiento Remoto Bajo", "rojo"),
        ("cobertura_sectorial", "Registro Sectorial Incompleto", "amarillo"),
        ("variacion_mensual", "Variación Mensual Abrupta", "amarillo"),
    ]

    for key, nombre, nivel_minimo in checks:
        k = kpis.get(key, {})
        if k.get("semaforo") in (["rojo"] if nivel_minimo == "rojo" else ["rojo", "amarillo"]):
            alertas.append({
                "tipo": nombre,
                "kpi": key,
                "valor": k.get("valor", 0),
                "unidad": k.get("unidad", "%"),
                "semaforo": k.get("semaforo", "gris"),
                "descripcion": k.get("descripcion", ""),
                "umbral_alerta": k.get("umbral_alerta"),
            })

    # Alerta especial: sector NO INFORMADO > 40%
    if "SECTOR" in df.columns:
        pct_no_informado = (df["SECTOR"] == "NO INFORMADO").mean() * 100
        if pct_no_informado > 40:
            alertas.append({
                "tipo": "Sector NO INFORMADO excesivo",
                "kpi": "sector_no_informado",
                "valor": round(pct_no_informado, 1),
                "unidad": "%",
                "semaforo": "rojo",
                "descripcion": "Más del 40% de registros sin sector territorial informado",
                "umbral_alerta": 40,
            })

    return alertas
