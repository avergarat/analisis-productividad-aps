"""
Cliente BigQuery para almacenamiento persistente de datos IRIS.
Reemplaza GitHub parquet para soportar datasets de millones de registros.

Tier gratuito permanente de BigQuery:
  - 10 GB almacenamiento/mes
  - 1 TB consultas/mes
  - Carga de datos: siempre gratis

Configuración en Streamlit Secrets:
  [bigquery]
  project_id       = "mi-proyecto-gcp"
  dataset          = "aps_ssmc"
  table            = "cupos"
  credentials_json = '''{ "type": "service_account", ... }'''
"""
from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone

import pandas as pd

# ── Mapeo columnas DataFrame (IRIS) ↔ BigQuery (snake_case) ─────────────────
_COL_TO_BQ: dict[str, str] = {
    "SS":                    "ss",
    "ESTABLECIMIENTO":       "establecimiento",
    "FECHA":                 "fecha",
    "MES_NUM":               "mes_num",
    "MES_NOMBRE":            "mes_nombre",
    "TRIMESTRE":             "trimestre",
    "TIPO ATENCION":         "tipo_atencion",
    "INSTRUMENTO":           "instrumento",
    "TIPO CUPO":             "tipo_cupo",
    "ESTADO CUPO":           "estado_cupo",
    "ESTADO CITA":           "estado_cita",
    "SECTOR":                "sector",
    "TIPO DE AGENDAMIENTO":  "tipo_agendamiento",
    "HORARIO_EXTENDIDO":     "horario_extendido",
    "AGENDAMIENTO_REMOTO":   "agendamiento_remoto",
    "GRUPO_ETARIO":          "grupo_etario",
    "PROFESIONAL":           "profesional",
    "DIA_SEMANA":            "dia_semana",
    "APERTURA_SABATINA":     "apertura_sabatina",
    "RENDIMIENTO":           "rendimiento",
    "CUPOS_UTIL_BIN":        "cupos_util_bin",
    "HORA_NUM":              "hora_num",
    "EDAD_ANO":              "edad_ano",
    "_archivo":              "archivo_fuente",
}
_COL_FROM_BQ: dict[str, str] = {v: k for k, v in _COL_TO_BQ.items()}

_SCHEMA = [
    ("ss",                 "STRING"),
    ("establecimiento",    "STRING"),
    ("fecha",              "DATE"),
    ("mes_num",            "INTEGER"),
    ("mes_nombre",         "STRING"),
    ("trimestre",          "STRING"),
    ("tipo_atencion",      "STRING"),
    ("instrumento",        "STRING"),
    ("tipo_cupo",          "STRING"),
    ("estado_cupo",        "STRING"),
    ("estado_cita",        "STRING"),
    ("sector",             "STRING"),
    ("tipo_agendamiento",  "STRING"),
    ("horario_extendido",  "STRING"),
    ("agendamiento_remoto","STRING"),
    ("grupo_etario",       "STRING"),
    ("profesional",        "STRING"),
    ("dia_semana",         "INTEGER"),
    ("apertura_sabatina",  "STRING"),
    ("rendimiento",        "FLOAT64"),
    ("cupos_util_bin",     "INTEGER"),
    ("hora_num",           "FLOAT64"),
    ("edad_ano",           "FLOAT64"),
    ("archivo_fuente",     "STRING"),
    ("_cargado_en",        "TIMESTAMP"),
]


# ── Configuración ─────────────────────────────────────────────────────────────

def _cfg() -> dict:
    try:
        import streamlit as st
        sec = st.secrets.get("bigquery", {})
        return {
            "project_id":       sec.get("project_id",       os.getenv("BQ_PROJECT_ID", "")),
            "dataset":          sec.get("dataset",          os.getenv("BQ_DATASET", "aps_ssmc")),
            "table":            sec.get("table",            os.getenv("BQ_TABLE", "cupos")),
            "credentials_json": sec.get("credentials_json", os.getenv("BQ_CREDENTIALS_JSON", "")),
        }
    except Exception:
        return {"project_id": "", "dataset": "aps_ssmc", "table": "cupos", "credentials_json": ""}


def bq_configured() -> bool:
    """True si las credenciales BigQuery están disponibles."""
    c = _cfg()
    return bool(c.get("project_id") and c.get("credentials_json"))


def _client():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    cfg = _cfg()
    info = json.loads(cfg["credentials_json"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(project=cfg["project_id"], credentials=creds)


def _full_table_id() -> str:
    cfg = _cfg()
    return f"{cfg['project_id']}.{cfg['dataset']}.{cfg['table']}"


def _tref() -> str:
    """Referencia quoted para usar en SQL."""
    cfg = _cfg()
    return f"`{cfg['project_id']}.{cfg['dataset']}.{cfg['table']}`"


# ── Crear tabla si no existe ──────────────────────────────────────────────────

def _ensure_table(client) -> None:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    cfg = _cfg()
    dataset_id = f"{cfg['project_id']}.{cfg['dataset']}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        ds = bigquery.Dataset(dataset_id)
        ds.location = "US"
        client.create_dataset(ds, exists_ok=True)

    schema = [bigquery.SchemaField(n, t) for n, t in _SCHEMA]
    tbl = bigquery.Table(_full_table_id(), schema=schema)
    # Partición mensual por fecha → queries con filtro de mes son ~10× más rápidas
    tbl.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="fecha",
    )
    # Clustering → WHERE establecimiento/tipo_atencion/instrumento escanea menos
    tbl.clustering_fields = ["establecimiento", "tipo_atencion", "instrumento"]
    client.create_table(tbl, exists_ok=True)

    # Agregar columnas nuevas a tabla existente (schema evolution)
    try:
        existing = client.get_table(_full_table_id())
        existing_names = {f.name for f in existing.schema}
        new_fields = [f for f in schema if f.name not in existing_names]
        if new_fields:
            updated_schema = list(existing.schema) + new_fields
            existing.schema = updated_schema
            client.update_table(existing, ["schema"])
    except Exception:
        pass  # Si falla, no bloquear la carga


# ── Conversión DataFrame ↔ BigQuery ───────────────────────────────────────────

def _to_bq(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte DataFrame IRIS al formato listo para BigQuery."""
    cols_map = {k: v for k, v in _COL_TO_BQ.items() if k in df.columns}
    out = df[list(cols_map.keys())].copy().rename(columns=cols_map)

    # Categoricals → string, NaN → None
    for col in out.select_dtypes(include="category").columns:
        out[col] = out[col].astype(str).replace({"nan": None, "<NA>": None, "NaN": None})

    # Asegurar tipos
    if "fecha" in out.columns:
        out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce").dt.date
    for c in ["mes_num", "cupos_util_bin"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
    for c in ["rendimiento", "hora_num", "edad_ano"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out["_cargado_en"] = datetime.now(timezone.utc)
    return out


def _from_bq(df_bq: pd.DataFrame) -> pd.DataFrame:
    """Convierte resultado de BigQuery de vuelta al formato IRIS."""
    rename = {k: v for k, v in _COL_FROM_BQ.items() if k in df_bq.columns}
    df = df_bq.rename(columns=rename).drop(columns=["_cargado_en"], errors="ignore")

    if "FECHA" in df.columns:
        df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    if "MES_NUM" in df.columns:
        df["MES_NUM"] = pd.to_numeric(df["MES_NUM"], errors="coerce")
    for c in ["RENDIMIENTO", "HORA_NUM", "EDAD_ANO", "CUPOS_UTIL_BIN"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    _CAT = [
        "SS", "ESTABLECIMIENTO", "TIPO ATENCION", "INSTRUMENTO", "TIPO CUPO",
        "ESTADO CUPO", "ESTADO CITA", "SECTOR", "TIPO DE AGENDAMIENTO",
        "TRIMESTRE", "MES_NOMBRE", "HORARIO_EXTENDIDO", "AGENDAMIENTO_REMOTO",
        "GRUPO_ETARIO", "PROFESIONAL", "APERTURA_SABATINA", "_archivo",
    ]
    for c in _CAT:
        if c in df.columns:
            df[c] = df[c].astype("category")

    if "GRUPO_ETARIO" in df.columns:
        from pandas.api.types import CategoricalDtype
        df["GRUPO_ETARIO"] = df["GRUPO_ETARIO"].astype(
            CategoricalDtype(categories=["0-5", "6-14", "15-29", "30-64", "65+"], ordered=True)
        )
    return df


# ── INSERT ────────────────────────────────────────────────────────────────────

def insert_data(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Inserta DataFrame en BigQuery.
    Elimina filas previas del mismo archivo antes de insertar (deduplicación).
    Usa load jobs (gratuitos) en vez de streaming inserts.
    """
    if not bq_configured():
        return False, "BigQuery no configurado."
    if df.empty:
        return False, "DataFrame vacío."
    try:
        from google.cloud import bigquery
        client = _client()
        _ensure_table(client)

        if "_archivo" in df.columns:
            archivos = df["_archivo"].dropna().unique().tolist()
            if archivos:
                escaped = ", ".join([f"'{a.replace(chr(39), chr(39)*2)}'" for a in archivos])
                client.query(
                    f"DELETE FROM {_tref()} WHERE archivo_fuente IN ({escaped})"
                ).result()

        df_bq = _to_bq(df)
        n_source = len(df)
        n_bq_ready = len(df_bq)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[bigquery.SchemaField(n, t) for n, t in _SCHEMA],
        )
        job = client.load_table_from_dataframe(df_bq, _full_table_id(), job_config=job_config)
        result = job.result()

        # Verificar filas efectivamente cargadas
        n_loaded = result.output_rows if hasattr(result, "output_rows") and result.output_rows else n_bq_ready
        if n_loaded < n_source:
            return True, (
                f"⚠️ {n_loaded:,} de {n_source:,} registros cargados en BigQuery "
                f"({n_source - n_loaded:,} posiblemente perdidos en conversión)."
            )
        return True, f"✅ {n_source:,} registros guardados en BigQuery."
    except Exception as e:
        return False, f"Error BigQuery al insertar: {e}"


# ── CONSULTAS ─────────────────────────────────────────────────────────────────

def get_record_count() -> int:
    """Cuenta total de filas en BigQuery."""
    if not bq_configured():
        return 0
    try:
        client = _client()
        row = list(client.query(f"SELECT COUNT(*) AS n FROM {_tref()}").result())[0]
        return int(row.n)
    except Exception:
        return 0


def get_filter_options() -> dict:
    """
    Retorna valores distintos de cada dimensión para poblar los filtros del sidebar.
    Una sola consulta que trae todo.
    """
    if not bq_configured():
        return {}
    try:
        client = _client()
        q = f"""
        SELECT
            ARRAY_AGG(DISTINCT establecimiento  IGNORE NULLS ORDER BY establecimiento)  AS establecimientos,
            ARRAY_AGG(DISTINCT sector           IGNORE NULLS ORDER BY sector)           AS sectores,
            ARRAY_AGG(DISTINCT instrumento      IGNORE NULLS ORDER BY instrumento)      AS instrumentos,
            ARRAY_AGG(DISTINCT tipo_atencion    IGNORE NULLS ORDER BY tipo_atencion)    AS tipos_atencion,
            ARRAY_AGG(DISTINCT tipo_cupo        IGNORE NULLS ORDER BY tipo_cupo)        AS tipos_cupo,
            ARRAY_AGG(DISTINCT mes_num          IGNORE NULLS ORDER BY mes_num)          AS meses
        FROM {_tref()}
        """
        row = list(client.query(q).result())[0]
        return {
            "establecimientos": list(row.establecimientos or []),
            "sectores":         list(row.sectores or []),
            "instrumentos":     list(row.instrumentos or []),
            "tipos_atencion":   list(row.tipos_atencion or []),
            "tipos_cupo":       list(row.tipos_cupo or []),
            "meses":            [int(m) for m in (row.meses or [])],
        }
    except Exception:
        return {}


def get_archivos_cargados() -> list[dict]:
    """Lista archivos cargados con conteo de registros."""
    if not bq_configured():
        return []
    try:
        client = _client()
        q = f"""
        SELECT archivo_fuente AS archivo,
               COUNT(*) AS registros,
               MAX(_cargado_en) AS ultima_carga
        FROM {_tref()}
        GROUP BY archivo_fuente
        ORDER BY ultima_carga DESC
        """
        return client.query(q).to_dataframe().to_dict("records")
    except Exception:
        return []


def load_filtered(
    centros:        list | None = None,
    meses:          list | None = None,
    instrumentos:   list | None = None,
    sectores:       list | None = None,
    tipos_atencion: list | None = None,
    tipos_cupo:     list | None = None,
    max_rows: int = 1_000_000,
) -> tuple[pd.DataFrame | None, str]:
    """
    Carga desde BigQuery solo las filas que coinciden con los filtros activos.
    Verifica el conteo ANTES de traer datos para proteger la RAM.
    """
    if not bq_configured():
        return None, "BigQuery no configurado."
    try:
        client = _client()

        def _in_str(col: str, vals: list) -> str:
            """Cláusula IN para columnas STRING."""
            escaped = ", ".join([f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in vals])
            return f"{col} IN ({escaped})"

        def _in_int(col: str, vals: list) -> str:
            """Cláusula IN para columnas INTEGER (sin comillas)."""
            nums = ", ".join([str(int(v)) for v in vals])
            return f"{col} IN ({nums})"

        conds = []
        if centros:         conds.append(_in_str("establecimiento", centros))
        if meses:           conds.append(_in_int("mes_num", meses))      # INT64
        if instrumentos:    conds.append(_in_str("instrumento", instrumentos))
        if sectores:        conds.append(_in_str("sector", sectores))
        if tipos_atencion:  conds.append(_in_str("tipo_atencion", tipos_atencion))
        if tipos_cupo:      conds.append(_in_str("tipo_cupo", tipos_cupo))
        where = f"WHERE {' AND '.join(conds)}" if conds else ""

        n = list(client.query(
            f"SELECT COUNT(*) AS n FROM {_tref()} {where}"
        ).result())[0].n

        if n == 0:
            return None, "Sin datos para los filtros seleccionados."
        if n > max_rows:
            return None, (
                f"La selección contiene **{n:,} filas** "
                f"(límite de seguridad: {max_rows:,}). "
                "Reduce los filtros (menos CESFAM o menos meses) y vuelve a cargar."
            )

        df_bq = client.query(
            f"SELECT * EXCEPT(_cargado_en) FROM {_tref()} {where}"
        ).to_dataframe()
        return _from_bq(df_bq), f"{n:,} registros cargados desde BigQuery."
    except Exception as e:
        return None, f"Error al consultar BigQuery: {e}"


# ── DELETE ────────────────────────────────────────────────────────────────────

def delete_archivo(archivo: str) -> tuple[bool, str]:
    """Elimina todos los registros de un archivo específico."""
    if not bq_configured():
        return False, "BigQuery no configurado."
    try:
        safe = archivo.replace("'", "''")
        _client().query(
            f"DELETE FROM {_tref()} WHERE archivo_fuente = '{safe}'"
        ).result()
        return True, f"'{archivo}' eliminado de BigQuery."
    except Exception as e:
        return False, f"Error: {e}"


def delete_all_data() -> tuple[bool, str]:
    """Elimina TODOS los registros de la tabla BigQuery."""
    if not bq_configured():
        return False, "BigQuery no configurado."
    try:
        _client().query(f"DELETE FROM {_tref()} WHERE TRUE").result()
        return True, "Todos los datos eliminados de BigQuery."
    except Exception as e:
        return False, f"Error: {e}"


# ── EXPORT ────────────────────────────────────────────────────────────────────

def export_csv_bytes(
    centros: list | None = None,
    meses:   list | None = None,
) -> bytes | None:
    """
    Exporta datos filtrados como CSV (bytes).
    Usa max_rows alto porque es on-demand y el usuario espera el archivo completo.
    """
    df, _ = load_filtered(centros=centros, meses=meses, max_rows=5_000_000)
    if df is None:
        return None
    for c in df.select_dtypes(include="category").columns:
        df[c] = df[c].astype(str)
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    return buf.getvalue()


# ── STATUS ────────────────────────────────────────────────────────────────────

def bq_status() -> dict:
    cfg = _cfg()
    return {
        "configurado": bq_configured(),
        "project_id":  cfg.get("project_id", ""),
        "dataset":     cfg.get("dataset", ""),
        "table":       cfg.get("table", ""),
    }
