"""
Módulo de procesamiento de archivos IRIS (.xlsx)
Estructura real IRIS SSMC: 8 filas de metadatos + fila vacía + encabezados en fila 10 + datos desde fila 11
"""
import pandas as pd
import numpy as np
from io import BytesIO

# Columnas con datos personales que deben eliminarse
PII_COLUMNS = {
    "NOMBRE", "NOMBRE SOCIAL", "NUMERO TIPO IDENTIFICACION",
    "FECHA DE NACIMIENTO", "TELEFONOS", "DETALLE CUPO", "OBSERVACIONES"
}

# Sectores territoriales válidos
VALID_SECTORS = {"VERDE", "LILA", "ROJO", "NO INFORMADO"}

# Columnas requeridas para el análisis (nombres normalizados)
REQUIRED_COLUMNS = {
    "SS", "ESTABLECIMIENTO", "FECHA", "TIPO ATENCION", "INSTRUMENTO",
    "TIPO CUPO", "ESTADO CUPO", "ESTADO CITA", "SECTOR",
    "TIPO DE AGENDAMIENTO"
}


def _normalize_col_name(col: str) -> str:
    """Normaliza nombres de columnas manejando problemas de codificación."""
    col = str(col).strip()
    # Fix encoding issues (ñ, tildes)
    replacements = {
        "\xd1": "N", "\xf1": "N",  # Ñ/ñ
        "\xe1": "A", "\xe9": "E", "\xed": "I", "\xf3": "O", "\xfa": "U",
        "\xc1": "A", "\xc9": "E", "\xcd": "I", "\xd3": "O", "\xda": "U",
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
        "á": "A", "é": "E", "í": "I", "ó": "O", "ú": "U",
        "Ñ": "N", "ñ": "N",
    }
    for bad, good in replacements.items():
        col = col.replace(bad, good)
    return col.upper()


def _read_metadata(file_obj) -> dict:
    """Lee metadatos de las filas 1-8 del archivo IRIS."""
    try:
        df_meta = pd.read_excel(file_obj, header=None, nrows=8, engine="openpyxl")
    except Exception:
        return {}

    metadata = {}
    key_map = {
        "Servicio Salud": "servicio_salud",
        "Comuna": "comuna",
        "Establecimientos": "establecimiento",
        "Fecha Desde": "fecha_desde",
        "Fecha Hasta": "fecha_hasta",
        "Estado Cupo": "estado_cupo_filtro",
    }
    for _, row in df_meta.iterrows():
        vals = [str(v).strip() for v in row.values if pd.notna(v) and str(v).strip() and str(v) != "nan"]
        if len(vals) >= 2:
            for k, mapped in key_map.items():
                if vals[0].startswith(k.split()[0]):
                    metadata[mapped] = vals[1]
    return metadata


def process_iris_file(file_obj, filename: str = "") -> tuple:
    """
    Procesa un archivo IRIS .xlsx.

    Returns:
        tuple: (df_limpio, metadata_dict, lista_errores)
    """
    errors = []

    # 1. Leer metadatos
    metadata = _read_metadata(file_obj)
    metadata["archivo"] = filename

    # 2. Leer datos (encabezados en fila 10, índice 9)
    # Leemos solo las columnas necesarias para reducir uso de memoria
    try:
        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        # Primera pasada: leer solo encabezados para mapear columnas útiles
        df_head = pd.read_excel(file_obj, header=9, nrows=0, engine="openpyxl")
        all_cols = list(df_head.columns)
        # Columnas que NO necesitamos (PII + redundantes)
        _DROP_RAW = {
            "NOMBRE", "NOMBRE SOCIAL", "NUMERO TIPO IDENTIFICACION",
            "FECHA DE NACIMIENTO", "TELEFONOS", "DETALLE CUPO", "OBSERVACIONES",
            "FUNCIONARIO CITADOR", "RUT PROFESIONAL", "PROFESIONAL",
            "FUNCIONARIO REALIZA BLOQUEO", "TELECONSULTA",
        }
        usecols = [c for c in all_cols if _normalize_col_name(c) not in _DROP_RAW]
        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        df = pd.read_excel(file_obj, header=9, engine="openpyxl", usecols=usecols)
    except Exception as e:
        return None, metadata, [f"Error al leer datos del archivo: {e}"]

    if df.empty:
        return None, metadata, ["El archivo no contiene datos."]

    # 3. Normalizar nombres de columnas
    col_map = {col: _normalize_col_name(col) for col in df.columns}
    df = df.rename(columns=col_map)

    # 4. Renombrar RENDIMIMENTO (typo en IRIS) → RENDIMIENTO
    if "RENDIMIMENTO" in df.columns:
        df = df.rename(columns={"RENDIMIMENTO": "RENDIMIENTO"})

    # 5. Unificar columna EDAD AÑO (puede tener problemas de codificación)
    edad_col = next((c for c in df.columns if c.startswith("EDAD") and "AN" in c), None)
    if edad_col and edad_col != "EDAD_ANO":
        df = df.rename(columns={edad_col: "EDAD_ANO"})

    # 6. Eliminar datos personales residuales (por si acaso)
    cols_to_drop = [c for c in df.columns if c in PII_COLUMNS]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    # 7. Limpiar y estandarizar columnas clave
    # FECHA
    if "FECHA" in df.columns:
        df["FECHA"] = pd.to_datetime(df["FECHA"], format="%d-%m-%Y", errors="coerce")
        # Derivar MES_NUM desde FECHA si no existe como columna propia
        if "MES_NUM" not in df.columns:
            df["MES_NUM"] = df["FECHA"].dt.month

    # HORA_NUM: derivar desde HORA INICIO cuando no existe como columna propia
    # (algunos exports IRIS incluyen HORA INICIO/TERMINO en vez de HORA_NUM directamente)
    if "HORA_NUM" not in df.columns and "HORA INICIO" in df.columns:
        def _hora_a_num(v):
            try:
                import datetime as _dt
                if pd.isna(v):
                    return np.nan
                if isinstance(v, _dt.time):
                    return v.hour + v.minute / 60
                s = str(v).strip()
                parts = s.replace(".", ":").split(":")
                return int(parts[0]) + int(parts[1]) / 60
            except Exception:
                return np.nan
        df["HORA_NUM"] = df["HORA INICIO"].apply(_hora_a_num)

    # SECTOR: normalizar valores no estándar → NO INFORMADO
    if "SECTOR" in df.columns:
        df["SECTOR"] = df["SECTOR"].fillna("NO INFORMADO").str.strip().str.upper()
        df["SECTOR"] = df["SECTOR"].where(df["SECTOR"].isin(VALID_SECTORS), "NO INFORMADO")

    # Columnas numéricas
    for col in ["RENDIMIENTO", "CUPOS UTILIZADOS", "MES_NUM", "HORA_NUM", "EDAD_ANO"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # CUPOS UTILIZADOS: binarizar (>1 → 1 para agregaciones correctas)
    if "CUPOS UTILIZADOS" in df.columns:
        df["CUPOS_UTIL_BIN"] = (df["CUPOS UTILIZADOS"] >= 1).astype(int)

    # 8. Agregar columnas derivadas
    if "MES_NUM" in df.columns:
        _TRIM_MAP = {1:"Q1",2:"Q1",3:"Q1",4:"Q2",5:"Q2",6:"Q2",
                     7:"Q3",8:"Q3",9:"Q3",10:"Q4",11:"Q4",12:"Q4"}
        df["TRIMESTRE"] = df["MES_NUM"].map(_TRIM_MAP).fillna("Sin dato")
        MESES_ES = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        df["MES_NOMBRE"] = df["MES_NUM"].map(MESES_ES)

    if "HORA_NUM" in df.columns:
        df["HORARIO_EXTENDIDO"] = np.where(
            df["HORA_NUM"].notna() & (df["HORA_NUM"] >= 18), "Extendido", "Normal"
        )

    if "EDAD_ANO" in df.columns:
        bins = [-1, 5, 14, 29, 64, 200]
        labels = ["0-5", "6-14", "15-29", "30-64", "65+"]
        df["GRUPO_ETARIO"] = pd.cut(df["EDAD_ANO"], bins=bins, labels=labels)

    # 9. Tipo de agendamiento: marcar remoto
    if "TIPO DE AGENDAMIENTO" in df.columns:
        _REMOTE = {"Telefónicamente", "Telefonicamente", "Telesalud"}
        df["AGENDAMIENTO_REMOTO"] = np.where(
            df["TIPO DE AGENDAMIENTO"].isin(_REMOTE), "Remoto", "Presencial/Otro"
        )

    # 10. Tracking de archivo fuente
    df["_archivo"] = filename

    # 11. Verificar columnas requeridas
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"Columnas requeridas no encontradas: {missing}")

    # 12. Optimización de memoria: convertir columnas de baja cardinalidad a categorical
    # Reduce uso de RAM ~60-70% en columnas string repetitivas
    _CAT_COLS = [
        "SS", "ESTABLECIMIENTO", "TIPO ATENCION", "INSTRUMENTO", "TIPO CUPO",
        "ESTADO CUPO", "ESTADO CITA", "SECTOR", "TIPO DE AGENDAMIENTO",
        "TRIMESTRE", "MES_NOMBRE", "HORARIO_EXTENDIDO", "AGENDAMIENTO_REMOTO",
        "_archivo",
    ]
    import gc
    for col in _CAT_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category")
    gc.collect()

    return df, metadata, errors


def consolidate_files(list_of_dfs: list) -> pd.DataFrame:
    """Consolida múltiples DataFrames IRIS eliminando duplicados."""
    if not list_of_dfs:
        return pd.DataFrame()
    df_all = pd.concat(list_of_dfs, ignore_index=True)
    # Eliminar duplicados exactos
    df_all = df_all.drop_duplicates()
    return df_all


def validate_structure(file_obj) -> tuple:
    """
    Valida que el archivo tenga la estructura IRIS esperada.
    Returns: (es_valido: bool, mensaje: str, num_registros: int)
    """
    try:
        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        df_meta = pd.read_excel(file_obj, header=None, nrows=1, engine="openpyxl")
        first_cell = str(df_meta.iloc[0, 0]) if not df_meta.empty else ""
        if "Cantidad de Cupos" not in first_cell and "IRIS" not in first_cell.upper():
            # Try to be flexible - just check if it has the right headers
            pass

        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        df_data = pd.read_excel(file_obj, header=9, engine="openpyxl", nrows=5)
        cols_norm = [_normalize_col_name(c) for c in df_data.columns]

        key_checks = ["ESTADO CUPO", "INSTRUMENTO", "TIPO ATENCION"]
        found = [k for k in key_checks if k in cols_norm]
        if len(found) < 2:
            return False, f"Estructura no reconocida. Columnas encontradas: {len(df_data.columns)}", 0

        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        df_count = pd.read_excel(file_obj, header=9, engine="openpyxl", usecols=[0])
        n = len(df_count)
        return True, f"Archivo válido. {n:,} registros detectados.", n

    except Exception as e:
        return False, f"Error validando archivo: {e}", 0
