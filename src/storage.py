"""
Módulo de persistencia duradera usando GitHub como almacén de datos.

Flujo:
  - Al iniciar: descarga consolidated.parquet desde el repo GitHub → carga en sesión.
  - Al procesar archivos IRIS: actualiza el parquet en GitHub → todos los usuarios
    ven los datos nuevos en su próxima recarga o al instante (si el admin llama a
    st.cache_data.clear()).

Configuración requerida en Streamlit Cloud → Settings → Secrets:
  [github_storage]
  token  = "ghp_xxxxxxxxxxxxxxxxxxxx"   # PAT con permiso 'contents:write'
  repo   = "avergarat/sistema-analisis-productividad-aps"
  branch = "main"
  path   = "data/consolidated.parquet"
"""

from __future__ import annotations

import base64
import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests

# ── Rutas de caché local (/tmp) — sigue funcionando como caché rápido ──────────
_TMP_PARQUET = "/tmp/ssmc_aps_df.parquet"
_TMP_META    = "/tmp/ssmc_aps_meta.json"


# ── Helpers de configuración ────────────────────────────────────────────────────

def _cfg() -> dict:
    """Lee config de GitHub desde Streamlit secrets (o variables de entorno)."""
    try:
        import streamlit as st
        sec = st.secrets.get("github_storage", {})
        return {
            "token":  sec.get("token",  os.getenv("GH_STORAGE_TOKEN",  "")),
            "repo":   sec.get("repo",   os.getenv("GH_STORAGE_REPO",   "")),
            "branch": sec.get("branch", os.getenv("GH_STORAGE_BRANCH", "main")),
            "path":   sec.get("path",   os.getenv("GH_STORAGE_PATH",   "data/consolidated.parquet")),
        }
    except Exception:
        return {"token": "", "repo": "", "branch": "main", "path": "data/consolidated.parquet"}


def github_configured() -> bool:
    """True si las credenciales de GitHub están disponibles."""
    c = _cfg()
    return bool(c.get("token") and c.get("repo"))


def _gh_headers(cfg: dict) -> dict:
    return {
        "Authorization": f"token {cfg['token']}",
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


# ── Guardar ─────────────────────────────────────────────────────────────────────

def save_data(df: pd.DataFrame, registro_cargas: list | None = None) -> tuple[bool, str]:
    """
    Guarda el DataFrame en GitHub (persistencia duradera) y en /tmp (caché local).

    Returns:
        (éxito: bool, mensaje: str)
    """
    # 1. Guardar en /tmp siempre (caché rápido, mismo proceso)
    try:
        df.to_parquet(_TMP_PARQUET, index=False, compression="snappy")
        meta = {
            "registros": len(df),
            "actualizado": datetime.now(timezone.utc).isoformat(),
            "registro_cargas": registro_cargas or [],
        }
        with open(_TMP_META, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, default=str)
    except Exception as e:
        pass  # /tmp puede no estar disponible en algunos entornos

    # 2. Intentar guardar en GitHub
    if not github_configured():
        return False, "GitHub no configurado (falta token/repo en secrets). Datos solo en caché local."

    # Límite de seguridad: no intentar subir DataFrames > 500k filas a GitHub
    # (evita OOM al serializar base64 en memoria en Streamlit Cloud)
    _MAX_ROWS_GITHUB = 500_000
    if len(df) > _MAX_ROWS_GITHUB:
        return False, (
            f"Dataset muy grande ({len(df):,} filas). "
            f"GitHub solo guarda hasta {_MAX_ROWS_GITHUB:,} filas. "
            "Datos disponibles en caché local de esta sesión."
        )

    cfg = _cfg()
    headers = _gh_headers(cfg)
    url = f"https://api.github.com/repos/{cfg['repo']}/contents/{cfg['path']}"

    try:
        # Serializar parquet con compresión gzip (mejor ratio para GitHub)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="gzip")
        content_b64 = base64.b64encode(buf.getvalue()).decode("ascii")

        # Obtener SHA del archivo existente (necesario para actualizar)
        r_get = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=15)
        sha = r_get.json().get("sha") if r_get.status_code == 200 else None

        payload: dict = {
            "message": f"[APS] Actualización datos consolidados — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content_b64,
            "branch": cfg["branch"],
        }
        if sha:
            payload["sha"] = sha

        r_put = requests.put(url, headers=headers, json=payload, timeout=60)

        if r_put.status_code in (200, 201):
            return True, f"✅ Datos guardados en GitHub ({len(df):,} registros)"
        else:
            err = r_put.json().get("message", r_put.text[:200])
            return False, f"Error GitHub ({r_put.status_code}): {err}"

    except requests.exceptions.Timeout:
        return False, "Timeout al conectar con GitHub (archivo muy grande o conexión lenta)"
    except Exception as e:
        return False, f"Error al guardar en GitHub: {e}"


# ── Cargar ──────────────────────────────────────────────────────────────────────

def load_data() -> tuple[pd.DataFrame | None, dict, str]:
    """
    Carga datos persistidos. Intenta en este orden:
      1. /tmp (caché local — más rápido, misma sesión del servidor)
      2. GitHub (fuente de verdad duradera)

    Returns:
        (df | None, metadata_dict, origen: str)
    """
    # 1. Intentar /tmp primero (casi instantáneo)
    try:
        if os.path.exists(_TMP_PARQUET):
            df = pd.read_parquet(_TMP_PARQUET)
            if not df.empty:
                meta = {}
                if os.path.exists(_TMP_META):
                    with open(_TMP_META, encoding="utf-8") as f:
                        meta = json.load(f)
                return df, meta, "tmp"
    except Exception:
        pass

    # 2. Intentar GitHub
    if not github_configured():
        return None, {}, "sin_config"

    cfg = _cfg()
    headers = _gh_headers(cfg)
    url = f"https://api.github.com/repos/{cfg['repo']}/contents/{cfg['path']}"

    try:
        r = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=30)
        if r.status_code == 404:
            return None, {}, "sin_datos"
        if r.status_code != 200:
            return None, {}, f"error_github_{r.status_code}"

        data = r.json()
        content = base64.b64decode(data["content"])
        df = pd.read_parquet(io.BytesIO(content))

        if df.empty:
            return None, {}, "vacio"

        # Guardar en /tmp para próximas cargas (evita roundtrip a GitHub)
        try:
            df.to_parquet(_TMP_PARQUET, index=False, compression="snappy")
        except Exception:
            pass

        meta = {"actualizado": data.get("commit", {}).get("committer", {}).get("date", "—")}
        return df, meta, "github"

    except requests.exceptions.Timeout:
        return None, {}, "timeout"
    except Exception as e:
        return None, {}, f"error: {e}"


# ── Borrar ──────────────────────────────────────────────────────────────────────

def delete_data() -> tuple[bool, str]:
    """
    Elimina datos de /tmp y de GitHub (reemplaza el archivo con DataFrame vacío).
    """
    # Limpiar /tmp
    for path in [_TMP_PARQUET, _TMP_META]:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    if not github_configured():
        return True, "Caché local limpiado (GitHub no configurado)."

    # En GitHub: eliminar el archivo
    cfg = _cfg()
    headers = _gh_headers(cfg)
    url = f"https://api.github.com/repos/{cfg['repo']}/contents/{cfg['path']}"

    try:
        r_get = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=15)
        if r_get.status_code == 404:
            return True, "No había datos en GitHub que borrar."

        sha = r_get.json().get("sha")
        if not sha:
            return False, "No se pudo obtener SHA del archivo para borrarlo."

        r_del = requests.delete(url, headers=headers, json={
            "message": "[APS] Eliminar datos consolidados",
            "sha": sha,
            "branch": cfg["branch"],
        }, timeout=15)

        if r_del.status_code == 200:
            return True, "Datos eliminados de GitHub y caché local."
        else:
            return False, f"Error al borrar de GitHub: {r_del.status_code}"

    except Exception as e:
        return False, f"Error al borrar: {e}"


# ── Info de estado ───────────────────────────────────────────────────────────────

def storage_status() -> dict:
    """Retorna información sobre el estado del almacenamiento."""
    cfg = _cfg()
    tmp_ok = os.path.exists(_TMP_PARQUET)
    gh_ok = github_configured()

    last_update = "—"
    if tmp_ok and os.path.exists(_TMP_META):
        try:
            with open(_TMP_META, encoding="utf-8") as f:
                m = json.load(f)
            last_update = m.get("actualizado", "—")
        except Exception:
            pass

    return {
        "github_configurado": gh_ok,
        "repo": cfg.get("repo", "no configurado"),
        "cache_local": tmp_ok,
        "ultima_actualizacion": last_update,
    }
