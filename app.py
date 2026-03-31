"""
Sistema de Análisis de Productividad APS
Servicio de Salud Metropolitano Central - Chile
"""
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import warnings

# ─────────────────────────────────────────────────────────────
# AUTENTICACIÓN SIMPLE POR CONTRASEÑA
# La contraseña vive en Streamlit Secrets → Settings → Secrets:
#   [auth]
#   password = "tu_clave_segura"
# ─────────────────────────────────────────────────────────────
def check_password():
    def password_entered():
        expected = st.secrets.get("auth", {}).get("password", "salud2026")
        if st.session_state["password"] == expected:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.title("🔒 Acceso restringido")
        st.text_input(
            "Ingresa la contraseña para acceder a la aplicación:",
            type="password",
            on_change=password_entered,
            key="password"
        )
        if "password_correct" in st.session_state and not st.session_state["password_correct"]:
            st.error("Contraseña incorrecta. Intenta nuevamente.")
        st.stop()

# ── Configuración de página (debe ser el primer comando st) ──
st.set_page_config(
    page_title="Productividad APS | SSMC",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

check_password()

warnings.filterwarnings("ignore")



from src.processor import process_iris_file, consolidate_files
from src.kpis import (
    calculate_all_kpis, kpis_por_mes, kpis_por_instrumento,
    kpis_por_centro, detectar_alertas, KPI_DEFINITIONS,
    kpis_por_tipo_atencion, kpis_tipo_atencion_mes,
    kpis_instrumento_mes
)
from src.charts import (
    chart_ranking_centros, chart_evolucion_mensual,
    chart_tipo_atencion, chart_sector, chart_noshow_vs_umbral,
    chart_rendimiento_instrumento, chart_estado_cupos, chart_multi_kpi,
    build_semaforo_table, chart_heatmap_pivot
)
from src.demo_data import generate_demo_data, get_demo_metadata
from src.storage import save_data, load_data, delete_data, github_configured, storage_status
from src import bigquery_client as bq
import gc

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1B4F72 0%, #2E86C1 100%);
        color: white;
        padding: 1.2rem 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
    }
    .main-header h1 { color: white; margin: 0; font-size: 1.6rem; }
    .main-header p { color: #AED6F1; margin: 0.2rem 0 0 0; font-size: 0.9rem; }

    .kpi-card {
        background: white;
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
        border-left: 5px solid #2E86C1;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    .kpi-card.verde { border-left-color: #27AE60; }
    .kpi-card.amarillo { border-left-color: #F39C12; }
    .kpi-card.rojo { border-left-color: #E74C3C; }

    .kpi-valor { font-size: 2rem; font-weight: 700; color: #1B4F72; }
    .kpi-nombre { font-size: 0.8rem; color: #555; margin-top: 0.2rem; }

    .alerta-rojo { background: #FDEDEC; border-left: 4px solid #E74C3C;
                   padding: 0.7rem 1rem; border-radius: 5px; margin: 0.4rem 0; }
    .alerta-amarillo { background: #FEF9E7; border-left: 4px solid #F39C12;
                       padding: 0.7rem 1rem; border-radius: 5px; margin: 0.4rem 0; }

    .metric-badge { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 12px;
                    font-size: 0.75rem; font-weight: 600; }
    .badge-verde { background: #D5F5E3; color: #1E8449; }
    .badge-amarillo { background: #FDEBD0; color: #9A7D0A; }
    .badge-rojo { background: #FADBD8; color: #922B21; }

    div[data-testid="stMetric"] { background: white; border-radius: 8px;
                                   padding: 0.8rem; box-shadow: 0 1px 4px rgba(0,0,0,0.06); }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# PERSISTENCIA — /tmp (caché rápido) + GitHub (durable, multi-usuario)
# Gestión delegada a src/storage.py
# ─────────────────────────────────────────────────────────────
def _save_session():
    """Guarda df en GitHub + /tmp (fallback). Solo para datos reales (no demo)."""
    if st.session_state.get("df") is None or st.session_state.get("demo_loaded", False):
        return
    # Guardar en /tmp + GitHub solo si BQ no está configurado (BQ es el almacén primario)
    if not bq.bq_configured():
        ok, msg = save_data(
            st.session_state.df,
            registro_cargas=st.session_state.get("registro_cargas", [])
        )
        if not ok and "GitHub no configurado" not in msg:
            st.toast(msg, icon="⚠️")


def _load_session() -> bool:
    """Carga datos desde /tmp o GitHub. Retorna True si se encontraron datos."""
    df, meta, origen = load_data()
    if df is not None and not df.empty:
        st.session_state.df = df
        reg = meta.get("registro_cargas", [])
        if reg:
            st.session_state.registro_cargas = reg
        return True
    return False


# ─────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────
def init_session():
    defaults = {
        "df": None,
        "metadata_list": [],
        "archivos_cargados": [],
        "demo_loaded": False,
        "registro_cargas": [],
        # BigQuery: opciones de filtro cacheadas + total de registros
        "bq_filter_options": {},
        "bq_total_registros": 0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_session()

# Auto-cargar al inicio de sesión
if st.session_state.df is None and not st.session_state.demo_loaded:
    if bq.bq_configured():
        # Con BigQuery: cargar solo metadatos/opciones de filtro (NO los datos crudos)
        # Los datos se cargan solo cuando el usuario aplica filtros
        if not st.session_state.bq_filter_options:
            try:
                opts = bq.get_filter_options()
                if opts:
                    st.session_state.bq_filter_options = opts
                    st.session_state.bq_total_registros = bq.get_record_count()
                    st.toast(
                        f"🗄️ BigQuery conectado · {st.session_state.bq_total_registros:,} registros disponibles",
                        icon="✅"
                    )
            except Exception:
                pass
    else:
        # Sin BigQuery: fallback a /tmp + GitHub (comportamiento anterior)
        try:
            if _load_session():
                n_rec = len(st.session_state.df)
                if n_rec > 800_000:
                    st.session_state.df = None
                    st.toast(
                        "⚠️ Datos guardados muy grandes para cargar automáticamente. "
                        "Sube los archivos manualmente.",
                        icon="⚠️"
                    )
                else:
                    st.toast(f"💾 Datos recuperados · {n_rec:,} registros", icon="✅")
        except MemoryError:
            st.session_state.df = None
            st.toast("⚠️ Memoria insuficiente para recuperar datos guardados.", icon="⚠️")
        except Exception:
            st.session_state.df = None


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def has_data() -> bool:
    if st.session_state.df is not None and not st.session_state.df.empty:
        return True
    if bq.bq_configured() and st.session_state.get("bq_total_registros", 0) > 0:
        return True
    return False


def has_df() -> bool:
    """True solo si hay datos cargados en memoria (session_state.df)."""
    return st.session_state.df is not None and not st.session_state.df.empty


def apply_filters(df: pd.DataFrame, filtros: dict) -> pd.DataFrame:
    dff = df.copy()
    if filtros.get("centros"):
        dff = dff[dff["ESTABLECIMIENTO"].isin(filtros["centros"])]
    if filtros.get("meses"):
        dff = dff[dff["MES_NUM"].isin(filtros["meses"])]
    if filtros.get("instrumentos"):
        dff = dff[dff["INSTRUMENTO"].isin(filtros["instrumentos"])]
    if filtros.get("sectores"):
        dff = dff[dff["SECTOR"].isin(filtros["sectores"])]
    if filtros.get("tipos_atencion"):
        dff = dff[dff["TIPO ATENCION"].isin(filtros["tipos_atencion"])]
    if filtros.get("tipo_cupo"):
        dff = dff[dff["TIPO CUPO"].isin(filtros["tipo_cupo"])]
    return dff


def semaforo_icon(s: str) -> str:
    return {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴", "gris": "⚪"}.get(s, "⚪")


def kpi_delta(valor: float, umbral: float, mayor_mejor: bool) -> str:
    diff = valor - umbral
    if mayor_mejor:
        return f"{diff:+.1f}pp vs meta"
    else:
        return f"{diff:+.1f}pp vs umbral"


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
def render_sidebar() -> dict:
    with st.sidebar:
        st.markdown("### 🏥 SSMC · Productividad APS")
        st.caption("Servicio de Salud Metropolitano Central")
        st.divider()

        # Navegación
        nav = st.radio(
            "Navegación",
            ["🏠 Inicio y Carga", "📊 Dashboard KPIs", "📈 Evolución Temporal",
             "🔍 Análisis Detallado", "⚠️ Alertas y Brechas", "📋 Informe por Centro"],
            label_visibility="collapsed",
        )
        st.divider()

        filtros = {}
        MESES_N = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
                   7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}

        # ── Determinar fuente de opciones de filtro ──────────────────────────
        # Prioridad: datos en memoria → opciones BQ cacheadas → vacío
        if has_df():
            df = st.session_state.df
            opts_centros    = sorted(df["ESTABLECIMIENTO"].dropna().unique().tolist())
            opts_meses      = sorted(df["MES_NUM"].dropna().unique().tolist())
            opts_inst       = sorted(df["INSTRUMENTO"].dropna().unique().tolist())
            opts_sectores   = sorted(df["SECTOR"].dropna().unique().tolist())
            opts_tc         = sorted(df["TIPO CUPO"].dropna().unique().tolist())
        elif bq.bq_configured() and st.session_state.bq_filter_options:
            opts_bq = st.session_state.bq_filter_options
            opts_centros    = opts_bq.get("establecimientos", [])
            opts_meses      = opts_bq.get("meses", [])
            opts_inst       = opts_bq.get("instrumentos", [])
            opts_sectores   = opts_bq.get("sectores", [])
            opts_tc         = opts_bq.get("tipos_cupo", [])
        else:
            opts_centros = opts_meses = opts_inst = opts_sectores = opts_tc = []

        if opts_centros:
            st.markdown("**Filtros**")

            centros_sel = st.multiselect("Centro de Salud", opts_centros,
                                          default=opts_centros, key="filt_centros")
            filtros["centros"] = centros_sel if centros_sel else opts_centros

            meses_labels = {m: f"{MESES_N.get(int(m), str(m))} ({int(m)})" for m in opts_meses}
            meses_sel_labels = st.multiselect(
                "Meses", options=list(meses_labels.values()),
                default=list(meses_labels.values()), key="filt_meses"
            )
            meses_sel = [m for m, lbl in meses_labels.items() if lbl in meses_sel_labels]
            filtros["meses"] = meses_sel if meses_sel else opts_meses

            inst_sel = st.multiselect("Instrumento/Profesional", opts_inst,
                                       default=opts_inst, key="filt_inst")
            filtros["instrumentos"] = inst_sel if inst_sel else opts_inst

            sect_sel = st.multiselect("Sector Territorial", opts_sectores,
                                       default=opts_sectores, key="filt_sect")
            filtros["sectores"] = sect_sel if sect_sel else opts_sectores

            tc_sel = st.multiselect("Tipo Cupo", opts_tc,
                                     default=opts_tc, key="filt_tc")
            filtros["tipo_cupo"] = tc_sel if tc_sel else opts_tc

            st.divider()

            # ── Botón "Cargar desde BigQuery" (solo si BQ configurado) ───────
            if bq.bq_configured() and not has_df():
                n_total = st.session_state.get("bq_total_registros", 0)
                st.caption(f"🗄️ **{n_total:,}** registros en BigQuery")
                if st.button("📥 Cargar datos filtrados", type="primary",
                             use_container_width=True, key="btn_bq_load"):
                    with st.spinner("Consultando BigQuery..."):
                        df_bq, msg_bq = bq.load_filtered(
                            centros=filtros.get("centros"),
                            meses=filtros.get("meses"),
                            instrumentos=filtros.get("instrumentos"),
                            sectores=filtros.get("sectores"),
                            tipos_cupo=filtros.get("tipo_cupo"),
                        )
                    if df_bq is not None:
                        st.session_state.df = df_bq
                        st.session_state.demo_loaded = False
                        st.toast(msg_bq, icon="✅")
                        st.rerun()
                    else:
                        st.warning(msg_bq)
            elif has_df():
                dff_count = apply_filters(st.session_state.df, filtros)
                st.caption(f"📋 **{len(dff_count):,}** registros seleccionados")
                if bq.bq_configured():
                    n_total = st.session_state.get("bq_total_registros", 0)
                    st.caption(f"🗄️ Total en BQ: **{n_total:,}**")
                    if st.button("🔄 Recargar desde BigQuery", use_container_width=True,
                                 key="btn_bq_reload"):
                        st.session_state.df = None
                        st.rerun()

            if st.session_state.demo_loaded:
                st.info("📊 Modo Demo activo", icon="ℹ️")

        return nav, filtros


# ─────────────────────────────────────────────────────────────
# PÁGINA 1: INICIO Y CARGA
# ─────────────────────────────────────────────────────────────
def page_inicio():
    st.markdown("""
    <div class="main-header">
        <h1>🏥 Sistema de Análisis de Productividad APS</h1>
        <p>Servicio de Salud Metropolitano Central · Atención Primaria de Salud · 2026</p>
    </div>
    """, unsafe_allow_html=True)

    col_info, col_carga = st.columns([1, 1], gap="large")

    with col_info:
        st.markdown("#### ¿Qué analiza este sistema?")
        st.markdown("""
        Consolida reportes **IRIS** de múltiples CESFAM y calcula automáticamente
        **10 indicadores clave** de productividad según el modelo APS-SSMC:

        | # | Indicador | Meta |
        |---|-----------|------|
        | 1 | Tasa de Ocupación | ≥ 65% |
        | 2 | Tasa de No-Show | ≤ 10% |
        | 3 | Tasa de Bloqueo | ≤ 10% |
        | 4 | Efectividad de Cita | ≥ 88% |
        | 5 | Rendimiento Promedio | Referencia |
        | 6 | Cupos Sobrecupo | ≤ 5% |
        | 7 | Cobertura Sectorial | ≥ 80% |
        | 8 | Agendamiento Remoto | > 20% |
        | 9 | Variación Mensual | ≤ 5pp |
        | 10 | Ocupación Hora Extendida | ≥ 50% |

        > **Privacidad**: los datos personales (RUT, nombre, teléfono) se eliminan
        > automáticamente durante el procesamiento.
        """)

    with col_carga:
        st.markdown("#### Cargar datos")

        tab_upload, tab_restore, tab_demo = st.tabs(["📂 Subir archivo(s) IRIS", "📥 Cargar datos guardados", "🎲 Usar datos demo"])

        with tab_upload:
            st.markdown("Sube uno o más archivos `.xlsx` exportados desde IRIS:")
            uploaded_files = st.file_uploader(
                "Archivos IRIS (.xlsx)",
                type=["xlsx"],
                accept_multiple_files=True,
                label_visibility="collapsed",
                help="Formato: 'Cantidad de Cupos por Citas' generado por IRIS"
            )

            if uploaded_files:
                if st.button("⚙️ Procesar archivos", type="primary", use_container_width=True):
                    dfs = []
                    meta_list = []
                    errores_globales = []

                    progress = st.progress(0, text="Procesando...")
                    for i, uf in enumerate(uploaded_files):
                        progress.progress((i + 1) / len(uploaded_files),
                                          text=f"Procesando {uf.name}...")
                        file_bytes = BytesIO(uf.read())

                        # Procesar directamente (sin pre-validación que duplica la lectura)
                        df_proc, meta, errs = process_iris_file(file_bytes, uf.name)
                        if df_proc is not None:
                            dfs.append(df_proc)
                            meta_list.append(meta)
                        if errs:
                            errores_globales.extend([f"{uf.name}: {e}" for e in errs])
                        del file_bytes
                        gc.collect()  # liberar memoria entre archivos

                    progress.empty()

                    if dfs:
                        n_archivos = len(dfs)
                        df_nuevos = consolidate_files(dfs)
                        del dfs
                        gc.collect()
                        n_nuevos = len(df_nuevos)

                        # ── Guardar en BigQuery (almacén primario) ───────────
                        ok_bq = False
                        if bq.bq_configured():
                            with st.spinner("Guardando en BigQuery..."):
                                ok_bq, msg_bq = bq.insert_data(df_nuevos)
                            if ok_bq:
                                # Actualizar metadatos BQ en sesión
                                st.session_state.bq_filter_options = bq.get_filter_options()
                                st.session_state.bq_total_registros = bq.get_record_count()
                                st.info(msg_bq, icon="🗄️")
                                # Verificación de integridad post-carga
                                n_bq_after = st.session_state.bq_total_registros
                                if n_bq_after < n_nuevos:
                                    st.warning(
                                        f"⚠️ **Verificación de integridad**: se procesaron "
                                        f"**{n_nuevos:,}** registros pero BigQuery reporta "
                                        f"**{n_bq_after:,}** en total. Posible pérdida de "
                                        f"**{n_nuevos - n_bq_after:,}** registros durante la "
                                        f"carga. Intente eliminar los datos en BigQuery y "
                                        f"re-subir el archivo.",
                                        icon="⚠️"
                                    )
                            else:
                                st.warning(f"BigQuery: {msg_bq}", icon="⚠️")

                        # ── Mantener en sesión para análisis inmediato ───────
                        if bq.bq_configured() and ok_bq:
                            # BQ es la fuente de verdad: no acumular en RAM
                            # Solo guardamos los nuevos para visualización inmediata
                            st.session_state.df = df_nuevos
                            del df_nuevos
                            df_final = st.session_state.df
                        else:
                            # Sin BQ: acumular en RAM (comportamiento original)
                            if st.session_state.df is not None and not st.session_state.df.empty and not st.session_state.demo_loaded:
                                df_final = consolidate_files([st.session_state.df, df_nuevos])
                                del df_nuevos
                            else:
                                df_final = df_nuevos
                            st.session_state.df = df_final
                        gc.collect()
                        st.session_state.metadata_list += meta_list
                        st.session_state.archivos_cargados += [f.name for f in uploaded_files]
                        st.session_state.demo_loaded = False
                        from datetime import datetime as _dt
                        for meta in meta_list:
                            st.session_state.registro_cargas.append({
                                "Archivo": meta.get("archivo", "—")[:45],
                                "Centro": meta.get("establecimiento", "—")[:35],
                                "Fecha desde": meta.get("fecha_desde", "—"),
                                "Fecha hasta": meta.get("fecha_hasta", "—"),
                                "Registros nuevos": n_nuevos,
                                "Cargado el": _dt.now().strftime("%d/%m/%Y %H:%M"),
                            })
                        _save_session()  # fallback /tmp+GitHub si BQ no configurado
                        n_bq_total = st.session_state.get("bq_total_registros", 0)
                        st.success(f"✅ {n_archivos} archivo(s) procesados · **{n_nuevos:,}** nuevos registros · **{n_bq_total:,}** total en BigQuery")
                        if not bq.bq_configured():
                            if len(df_final) > 800_000:
                                st.warning(
                                    f"⚠️ Dataset muy grande ({len(df_final):,} filas). "
                                    "Configura BigQuery en Secrets para manejar millones de registros.",
                                    icon="⚠️"
                                )
                            if github_configured():
                                st.info("💾 Datos guardados en GitHub (sin BigQuery configurado).", icon="✅")
                            else:
                                st.warning("⚠️ Sin BigQuery ni GitHub: datos solo en caché local.", icon="⚠️")

                    for err in errores_globales:
                        st.warning(err)

        with tab_restore:
            st.markdown("Sube un archivo **`.csv`** descargado previamente desde esta app para continuar el análisis acumulado sin recargar los archivos IRIS originales:")
            uploaded_csv = st.file_uploader(
                "Datos consolidados guardados (.csv)",
                type=["csv"],
                label_visibility="collapsed",
                key="restore_csv",
            )
            if uploaded_csv:
                if st.button("📥 Restaurar datos guardados", type="primary", use_container_width=True):
                    with st.spinner("Restaurando datos..."):
                        df_rest = pd.read_csv(uploaded_csv)
                        # Restaurar tipos de columnas
                        if "FECHA" in df_rest.columns:
                            df_rest["FECHA"] = pd.to_datetime(df_rest["FECHA"], errors="coerce")
                        for _col in ["MES_NUM", "HORA_NUM", "RENDIMIENTO", "CUPOS UTILIZADOS", "EDAD_ANO"]:
                            if _col in df_rest.columns:
                                df_rest[_col] = pd.to_numeric(df_rest[_col], errors="coerce")
                        if st.session_state.df is not None and not st.session_state.df.empty and not st.session_state.demo_loaded:
                            df_rest = consolidate_files([st.session_state.df, df_rest])
                        st.session_state.df = df_rest
                        st.session_state.demo_loaded = False
                        from datetime import datetime as _dt
                        st.session_state.registro_cargas.append({
                            "Archivo": uploaded_csv.name[:45],
                            "Centro": "Datos restaurados desde CSV",
                            "Fecha desde": "—",
                            "Fecha hasta": "—",
                            "Registros nuevos": len(df_rest),
                            "Cargado el": _dt.now().strftime("%d/%m/%Y %H:%M"),
                        })
                    _save_session()
                    st.success(f"✅ Datos restaurados · **{len(df_rest):,}** registros cargados")
                    st.info("Navega al **Dashboard KPIs** en el menú lateral para ver los resultados.")

        with tab_demo:
            st.markdown("""
            Carga datos **sintéticos** generados a partir de las distribuciones
            reales del CESFAM N°5 (2025) para explorar todas las funcionalidades.
            """)
            n_demo = st.slider("Número de registros demo", 20_000, 150_000, 80_000, 10_000)
            if st.button("🎲 Cargar datos demo", type="secondary", use_container_width=True):
                with st.spinner("Generando datos demo..."):
                    df_demo = generate_demo_data(n_records=n_demo)
                    st.session_state.df = df_demo
                    st.session_state.metadata_list = [get_demo_metadata()]
                    st.session_state.demo_loaded = True
                st.success(f"✅ Demo cargado · **{len(df_demo):,}** registros · 7 CESFAM · 12 meses")
                st.info("Navega al **Dashboard KPIs** en el menú lateral para ver los resultados.")

    # ── Tabla de definiciones de indicadores ──
    st.divider()
    with st.expander("📖 Definición y fórmula de los 10 indicadores de productividad", expanded=False):
        st.markdown("""
        Referencia técnica para que cualquier persona de la audiencia pueda interpretar correctamente cada indicador.
        """)
        st.markdown("""
| N° | Indicador | ¿Qué mide? | Fórmula de cálculo | Meta | Alerta |
|----|-----------|-----------|-------------------|------|--------|
| 1 | **Tasa de Ocupación** | % de cupos citados sobre el total de cupos disponibles para atención. Refleja qué tan bien se aprovechan las horas clínicas programadas. | Citados ÷ (Citados + Disponibles) × 100 | ≥ 65% | < 50% |
| 2 | **Tasa de No-Show** | % de pacientes con cita confirmada que **no asistieron ni cancelaron**. El término "no-show" (no se presentó) implica un cupo perdido que no puede reasignarse a otro paciente a tiempo. | (Citados − Completados) ÷ Citados × 100 | ≤ 10% | > 15% |
| 3 | **Tasa de Bloqueo** | % de cupos bloqueados administrativamente (vacaciones, capacitaciones, fallas de equipos, reuniones, etc.) sobre el total de cupos. Reducen la capacidad real de atención. | Bloqueados ÷ Total cupos × 100 | ≤ 10% | > 15% |
| 4 | **Efectividad de Cita** | % de citas confirmadas que terminaron en atención efectiva. Refleja conjuntamente el impacto del no-show más las cancelaciones de último minuto. | Completados ÷ Citados × 100 | ≥ 88% | < 80% |
| 5 | **Rendimiento Promedio** | Tiempo promedio en minutos por atención registrado en el sistema. Agendas muy cortas (< 10 min) sugieren riesgo de calidad; muy largas pueden indicar baja productividad. | Promedio de minutos por atención | Según instrumento | Desviación > 30% |
| 6 | **Cupos Sobrecupo** | % de cupos en modalidad "sobrecupo" (cupos extra agregados fuera de la agenda regular). Un alto sobrecupo indica presión asistencial o subestimación de la demanda real. | Sobrecupos ÷ Total cupos × 100 | ≤ 5% | > 10% |
| 7 | **Cobertura Sectorial** | % de registros de atención que tienen informado el sector territorial del paciente (Verde / Lila / Rojo). Mide la calidad del registro para análisis por territorio. | Con sector informado ÷ Total × 100 | ≥ 80% | < 60% |
| 8 | **Agendamiento Remoto** | % de citas agendadas por vía remota (teléfono o telesalud). Un valor bajo indica que los pacientes deben concurrir presencialmente a agendar, lo que dificulta el acceso. | (Telefónico + Telesalud) ÷ Total × 100 | > 20% | < 5% |
| 9 | **Variación Mensual de Ocupación** | Máximo cambio mes a mes en la tasa de ocupación (en puntos porcentuales). Detecta caídas o alzas bruscas que pueden indicar eventos críticos (paros, emergencias, cierres, etc.). | Máx \|Ocupación mes N − Ocupación mes N-1\| | ≤ 5 pp | > 10 pp |
| 10 | **Ocupación Horario Extendido** | Tasa de ocupación en el horario extendido (≥ 18:00 hrs). Evalúa si los cupos de jornada extendida —que representan un costo adicional para el establecimiento— están siendo bien utilizados. | Citados ≥18h ÷ (Citados + Disponibles ≥18h) × 100 | ≥ 50% | < 30% |
        """)
        st.info("💡 **Semáforo:** 🟢 Verde = cumple la meta · 🟡 Amarillo = zona de observación · 🔴 Rojo = requiere intervención")
        st.caption("Fuente: Modelo de análisis de productividad APS · Servicio de Salud Metropolitano Central · 2026")

    # ── Resumen de datos cargados ──
    if has_data():
        st.divider()
        st.markdown("#### Resumen de datos cargados")

        # ── Métricas de resumen ─────────────────────────────────────────────
        if bq.bq_configured() and st.session_state.get("bq_total_registros", 0) > 0 and not has_df():
            # Solo tenemos metadatos BQ — mostrar resumen desde filter_options
            opts = st.session_state.bq_filter_options
            n_total = st.session_state.bq_total_registros
            cols = st.columns(5)
            cols[0].metric("📋 Total registros", f"{n_total:,}")
            cols[1].metric("🏥 CESFAM", str(len(opts.get("establecimientos", []))))
            cols[2].metric("👤 Instrumentos", str(len(opts.get("instrumentos", []))))
            cols[3].metric("📅 Meses", str(len(opts.get("meses", []))))
            cols[4].metric("📌 Tipos atención", str(len(opts.get("tipos_atencion", []))))
            st.info(
                "Los datos están almacenados en BigQuery. "
                "Usa el botón **📥 Cargar datos filtrados** en el panel lateral para analizar un subconjunto.",
                icon="🗄️"
            )
            # ── Archivos cargados en BQ ─────────────────────────────────────
            archivos_bq = bq.get_archivos_cargados()
            if archivos_bq:
                st.markdown("##### 📂 Archivos en BigQuery")
                df_arch = pd.DataFrame(archivos_bq)
                if "ultima_carga" in df_arch.columns:
                    df_arch["ultima_carga"] = pd.to_datetime(df_arch["ultima_carga"]).dt.strftime("%d/%m/%Y %H:%M")
                st.dataframe(df_arch, width="stretch", hide_index=True)
            # ── Descarga completa desde BQ ──────────────────────────────────
            st.markdown("##### 💾 Descargar datos consolidados")
            st.caption("Descarga todos los datos almacenados en BigQuery como CSV.")
            if st.button("⬇️ Preparar descarga completa desde BigQuery", use_container_width=True):
                with st.spinner("Exportando desde BigQuery... (puede tomar unos segundos)"):
                    csv_bq = bq.export_csv_bytes()
                if csv_bq:
                    st.download_button(
                        "💾 Descargar datos_consolidados_aps.csv",
                        data=csv_bq,
                        file_name="datos_consolidados_aps.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                else:
                    st.error("No se pudo exportar desde BigQuery.")

            if st.button("🗑️ Limpiar TODOS los datos de BigQuery", type="secondary", use_container_width=True):
                ok_del, msg_del = bq.delete_all_data()
                if ok_del:
                    st.session_state.df = None
                    st.session_state.metadata_list = []
                    st.session_state.archivos_cargados = []
                    st.session_state.demo_loaded = False
                    st.session_state.registro_cargas = []
                    st.session_state.bq_filter_options = {}
                    st.session_state.bq_total_registros = 0
                    delete_data()
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(msg_del)

        elif has_df():
            df = st.session_state.df
            cols = st.columns(5)
            metrics = [
                ("Total registros", f"{len(df):,}", "📋"),
                ("CESFAM", f"{df['ESTABLECIMIENTO'].nunique()}", "🏥"),
                ("Instrumentos", f"{df['INSTRUMENTO'].nunique()}", "👤"),
                ("Meses", f"{df['MES_NUM'].nunique()}", "📅"),
                ("Tipos atención", f"{df['TIPO ATENCION'].nunique()}", "📌"),
            ]
            for col, (label, val, icon) in zip(cols, metrics):
                col.metric(f"{icon} {label}", val)

            st.markdown("##### Composición de cupos")
            col1, col2 = st.columns([2, 1])
            with col1:
                fig_est = chart_estado_cupos(df)
                st.plotly_chart(fig_est)
            with col2:
                st.markdown("**Archivos en sesión:**")
                archivos = df["_archivo"].unique()
                for a in archivos:
                    n = (df["_archivo"] == a).sum()
                    st.write(f"• {a[:40]} ({n:,} reg.)")
                st.divider()
                # Descarga desde sesión
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "💾 Descargar datos consolidados (.csv)",
                    data=csv_bytes,
                    file_name="datos_consolidados_aps.csv",
                    mime="text/csv",
                    width="stretch",
                    help="Descarga los datos actualmente cargados en sesión.",
                )
                if st.button("🗑️ Limpiar todos los datos", type="secondary", use_container_width=True):
                    st.session_state.df = None
                    st.session_state.metadata_list = []
                    st.session_state.archivos_cargados = []
                    st.session_state.demo_loaded = False
                    st.session_state.registro_cargas = []
                    if bq.bq_configured():
                        bq.delete_all_data()
                        st.session_state.bq_filter_options = {}
                        st.session_state.bq_total_registros = 0
                    delete_data()
                    st.cache_data.clear()
                    st.rerun()

        if st.session_state.registro_cargas:
            st.markdown("##### 📋 Registro de cargas (sesión actual)")
            st.caption("Historial incremental de archivos procesados. La columna **'Fecha hasta'** indica el último período cargado por archivo; úsala para saber desde qué fecha debes generar el próximo reporte IRIS.")
            df_reg = pd.DataFrame(st.session_state.registro_cargas)
            st.dataframe(df_reg, width="stretch", hide_index=True)


# ─────────────────────────────────────────────────────────────
# PÁGINA 2: DASHBOARD KPIs
# ─────────────────────────────────────────────────────────────
def page_dashboard(dff: pd.DataFrame):
    st.markdown("""
    <div class="main-header">
        <h1>📊 Dashboard de KPIs</h1>
        <p>10 indicadores clave de productividad · Semáforo de alertas</p>
    </div>
    """, unsafe_allow_html=True)

    if dff.empty:
        st.warning("Sin datos con los filtros seleccionados.")
        return

    kpis = calculate_all_kpis(dff)

    # ── Tarjetas KPI ──
    st.markdown("#### Indicadores Clave")

    kpi_order = [
        ("ocupacion", "Tasa Ocupación"),
        ("no_show", "No-Show"),
        ("bloqueo", "Bloqueo"),
        ("efectividad", "Efectividad Cita"),
        ("rendimiento", "Rendimiento"),
        ("sobrecupo", "Sobrecupo"),
        ("cobertura_sectorial", "Cob. Sectorial"),
        ("agendamiento_remoto", "Ag. Remoto"),
        ("variacion_mensual", "Var. Mensual"),
        ("ocupacion_extendida", "Ocup. Extendida"),
    ]

    cols = st.columns(5)
    for i, (key, label) in enumerate(kpi_order):
        k = kpis.get(key, {})
        valor = k.get("valor", 0)
        unidad = k.get("unidad", "%")
        sem = k.get("semaforo", "gris")
        icon = semaforo_icon(sem)
        delta = None
        if k.get("umbral_ok") and unidad == "%":
            delta = kpi_delta(valor, k["umbral_ok"], k.get("direccion") == "mayor_es_mejor")

        with cols[i % 5]:
            st.metric(
                label=f"{icon} {label}",
                value=f"{valor:.1f} {unidad}",
                delta=delta,
                delta_color="normal" if k.get("direccion") == "mayor_es_mejor" else "inverse",
            )

    st.divider()

    # ── Tabla semáforo ──
    col_tabla, col_sector = st.columns([2, 1])
    with col_tabla:
        st.markdown("#### Semáforo de KPIs")
        df_sem = build_semaforo_table(kpis)
        # Estilo: resaltar filas rojas
        def highlight_row(row):
            if row.get("_semaforo") == "rojo":
                return ["background-color: #FDEDEC"] * len(row)
            elif row.get("_semaforo") == "amarillo":
                return ["background-color: #FEF9E7"] * len(row)
            return [""] * len(row)

        display_cols = ["Estado", "Indicador", "Valor", "Meta", "Alerta si", "Descripción"]
        styled = (df_sem[display_cols + ["_semaforo"]]
                  .style
                  .apply(highlight_row, axis=1)
                  .hide(axis="index"))
        st.dataframe(styled, width="stretch", hide_index=True,
                     column_config={"_semaforo": None})

    with col_sector:
        st.markdown("#### Distribución Sectorial")
        fig_sec = chart_sector(dff)
        st.plotly_chart(fig_sec)

    st.divider()

    # ── Ranking centros + Multi-KPI ──
    col1, col2 = st.columns(2)
    with col1:
        df_centros = kpis_por_centro(dff)
        if not df_centros.empty:
            fig_rank = chart_ranking_centros(df_centros)
            st.plotly_chart(fig_rank)
    with col2:
        df_meses = kpis_por_mes(dff)
        if not df_meses.empty:
            fig_multi = chart_multi_kpi(df_meses)
            st.plotly_chart(fig_multi)


# ─────────────────────────────────────────────────────────────
# PÁGINA 3: EVOLUCIÓN TEMPORAL
# ─────────────────────────────────────────────────────────────
def page_evolucion(dff: pd.DataFrame):
    st.markdown("""
    <div class="main-header">
        <h1>📈 Evolución Temporal de KPIs</h1>
        <p>Tendencias mes a mes · Comparativo vs umbrales</p>
    </div>
    """, unsafe_allow_html=True)

    if dff.empty:
        st.warning("Sin datos con los filtros seleccionados.")
        return

    df_meses = kpis_por_mes(dff)
    if df_meses.empty:
        st.warning("No hay suficientes datos mensuales.")
        return

    # Fila 1
    col1, col2 = st.columns(2)
    with col1:
        fig = chart_evolucion_mensual(df_meses, "ocupacion", "Tasa de Ocupación",
                                      umbral_ok=65, umbral_alerta=50)
        st.plotly_chart(fig)
    with col2:
        fig = chart_noshow_vs_umbral(df_meses)
        st.plotly_chart(fig)

    # Fila 2
    col3, col4 = st.columns(2)
    with col3:
        fig = chart_evolucion_mensual(df_meses, "bloqueo", "Tasa de Bloqueo",
                                      umbral_ok=10, umbral_alerta=15)
        st.plotly_chart(fig)
    with col4:
        fig = chart_evolucion_mensual(df_meses, "efectividad", "Efectividad de Cita",
                                      umbral_ok=88, umbral_alerta=80)
        st.plotly_chart(fig)

    # Fila 3
    col5, col6 = st.columns(2)
    with col5:
        fig = chart_evolucion_mensual(df_meses, "agendamiento_remoto", "Agendamiento Remoto",
                                      umbral_ok=20, umbral_alerta=5)
        st.plotly_chart(fig)
    with col6:
        fig = chart_evolucion_mensual(df_meses, "cobertura_sectorial", "Cobertura Sectorial",
                                      umbral_ok=80, umbral_alerta=60)
        st.plotly_chart(fig)

    # Fila 4: Volumen mensual
    st.markdown("#### Volumen de Registros por Mes")
    col7, col8 = st.columns(2)
    with col7:
        import plotly.graph_objects as go
        fig_vol = go.Figure(go.Bar(
            x=df_meses["mes_nombre"],
            y=df_meses["total_registros"],
            marker_color="#2E86C1",
            text=[f"{v:,}" for v in df_meses["total_registros"]],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Registros: %{y:,}<extra></extra>",
        ))
        fig_vol.update_layout(
            title="Total Registros por Mes",
            template="plotly_white",
            height=380,
            xaxis_title="Mes",
            yaxis_title="Registros",
            margin=dict(l=40, r=20, t=50, b=40),
        )
        st.plotly_chart(fig_vol)
    with col8:
        fig_rend = chart_evolucion_mensual(
            df_meses, "rendimiento", "Rendimiento Promedio", unidad=" min"
        )
        st.plotly_chart(fig_rend)


# ─────────────────────────────────────────────────────────────
# PÁGINA 4: ANÁLISIS DETALLADO
# ─────────────────────────────────────────────────────────────
def page_analisis(dff: pd.DataFrame):
    st.markdown("""
    <div class="main-header">
        <h1>🔍 Análisis Detallado</h1>
        <p>Desagregación por instrumento · Tipo de atención · Sector · Grupo etario</p>
    </div>
    """, unsafe_allow_html=True)

    if dff.empty:
        st.warning("Sin datos con los filtros seleccionados.")
        return

    sub_tab1, sub_tab2, sub_tab3, sub_tab4, sub_tab5 = st.tabs([
        "Por Instrumento", "Por Tipo Atención", "Mapa de Calor", "Grupo Etario", "Horario Extendido"
    ])

    with sub_tab1:
        import plotly.graph_objects as go
        import plotly.express as px

        if "INSTRUMENTO" not in dff.columns or dff.empty:
            st.warning("No hay datos de Instrumento con los filtros seleccionados.")
        else:
            # ── Filtro propio del tab ──────────────────────────────────
            todos_inst = sorted(dff["INSTRUMENTO"].dropna().unique().tolist())
            inst_sel = st.multiselect(
                "Filtrar Instrumentos / Profesionales",
                options=todos_inst,
                default=todos_inst,
                key="filt_inst_det",
                help="Selecciona uno o más instrumentos para analizar.",
            )
            if not inst_sel:
                inst_sel = todos_inst
            dff_inst = dff[dff["INSTRUMENTO"].isin(inst_sel)]

            # ── Sección 1: Rendimiento y Ocupación ────────────────────
            st.markdown("##### Rendimiento y Ocupación por Instrumento")
            col1, col2 = st.columns(2)
            with col1:
                fig_rend = chart_rendimiento_instrumento(dff_inst)
                st.plotly_chart(fig_rend, width="stretch")
            with col2:
                df_inst_kpi = kpis_por_instrumento(dff_inst)
                if not df_inst_kpi.empty:
                    colors = [
                        "#27AE60" if v >= 65 else "#F39C12" if v >= 50 else "#E74C3C"
                        for v in df_inst_kpi["ocupacion"]
                    ]
                    fig_ocu = go.Figure(go.Bar(
                        x=df_inst_kpi["ocupacion"],
                        y=df_inst_kpi["instrumento"].str[:28],
                        orientation="h",
                        marker_color=colors,
                        text=[f"{v:.1f}%" for v in df_inst_kpi["ocupacion"]],
                        textposition="outside",
                        hovertemplate="<b>%{y}</b><br>Ocupación: %{x:.1f}%<extra></extra>",
                    ))
                    fig_ocu.update_layout(
                        title="Ocupación por Instrumento (%)",
                        template="plotly_white",
                        height=max(350, len(df_inst_kpi) * 32 + 80),
                        margin=dict(l=10, r=60, t=50, b=40),
                        xaxis=dict(range=[0, 110], title="Ocupación (%)"),
                    )
                    st.plotly_chart(fig_ocu, width="stretch")

            # ── Sección 2: Tabla completa de KPIs con semáforos ───────
            st.markdown("##### KPIs por Instrumento")
            df_inst_kpi2 = kpis_por_instrumento(dff_inst)
            if not df_inst_kpi2.empty:
                from src.kpis import semaforo as _sem

                def _icon(val, kpi):
                    return {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴"}.get(_sem(val, kpi), "⚪")

                df_disp = df_inst_kpi2.copy()
                df_disp["Ocupación"]      = df_disp.apply(lambda r: f"{_icon(r['ocupacion'],'ocupacion')} {r['ocupacion']:.1f}%", axis=1)
                df_disp["No-Show"]        = df_disp.apply(lambda r: f"{_icon(r['no_show'],'no_show')} {r['no_show']:.1f}%", axis=1)
                df_disp["Efectividad"]    = df_disp.apply(lambda r: f"{_icon(r['efectividad'],'efectividad')} {r['efectividad']:.1f}%", axis=1)
                df_disp["Rendim. (min)"]  = df_disp["rendimiento"].round(1)
                df_disp["Total"]          = df_disp["total"].apply(lambda v: f"{v:,}")
                df_disp["Citados"]        = df_disp["citados"].apply(lambda v: f"{v:,}")
                df_disp["Disponibles"]    = df_disp["disponibles"].apply(lambda v: f"{v:,}")
                df_disp["Bloqueados"]     = df_disp["bloqueados"].apply(lambda v: f"{v:,}")
                df_disp["Completados"]    = df_disp["completados"].apply(lambda v: f"{v:,}")
                st.dataframe(
                    df_disp[["instrumento","Total","Citados","Disponibles","Bloqueados","Completados","Ocupación","No-Show","Efectividad","Rendim. (min)"]].rename(columns={"instrumento":"Instrumento"}),
                    width="stretch", hide_index=True
                )

            # ── Sección 3: Series temporales ──────────────────────────
            st.markdown("##### Evolución Temporal por Instrumento")
            if "MES_NUM" not in dff_inst.columns or dff_inst["MES_NUM"].nunique() < 2:
                st.info("Se necesitan al menos 2 meses de datos para mostrar series temporales.")
            else:
                # Limitar a 8 instrumentos para legibilidad
                top8_inst = (
                    dff_inst["INSTRUMENTO"].value_counts().head(8).index.tolist()
                )
                inst_serie = [i for i in inst_sel if i in top8_inst][:8] or inst_sel[:8]
                df_serie = kpis_instrumento_mes(dff_inst, tuple(inst_serie))

                if not df_serie.empty:
                    ts_c1, ts_c2 = st.columns(2)

                    with ts_c1:
                        fig_ocu_ts = px.line(
                            df_serie, x="mes_label", y="ocupacion", color="instrumento",
                            markers=True,
                            labels={"mes_label":"Mes","ocupacion":"Ocupación (%)","instrumento":"Instrumento"},
                            title="Tasa de Ocupación por Mes",
                            template="plotly_white", height=380,
                        )
                        fig_ocu_ts.add_hline(y=65, line_dash="dash", line_color="#27AE60",
                                             annotation_text="Meta 65%", annotation_position="bottom right")
                        fig_ocu_ts.update_layout(margin=dict(l=20,r=20,t=50,b=40), legend=dict(font_size=10))
                        st.plotly_chart(fig_ocu_ts, width="stretch")

                    with ts_c2:
                        fig_ns_ts = px.line(
                            df_serie, x="mes_label", y="no_show", color="instrumento",
                            markers=True,
                            labels={"mes_label":"Mes","no_show":"No-Show (%)","instrumento":"Instrumento"},
                            title="Tasa de No-Show por Mes",
                            template="plotly_white", height=380,
                        )
                        fig_ns_ts.add_hline(y=10, line_dash="dash", line_color="#E74C3C",
                                            annotation_text="Umbral 10%", annotation_position="top right")
                        fig_ns_ts.update_layout(margin=dict(l=20,r=20,t=50,b=40), legend=dict(font_size=10))
                        st.plotly_chart(fig_ns_ts, width="stretch")

                    ts_c3, ts_c4 = st.columns(2)

                    with ts_c3:
                        fig_ef_ts = px.line(
                            df_serie, x="mes_label", y="efectividad", color="instrumento",
                            markers=True,
                            labels={"mes_label":"Mes","efectividad":"Efectividad (%)","instrumento":"Instrumento"},
                            title="Efectividad de Cita por Mes",
                            template="plotly_white", height=380,
                        )
                        fig_ef_ts.add_hline(y=88, line_dash="dash", line_color="#27AE60",
                                            annotation_text="Meta 88%", annotation_position="bottom right")
                        fig_ef_ts.update_layout(margin=dict(l=20,r=20,t=50,b=40), legend=dict(font_size=10))
                        st.plotly_chart(fig_ef_ts, width="stretch")

                    with ts_c4:
                        fig_rend_ts = px.line(
                            df_serie, x="mes_label", y="rendimiento", color="instrumento",
                            markers=True,
                            labels={"mes_label":"Mes","rendimiento":"Rendimiento (min)","instrumento":"Instrumento"},
                            title="Rendimiento Promedio por Mes (min)",
                            template="plotly_white", height=380,
                        )
                        fig_rend_ts.update_layout(margin=dict(l=20,r=20,t=50,b=40), legend=dict(font_size=10))
                        st.plotly_chart(fig_rend_ts, width="stretch")

                    fig_cit_ts = px.bar(
                        df_serie, x="mes_label", y="citados", color="instrumento",
                        barmode="group",
                        labels={"mes_label":"Mes","citados":"Citados","instrumento":"Instrumento"},
                        title="Citados por Mes e Instrumento",
                        template="plotly_white", height=360,
                    )
                    fig_cit_ts.update_layout(margin=dict(l=20,r=20,t=50,b=40), legend=dict(font_size=10))
                    st.plotly_chart(fig_cit_ts, width="stretch")

                    if len(inst_sel) > 8:
                        st.caption(f"ℹ️ Series temporales muestran los 8 instrumentos con mayor volumen. La tabla incluye todos los {len(inst_sel)} seleccionados.")

    with sub_tab2:
        import plotly.graph_objects as go
        import plotly.express as px

        if "TIPO ATENCION" not in dff.columns or dff.empty:
            st.warning("No hay datos de Tipo de Atención con los filtros seleccionados.")
        else:
            # ── Filtro propio del tab ──────────────────────────────────
            todos_tipos = sorted(dff["TIPO ATENCION"].dropna().unique().tolist())
            top_default = (
                dff["TIPO ATENCION"].value_counts().head(10).index.tolist()
            )
            tipos_sel = st.multiselect(
                "Filtrar Tipos de Atención",
                options=todos_tipos,
                default=top_default,
                key="filt_ta_det",
                help="Selecciona los tipos de atención a analizar. Por defecto se muestran los 10 con más registros.",
            )
            if not tipos_sel:
                tipos_sel = todos_tipos
            dff_ta = dff[dff["TIPO ATENCION"].isin(tipos_sel)]

            # ── Sección 1: Volumen y No-Show ───────────────────────────
            st.markdown("##### Volumen y No-Show por Tipo de Atención")
            col1, col2 = st.columns(2)
            with col1:
                fig_ta = chart_tipo_atencion(dff_ta, top_n=len(tipos_sel))
                st.plotly_chart(fig_ta, width="stretch")
            with col2:
                from src.kpis import calc_no_show
                ta_noshow = (
                    dff_ta.groupby("TIPO ATENCION", observed=True)
                    .apply(calc_no_show)
                    .reset_index(name="no_show")
                    .sort_values("no_show", ascending=False)
                )
                colors_ns = [
                    "#E74C3C" if v > 15 else "#F39C12" if v > 10 else "#27AE60"
                    for v in ta_noshow["no_show"]
                ]
                fig_ns = go.Figure(go.Bar(
                    x=ta_noshow["no_show"],
                    y=ta_noshow["TIPO ATENCION"].str[:35],
                    orientation="h",
                    marker_color=colors_ns,
                    text=[f"{v:.1f}%" for v in ta_noshow["no_show"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>No-Show: %{x:.1f}%<extra></extra>",
                ))
                fig_ns.update_layout(
                    title="No-Show por Tipo de Atención",
                    template="plotly_white",
                    height=max(380, len(ta_noshow) * 28 + 80),
                    margin=dict(l=10, r=60, t=50, b=40),
                    xaxis=dict(range=[0, max(ta_noshow["no_show"].max() * 1.2, 20)], title="No-Show (%)"),
                )
                st.plotly_chart(fig_ns, width="stretch")

            # ── Sección 2: KPIs completos por tipo ────────────────────
            st.markdown("##### KPIs por Tipo de Atención")
            df_kpis_ta = kpis_por_tipo_atencion(dff_ta)
            if not df_kpis_ta.empty:
                # Semáforos visuales en la tabla
                def _sem_icon(val, kpi):
                    from src.kpis import semaforo
                    s = semaforo(val, kpi)
                    return {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴"}.get(s, "⚪")

                df_kpis_disp = df_kpis_ta.copy()
                df_kpis_disp["Ocupación"] = df_kpis_disp.apply(
                    lambda r: f"{_sem_icon(r['ocupacion'], 'ocupacion')} {r['ocupacion']:.1f}%", axis=1)
                df_kpis_disp["No-Show"] = df_kpis_disp.apply(
                    lambda r: f"{_sem_icon(r['no_show'], 'no_show')} {r['no_show']:.1f}%", axis=1)
                df_kpis_disp["Bloqueo"] = df_kpis_disp.apply(
                    lambda r: f"{_sem_icon(r['bloqueo'], 'bloqueo')} {r['bloqueo']:.1f}%", axis=1)
                df_kpis_disp["Efectividad"] = df_kpis_disp.apply(
                    lambda r: f"{_sem_icon(r['efectividad'], 'efectividad')} {r['efectividad']:.1f}%", axis=1)
                df_kpis_disp["Sobrecupo"] = df_kpis_disp.apply(
                    lambda r: f"{_sem_icon(r['sobrecupo'], 'sobrecupo')} {r['sobrecupo']:.1f}%", axis=1)
                df_kpis_disp["Ag. Remoto"] = df_kpis_disp.apply(
                    lambda r: f"{_sem_icon(r['agendamiento_remoto'], 'agendamiento_remoto')} {r['agendamiento_remoto']:.1f}%", axis=1)
                df_kpis_disp["Rendim. (min)"] = df_kpis_disp["rendimiento"].round(1)
                df_kpis_disp["Total"] = df_kpis_disp["total"].apply(lambda v: f"{v:,}")
                df_kpis_disp["Citados"] = df_kpis_disp["citados"].apply(lambda v: f"{v:,}")
                df_kpis_disp["Disponibles"] = df_kpis_disp["disponibles"].apply(lambda v: f"{v:,}")
                df_kpis_disp["Bloqueados"]  = df_kpis_disp["bloqueados"].apply(lambda v: f"{v:,}")
                df_kpis_disp["Completados"] = df_kpis_disp["completados"].apply(lambda v: f"{v:,}") if "completados" in df_kpis_disp.columns else "—"

                cols_show = [
                    "tipo_atencion", "Total", "Citados", "Disponibles", "Bloqueados", "Completados",
                    "Ocupación", "No-Show", "Efectividad", "Bloqueo",
                    "Sobrecupo", "Ag. Remoto", "Rendim. (min)"
                ]
                df_kpis_disp = df_kpis_disp[cols_show].rename(columns={"tipo_atencion": "Tipo de Atención"})
                st.dataframe(df_kpis_disp, width="stretch", hide_index=True)

            # ── Sección 3: Series temporales ──────────────────────────
            st.markdown("##### Evolución Temporal por Tipo de Atención")
            if "MES_NUM" not in dff_ta.columns or dff_ta["MES_NUM"].nunique() < 2:
                st.info("Se necesitan al menos 2 meses de datos para mostrar series temporales.")
            else:
                # Limitar a máximo 8 tipos para legibilidad del gráfico
                top8 = (
                    dff_ta["TIPO ATENCION"].value_counts().head(8).index.tolist()
                )
                tipos_serie = [t for t in tipos_sel if t in top8][:8] or tipos_sel[:8]
                df_serie = kpis_tipo_atencion_mes(dff_ta, tuple(tipos_serie))

                if not df_serie.empty:
                    MESES_ES = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
                                7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}
                    df_serie["mes_label"] = df_serie["mes"].map(
                        lambda m: MESES_ES.get(int(m), str(m))
                    )

                    met_col1, met_col2 = st.columns(2)

                    with met_col1:
                        fig_ocu_ts = px.line(
                            df_serie,
                            x="mes_label", y="ocupacion",
                            color="tipo_atencion",
                            markers=True,
                            labels={"mes_label": "Mes", "ocupacion": "Ocupación (%)", "tipo_atencion": "Tipo"},
                            title="Tasa de Ocupación por Mes",
                            template="plotly_white",
                            height=380,
                        )
                        fig_ocu_ts.add_hline(y=65, line_dash="dash", line_color="#27AE60",
                                             annotation_text="Meta 65%", annotation_position="bottom right")
                        fig_ocu_ts.update_layout(margin=dict(l=20, r=20, t=50, b=40),
                                                  legend=dict(font_size=10))
                        st.plotly_chart(fig_ocu_ts, width="stretch")

                    with met_col2:
                        fig_ns_ts = px.line(
                            df_serie,
                            x="mes_label", y="no_show",
                            color="tipo_atencion",
                            markers=True,
                            labels={"mes_label": "Mes", "no_show": "No-Show (%)", "tipo_atencion": "Tipo"},
                            title="Tasa de No-Show por Mes",
                            template="plotly_white",
                            height=380,
                        )
                        fig_ns_ts.add_hline(y=10, line_dash="dash", line_color="#E74C3C",
                                            annotation_text="Umbral 10%", annotation_position="top right")
                        fig_ns_ts.update_layout(margin=dict(l=20, r=20, t=50, b=40),
                                                 legend=dict(font_size=10))
                        st.plotly_chart(fig_ns_ts, width="stretch")

                    met_col3, met_col4 = st.columns(2)

                    with met_col3:
                        fig_ef_ts = px.line(
                            df_serie,
                            x="mes_label", y="efectividad",
                            color="tipo_atencion",
                            markers=True,
                            labels={"mes_label": "Mes", "efectividad": "Efectividad (%)", "tipo_atencion": "Tipo"},
                            title="Efectividad de Cita por Mes",
                            template="plotly_white",
                            height=380,
                        )
                        fig_ef_ts.add_hline(y=88, line_dash="dash", line_color="#27AE60",
                                            annotation_text="Meta 88%", annotation_position="bottom right")
                        fig_ef_ts.update_layout(margin=dict(l=20, r=20, t=50, b=40),
                                                 legend=dict(font_size=10))
                        st.plotly_chart(fig_ef_ts, width="stretch")

                    with met_col4:
                        fig_vol_ts = px.bar(
                            df_serie,
                            x="mes_label", y="citados",
                            color="tipo_atencion",
                            barmode="group",
                            labels={"mes_label": "Mes", "citados": "Citados", "tipo_atencion": "Tipo"},
                            title="Citados por Mes",
                            template="plotly_white",
                            height=380,
                        )
                        fig_vol_ts.update_layout(margin=dict(l=20, r=20, t=50, b=40),
                                                  legend=dict(font_size=10))
                        st.plotly_chart(fig_vol_ts, width="stretch")

                    if len(tipos_sel) > 8:
                        st.caption(f"ℹ️ Series temporales muestran los 8 tipos con mayor volumen. La tabla de KPIs incluye todos los {len(tipos_sel)} tipos seleccionados.")

    with sub_tab3:
        from src.kpis import calc_ocupacion, calc_no_show, calc_efectividad, calc_bloqueo

        MESES_ES = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
                    7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}

        # Configuración de cada métrica disponible
        METRICAS_HEATMAP = {
            "Ocupación (%)": {
                "fn": calc_ocupacion, "col": "INSTRUMENTO",
                "colorscale": [[0.0,"#E74C3C"],[0.5,"#F39C12"],[0.65,"#27AE60"],[1.0,"#0B5345"]],
                "zmin": 0, "zmax": 100, "suffix": "%",
                "mejor": "mayor",
            },
            "No-Show (%)": {
                "fn": calc_no_show, "col": "INSTRUMENTO",
                "colorscale": [[0.0,"#27AE60"],[0.1,"#F39C12"],[0.15,"#E74C3C"],[1.0,"#7B241C"]],
                "zmin": 0, "zmax": 40, "suffix": "%",
                "mejor": "menor",
            },
            "Efectividad (%)": {
                "fn": calc_efectividad, "col": "INSTRUMENTO",
                "colorscale": [[0.0,"#E74C3C"],[0.8,"#F39C12"],[0.88,"#27AE60"],[1.0,"#0B5345"]],
                "zmin": 0, "zmax": 100, "suffix": "%",
                "mejor": "mayor",
            },
            "Bloqueo (%)": {
                "fn": calc_bloqueo, "col": "INSTRUMENTO",
                "colorscale": [[0.0,"#27AE60"],[0.1,"#F39C12"],[0.15,"#E74C3C"],[1.0,"#7B241C"]],
                "zmin": 0, "zmax": 40, "suffix": "%",
                "mejor": "menor",
            },
        }

        # ── Selector de métrica ───────────────────────────────────────
        metrica_sel = st.radio(
            "Métrica a visualizar",
            list(METRICAS_HEATMAP.keys()),
            horizontal=True,
            key="heatmap_metrica",
        )
        cfg = METRICAS_HEATMAP[metrica_sel]

        def _build_pivot(df_src, group_col, fn, meses_map):
            pivot_data = {}
            for (ent, mes), grp in df_src.groupby([group_col, "MES_NUM"]):
                if ent not in pivot_data:
                    pivot_data[ent] = {}
                pivot_data[ent][meses_map.get(int(mes), str(mes))] = fn(grp)
            if not pivot_data:
                return pd.DataFrame()
            col_order = [meses_map[m] for m in sorted(meses_map) if meses_map[m] in
                         list(pd.DataFrame(pivot_data).T.columns)]
            df_p = pd.DataFrame(pivot_data).T.fillna(0)
            df_p = df_p[[c for c in col_order if c in df_p.columns]]
            df_p["_media"] = df_p.mean(axis=1)
            asc = cfg["mejor"] == "menor"
            df_p = df_p.sort_values("_media", ascending=asc).drop(columns=["_media"])
            return df_p

        # ── Heatmap 1: Instrumento × Mes ─────────────────────────────
        st.markdown(f"##### {metrica_sel} — Instrumento × Mes")
        if "INSTRUMENTO" in dff.columns and "MES_NUM" in dff.columns:
            df_piv_inst = _build_pivot(dff, "INSTRUMENTO", cfg["fn"], MESES_ES)
            if not df_piv_inst.empty:
                fig_h1 = chart_heatmap_pivot(
                    df_piv_inst,
                    title=f"{metrica_sel} por Instrumento y Mes",
                    metric_label=metrica_sel,
                    colorscale=cfg["colorscale"],
                    zmin=cfg["zmin"], zmax=cfg["zmax"], suffix=cfg["suffix"],
                )
                st.plotly_chart(fig_h1, width="stretch")
        else:
            st.info("Sin datos suficientes para el mapa de calor por instrumento.")

        # ── Heatmap 2: Tipo Atención × Mes ───────────────────────────
        if "TIPO ATENCION" in dff.columns and "MES_NUM" in dff.columns:
            st.markdown(f"##### {metrica_sel} — Tipo de Atención × Mes")
            # Filtrar top 15 tipos por volumen para mantener legibilidad
            top15_ta = dff["TIPO ATENCION"].value_counts().head(15).index.tolist()
            dff_ta15 = dff[dff["TIPO ATENCION"].isin(top15_ta)]
            df_piv_ta = _build_pivot(dff_ta15, "TIPO ATENCION", cfg["fn"], MESES_ES)
            if not df_piv_ta.empty:
                fig_h2 = chart_heatmap_pivot(
                    df_piv_ta,
                    title=f"{metrica_sel} por Tipo de Atención y Mes (Top 15)",
                    metric_label=metrica_sel,
                    colorscale=cfg["colorscale"],
                    zmin=cfg["zmin"], zmax=cfg["zmax"], suffix=cfg["suffix"],
                )
                st.plotly_chart(fig_h2, width="stretch")

        # ── Tabla de extremos críticos ────────────────────────────────
        st.markdown(f"##### Combinaciones críticas — {metrica_sel}")
        if "INSTRUMENTO" in dff.columns and "MES_NUM" in dff.columns:
            rows_ext = []
            for (inst, mes), grp in dff.groupby(["INSTRUMENTO", "MES_NUM"]):
                rows_ext.append({
                    "Instrumento": inst,
                    "Mes": MESES_ES.get(int(mes), str(mes)),
                    "Valor": cfg["fn"](grp),
                    "Registros": len(grp),
                })
            df_ext = pd.DataFrame(rows_ext)
            if not df_ext.empty:
                asc_sort = cfg["mejor"] == "menor"
                df_ext = df_ext.sort_values("Valor", ascending=asc_sort).head(10)
                df_ext["Valor"] = df_ext["Valor"].apply(lambda v: f"{v:.1f}{cfg['suffix']}")
                df_ext["Registros"] = df_ext["Registros"].apply(lambda v: f"{v:,}")
                lbl = "peores" if cfg["mejor"] == "mayor" else "más altos"
                st.caption(f"Top 10 combinaciones instrumento/mes con {lbl} resultado")
                st.dataframe(df_ext, width="stretch", hide_index=True)

    with sub_tab4:
        if "GRUPO_ETARIO" in dff.columns:
            import plotly.graph_objects as go
            import plotly.express as px
            from src.kpis import calc_no_show, calc_efectividad

            # ── Filtro de grupos etarios ───────────────────────────────
            # Orden definido por rangos (no alfabético)
            _orden_ge = ["0-5", "6-14", "15-29", "30-64", "65+"]
            grupos_disp = [g for g in _orden_ge if g in dff["GRUPO_ETARIO"].dropna().unique()]
            # Si hay grupos fuera del orden conocido, los agrega al final
            grupos_disp += [g for g in sorted(dff["GRUPO_ETARIO"].dropna().unique()) if g not in grupos_disp]

            grupos_sel = st.multiselect(
                "Filtrar Grupos Etarios",
                options=grupos_disp,
                default=grupos_disp,
                key="filt_ge_det",
                help="Selecciona uno o más grupos etarios para comparar.",
            )
            if not grupos_sel:
                grupos_sel = grupos_disp

            dff_cit = dff[
                (dff["ESTADO CUPO"] == "CITADO") &
                (dff["GRUPO_ETARIO"].isin(grupos_sel))
            ]
            MESES_ES = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
                        7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}

            # ── Sección 1: Distribución · No-Show · Efectividad ───────
            st.markdown("##### Resumen por Grupo Etario")
            col1, col2, col3 = st.columns(3)

            with col1:
                ge_counts = dff_cit["GRUPO_ETARIO"].value_counts().sort_index()
                fig_ge = go.Figure(go.Bar(
                    x=ge_counts.index.astype(str),
                    y=ge_counts.values,
                    marker_color="#2E86C1",
                    text=[f"{v:,}" for v in ge_counts.values],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>%{y:,} citados<extra></extra>",
                ))
                fig_ge.update_layout(
                    title="Citados por Grupo Etario",
                    template="plotly_white",
                    height=360,
                    xaxis_title="Grupo Etario",
                    yaxis_title="Citados",
                    margin=dict(l=30, r=20, t=50, b=40),
                )
                st.plotly_chart(fig_ge, width="stretch")

            with col2:
                ge_noshow = (
                    dff_cit.groupby("GRUPO_ETARIO", observed=True)
                    .apply(calc_no_show)
                    .reset_index(name="no_show")
                )
                colors_ns = [
                    "#E74C3C" if v > 15 else "#F39C12" if v > 10 else "#27AE60"
                    for v in ge_noshow["no_show"]
                ]
                fig_ns = go.Figure(go.Bar(
                    x=ge_noshow["GRUPO_ETARIO"].astype(str),
                    y=ge_noshow["no_show"],
                    marker_color=colors_ns,
                    text=[f"{v:.1f}%" for v in ge_noshow["no_show"]],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>No-Show: %{y:.1f}%<extra></extra>",
                ))
                fig_ns.add_hline(y=10, line_dash="dash", line_color="#E74C3C",
                                  annotation_text="Umbral 10%")
                fig_ns.update_layout(
                    title="No-Show por Grupo Etario",
                    template="plotly_white",
                    height=360,
                    xaxis_title="Grupo Etario",
                    yaxis_title="No-Show (%)",
                    margin=dict(l=30, r=20, t=50, b=40),
                )
                st.plotly_chart(fig_ns, width="stretch")

            with col3:
                ge_efec = (
                    dff_cit.groupby("GRUPO_ETARIO", observed=True)
                    .apply(calc_efectividad)
                    .reset_index(name="efectividad")
                )
                colors_ef = [
                    "#27AE60" if v >= 88 else "#F39C12" if v >= 80 else "#E74C3C"
                    for v in ge_efec["efectividad"]
                ]
                fig_ef = go.Figure(go.Bar(
                    x=ge_efec["GRUPO_ETARIO"].astype(str),
                    y=ge_efec["efectividad"],
                    marker_color=colors_ef,
                    text=[f"{v:.1f}%" for v in ge_efec["efectividad"]],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>Efectividad: %{y:.1f}%<extra></extra>",
                ))
                fig_ef.add_hline(y=88, line_dash="dash", line_color="#27AE60",
                                  annotation_text="Meta 88%")
                fig_ef.update_layout(
                    title="Efectividad por Grupo Etario",
                    template="plotly_white",
                    height=360,
                    xaxis_title="Grupo Etario",
                    yaxis_title="Efectividad (%)",
                    yaxis=dict(range=[0, 110]),
                    margin=dict(l=30, r=20, t=50, b=40),
                )
                st.plotly_chart(fig_ef, width="stretch")

            # ── Sección 2: Series temporales por grupo etario ─────────
            st.markdown("##### Evolución Mensual por Grupo Etario")
            if "MES_NUM" not in dff.columns or dff["MES_NUM"].nunique() < 2:
                st.info("Se necesitan al menos 2 meses de datos para mostrar series temporales.")
            else:
                ge_mes_rows = []
                for (ge, mes), grp in dff_cit.groupby(["GRUPO_ETARIO", "MES_NUM"], observed=True):
                    ge_mes_rows.append({
                        "grupo_etario": ge,
                        "mes": int(mes),
                        "mes_label": MESES_ES.get(int(mes), str(mes)),
                        "no_show": calc_no_show(grp),
                        "efectividad": calc_efectividad(grp),
                        "citados": len(grp),
                    })
                df_ge_mes = pd.DataFrame(ge_mes_rows).sort_values(["grupo_etario", "mes"])

                ts_col1, ts_col2 = st.columns(2)

                with ts_col1:
                    fig_ns_ts = px.line(
                        df_ge_mes,
                        x="mes_label", y="no_show",
                        color="grupo_etario",
                        markers=True,
                        labels={"mes_label": "Mes", "no_show": "No-Show (%)", "grupo_etario": "Grupo"},
                        title="No-Show por Mes y Grupo Etario",
                        template="plotly_white",
                        height=380,
                    )
                    fig_ns_ts.add_hline(y=10, line_dash="dash", line_color="#E74C3C",
                                        annotation_text="Umbral 10%", annotation_position="top right")
                    fig_ns_ts.update_layout(margin=dict(l=20, r=20, t=50, b=40),
                                             legend=dict(font_size=11))
                    st.plotly_chart(fig_ns_ts, width="stretch")

                with ts_col2:
                    fig_ef_ts = px.line(
                        df_ge_mes,
                        x="mes_label", y="efectividad",
                        color="grupo_etario",
                        markers=True,
                        labels={"mes_label": "Mes", "efectividad": "Efectividad (%)", "grupo_etario": "Grupo"},
                        title="Efectividad por Mes y Grupo Etario",
                        template="plotly_white",
                        height=380,
                    )
                    fig_ef_ts.add_hline(y=88, line_dash="dash", line_color="#27AE60",
                                        annotation_text="Meta 88%", annotation_position="bottom right")
                    fig_ef_ts.update_layout(margin=dict(l=20, r=20, t=50, b=40),
                                             legend=dict(font_size=11))
                    st.plotly_chart(fig_ef_ts, width="stretch")

                # Volumen de citados por mes (stacked bars para ver predominancia)
                fig_vol_ts = px.bar(
                    df_ge_mes,
                    x="mes_label", y="citados",
                    color="grupo_etario",
                    barmode="stack",
                    labels={"mes_label": "Mes", "citados": "Citados", "grupo_etario": "Grupo Etario"},
                    title="Volumen de Citados por Mes y Grupo Etario",
                    template="plotly_white",
                    height=360,
                )
                fig_vol_ts.update_layout(margin=dict(l=20, r=20, t=50, b=40),
                                          legend=dict(font_size=11))
                st.plotly_chart(fig_vol_ts, width="stretch")
        else:
            st.info("Columna de grupo etario no disponible en los datos cargados.")

    with sub_tab5:
        import plotly.graph_objects as go
        import plotly.express as px
        from src.kpis import calc_ocupacion, calc_no_show, calc_efectividad

        MESES_ES_HE = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
                       7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}

        if "HORA_NUM" not in dff.columns:
            st.info("Los datos cargados no contienen información de hora de inicio. Recarga los archivos IRIS para habilitar este análisis.")
        else:
            dff_ext = dff[dff["HORA_NUM"] >= 18].copy()
            dff_norm = dff[dff["HORA_NUM"] < 18].copy()
            total = len(dff)
            n_ext = len(dff_ext)

            # ── KPIs globales comparativos ─────────────────────────────
            st.markdown("##### Comparativo: Horario Normal vs. Extendido (≥ 18:00 hrs)")
            m1, m2, m3, m4 = st.columns(4)
            ocu_ext  = calc_ocupacion(dff_ext)  if not dff_ext.empty  else 0.0
            ocu_norm = calc_ocupacion(dff_norm) if not dff_norm.empty else 0.0
            ns_ext   = calc_no_show(dff_ext)    if not dff_ext.empty  else 0.0
            ns_norm  = calc_no_show(dff_norm)   if not dff_norm.empty else 0.0
            ef_ext   = calc_efectividad(dff_ext)  if not dff_ext.empty  else 0.0
            ef_norm  = calc_efectividad(dff_norm) if not dff_norm.empty else 0.0

            with m1:
                pct_ext = (n_ext / total * 100) if total > 0 else 0
                st.metric("Cupos en Horario Extendido", f"{n_ext:,}", f"{pct_ext:.1f}% del total")
            with m2:
                st.metric("Ocupación Extendido", f"{ocu_ext:.1f}%",
                          f"{ocu_ext - ocu_norm:+.1f}pp vs Normal",
                          delta_color="normal")
            with m3:
                st.metric("No-Show Extendido", f"{ns_ext:.1f}%",
                          f"{ns_ext - ns_norm:+.1f}pp vs Normal",
                          delta_color="inverse")
            with m4:
                st.metric("Efectividad Extendido", f"{ef_ext:.1f}%",
                          f"{ef_ext - ef_norm:+.1f}pp vs Normal",
                          delta_color="normal")

            # ── Gráfico comparativo barras ─────────────────────────────
            df_comp = pd.DataFrame({
                "Horario": ["Normal (< 18:00)", "Extendido (≥ 18:00)"],
                "Ocupación (%)":   [ocu_norm, ocu_ext],
                "No-Show (%)":     [ns_norm,  ns_ext],
                "Efectividad (%)": [ef_norm,  ef_ext],
            })
            c1, c2, c3 = st.columns(3)
            for col_chart, kpi_col, umbral, mejor in [
                (c1, "Ocupación (%)",   65,  True),
                (c2, "No-Show (%)",     10,  False),
                (c3, "Efectividad (%)", 88,  True),
            ]:
                with col_chart:
                    colores = ["#2E86C1", "#1ABC9C"]
                    fig_c = go.Figure(go.Bar(
                        x=df_comp["Horario"],
                        y=df_comp[kpi_col],
                        marker_color=colores,
                        text=[f"{v:.1f}%" for v in df_comp[kpi_col]],
                        textposition="outside",
                    ))
                    fig_c.add_hline(y=umbral, line_dash="dash",
                                    line_color="#27AE60" if mejor else "#E74C3C",
                                    annotation_text=f"{'Meta' if mejor else 'Umbral'} {umbral}%")
                    fig_c.update_layout(
                        title=kpi_col, template="plotly_white", height=320,
                        yaxis=dict(range=[0, 110]), showlegend=False,
                        margin=dict(l=20, r=20, t=50, b=40),
                    )
                    st.plotly_chart(fig_c, width="stretch")

            # ── Ocupación extendida por Instrumento ───────────────────
            if not dff_ext.empty and "INSTRUMENTO" in dff_ext.columns:
                st.markdown("##### Ocupación en Horario Extendido por Instrumento")
                inst_ext = (
                    dff_ext.groupby("INSTRUMENTO", observed=True)
                    .apply(calc_ocupacion)
                    .reset_index(name="ocupacion")
                    .sort_values("ocupacion", ascending=False)
                )
                if not inst_ext.empty:
                    colors_ie = [
                        "#27AE60" if v >= 65 else "#F39C12" if v >= 50 else "#E74C3C"
                        for v in inst_ext["ocupacion"]
                    ]
                    fig_ie = go.Figure(go.Bar(
                        x=inst_ext["ocupacion"],
                        y=inst_ext["INSTRUMENTO"].str[:30],
                        orientation="h",
                        marker_color=colors_ie,
                        text=[f"{v:.1f}%" for v in inst_ext["ocupacion"]],
                        textposition="outside",
                        hovertemplate="<b>%{y}</b><br>Ocupación extendida: %{x:.1f}%<extra></extra>",
                    ))
                    fig_ie.add_vline(x=50, line_dash="dash", line_color="#F39C12",
                                     annotation_text="Meta 50%")
                    fig_ie.update_layout(
                        title="Ocupación Horario Extendido por Instrumento (%)",
                        template="plotly_white",
                        height=max(350, len(inst_ext) * 32 + 80),
                        margin=dict(l=10, r=60, t=50, b=40),
                        xaxis=dict(range=[0, 110], title="Ocupación (%)"),
                    )
                    st.plotly_chart(fig_ie, width="stretch")

            # ── Serie temporal: ocupación extendida por mes ───────────
            if "MES_NUM" in dff.columns and dff["MES_NUM"].nunique() >= 2:
                st.markdown("##### Evolución Mensual — Horario Extendido vs Normal")

                rows_ts = []
                for mes, grp in dff.groupby("MES_NUM", observed=True):
                    g_ext  = grp[grp["HORA_NUM"] >= 18]
                    g_norm = grp[grp["HORA_NUM"] <  18]
                    rows_ts.append({
                        "mes": int(mes),
                        "mes_label": MESES_ES_HE.get(int(mes), str(mes)),
                        "Extendido": calc_ocupacion(g_ext)  if not g_ext.empty  else 0.0,
                        "Normal":    calc_ocupacion(g_norm) if not g_norm.empty else 0.0,
                    })
                df_ts_he = pd.DataFrame(rows_ts).sort_values("mes")

                fig_ts_he = px.line(
                    df_ts_he.melt(id_vars=["mes", "mes_label"],
                                  value_vars=["Extendido", "Normal"],
                                  var_name="Horario", value_name="Ocupación (%)"),
                    x="mes_label", y="Ocupación (%)", color="Horario",
                    markers=True,
                    color_discrete_map={"Extendido": "#1ABC9C", "Normal": "#2E86C1"},
                    labels={"mes_label": "Mes"},
                    title="Ocupación por Mes: Extendido vs Normal",
                    template="plotly_white", height=380,
                )
                fig_ts_he.add_hline(y=50, line_dash="dash", line_color="#F39C12",
                                    annotation_text="Meta Extendido 50%", annotation_position="bottom right")
                fig_ts_he.add_hline(y=65, line_dash="dot", line_color="#27AE60",
                                    annotation_text="Meta Normal 65%", annotation_position="bottom right")
                fig_ts_he.update_layout(margin=dict(l=20, r=20, t=50, b=40))
                st.plotly_chart(fig_ts_he, width="stretch")

            # ── Distribución de cupos por hora ────────────────────────
            st.markdown("##### Distribución de Cupos por Hora del Día")
            _dff_hora = dff[dff["HORA_NUM"].notna()].copy()
            _dff_hora["hora_int"] = _dff_hora["HORA_NUM"].astype(int)
            hora_counts = (
                _dff_hora.groupby("hora_int", observed=True)
                .size()
                .reset_index(name="cupos")
                .rename(columns={"hora_int": "hora"})
            )
            hora_counts["tipo"] = hora_counts["hora"].apply(
                lambda h: "Extendido (≥ 18:00)" if h >= 18 else "Normal"
            )
            fig_horas = px.bar(
                hora_counts, x="hora", y="cupos", color="tipo",
                color_discrete_map={"Extendido (≥ 18:00)": "#1ABC9C", "Normal": "#2E86C1"},
                labels={"hora": "Hora de Inicio", "cupos": "Cantidad de Cupos", "tipo": "Horario"},
                title="Cupos por Hora del Día",
                template="plotly_white", height=360,
            )
            fig_horas.add_vline(x=17.5, line_dash="dash", line_color="#E74C3C",
                                annotation_text="18:00 hrs", annotation_position="top right")
            fig_horas.update_layout(margin=dict(l=20, r=20, t=50, b=40))
            st.plotly_chart(fig_horas, width="stretch")

            # ══════════════════════════════════════════════════════════════
            # ANÁLISIS PROFUNDO: SEGMENTACIÓN NORMAL / EXTENDIDO / SÁBADO
            # ══════════════════════════════════════════════════════════════
            st.markdown("---")
            st.markdown("#### Análisis Segmentado: Normal · Extendido · Apertura Sabatina")
            st.caption(
                "Comparación diferenciada de los tres componentes horarios: "
                "**Normal** (Lun-Vie < 18 h), **Extendido** (Lun-Vie ≥ 18 h) y "
                "**Apertura Sabatina** (atenciones los días sábado)."
            )

            from src.kpis import (
                kpis_horario_segmentado, kpis_por_profesional,
                kpis_profesional_sabatino, kpis_profesional_extendido,
                kpis_sabatino_por_mes, kpis_extendido_por_mes,
                kpis_sabatino_por_instrumento, kpis_extendido_por_instrumento,
                calc_bloqueo,
            )

            # ── Tabla comparativa de 3 segmentos ─────────────────────
            df_seg = kpis_horario_segmentado(dff)
            if not df_seg.empty:
                st.markdown("##### Tabla Comparativa por Segmento Horario")
                _seg_display = df_seg.rename(columns={
                    "segmento": "Segmento", "total": "Total Cupos",
                    "citados": "Citados", "disponibles": "Disponibles",
                    "bloqueados": "Bloqueados", "completados": "Completados",
                    "ocupacion": "Ocupación %", "no_show": "No-Show %",
                    "bloqueo": "Bloqueo %", "efectividad": "Efectividad %",
                    "rendimiento": "Rendimiento", "sobrecupo": "Sobrecupo %",
                })
                st.dataframe(_seg_display, use_container_width=True, hide_index=True)

                # Gráfico radar-like de barras agrupadas por segmento
                _kpi_cols = ["ocupacion", "no_show", "bloqueo", "efectividad"]
                _kpi_labels = ["Ocupación %", "No-Show %", "Bloqueo %", "Efectividad %"]
                rows_radar = []
                for _, r in df_seg.iterrows():
                    for kc, kl in zip(_kpi_cols, _kpi_labels):
                        rows_radar.append({"Segmento": r["segmento"], "KPI": kl, "Valor": r[kc]})
                df_radar = pd.DataFrame(rows_radar)
                fig_seg = px.bar(
                    df_radar, x="KPI", y="Valor", color="Segmento", barmode="group",
                    color_discrete_map={
                        "Normal (Lun-Vie <18h)": "#2E86C1",
                        "Extendido (Lun-Vie ≥18h)": "#1ABC9C",
                        "Apertura Sabatina": "#E67E22",
                    },
                    text_auto=".1f",
                    title="KPIs por Segmento Horario",
                    template="plotly_white", height=400,
                )
                fig_seg.update_layout(margin=dict(l=20, r=20, t=50, b=40))
                st.plotly_chart(fig_seg, width="stretch")

            # ══════════════════════════════════════════════════════════════
            # APERTURA SABATINA — análisis dedicado
            # ══════════════════════════════════════════════════════════════
            has_sabado = "APERTURA_SABATINA" in dff.columns and (dff["APERTURA_SABATINA"] == "Sábado").any()
            if has_sabado:
                st.markdown("---")
                st.markdown("#### 🗓️ Apertura Sabatina — Análisis Detallado")
                dff_sab = dff[dff["APERTURA_SABATINA"] == "Sábado"]
                n_sab = len(dff_sab)
                ocu_sab = calc_ocupacion(dff_sab) if not dff_sab.empty else 0.0
                ns_sab = calc_no_show(dff_sab) if not dff_sab.empty else 0.0
                ef_sab = calc_efectividad(dff_sab) if not dff_sab.empty else 0.0
                bl_sab = calc_bloqueo(dff_sab) if not dff_sab.empty else 0.0

                ms1, ms2, ms3, ms4, ms5 = st.columns(5)
                with ms1:
                    pct_sab = (n_sab / total * 100) if total > 0 else 0
                    st.metric("Cupos Sábado", f"{n_sab:,}", f"{pct_sab:.1f}% del total")
                with ms2:
                    st.metric("Ocupación", f"{ocu_sab:.1f}%",
                              f"{ocu_sab - ocu_norm:+.1f}pp vs Normal", delta_color="normal")
                with ms3:
                    st.metric("No-Show", f"{ns_sab:.1f}%",
                              f"{ns_sab - ns_norm:+.1f}pp vs Normal", delta_color="inverse")
                with ms4:
                    st.metric("Efectividad", f"{ef_sab:.1f}%",
                              f"{ef_sab - ef_norm:+.1f}pp vs Normal", delta_color="normal")
                with ms5:
                    st.metric("Bloqueo", f"{bl_sab:.1f}%",
                              f"{bl_sab - calc_bloqueo(dff_norm):+.1f}pp vs Normal", delta_color="inverse")

                # ── Ranking de profesionales — Sábado ──────────────────
                df_prof_sab = kpis_profesional_sabatino(dff)
                if not df_prof_sab.empty:
                    st.markdown("##### Ranking de Profesionales — Apertura Sabatina")
                    _prof_sab_display = df_prof_sab.rename(columns={
                        "profesional": "Profesional", "total": "Total Cupos",
                        "citados": "Citados", "disponibles": "Disponibles",
                        "bloqueados": "Bloqueados", "completados": "Completados",
                        "ocupacion": "Ocupación %", "no_show": "No-Show %",
                        "bloqueo": "Bloqueo %", "efectividad": "Efectividad %",
                        "rendimiento": "Rendimiento",
                    })
                    st.dataframe(_prof_sab_display, use_container_width=True, hide_index=True)

                    # Gráfico: top 15 profesionales sábado por volumen
                    _top_sab = df_prof_sab.head(15)
                    fig_prof_sab = go.Figure(go.Bar(
                        x=_top_sab["total"],
                        y=_top_sab["profesional"].str[:35],
                        orientation="h",
                        marker_color="#E67E22",
                        text=[f"{t:,} ({o:.0f}%)" for t, o in zip(_top_sab["total"], _top_sab["ocupacion"])],
                        textposition="outside",
                    ))
                    fig_prof_sab.update_layout(
                        title="Top Profesionales por Cupos — Apertura Sabatina",
                        template="plotly_white",
                        height=max(350, len(_top_sab) * 32 + 80),
                        margin=dict(l=10, r=80, t=50, b=40),
                        xaxis_title="Total Cupos",
                        yaxis=dict(autorange="reversed"),
                    )
                    st.plotly_chart(fig_prof_sab, width="stretch")

                # ── Instrumentos — Sábado ──────────────────────────────
                df_instr_sab = kpis_sabatino_por_instrumento(dff)
                if not df_instr_sab.empty:
                    st.markdown("##### Instrumentos en Apertura Sabatina")
                    _instr_sab_display = df_instr_sab.rename(columns={
                        "instrumento": "Instrumento", "total": "Total",
                        "citados": "Citados", "disponibles": "Disp.", "bloqueados": "Bloq.",
                        "completados": "Complet.", "ocupacion": "Ocup.%",
                        "no_show": "NoShow%", "bloqueo": "Bloq.%",
                        "efectividad": "Efect.%", "rendimiento": "Rend.",
                    })
                    st.dataframe(_instr_sab_display, use_container_width=True, hide_index=True)

                # ── Evolución mensual — Sábado ─────────────────────────
                df_sab_mes = kpis_sabatino_por_mes(dff)
                if not df_sab_mes.empty and len(df_sab_mes) >= 2:
                    st.markdown("##### Evolución Mensual — Apertura Sabatina")
                    fig_sab_mes = px.line(
                        df_sab_mes, x="mes_nombre", y="ocupacion", markers=True,
                        title="Ocupación Sabatina por Mes",
                        labels={"mes_nombre": "Mes", "ocupacion": "Ocupación (%)"},
                        template="plotly_white", height=350,
                    )
                    fig_sab_mes.add_hline(y=50, line_dash="dash", line_color="#F39C12",
                                          annotation_text="Meta 50%")
                    fig_sab_mes.update_traces(line_color="#E67E22")
                    fig_sab_mes.update_layout(margin=dict(l=20, r=20, t=50, b=40))
                    st.plotly_chart(fig_sab_mes, width="stretch")

                    # Volumen sábado por mes
                    fig_sab_vol = px.bar(
                        df_sab_mes, x="mes_nombre", y="total",
                        text="total",
                        title="Volumen de Cupos Sabatinos por Mes",
                        labels={"mes_nombre": "Mes", "total": "Total Cupos"},
                        template="plotly_white", height=320,
                        color_discrete_sequence=["#E67E22"],
                    )
                    fig_sab_vol.update_traces(textposition="outside")
                    fig_sab_vol.update_layout(margin=dict(l=20, r=20, t=50, b=40))
                    st.plotly_chart(fig_sab_vol, width="stretch")

            # ══════════════════════════════════════════════════════════════
            # RANKING PROFESIONALES — HORARIO EXTENDIDO (Lun-Vie ≥18h)
            # ══════════════════════════════════════════════════════════════
            df_prof_ext = kpis_profesional_extendido(dff)
            if not df_prof_ext.empty:
                st.markdown("---")
                st.markdown("#### Ranking de Profesionales — Horario Extendido (Lun-Vie ≥ 18 h)")
                _prof_ext_display = df_prof_ext.rename(columns={
                    "profesional": "Profesional", "total": "Total Cupos",
                    "citados": "Citados", "disponibles": "Disponibles",
                    "bloqueados": "Bloqueados", "completados": "Completados",
                    "ocupacion": "Ocupación %", "no_show": "No-Show %",
                    "bloqueo": "Bloqueo %", "efectividad": "Efectividad %",
                    "rendimiento": "Rendimiento",
                })
                st.dataframe(_prof_ext_display, use_container_width=True, hide_index=True)

                _top_ext = df_prof_ext.head(15)
                fig_prof_ext = go.Figure(go.Bar(
                    x=_top_ext["total"],
                    y=_top_ext["profesional"].str[:35],
                    orientation="h",
                    marker_color="#1ABC9C",
                    text=[f"{t:,} ({o:.0f}%)" for t, o in zip(_top_ext["total"], _top_ext["ocupacion"])],
                    textposition="outside",
                ))
                fig_prof_ext.update_layout(
                    title="Top Profesionales por Cupos — Horario Extendido",
                    template="plotly_white",
                    height=max(350, len(_top_ext) * 32 + 80),
                    margin=dict(l=10, r=80, t=50, b=40),
                    xaxis_title="Total Cupos",
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_prof_ext, width="stretch")

            # ── Instrumentos — Extendido Lun-Vie ─────────────────────
            df_instr_ext = kpis_extendido_por_instrumento(dff)
            if not df_instr_ext.empty:
                st.markdown("##### Instrumentos en Horario Extendido (Lun-Vie ≥ 18 h)")
                _instr_ext_display = df_instr_ext.rename(columns={
                    "instrumento": "Instrumento", "total": "Total",
                    "citados": "Citados", "disponibles": "Disp.", "bloqueados": "Bloq.",
                    "completados": "Complet.", "ocupacion": "Ocup.%",
                    "no_show": "NoShow%", "bloqueo": "Bloq.%",
                    "efectividad": "Efect.%", "rendimiento": "Rend.",
                })
                st.dataframe(_instr_ext_display, use_container_width=True, hide_index=True)

            # ── Evolución mensual — Extendido Lun-Vie ─────────────────
            df_ext_mes = kpis_extendido_por_mes(dff)
            if not df_ext_mes.empty and len(df_ext_mes) >= 2:
                st.markdown("##### Evolución Mensual — Horario Extendido (Lun-Vie)")
                fig_ext_mes = px.line(
                    df_ext_mes, x="mes_nombre", y="ocupacion", markers=True,
                    title="Ocupación Extendida por Mes (Lun-Vie ≥ 18 h)",
                    labels={"mes_nombre": "Mes", "ocupacion": "Ocupación (%)"},
                    template="plotly_white", height=350,
                )
                fig_ext_mes.add_hline(y=50, line_dash="dash", line_color="#F39C12",
                                      annotation_text="Meta 50%")
                fig_ext_mes.update_traces(line_color="#1ABC9C")
                fig_ext_mes.update_layout(margin=dict(l=20, r=20, t=50, b=40))
                st.plotly_chart(fig_ext_mes, width="stretch")

            # ══════════════════════════════════════════════════════════════
            # RANKING GLOBAL DE PROFESIONALES (todos los horarios)
            # ══════════════════════════════════════════════════════════════
            df_prof_all = kpis_por_profesional(dff)
            if not df_prof_all.empty:
                st.markdown("---")
                st.markdown("#### Ranking General de Profesionales (todos los horarios)")
                _prof_all_display = df_prof_all.head(30).rename(columns={
                    "profesional": "Profesional", "total": "Total Cupos",
                    "citados": "Citados", "disponibles": "Disponibles",
                    "bloqueados": "Bloqueados", "completados": "Completados",
                    "ocupacion": "Ocupación %", "no_show": "No-Show %",
                    "bloqueo": "Bloqueo %", "efectividad": "Efectividad %",
                    "rendimiento": "Rendimiento",
                })
                st.dataframe(_prof_all_display, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────
# PÁGINA 5: ALERTAS Y BRECHAS
# ─────────────────────────────────────────────────────────────
def page_alertas(dff: pd.DataFrame):
    st.markdown("""
    <div class="main-header">
        <h1>⚠️ Alertas y Brechas Críticas</h1>
        <p>Detección automática de brechas según el modelo APS-SSMC</p>
    </div>
    """, unsafe_allow_html=True)

    if dff.empty:
        st.warning("Sin datos con los filtros seleccionados.")
        return

    alertas = detectar_alertas(dff)

    if not alertas:
        st.success("✅ No se detectaron brechas críticas con los filtros actuales.", icon="✅")
    else:
        st.error(f"Se detectaron **{len(alertas)}** brecha(s) que requieren atención.", icon="⚠️")

    col_alertas, col_resumen = st.columns([2, 1])

    with col_alertas:
        st.markdown("#### Detalle de Brechas")
        for a in alertas:
            sem = a.get("semaforo", "gris")
            icon = "🔴" if sem == "rojo" else "🟡"
            css_class = "alerta-rojo" if sem == "rojo" else "alerta-amarillo"
            umbral = a.get("umbral_alerta", "")
            st.markdown(f"""
            <div class="{css_class}">
                <strong>{icon} {a['tipo']}</strong><br>
                Valor actual: <strong>{a['valor']:.1f} {a['unidad']}</strong>
                {'· Umbral: ' + str(umbral) + a['unidad'] if umbral else ''}<br>
                <small>{a['descripcion']}</small>
            </div>
            """, unsafe_allow_html=True)

        if not alertas:
            st.markdown("""
            <div style="background:#D5F5E3; border-left:4px solid #27AE60;
                        padding:1rem; border-radius:5px;">
                <strong>🟢 Sin brechas detectadas</strong><br>
                Todos los indicadores están dentro de los umbrales aceptables.
            </div>
            """, unsafe_allow_html=True)

    with col_resumen:
        st.markdown("#### Resumen de Brechas")
        n_rojo = sum(1 for a in alertas if a.get("semaforo") == "rojo")
        n_amarillo = sum(1 for a in alertas if a.get("semaforo") == "amarillo")
        st.metric("🔴 Críticas", n_rojo)
        st.metric("🟡 En observación", n_amarillo)
        st.metric("🟢 OK", 10 - len(alertas))

    st.divider()

    # ── Detalle por mes y centro ──
    st.markdown("#### Análisis por Centro y Mes")
    df_centros = kpis_por_centro(dff)
    if not df_centros.empty:
        # Identificar centros bajo umbral
        centros_criticos = df_centros[df_centros["ocupacion"] < 50]
        if not centros_criticos.empty:
            st.warning(f"**{len(centros_criticos)} centro(s)** con ocupación < 50%:")
            for _, row in centros_criticos.iterrows():
                st.write(f"• **{row['centro']}**: {row['ocupacion']:.1f}% ocupación")
        else:
            st.success("Todos los centros superan el umbral crítico de ocupación (50%).")

        st.markdown("##### KPIs por Centro")
        df_c_display = df_centros.copy()
        # Renombrar con nombres legibles (orden debe coincidir con kpis_por_centro)
        rename_centro = {
            "centro": "Centro",
            "total": "Total Registros",
            "citados": "Citados",
            "disponibles": "Disponibles",
            "bloqueados": "Bloqueados",
            "completados": "Completados",
            "ocupacion": "Ocupación %",
            "no_show": "No-Show %",
            "bloqueo": "Bloqueo %",
            "efectividad": "Efectividad %",
            "rendimiento": "Rendimiento (min)",
        }
        df_c_display = df_c_display.rename(columns=rename_centro)

        def color_ocupacion(val):
            if isinstance(val, float):
                if val >= 65:
                    return "color: #27AE60; font-weight: bold"
                elif val >= 50:
                    return "color: #F39C12; font-weight: bold"
                else:
                    return "color: #E74C3C; font-weight: bold"
            return ""

        fmt = {
            "Ocupación %": "{:.1f}", "No-Show %": "{:.1f}",
            "Bloqueo %": "{:.1f}", "Efectividad %": "{:.1f}",
            "Rendimiento (min)": "{:.1f}",
            "Total Registros": "{:,.0f}", "Citados": "{:,.0f}",
            "Disponibles": "{:,.0f}", "Bloqueados": "{:,.0f}", "Completados": "{:,.0f}",
        }
        fmt_valido = {k: v for k, v in fmt.items() if k in df_c_display.columns}
        styled_c = df_c_display.style.map(color_ocupacion, subset=["Ocupación %"]).format(fmt_valido)
        st.dataframe(styled_c, width="stretch", hide_index=True)


# ─────────────────────────────────────────────────────────────
# PÁGINA 6: INFORME POR CENTRO DE SALUD
# ─────────────────────────────────────────────────────────────
def page_informe_centro(dff: pd.DataFrame):
    """Informe analítico completo por CESFAM/Centro de Salud seleccionado."""
    import plotly.graph_objects as go
    from src.kpis import (
        semaforo, calc_ocupacion, calc_no_show, calc_bloqueo,
        calc_efectividad, calc_rendimiento, calc_sobrecupo,
        calc_cobertura_sectorial, calc_agendamiento_remoto,
        calc_ocupacion_extendida,
    )

    st.markdown("""
    <div class="main-header">
        <h1>📋 Informe Analítico por Centro de Salud</h1>
        <p>Reporte integral de productividad · Análisis descriptivo de todos los indicadores</p>
    </div>
    """, unsafe_allow_html=True)

    if dff.empty:
        st.warning("Sin datos con los filtros seleccionados.")
        return

    if "ESTABLECIMIENTO" not in dff.columns:
        st.warning("Los datos no contienen la columna ESTABLECIMIENTO.")
        return

    # ── Selector de Centro ────────────────────────────────────────────────────
    centros_disponibles = sorted(dff["ESTABLECIMIENTO"].dropna().unique().tolist())
    centro_sel = st.selectbox(
        "Seleccionar Centro de Salud para generar informe",
        centros_disponibles,
        key="informe_centro_sel",
    )

    if not centro_sel:
        return

    df_centro = dff[dff["ESTABLECIMIENTO"] == centro_sel].copy()
    if df_centro.empty:
        st.warning(f"Sin registros para **{centro_sel}**.")
        return

    # ── Variables base ────────────────────────────────────────────────────────
    total_registros = len(df_centro)
    citados = int((df_centro["ESTADO CUPO"] == "CITADO").sum())
    disponibles = int((df_centro["ESTADO CUPO"] == "DISPONIBLE").sum())
    bloqueados = int((df_centro["ESTADO CUPO"] == "BLOQUEADO").sum())
    completados = int((df_centro["ESTADO CITA"] == "Completado").sum()) if "ESTADO CITA" in df_centro.columns else 0

    n_meses = df_centro["MES_NUM"].nunique() if "MES_NUM" in df_centro.columns else 0
    rango_meses = ""
    if "MES_NUM" in df_centro.columns and n_meses > 0:
        MESES_N = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
                   7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
        meses_ord = sorted(df_centro["MES_NUM"].dropna().unique().tolist())
        rango_meses = f"{MESES_N.get(int(meses_ord[0]), '?')} a {MESES_N.get(int(meses_ord[-1]), '?')}"

    # ── Calcular todos los KPIs del centro ────────────────────────────────────
    kpis = calculate_all_kpis(df_centro)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 1: RESUMEN EJECUTIVO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown(f"## 1. Resumen Ejecutivo — {centro_sel}")
    st.markdown(
        f"El presente informe analiza la productividad del centro **{centro_sel}** "
        f"durante el período **{rango_meses}** ({n_meses} meses), "
        f"abarcando un total de **{total_registros:,}** registros de cupos programados en el sistema IRIS. "
        f"De estos, **{citados:,}** corresponden a cupos citados, **{disponibles:,}** permanecieron disponibles "
        f"(sin asignar), **{bloqueados:,}** fueron bloqueados administrativamente y **{completados:,}** "
        f"registraron cita completada."
    )

    # Tarjetas resumen
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Registros", f"{total_registros:,}")
    c2.metric("Citados", f"{citados:,}")
    c3.metric("Disponibles", f"{disponibles:,}")
    c4.metric("Bloqueados", f"{bloqueados:,}")
    c5.metric("Completados", f"{completados:,}")

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 2: SEMÁFORO DE INDICADORES
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 2. Semáforo de Indicadores")
    st.markdown(
        "A continuación se presenta el estado de los **10 indicadores clave** del modelo de "
        "productividad APS. Cada indicador se clasifica según semáforo: "
        "🟢 dentro de meta, 🟡 en zona de observación, 🔴 brecha crítica."
    )

    kpi_order = [
        ("ocupacion", "Tasa de Ocupación"),
        ("no_show", "Tasa de No-Show"),
        ("bloqueo", "Tasa de Bloqueo"),
        ("efectividad", "Efectividad de Cita"),
        ("rendimiento", "Rendimiento Promedio"),
        ("sobrecupo", "Cupos Sobrecupo"),
        ("cobertura_sectorial", "Cobertura Sectorial"),
        ("agendamiento_remoto", "Agendamiento Remoto"),
        ("variacion_mensual", "Variación Mensual"),
        ("ocupacion_extendida", "Ocupación Horario Extendido"),
    ]
    cols_kpi = st.columns(5)
    for i, (key, label) in enumerate(kpi_order):
        k = kpis.get(key, {})
        valor = k.get("valor", 0)
        unidad = k.get("unidad", "%")
        sem = k.get("semaforo", "gris")
        icon = semaforo_icon(sem)
        with cols_kpi[i % 5]:
            st.metric(label=f"{icon} {label}", value=f"{valor:.1f} {unidad}")

    # Tabla semáforo detallada
    df_sem = build_semaforo_table(kpis)
    display_cols = ["Estado", "Indicador", "Valor", "Meta", "Alerta si", "Descripción"]
    def _highlight_sem(row):
        if row.get("_semaforo") == "rojo":
            return ["background-color: #FDEDEC"] * len(row)
        elif row.get("_semaforo") == "amarillo":
            return ["background-color: #FEF9E7"] * len(row)
        return [""] * len(row)
    styled_sem = (df_sem[display_cols + ["_semaforo"]]
                  .style.apply(_highlight_sem, axis=1).hide(axis="index"))
    st.dataframe(styled_sem, width="stretch", hide_index=True,
                 column_config={"_semaforo": None})

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 3: DISTRIBUCIÓN DE CUPOS
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 3. Distribución de Estado de Cupos")
    st.markdown(
        "**Gráfico 1.** Composición de cupos según su estado final "
        "(Citado, Disponible, Bloqueado). Este gráfico permite identificar rápidamente "
        "qué proporción de la oferta programada fue efectivamente utilizada versus "
        "la que quedó sin asignar o fue retirada por bloqueo administrativo. "
        "Se calcula como el conteo absoluto de registros agrupados por el campo "
        "`ESTADO CUPO` del sistema IRIS."
    )
    fig_cupos = chart_estado_cupos(df_centro)
    fig_cupos.update_layout(height=450, width=None)
    st.plotly_chart(fig_cupos, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 4: TASA DE OCUPACIÓN
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 4. Análisis de Tasa de Ocupación")
    v_ocu = kpis.get("ocupacion", {}).get("valor", 0)
    sem_ocu = semaforo(v_ocu, "ocupacion")
    st.markdown(
        f"La **Tasa de Ocupación** mide el porcentaje de cupos que fueron asignados a un paciente "
        f"respecto del total de cupos disponibles para atención. Se calcula como: "
        f"`Citados ÷ (Citados + Disponibles) × 100`. "
        f"El centro **{centro_sel}** registra una ocupación de **{v_ocu:.1f}%**, "
        f"clasificada como **{'dentro de meta (≥65%)' if sem_ocu == 'verde' else 'en observación (50-65%)' if sem_ocu == 'amarillo' else 'brecha crítica (<50%)'}**. "
        f"La meta institucional es ≥ 65% y el umbral de alerta es < 50%."
    )

    if "MES_NUM" in df_centro.columns and n_meses >= 2:
        # KPIs por mes del centro
        df_meses_c = _kpis_por_mes_centro(df_centro)
        if not df_meses_c.empty:
            st.markdown(
                "**Gráfico 2.** Evolución mensual de la Tasa de Ocupación. "
                "Muestra la tendencia mes a mes, permitiendo identificar períodos "
                "de subutilización o mejoras sostenidas. La línea punteada verde "
                "indica la meta (65%) y la roja el umbral de alerta (50%)."
            )
            fig_ocu = chart_evolucion_mensual(
                df_meses_c, "ocupacion", "Tasa de Ocupación",
                umbral_ok=65, umbral_alerta=50
            )
            fig_ocu.update_layout(height=450)
            st.plotly_chart(fig_ocu, use_container_width=True)

    # Ocupación por instrumento
    df_inst_c = kpis_por_instrumento(df_centro)
    if not df_inst_c.empty:
        st.markdown(
            "**Gráfico 3.** Ocupación desglosada por instrumento (profesional). "
            "Permite comparar el nivel de aprovechamiento de la agenda entre los distintos "
            "tipos de profesionales del centro. Se calcula como `Citados ÷ (Citados + Disponibles) × 100` "
            "para cada instrumento por separado."
        )
        df_plot = df_inst_c.sort_values("ocupacion")
        colors_ocu = [
            "#27AE60" if v >= 65 else "#F39C12" if v >= 50 else "#E74C3C"
            for v in df_plot["ocupacion"]
        ]
        fig_ocu_inst = go.Figure(go.Bar(
            x=df_plot["ocupacion"], y=df_plot["instrumento"].str[:30],
            orientation="h", marker_color=colors_ocu,
            text=[f"{v:.1f}%" for v in df_plot["ocupacion"]],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Ocupación: %{x:.1f}%<extra></extra>",
        ))
        fig_ocu_inst.add_vline(x=65, line_dash="dash", line_color="#27AE60",
                               annotation_text="Meta 65%")
        fig_ocu_inst.update_layout(
            title="Ocupación por Instrumento/Profesional",
            height=max(400, len(df_plot) * 40 + 100),
            xaxis=dict(title="Ocupación (%)", range=[0, 105]),
            yaxis=dict(title=""), template="plotly_white",
            margin=dict(l=40, r=20, t=60, b=40),
        )
        st.plotly_chart(fig_ocu_inst, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 5: TASA DE NO-SHOW
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 5. Análisis de Tasa de No-Show (Inasistencia)")
    v_ns = kpis.get("no_show", {}).get("valor", 0)
    sem_ns = semaforo(v_ns, "no_show")
    st.markdown(
        f"La **Tasa de No-Show** representa el porcentaje de pacientes que, habiendo sido citados, "
        f"no asistieron a su atención. Se calcula como: "
        f"`(Citados − Completados) ÷ Citados × 100`. "
        f"El centro **{centro_sel}** presenta un No-Show de **{v_ns:.1f}%**, "
        f"clasificado como **{'aceptable (≤10%)' if sem_ns == 'verde' else 'en observación (10-15%)' if sem_ns == 'amarillo' else 'crítico (>15%)'}**. "
        f"Cada punto porcentual de No-Show representa horas clínicas perdidas y pacientes sin atención."
    )

    if "MES_NUM" in df_centro.columns and n_meses >= 2:
        df_meses_c = _kpis_por_mes_centro(df_centro)
        if not df_meses_c.empty:
            st.markdown(
                "**Gráfico 4.** Evolución mensual de la Tasa de No-Show comparada con el umbral "
                "institucional (10%). Las barras cambian de color según gravedad: "
                "verde (≤10%), amarillo (10-15%), rojo (>15%)."
            )
            fig_ns = chart_noshow_vs_umbral(df_meses_c)
            fig_ns.update_layout(height=450)
            st.plotly_chart(fig_ns, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 6: TASA DE BLOQUEO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 6. Análisis de Tasa de Bloqueo")
    v_bloq = kpis.get("bloqueo", {}).get("valor", 0)
    sem_bloq = semaforo(v_bloq, "bloqueo")
    st.markdown(
        f"La **Tasa de Bloqueo** mide el porcentaje de cupos que fueron bloqueados "
        f"administrativamente (vacaciones, capacitaciones, fallas de equipos, reuniones, etc.), "
        f"reduciendo la capacidad real de atención del centro. Se calcula como: "
        f"`Bloqueados ÷ Total cupos × 100`. "
        f"El centro **{centro_sel}** registra un bloqueo de **{v_bloq:.1f}%**, "
        f"clasificado como **{'aceptable (≤10%)' if sem_bloq == 'verde' else 'en observación (10-15%)' if sem_bloq == 'amarillo' else 'excesivo (>15%)'}**."
    )

    if "MES_NUM" in df_centro.columns and n_meses >= 2:
        df_meses_c = _kpis_por_mes_centro(df_centro)
        if not df_meses_c.empty:
            st.markdown(
                "**Gráfico 5.** Evolución mensual de la Tasa de Bloqueo. "
                "Permite detectar meses con mayor pérdida de capacidad instalada "
                "por causas administrativas. La meta es mantener el bloqueo ≤ 10%."
            )
            fig_bloq = chart_evolucion_mensual(
                df_meses_c, "bloqueo", "Tasa de Bloqueo",
                umbral_ok=10, umbral_alerta=15
            )
            fig_bloq.update_layout(height=450)
            st.plotly_chart(fig_bloq, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 7: EFECTIVIDAD DE CITA
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 7. Análisis de Efectividad de Cita")
    v_efec = kpis.get("efectividad", {}).get("valor", 0)
    sem_efec = semaforo(v_efec, "efectividad")
    st.markdown(
        f"La **Efectividad de Cita** mide el porcentaje de citas que fueron completadas "
        f"exitosamente respecto del total de citas agendadas. Se calcula como: "
        f"`Completados ÷ Citados × 100`. "
        f"El centro **{centro_sel}** alcanza una efectividad de **{v_efec:.1f}%**, "
        f"clasificada como **{'óptima (≥88%)' if sem_efec == 'verde' else 'en observación (80-88%)' if sem_efec == 'amarillo' else 'baja (<80%)'}**. "
        f"Una efectividad baja puede indicar problemas de confirmación de citas o verificación "
        f"de asistencia."
    )

    if "MES_NUM" in df_centro.columns and n_meses >= 2:
        df_meses_c = _kpis_por_mes_centro(df_centro)
        if not df_meses_c.empty:
            st.markdown(
                "**Gráfico 6.** Evolución mensual de la Efectividad de Cita. "
                "Muestra la proporción de citas que terminaron en atención efectiva mes a mes. "
                "La meta es ≥ 88% y el umbral de alerta es < 80%."
            )
            fig_efec = chart_evolucion_mensual(
                df_meses_c, "efectividad", "Efectividad de Cita",
                umbral_ok=88, umbral_alerta=80
            )
            fig_efec.update_layout(height=450)
            st.plotly_chart(fig_efec, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 8: RENDIMIENTO POR INSTRUMENTO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 8. Rendimiento Promedio por Instrumento")
    v_rend = kpis.get("rendimiento", {}).get("valor", 0)
    st.markdown(
        f"El **Rendimiento Promedio** indica la cantidad de minutos que en promedio dura "
        f"cada atención por profesional. Se calcula como el promedio del campo `RENDIMIENTO` "
        f"del sistema IRIS, que registra los minutos programados por cupo. "
        f"El centro **{centro_sel}** presenta un rendimiento promedio de **{v_rend:.1f} min/atención**. "
        f"Valores muy bajos pueden indicar atenciones superficiales; valores muy altos pueden "
        f"significar ineficiencia o complejidad clínica elevada."
    )

    st.markdown(
        "**Gráfico 7.** Rendimiento promedio desglosado por instrumento (tipo de profesional). "
        "Muestra la cantidad de minutos que cada profesional destina en promedio por atención, "
        "calculado como `Promedio(RENDIMIENTO)` agrupado por `INSTRUMENTO`. "
        "Permite identificar profesionales con rendimientos atípicos."
    )
    fig_rend = chart_rendimiento_instrumento(df_centro)
    fig_rend.update_layout(height=max(400, len(df_inst_c) * 40 + 100) if not df_inst_c.empty else 400)
    st.plotly_chart(fig_rend, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 9: SOBRECUPO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 9. Análisis de Cupos Sobrecupo")
    v_sobre = kpis.get("sobrecupo", {}).get("valor", 0)
    sem_sobre = semaforo(v_sobre, "sobrecupo")
    st.markdown(
        f"El indicador de **Sobrecupo** mide el porcentaje de atenciones que fueron "
        f"agendadas por sobre la capacidad programada del profesional. Se calcula como: "
        f"`Sobrecupos ÷ Total cupos × 100`. "
        f"El centro **{centro_sel}** registra un **{v_sobre:.1f}%** de sobrecupo, "
        f"clasificado como **{'aceptable (≤5%)' if sem_sobre == 'verde' else 'en observación (5-10%)' if sem_sobre == 'amarillo' else 'excesivo (>10%)'}**. "
        f"Un sobrecupo elevado genera sobrecarga asistencial y puede afectar la calidad de atención."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 10: COBERTURA SECTORIAL
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 10. Cobertura Sectorial")
    v_cob = kpis.get("cobertura_sectorial", {}).get("valor", 0)
    sem_cob = semaforo(v_cob, "cobertura_sectorial")
    st.markdown(
        f"La **Cobertura Sectorial** mide el porcentaje de registros que tienen un sector "
        f"territorial informado (Verde, Lila, Rojo) versus aquellos marcados como 'No Informado'. "
        f"Se calcula como: `Registros con sector ÷ Total × 100`. "
        f"El centro **{centro_sel}** tiene una cobertura de **{v_cob:.1f}%**, "
        f"clasificada como **{'óptima (≥80%)' if sem_cob == 'verde' else 'parcial (60-80%)' if sem_cob == 'amarillo' else 'deficiente (<60%)'}**. "
        f"Una baja cobertura dificulta el análisis territorial de la demanda."
    )

    st.markdown(
        "**Gráfico 8.** Distribución de cupos por sector territorial. "
        "Muestra la proporción de atenciones asignadas a cada sector (Verde, Lila, Rojo) "
        "versus las que no tienen sector informado. Se calcula como el conteo de registros "
        "agrupados por el campo `SECTOR`."
    )
    fig_sector = chart_sector(df_centro)
    fig_sector.update_layout(height=450)
    st.plotly_chart(fig_sector, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 11: AGENDAMIENTO REMOTO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 11. Agendamiento Remoto")
    v_ag = kpis.get("agendamiento_remoto", {}).get("valor", 0)
    sem_ag = semaforo(v_ag, "agendamiento_remoto")
    st.markdown(
        f"El **Agendamiento Remoto** mide el porcentaje de citas gestionadas mediante "
        f"canales no presenciales (telefónico y telesalud). Se calcula como: "
        f"`(Telefónico + Telesalud) ÷ Total × 100`. "
        f"El centro **{centro_sel}** registra un **{v_ag:.1f}%** de agendamiento remoto, "
        f"clasificado como **{'adecuado (≥20%)' if sem_ag == 'verde' else 'incipiente (5-20%)' if sem_ag == 'amarillo' else 'insuficiente (<5%)'}**. "
        f"Fomentar el agendamiento remoto reduce barreras de acceso y carga administrativa presencial."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 12: OCUPACIÓN HORARIO EXTENDIDO + APERTURA SABATINA
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 12. Horario Extendido y Apertura Sabatina")
    v_ext = kpis.get("ocupacion_extendida", {}).get("valor", 0)
    sem_ext = semaforo(v_ext, "ocupacion_extendida")
    st.markdown(
        f"La **Ocupación en Horario Extendido** evalúa el uso de los cupos programados "
        f"a partir de las 18:00 horas, correspondientes a la jornada extendida que implica "
        f"un costo adicional para el establecimiento. Se calcula como: "
        f"`Citados ≥18h ÷ (Citados + Disponibles ≥18h) × 100`. "
        f"El centro **{centro_sel}** registra un **{v_ext:.1f}%** de ocupación extendida, "
        f"clasificado como **{'adecuado (≥50%)' if sem_ext == 'verde' else 'bajo (30-50%)' if sem_ext == 'amarillo' else 'muy bajo (<30%)'}**. "
        f"Una baja ocupación en este horario cuestiona la eficiencia del gasto asociado."
    )

    # ── Tabla segmentada Normal / Extendido / Sábado ──────────────
    from src.kpis import (
        kpis_horario_segmentado, kpis_profesional_sabatino,
        kpis_profesional_extendido,
    )
    df_seg_c = kpis_horario_segmentado(df_centro)
    if not df_seg_c.empty:
        st.markdown("### 12.1 Comparativa por Segmento Horario")
        st.dataframe(
            df_seg_c.rename(columns={
                "segmento": "Segmento", "total": "Total",
                "citados": "Citados", "disponibles": "Disp.", "bloqueados": "Bloq.",
                "completados": "Complet.", "ocupacion": "Ocup.%",
                "no_show": "NoShow%", "bloqueo": "Bloq.%",
                "efectividad": "Efect.%", "rendimiento": "Rend.", "sobrecupo": "Sobrec.%",
            }),
            use_container_width=True, hide_index=True,
        )

    # ── Profesionales en Apertura Sabatina ────────────────────────
    df_prof_sab_c = kpis_profesional_sabatino(df_centro)
    if not df_prof_sab_c.empty:
        st.markdown("### 12.2 Profesionales en Apertura Sabatina")
        st.markdown(
            f"Se identifican **{len(df_prof_sab_c)} profesionales** con cupos asignados "
            f"los días sábado en **{centro_sel}**."
        )
        st.dataframe(
            df_prof_sab_c.rename(columns={
                "profesional": "Profesional", "total": "Total",
                "citados": "Citados", "disponibles": "Disp.", "bloqueados": "Bloq.",
                "completados": "Complet.", "ocupacion": "Ocup.%",
                "no_show": "NoShow%", "bloqueo": "Bloq.%",
                "efectividad": "Efect.%", "rendimiento": "Rend.",
            }),
            use_container_width=True, hide_index=True,
        )

    # ── Profesionales en Horario Extendido ────────────────────────
    df_prof_ext_c = kpis_profesional_extendido(df_centro)
    if not df_prof_ext_c.empty:
        st.markdown("### 12.3 Profesionales en Horario Extendido (Lun-Vie ≥ 18 h)")
        st.markdown(
            f"Se identifican **{len(df_prof_ext_c)} profesionales** con cupos en jornada "
            f"extendida los días de semana en **{centro_sel}**."
        )
        st.dataframe(
            df_prof_ext_c.rename(columns={
                "profesional": "Profesional", "total": "Total",
                "citados": "Citados", "disponibles": "Disp.", "bloqueados": "Bloq.",
                "completados": "Complet.", "ocupacion": "Ocup.%",
                "no_show": "NoShow%", "bloqueo": "Bloq.%",
                "efectividad": "Efect.%", "rendimiento": "Rend.",
            }),
            use_container_width=True, hide_index=True,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 13: TIPO DE ATENCIÓN
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 13. Distribución por Tipo de Atención")
    st.markdown(
        "**Gráfico 9.** Distribución de los registros según tipo de atención "
        "(Morbilidad, Control, Urgencia, Procedimiento, etc.). "
        "Muestra el volumen absoluto de cupos agrupados por `TIPO ATENCION`, "
        "permitiendo identificar la composición de la cartera de servicios del centro."
    )
    fig_tipo = chart_tipo_atencion(df_centro, top_n=15)
    fig_tipo.update_layout(height=500)
    st.plotly_chart(fig_tipo, use_container_width=True)

    # Tabla KPIs por tipo atención
    df_kpis_ta = kpis_por_tipo_atencion(df_centro)
    if not df_kpis_ta.empty:
        st.markdown(
            "**Tabla 1.** KPIs desglosados por tipo de atención. Cada fila muestra "
            "el total de registros, citados, disponibles, bloqueados, completados y las "
            "tasas de ocupación, no-show, bloqueo, efectividad, sobrecupo, agendamiento "
            "remoto y rendimiento para cada tipo de atención del centro."
        )
        _sem_icon = lambda val, kpi: {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴"}.get(semaforo(val, kpi), "⚪")
        df_ta_disp = df_kpis_ta.copy()
        df_ta_disp["Ocupación"] = df_ta_disp.apply(lambda r: f"{_sem_icon(r['ocupacion'],'ocupacion')} {r['ocupacion']:.1f}%", axis=1)
        df_ta_disp["No-Show"] = df_ta_disp.apply(lambda r: f"{_sem_icon(r['no_show'],'no_show')} {r['no_show']:.1f}%", axis=1)
        df_ta_disp["Efectividad"] = df_ta_disp.apply(lambda r: f"{_sem_icon(r['efectividad'],'efectividad')} {r['efectividad']:.1f}%", axis=1)
        df_ta_disp["Rendim. (min)"] = df_ta_disp["rendimiento"].round(1)
        df_ta_disp["Total"] = df_ta_disp["total"].apply(lambda v: f"{v:,}")
        df_ta_disp["Citados"] = df_ta_disp["citados"].apply(lambda v: f"{v:,}")
        df_ta_disp["Disponibles"] = df_ta_disp["disponibles"].apply(lambda v: f"{v:,}")
        df_ta_disp["Bloqueados"] = df_ta_disp["bloqueados"].apply(lambda v: f"{v:,}")
        df_ta_disp["Completados"] = df_ta_disp["completados"].apply(lambda v: f"{v:,}")
        cols_show = ["tipo_atencion","Total","Citados","Disponibles","Bloqueados","Completados",
                     "Ocupación","No-Show","Efectividad","Rendim. (min)"]
        st.dataframe(
            df_ta_disp[cols_show].rename(columns={"tipo_atencion": "Tipo de Atención"}),
            width="stretch", hide_index=True,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 14: ANÁLISIS POR INSTRUMENTO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 14. KPIs por Instrumento / Profesional")
    st.markdown(
        "**Tabla 2.** Resumen de indicadores por instrumento (profesional) del centro. "
        "Incluye el volumen total, citados, disponibles, bloqueados, completados y las "
        "tasas de ocupación, no-show, efectividad y rendimiento. Permite identificar "
        "profesionales con mayor o menor aprovechamiento de agenda."
    )

    if not df_inst_c.empty:
        _sem_i = lambda val, kpi: {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴"}.get(semaforo(val, kpi), "⚪")
        df_id = df_inst_c.copy()
        df_id["Ocupación"] = df_id.apply(lambda r: f"{_sem_i(r['ocupacion'],'ocupacion')} {r['ocupacion']:.1f}%", axis=1)
        df_id["No-Show"] = df_id.apply(lambda r: f"{_sem_i(r['no_show'],'no_show')} {r['no_show']:.1f}%", axis=1)
        df_id["Efectividad"] = df_id.apply(lambda r: f"{_sem_i(r['efectividad'],'efectividad')} {r['efectividad']:.1f}%", axis=1)
        df_id["Rendim. (min)"] = df_id["rendimiento"].round(1)
        df_id["Total"] = df_id["total"].apply(lambda v: f"{v:,}")
        df_id["Citados"] = df_id["citados"].apply(lambda v: f"{v:,}")
        df_id["Disponibles"] = df_id["disponibles"].apply(lambda v: f"{v:,}")
        df_id["Bloqueados"] = df_id["bloqueados"].apply(lambda v: f"{v:,}")
        df_id["Completados"] = df_id["completados"].apply(lambda v: f"{v:,}")
        cols_i = ["instrumento","Total","Citados","Disponibles","Bloqueados","Completados",
                  "Ocupación","No-Show","Efectividad","Rendim. (min)"]
        st.dataframe(
            df_id[cols_i].rename(columns={"instrumento": "Instrumento"}),
            width="stretch", hide_index=True,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 15: EVOLUCIÓN MULTI-KPI
    # ══════════════════════════════════════════════════════════════════════════
    if "MES_NUM" in df_centro.columns and n_meses >= 2:
        st.markdown("---")
        st.markdown("## 15. Evolución Conjunta de KPIs Principales")
        st.markdown(
            "**Gráfico 10.** Evolución simultánea de Ocupación, No-Show y Bloqueo mes a mes. "
            "Permite visualizar la interacción entre estos tres indicadores: un aumento de "
            "bloqueo típicamente reduce la ocupación; un No-Show elevado reduce la efectividad. "
            "Se calculan como las respectivas tasas mensuales del centro."
        )
        df_meses_c = _kpis_por_mes_centro(df_centro)
        if not df_meses_c.empty:
            fig_multi = chart_multi_kpi(df_meses_c)
            fig_multi.update_layout(height=480)
            st.plotly_chart(fig_multi, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 16: MAPA DE CALOR
    # ══════════════════════════════════════════════════════════════════════════
    if "MES_NUM" in df_centro.columns and "INSTRUMENTO" in df_centro.columns and n_meses >= 2:
        st.markdown("---")
        st.markdown("## 16. Mapa de Calor: Ocupación por Instrumento y Mes")
        st.markdown(
            "**Gráfico 11.** Mapa de calor que cruza cada instrumento (fila) con cada mes (columna), "
            "coloreando según la tasa de ocupación. Los tonos verdes indican ocupación ≥ 65%, "
            "amarillos zona intermedia, y rojos ocupación crítica. Se calcula como "
            "`Citados ÷ (Citados + Disponibles) × 100` para cada combinación instrumento-mes. "
            "Permite detectar patrones estacionales y profesionales con baja utilización sostenida."
        )
        from src.charts import chart_heatmap_instrumento_mes
        fig_hm = chart_heatmap_instrumento_mes(df_centro)
        n_inst_hm = df_centro["INSTRUMENTO"].nunique()
        fig_hm.update_layout(height=max(450, n_inst_hm * 38 + 120))
        st.plotly_chart(fig_hm, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 17: ALERTAS DEL CENTRO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 17. Alertas y Brechas del Centro")
    alertas_centro = detectar_alertas(df_centro)
    if not alertas_centro:
        st.success(
            f"✅ **{centro_sel}** no presenta brechas críticas. "
            "Todos los indicadores se encuentran dentro de los umbrales aceptables.",
            icon="✅"
        )
    else:
        st.error(
            f"Se detectaron **{len(alertas_centro)}** brecha(s) en **{centro_sel}** "
            "que requieren atención prioritaria.", icon="⚠️"
        )
        for a in alertas_centro:
            sem_a = a.get("semaforo", "gris")
            icon_a = "🔴" if sem_a == "rojo" else "🟡"
            st.markdown(
                f"- {icon_a} **{a['tipo']}**: {a['valor']:.1f} {a['unidad']} — {a['descripcion']}"
            )

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIÓN 18: CONCLUSIÓN
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("## 18. Conclusión del Informe")

    n_verde = sum(1 for k in kpis.values() if isinstance(k, dict) and k.get("semaforo") == "verde")
    n_amarillo = sum(1 for k in kpis.values() if isinstance(k, dict) and k.get("semaforo") == "amarillo")
    n_rojo = sum(1 for k in kpis.values() if isinstance(k, dict) and k.get("semaforo") == "rojo")

    st.markdown(
        f"El centro **{centro_sel}** presenta **{n_verde}** indicadores en estado óptimo (🟢), "
        f"**{n_amarillo}** en zona de observación (🟡) y **{n_rojo}** en brecha crítica (🔴) "
        f"durante el período analizado ({rango_meses}). "
    )
    if n_rojo > 0:
        kpis_rojos = [
            k.get("nombre", key) for key, k in kpis.items()
            if isinstance(k, dict) and k.get("semaforo") == "rojo"
        ]
        st.markdown(
            f"Los indicadores en estado crítico que requieren intervención inmediata son: "
            f"**{', '.join(kpis_rojos)}**. Se recomienda priorizar acciones correctivas en estas áreas."
        )
    if n_amarillo > 0:
        kpis_amarillos = [
            k.get("nombre", key) for key, k in kpis.items()
            if isinstance(k, dict) and k.get("semaforo") == "amarillo"
        ]
        st.markdown(
            f"Los indicadores en observación son: **{', '.join(kpis_amarillos)}**. "
            f"Se sugiere monitoreo continuo para evitar que evolucionen a brecha crítica."
        )
    if n_rojo == 0 and n_amarillo == 0:
        st.markdown(
            "Todos los indicadores se encuentran dentro de los umbrales establecidos. "
            "Se recomienda mantener las estrategias actuales y continuar con el monitoreo periódico."
        )

    st.caption(
        f"*Informe generado automáticamente por el Sistema de Análisis de Productividad APS — "
        f"Servicio de Salud Metropolitano Central · {centro_sel} · Período: {rango_meses}*"
    )

    # ══════════════════════════════════════════════════════════════════════════
    # BOTÓN DE DESCARGA
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 📥 Descargar Informe")
    st.markdown("Descarga el informe completo con todos los gráficos embebidos.")

    _informe_args = dict(
        centro_sel=centro_sel,
        rango_meses=rango_meses,
        n_meses=n_meses,
        total_registros=total_registros,
        citados=citados,
        disponibles=disponibles,
        bloqueados=bloqueados,
        completados=completados,
        kpis=kpis,
        df_centro=df_centro,
        df_inst_c=df_inst_c,
        df_kpis_ta=df_kpis_ta if not df_kpis_ta.empty else pd.DataFrame(),
        alertas_centro=alertas_centro,
        n_verde=n_verde,
        n_amarillo=n_amarillo,
        n_rojo=n_rojo,
    )

    col_dl1, col_dl2 = st.columns(2)

    with col_dl1:
        with st.spinner("Generando HTML..."):
            html_report = _generar_html_informe(**_informe_args)
        nombre_html = f"Informe_{centro_sel.replace(' ', '_')}_{rango_meses.replace(' ', '_')}.html"
        st.download_button(
            label="📄 Descargar HTML",
            data=html_report.encode("utf-8"),
            file_name=nombre_html,
            mime="text/html",
            type="secondary",
            use_container_width=True,
        )

    with col_dl2:
        with st.spinner("Generando PDF (puede tardar unos segundos)..."):
            try:
                pdf_bytes = _generar_pdf_informe(**_informe_args)
                nombre_pdf = f"Informe_{centro_sel.replace(' ', '_')}_{rango_meses.replace(' ', '_')}.pdf"
                st.download_button(
                    label="📕 Descargar PDF",
                    data=pdf_bytes,
                    file_name=nombre_pdf,
                    mime="application/pdf",
                    type="primary",
                    use_container_width=True,
                )
            except Exception as e:
                st.error(f"Error generando PDF: {e}")


def _generar_html_informe(
    centro_sel, rango_meses, n_meses, total_registros,
    citados, disponibles, bloqueados, completados,
    kpis, df_centro, df_inst_c, df_kpis_ta,
    alertas_centro, n_verde, n_amarillo, n_rojo,
) -> str:
    """Genera informe HTML autocontenido con gráficos Plotly embebidos."""
    import plotly.graph_objects as go
    from plotly.offline import get_plotlyjs_version
    from src.kpis import semaforo, KPI_DEFINITIONS
    from src.charts import (
        chart_estado_cupos, chart_evolucion_mensual, chart_noshow_vs_umbral,
        chart_rendimiento_instrumento, chart_sector, chart_tipo_atencion,
        chart_multi_kpi, chart_heatmap_instrumento_mes,
    )

    plotly_js_ver = get_plotlyjs_version()

    def _fig_to_img(fig, width=900, height=450):
        """Convierte fig Plotly a div HTML embebido (Plotly JS se carga una vez en <head>)."""
        fig.update_layout(width=width, height=height)
        return fig.to_html(include_plotlyjs=False, full_html=False,
                           config={"staticPlot": True, "displayModeBar": False})

    def _sem_icon(val, kpi):
        s = semaforo(val, kpi)
        return {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴"}.get(s, "⚪")

    def _sem_text(val, kpi):
        s = semaforo(val, kpi)
        return {"verde": "Óptimo", "amarillo": "Observación", "rojo": "Crítico"}.get(s, "—")

    # ── Generar gráficos ──────────────────────────────────────────────────────
    charts_html = {}

    # G1: Estado de cupos
    fig1 = chart_estado_cupos(df_centro)
    fig1.update_layout(height=420)
    charts_html["cupos"] = _fig_to_img(fig1)

    # G2: Ocupación mensual
    df_meses_c = _kpis_por_mes_centro(df_centro)
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig2 = chart_evolucion_mensual(df_meses_c, "ocupacion", "Tasa de Ocupación",
                                        umbral_ok=65, umbral_alerta=50)
        fig2.update_layout(height=420)
        charts_html["ocu_mensual"] = _fig_to_img(fig2)

    # G3: Ocupación por instrumento
    if not df_inst_c.empty:
        df_plot = df_inst_c.sort_values("ocupacion")
        colors_ocu = ["#27AE60" if v >= 65 else "#F39C12" if v >= 50 else "#E74C3C"
                      for v in df_plot["ocupacion"]]
        fig3 = go.Figure(go.Bar(
            x=df_plot["ocupacion"], y=df_plot["instrumento"].str[:30],
            orientation="h", marker_color=colors_ocu,
            text=[f"{v:.1f}%" for v in df_plot["ocupacion"]], textposition="outside",
        ))
        fig3.add_vline(x=65, line_dash="dash", line_color="#27AE60", annotation_text="Meta 65%")
        fig3.update_layout(title="Ocupación por Instrumento", template="plotly_white",
                           height=max(400, len(df_plot)*40+100),
                           xaxis=dict(title="Ocupación (%)", range=[0, 105]), yaxis=dict(title=""))
        charts_html["ocu_inst"] = _fig_to_img(fig3, height=max(400, len(df_plot)*40+100))

    # G4: No-Show mensual
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig4 = chart_noshow_vs_umbral(df_meses_c)
        fig4.update_layout(height=420)
        charts_html["noshow"] = _fig_to_img(fig4)

    # G5: Bloqueo mensual
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig5 = chart_evolucion_mensual(df_meses_c, "bloqueo", "Tasa de Bloqueo",
                                        umbral_ok=10, umbral_alerta=15)
        fig5.update_layout(height=420)
        charts_html["bloqueo"] = _fig_to_img(fig5)

    # G6: Efectividad mensual
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig6 = chart_evolucion_mensual(df_meses_c, "efectividad", "Efectividad de Cita",
                                        umbral_ok=88, umbral_alerta=80)
        fig6.update_layout(height=420)
        charts_html["efectividad"] = _fig_to_img(fig6)

    # G7: Rendimiento por instrumento
    fig7 = chart_rendimiento_instrumento(df_centro)
    fig7.update_layout(height=max(400, len(df_inst_c)*40+100) if not df_inst_c.empty else 400)
    charts_html["rendimiento"] = _fig_to_img(fig7)

    # G8: Sector territorial
    fig8 = chart_sector(df_centro)
    fig8.update_layout(height=420)
    charts_html["sector"] = _fig_to_img(fig8)

    # G9: Tipo de atención
    fig9 = chart_tipo_atencion(df_centro, top_n=15)
    fig9.update_layout(height=500)
    charts_html["tipo_atencion"] = _fig_to_img(fig9, height=500)

    # G10: Multi-KPI
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig10 = chart_multi_kpi(df_meses_c)
        fig10.update_layout(height=450)
        charts_html["multi_kpi"] = _fig_to_img(fig10)

    # G11: Heatmap
    if "MES_NUM" in df_centro.columns and "INSTRUMENTO" in df_centro.columns:
        fig11 = chart_heatmap_instrumento_mes(df_centro)
        n_i = df_centro["INSTRUMENTO"].nunique()
        fig11.update_layout(height=max(450, n_i*38+120))
        charts_html["heatmap"] = _fig_to_img(fig11, height=max(450, n_i*38+120))

    # ── Tabla semáforo KPIs ───────────────────────────────────────────────────
    kpi_rows_html = ""
    kpi_order = [
        ("ocupacion", "Tasa de Ocupación"), ("no_show", "Tasa de No-Show"),
        ("bloqueo", "Tasa de Bloqueo"), ("efectividad", "Efectividad de Cita"),
        ("rendimiento", "Rendimiento Promedio"), ("sobrecupo", "Cupos Sobrecupo"),
        ("cobertura_sectorial", "Cobertura Sectorial"), ("agendamiento_remoto", "Agendamiento Remoto"),
        ("variacion_mensual", "Variación Mensual"), ("ocupacion_extendida", "Ocupación Horario Extendido"),
    ]
    for key, label in kpi_order:
        k = kpis.get(key, {})
        valor = k.get("valor", 0)
        unidad = k.get("unidad", "%")
        sem = k.get("semaforo", "gris")
        icon = {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴"}.get(sem, "⚪")
        meta = k.get("umbral_ok", "—")
        alerta = k.get("umbral_alerta", "—")
        desc = k.get("descripcion", "")
        bg = {"rojo": "#FDEDEC", "amarillo": "#FEF9E7", "verde": "#EAFAF1"}.get(sem, "#fff")
        kpi_rows_html += f'<tr style="background:{bg}"><td>{icon}</td><td><strong>{label}</strong></td><td>{valor:.1f} {unidad}</td><td>{meta}{unidad if meta != "—" else ""}</td><td>{alerta}{unidad if alerta != "—" else ""}</td><td style="font-size:0.85em">{desc}</td></tr>\n'

    # ── Tabla instrumentos ────────────────────────────────────────────────────
    inst_rows_html = ""
    if not df_inst_c.empty:
        for _, r in df_inst_c.iterrows():
            inst_rows_html += (
                f'<tr><td>{r["instrumento"]}</td><td>{r["total"]:,}</td><td>{r["citados"]:,}</td>'
                f'<td>{r["disponibles"]:,}</td><td>{r["bloqueados"]:,}</td><td>{r["completados"]:,}</td>'
                f'<td>{_sem_icon(r["ocupacion"],"ocupacion")} {r["ocupacion"]:.1f}%</td>'
                f'<td>{_sem_icon(r["no_show"],"no_show")} {r["no_show"]:.1f}%</td>'
                f'<td>{_sem_icon(r["efectividad"],"efectividad")} {r["efectividad"]:.1f}%</td>'
                f'<td>{r["rendimiento"]:.1f}</td></tr>\n'
            )

    # ── Tabla tipo atención ───────────────────────────────────────────────────
    ta_rows_html = ""
    if not df_kpis_ta.empty:
        for _, r in df_kpis_ta.iterrows():
            ta_rows_html += (
                f'<tr><td>{r["tipo_atencion"]}</td><td>{r["total"]:,}</td><td>{r["citados"]:,}</td>'
                f'<td>{r["disponibles"]:,}</td><td>{r["bloqueados"]:,}</td><td>{r["completados"]:,}</td>'
                f'<td>{_sem_icon(r["ocupacion"],"ocupacion")} {r["ocupacion"]:.1f}%</td>'
                f'<td>{_sem_icon(r["no_show"],"no_show")} {r["no_show"]:.1f}%</td>'
                f'<td>{_sem_icon(r["efectividad"],"efectividad")} {r["efectividad"]:.1f}%</td>'
                f'<td>{r["rendimiento"]:.1f}</td></tr>\n'
            )

    # ── Alertas ───────────────────────────────────────────────────────────────
    alertas_html = ""
    if not alertas_centro:
        alertas_html = '<div style="background:#D5F5E3;border-left:4px solid #27AE60;padding:1rem;border-radius:5px;"><strong>🟢 Sin brechas detectadas.</strong> Todos los indicadores dentro de umbrales aceptables.</div>'
    else:
        for a in alertas_centro:
            sem_a = a.get("semaforo", "gris")
            icon_a = "🔴" if sem_a == "rojo" else "🟡"
            bg_a = "#FDEDEC" if sem_a == "rojo" else "#FEF9E7"
            alertas_html += (
                f'<div style="background:{bg_a};border-left:4px solid {"#E74C3C" if sem_a=="rojo" else "#F39C12"};'
                f'padding:0.8rem;border-radius:5px;margin-bottom:0.5rem;">'
                f'<strong>{icon_a} {a["tipo"]}</strong>: {a["valor"]:.1f} {a["unidad"]} — {a["descripcion"]}</div>\n'
            )

    # ── Conclusión ────────────────────────────────────────────────────────────
    conclusion_html = (
        f"<p>El centro <strong>{centro_sel}</strong> presenta <strong>{n_verde}</strong> indicadores en estado óptimo (🟢), "
        f"<strong>{n_amarillo}</strong> en zona de observación (🟡) y <strong>{n_rojo}</strong> en brecha crítica (🔴) "
        f"durante el período analizado ({rango_meses}).</p>"
    )
    if n_rojo > 0:
        kpis_rojos = [k.get("nombre", key) for key, k in kpis.items()
                      if isinstance(k, dict) and k.get("semaforo") == "rojo"]
        conclusion_html += f"<p>Indicadores críticos: <strong>{', '.join(kpis_rojos)}</strong>. Se recomienda intervención inmediata.</p>"
    if n_amarillo > 0:
        kpis_amarillos = [k.get("nombre", key) for key, k in kpis.items()
                          if isinstance(k, dict) and k.get("semaforo") == "amarillo"]
        conclusion_html += f"<p>Indicadores en observación: <strong>{', '.join(kpis_amarillos)}</strong>. Se sugiere monitoreo continuo.</p>"
    if n_rojo == 0 and n_amarillo == 0:
        conclusion_html += "<p>Todos los indicadores se encuentran dentro de los umbrales. Se recomienda mantener las estrategias actuales.</p>"

    # ── KPIs del centro ──
    v_ocu = kpis.get("ocupacion", {}).get("valor", 0)
    v_ns = kpis.get("no_show", {}).get("valor", 0)
    v_bloq = kpis.get("bloqueo", {}).get("valor", 0)
    v_efec = kpis.get("efectividad", {}).get("valor", 0)
    v_rend = kpis.get("rendimiento", {}).get("valor", 0)
    v_sobre = kpis.get("sobrecupo", {}).get("valor", 0)
    v_cob = kpis.get("cobertura_sectorial", {}).get("valor", 0)
    v_ag = kpis.get("agendamiento_remoto", {}).get("valor", 0)
    v_ext = kpis.get("ocupacion_extendida", {}).get("valor", 0)

    from datetime import datetime
    fecha_gen = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Informe Productividad — {centro_sel}</title>
<script src="https://cdn.plot.ly/plotly-{plotly_js_ver}.min.js" charset="utf-8"></script>
<style>
  @media print {{ body {{ margin: 0.5cm; }} .no-print {{ display: none; }} .page-break {{ page-break-before: always; }} }}
  body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #2C3E50; max-width: 1100px; margin: 0 auto; padding: 20px; line-height: 1.6; }}
  h1 {{ color: #1B4F72; border-bottom: 3px solid #2E86C1; padding-bottom: 10px; }}
  h2 {{ color: #1B4F72; margin-top: 2rem; border-left: 4px solid #2E86C1; padding-left: 12px; }}
  .header {{ background: linear-gradient(135deg, #1B4F72, #2E86C1); color: white; padding: 25px 30px; border-radius: 10px; margin-bottom: 25px; }}
  .header h1 {{ color: white; border: none; margin: 0; font-size: 1.8rem; }}
  .header p {{ color: #AED6F1; margin: 5px 0 0; }}
  .cards {{ display: flex; gap: 15px; flex-wrap: wrap; margin: 15px 0; }}
  .card {{ background: #F8F9FA; border-radius: 8px; padding: 15px 20px; text-align: center; flex: 1; min-width: 140px; border-top: 4px solid #2E86C1; }}
  .card .val {{ font-size: 1.6rem; font-weight: 700; color: #1B4F72; }}
  .card .lbl {{ font-size: 0.8rem; color: #666; }}
  table {{ border-collapse: collapse; width: 100%; margin: 15px 0; font-size: 0.9rem; }}
  th {{ background: #1B4F72; color: white; padding: 10px 8px; text-align: left; }}
  td {{ padding: 8px; border-bottom: 1px solid #ddd; }}
  tr:nth-child(even) {{ background: #F8F9FA; }}
  .chart-container {{ margin: 20px 0; text-align: center; }}
  .chart-caption {{ font-size: 0.9rem; color: #555; margin-bottom: 10px; text-align: left; font-style: italic; }}
  .footer {{ margin-top: 30px; padding-top: 15px; border-top: 2px solid #2E86C1; font-size: 0.8rem; color: #777; text-align: center; }}
</style>
</head>
<body>

<div class="header">
  <h1>📋 Informe Analítico de Productividad</h1>
  <p>{centro_sel} · Servicio de Salud Metropolitano Central · Período: {rango_meses}</p>
</div>

<h2>1. Resumen Ejecutivo</h2>
<p>El presente informe analiza la productividad del centro <strong>{centro_sel}</strong>
durante el período <strong>{rango_meses}</strong> ({n_meses} meses),
abarcando un total de <strong>{total_registros:,}</strong> registros de cupos programados en IRIS.
De estos, <strong>{citados:,}</strong> corresponden a cupos citados, <strong>{disponibles:,}</strong>
permanecieron disponibles, <strong>{bloqueados:,}</strong> fueron bloqueados administrativamente y
<strong>{completados:,}</strong> registraron cita completada.</p>

<div class="cards">
  <div class="card"><div class="val">{total_registros:,}</div><div class="lbl">Total Registros</div></div>
  <div class="card"><div class="val">{citados:,}</div><div class="lbl">Citados</div></div>
  <div class="card"><div class="val">{disponibles:,}</div><div class="lbl">Disponibles</div></div>
  <div class="card"><div class="val">{bloqueados:,}</div><div class="lbl">Bloqueados</div></div>
  <div class="card"><div class="val">{completados:,}</div><div class="lbl">Completados</div></div>
</div>

<h2>2. Semáforo de Indicadores</h2>
<p>Estado de los 10 indicadores clave del modelo de productividad APS.
🟢 Dentro de meta · 🟡 En observación · 🔴 Brecha crítica.</p>
<table>
<tr><th>Estado</th><th>Indicador</th><th>Valor</th><th>Meta</th><th>Alerta</th><th>Descripción</th></tr>
{kpi_rows_html}
</table>

<div class="page-break"></div>

<h2>3. Distribución de Estado de Cupos</h2>
<p class="chart-caption"><strong>Gráfico 1.</strong> Composición de cupos según estado final (Citado, Disponible, Bloqueado).
Muestra qué proporción de la oferta programada fue efectivamente utilizada versus la que quedó sin asignar
o fue retirada por bloqueo administrativo. Calculado como conteo de registros agrupados por <code>ESTADO CUPO</code>.</p>
<div class="chart-container">{charts_html.get("cupos", "<p>Sin datos</p>")}</div>

<h2>4. Análisis de Tasa de Ocupación</h2>
<p>La <strong>Tasa de Ocupación</strong> mide el porcentaje de cupos asignados a un paciente
respecto del total disponible para atención: <code>Citados ÷ (Citados + Disponibles) × 100</code>.
El centro registra una ocupación de <strong>{v_ocu:.1f}%</strong> ({_sem_text(v_ocu, "ocupacion")}).
Meta ≥ 65%, alerta < 50%.</p>
{"<p class='chart-caption'><strong>Gráfico 2.</strong> Evolución mensual de la Tasa de Ocupación. La línea punteada verde indica la meta (65%%) y la roja el umbral de alerta (50%%).</p><div class='chart-container'>" + charts_html["ocu_mensual"] + "</div>" if "ocu_mensual" in charts_html else ""}
{"<p class='chart-caption'><strong>Gráfico 3.</strong> Ocupación por instrumento (profesional). Permite comparar el aprovechamiento de la agenda entre profesionales. Calculado como <code>Citados ÷ (Citados + Disponibles) × 100</code> por instrumento.</p><div class='chart-container'>" + charts_html["ocu_inst"] + "</div>" if "ocu_inst" in charts_html else ""}

<div class="page-break"></div>

<h2>5. Análisis de Tasa de No-Show (Inasistencia)</h2>
<p>La <strong>Tasa de No-Show</strong> representa el porcentaje de pacientes citados que no asistieron:
<code>(Citados − Completados) ÷ Citados × 100</code>.
El centro presenta un No-Show de <strong>{v_ns:.1f}%</strong> ({_sem_text(v_ns, "no_show")}).
Meta ≤ 10%, alerta > 15%.</p>
{"<p class='chart-caption'><strong>Gráfico 4.</strong> Evolución mensual del No-Show vs umbral institucional (10%%). Barras coloreadas según gravedad.</p><div class='chart-container'>" + charts_html["noshow"] + "</div>" if "noshow" in charts_html else ""}

<h2>6. Análisis de Tasa de Bloqueo</h2>
<p>La <strong>Tasa de Bloqueo</strong> mide cupos bloqueados administrativamente (vacaciones, capacitaciones, fallas):
<code>Bloqueados ÷ Total × 100</code>.
El centro registra <strong>{v_bloq:.1f}%</strong> ({_sem_text(v_bloq, "bloqueo")}).
Meta ≤ 10%, alerta > 15%.</p>
{"<p class='chart-caption'><strong>Gráfico 5.</strong> Evolución mensual de la Tasa de Bloqueo. Detecta meses con mayor pérdida de capacidad instalada.</p><div class='chart-container'>" + charts_html["bloqueo"] + "</div>" if "bloqueo" in charts_html else ""}

<h2>7. Análisis de Efectividad de Cita</h2>
<p>La <strong>Efectividad de Cita</strong> mide citas completadas exitosamente:
<code>Completados ÷ Citados × 100</code>.
El centro alcanza <strong>{v_efec:.1f}%</strong> ({_sem_text(v_efec, "efectividad")}).
Meta ≥ 88%, alerta < 80%.</p>
{"<p class='chart-caption'><strong>Gráfico 6.</strong> Evolución mensual de la Efectividad. Proporción de citas que terminaron en atención efectiva.</p><div class='chart-container'>" + charts_html["efectividad"] + "</div>" if "efectividad" in charts_html else ""}

<div class="page-break"></div>

<h2>8. Rendimiento Promedio por Instrumento</h2>
<p>El <strong>Rendimiento Promedio</strong> indica los minutos promedio por atención:
<code>Promedio(RENDIMIENTO)</code>.
El centro presenta <strong>{v_rend:.1f} min/atención</strong>.</p>
<p class="chart-caption"><strong>Gráfico 7.</strong> Rendimiento por profesional. Permite identificar profesionales con rendimientos atípicos.</p>
<div class="chart-container">{charts_html.get("rendimiento", "<p>Sin datos</p>")}</div>

<h2>9. Análisis de Cupos Sobrecupo</h2>
<p>El <strong>Sobrecupo</strong> mide atenciones sobre la capacidad programada:
<code>Sobrecupos ÷ Total × 100</code>.
El centro registra <strong>{v_sobre:.1f}%</strong> ({_sem_text(v_sobre, "sobrecupo")}).
Meta ≤ 5%, alerta > 10%.</p>

<h2>10. Cobertura Sectorial</h2>
<p>La <strong>Cobertura Sectorial</strong> mide registros con sector territorial informado:
<code>Con sector ÷ Total × 100</code>.
Cobertura: <strong>{v_cob:.1f}%</strong> ({_sem_text(v_cob, "cobertura_sectorial")}).
Meta ≥ 80%, alerta < 60%.</p>
<p class="chart-caption"><strong>Gráfico 8.</strong> Distribución por sector territorial (Verde, Lila, Rojo, No Informado).</p>
<div class="chart-container">{charts_html.get("sector", "<p>Sin datos</p>")}</div>

<h2>11. Agendamiento Remoto</h2>
<p>Mide citas gestionadas por canales no presenciales:
<code>(Telefónico + Telesalud) ÷ Total × 100</code>.
Resultado: <strong>{v_ag:.1f}%</strong> ({_sem_text(v_ag, "agendamiento_remoto")}).
Meta ≥ 20%, alerta < 5%.</p>

<h2>12. Ocupación en Horario Extendido</h2>
<p>Uso de cupos a partir de las 18:00 hrs (jornada extendida con costo adicional):
<code>Citados ≥18h ÷ (Citados + Disponibles ≥18h) × 100</code>.
Resultado: <strong>{v_ext:.1f}%</strong> ({_sem_text(v_ext, "ocupacion_extendida")}).
Meta ≥ 50%, alerta < 30%.</p>

<div class="page-break"></div>

<h2>13. Distribución por Tipo de Atención</h2>
<p class="chart-caption"><strong>Gráfico 9.</strong> Volumen de cupos por tipo de atención (Morbilidad, Control, Urgencia, etc.).
Identifica la composición de la cartera de servicios del centro.</p>
<div class="chart-container">{charts_html.get("tipo_atencion", "<p>Sin datos</p>")}</div>

{"<p><strong>Tabla 1.</strong> KPIs por tipo de atención.</p><table><tr><th>Tipo Atención</th><th>Total</th><th>Citados</th><th>Disp.</th><th>Bloq.</th><th>Complet.</th><th>Ocupación</th><th>No-Show</th><th>Efectividad</th><th>Rend.(min)</th></tr>" + ta_rows_html + "</table>" if ta_rows_html else ""}

<h2>14. KPIs por Instrumento / Profesional</h2>
<p><strong>Tabla 2.</strong> Resumen de indicadores por profesional del centro.</p>
{"<table><tr><th>Instrumento</th><th>Total</th><th>Citados</th><th>Disp.</th><th>Bloq.</th><th>Complet.</th><th>Ocupación</th><th>No-Show</th><th>Efectividad</th><th>Rend.(min)</th></tr>" + inst_rows_html + "</table>" if inst_rows_html else "<p>Sin datos de instrumentos.</p>"}

{"<div class='page-break'></div><h2>15. Evolución Conjunta de KPIs Principales</h2><p class='chart-caption'><strong>Gráfico 10.</strong> Ocupación, No-Show y Bloqueo mes a mes. Visualiza la interacción: un aumento de bloqueo típicamente reduce la ocupación; un No-Show elevado reduce la efectividad.</p><div class='chart-container'>" + charts_html["multi_kpi"] + "</div>" if "multi_kpi" in charts_html else ""}

{"<h2>16. Mapa de Calor: Ocupación por Instrumento y Mes</h2><p class='chart-caption'><strong>Gráfico 11.</strong> Cruza cada instrumento (fila) con cada mes (columna), coloreando según la tasa de ocupación. Tonos verdes ≥ 65%%, amarillos zona intermedia, rojos ocupación crítica.</p><div class='chart-container'>" + charts_html["heatmap"] + "</div>" if "heatmap" in charts_html else ""}

<div class="page-break"></div>

<h2>17. Alertas y Brechas del Centro</h2>
{alertas_html}

<h2>18. Conclusión del Informe</h2>
{conclusion_html}

<div class="footer">
  <p>Informe generado automáticamente por el Sistema de Análisis de Productividad APS<br>
  Servicio de Salud Metropolitano Central · {centro_sel} · Período: {rango_meses}<br>
  Fecha de generación: {fecha_gen}</p>
</div>

</body>
</html>"""

    return html


# ══════════════════════════════════════════════════════════════════════════════
#  GENERACIÓN DE INFORME PDF
# ══════════════════════════════════════════════════════════════════════════════

def _generar_pdf_informe(
    centro_sel, rango_meses, n_meses, total_registros,
    citados, disponibles, bloqueados, completados,
    kpis, df_centro, df_inst_c, df_kpis_ta,
    alertas_centro, n_verde, n_amarillo, n_rojo,
) -> bytes:
    """Genera informe PDF profesional con portada estilo Canva y gráficos embebidos."""
    import io
    import plotly.graph_objects as go
    from fpdf import FPDF
    from src.kpis import semaforo
    from src.charts import (
        chart_estado_cupos, chart_evolucion_mensual, chart_noshow_vs_umbral,
        chart_rendimiento_instrumento, chart_sector, chart_tipo_atencion,
        chart_multi_kpi, chart_heatmap_instrumento_mes,
    )
    from datetime import datetime

    fecha_gen = datetime.now().strftime("%d/%m/%Y %H:%M")

    # ── Helper: Plotly fig → PNG bytes ────────────────────────────────────────
    _kaleido_ok = True  # flag para evitar reintentos costosos si falló

    def _fig_to_png(fig, w=900, h=450):
        nonlocal _kaleido_ok
        # Asegurar etiquetas de datos visibles en todas las trazas
        for trace in fig.data:
            ttype = trace.type
            if ttype in ("bar", "scatter", "waterfall"):
                if trace.text is not None and not getattr(trace, "textposition", None):
                    trace.textposition = "outside"
                if trace.text is not None:
                    trace.textfont = dict(size=11)
            elif ttype == "pie":
                trace.textinfo = "label+percent+value"
                trace.textfont = dict(size=11)

        fig.update_layout(
            width=w, height=h, template="plotly_white",
            paper_bgcolor="white", plot_bgcolor="white",
        )
        if not _kaleido_ok:
            return None

        # Intento 1: kaleido con escala 2 (alta calidad)
        try:
            return fig.to_image(format="png", scale=2, engine="kaleido")
        except Exception:
            pass

        # Intento 2: kaleido sin especificar escala
        try:
            return fig.to_image(format="png", engine="kaleido")
        except Exception:
            pass

        # Intento 3: sin especificar engine (plotly auto-detect)
        try:
            return fig.to_image(format="png", scale=2)
        except Exception as e:
            import logging
            logging.warning(f"PDF chart export failed: {e}")
            _kaleido_ok = False
            return None

    # ── Helper: semáforo ──────────────────────────────────────────────────────
    def _sem_text(val, kpi):
        s = semaforo(val, kpi)
        return {"verde": "Optimo", "amarillo": "Observacion", "rojo": "Critico"}.get(s, "-")

    def _sem_color(val, kpi):
        s = semaforo(val, kpi)
        return {"verde": (39, 174, 96), "amarillo": (243, 156, 18), "rojo": (231, 76, 60)}.get(s, (149, 165, 166))

    # ── Colores corporativos ──────────────────────────────────────────────────
    AZUL_OSCURO = (27, 79, 114)
    AZUL_MEDIO = (46, 134, 193)
    AZUL_CLARO = (174, 214, 241)
    BLANCO = (255, 255, 255)
    GRIS_TEXTO = (44, 62, 80)
    GRIS_CLARO = (248, 249, 250)
    VERDE = (39, 174, 96)
    AMARILLO = (243, 156, 18)
    ROJO = (231, 76, 60)

    # ── Clase PDF personalizada ───────────────────────────────────────────────
    class InformePDF(FPDF):
        def __init__(self):
            super().__init__(orientation="P", unit="mm", format="A4")
            self.set_auto_page_break(auto=True, margin=20)
            self._is_cover = False

        def header(self):
            if self._is_cover or self.page_no() == 1:
                return
            # Barra superior azul
            self.set_fill_color(*AZUL_OSCURO)
            self.rect(0, 0, 210, 12, "F")
            self.set_font("Helvetica", "B", 7)
            self.set_text_color(*BLANCO)
            self.set_xy(10, 3)
            self.cell(0, 5, f"Informe de Productividad APS  |  {centro_sel}  |  {rango_meses}", align="L")
            self.set_xy(0, 3)
            self.cell(200, 5, f"Pag. {self.page_no()}", align="R")
            # Línea decorativa
            self.set_draw_color(*AZUL_MEDIO)
            self.set_line_width(0.5)
            self.line(10, 13, 200, 13)
            self.set_y(18)

        def footer(self):
            if self._is_cover or self.page_no() == 1:
                return
            self.set_y(-15)
            self.set_draw_color(*AZUL_CLARO)
            self.set_line_width(0.3)
            self.line(10, self.get_y(), 200, self.get_y())
            self.set_font("Helvetica", "I", 7)
            self.set_text_color(150, 150, 150)
            self.cell(0, 8, f"Servicio de Salud Metropolitano Central  |  Generado: {fecha_gen}", align="C")

        def section_title(self, num, title):
            self.set_font("Helvetica", "B", 13)
            self.set_text_color(*AZUL_OSCURO)
            # Barra lateral decorativa
            y_start = self.get_y()
            self.set_fill_color(*AZUL_MEDIO)
            self.rect(10, y_start, 3, 8, "F")
            self.set_xy(16, y_start)
            self.cell(0, 8, f"{num}. {title}")
            self.ln(12)

        def body_text(self, txt):
            self.set_font("Helvetica", "", 9)
            self.set_text_color(*GRIS_TEXTO)
            self.multi_cell(0, 5, txt)
            self.ln(2)

        def add_chart(self, png_bytes, w=180, title_hint=""):
            if png_bytes is None:
                # Placeholder cuando kaleido no pudo renderizar
                y = self.get_y()
                self.set_fill_color(245, 245, 245)
                self.set_draw_color(200, 200, 200)
                self.rect(15, y, 180, 25, "FD")
                self.set_font("Helvetica", "I", 9)
                self.set_text_color(130, 130, 130)
                self.set_xy(15, y + 5)
                msg = f"[Grafico no disponible"
                if title_hint:
                    msg += f": {title_hint}"
                msg += " — instale kaleido: pip install kaleido]"
                self.cell(180, 8, msg, align="C")
                self.set_y(y + 28)
                return
            img_stream = io.BytesIO(png_bytes)
            x = (210 - w) / 2
            self.image(img_stream, x=x, w=w)
            self.ln(5)

        def kpi_card_row(self, cards):
            """Dibuja tarjetas KPI en fila (max 5)."""
            n = len(cards)
            card_w = 36
            gap = 2
            total_w = n * card_w + (n - 1) * gap
            x_start = (210 - total_w) / 2
            y_start = self.get_y()
            for i, (val, label) in enumerate(cards):
                x = x_start + i * (card_w + gap)
                # Sombra
                self.set_fill_color(220, 220, 220)
                self.rect(x + 0.5, y_start + 0.5, card_w, 22, style="F")
                # Tarjeta
                self.set_fill_color(*BLANCO)
                self.set_draw_color(*AZUL_CLARO)
                self.rect(x, y_start, card_w, 22, style="FD")
                # Barra superior
                self.set_fill_color(*AZUL_MEDIO)
                self.rect(x, y_start, card_w, 3, "F")
                # Valor
                self.set_font("Helvetica", "B", 12)
                self.set_text_color(*AZUL_OSCURO)
                self.set_xy(x, y_start + 4)
                self.cell(card_w, 7, str(val), align="C")
                # Label
                self.set_font("Helvetica", "", 6)
                self.set_text_color(100, 100, 100)
                self.set_xy(x, y_start + 12)
                self.cell(card_w, 5, label, align="C")
            self.set_y(y_start + 28)

    # ── Crear PDF ─────────────────────────────────────────────────────────────
    pdf = InformePDF()
    pdf.set_left_margin(10)
    pdf.set_right_margin(10)

    # ══════════════════════════════════════════════════════════════════════════
    # PORTADA (estilo Canva)
    # ══════════════════════════════════════════════════════════════════════════
    pdf._is_cover = True
    pdf.add_page()

    # Fondo degradado simulado (bandas verticales azul oscuro → azul medio)
    for i in range(297):
        ratio = i / 297
        r = int(AZUL_OSCURO[0] + (AZUL_MEDIO[0] - AZUL_OSCURO[0]) * ratio)
        g = int(AZUL_OSCURO[1] + (AZUL_MEDIO[1] - AZUL_OSCURO[1]) * ratio)
        b = int(AZUL_OSCURO[2] + (AZUL_MEDIO[2] - AZUL_OSCURO[2]) * ratio)
        pdf.set_fill_color(r, g, b)
        pdf.rect(0, i, 210, 1.1, "F")

    # Elementos decorativos
    # Líneas diagonales sutiles
    pdf.set_draw_color(255, 255, 255)
    pdf.set_line_width(0.15)
    for offset in range(-300, 300, 40):
        pdf.line(offset, 0, offset + 210, 297)

    # Rectángulo central semi-transparente (marco blanco sutil)
    pdf.set_fill_color(255, 255, 255)
    pdf.set_draw_color(255, 255, 255)
    pdf.set_line_width(0.8)
    # Marco exterior decorativo
    pdf.rect(15, 20, 180, 257, "D")
    pdf.rect(17, 22, 176, 253, "D")

    # Barra decorativa superior
    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(30, 40, 150, 1.5, "F")

    # Ícono/badge superior
    pdf.set_fill_color(255, 255, 255)
    pdf.rect(80, 30, 50, 18, style="F")
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.set_xy(80, 33)
    pdf.cell(50, 5, "INFORME ANALITICO", align="C")
    pdf.set_font("Helvetica", "", 7)
    pdf.set_xy(80, 39)
    pdf.cell(50, 5, "PRODUCTIVIDAD APS", align="C")

    # Título principal
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(*BLANCO)
    pdf.set_xy(20, 65)
    pdf.multi_cell(170, 14, "Informe Analitico\nde Productividad", align="C")

    # Línea decorativa
    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(60, 100, 90, 1, "F")

    # Nombre del centro
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*BLANCO)
    pdf.set_xy(20, 110)
    pdf.multi_cell(170, 10, centro_sel, align="C")

    # Período
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(*AZUL_CLARO)
    pdf.set_xy(20, 140)
    pdf.cell(170, 8, f"Periodo: {rango_meses}", align="C")
    pdf.set_xy(20, 150)
    pdf.cell(170, 8, f"{n_meses} meses evaluados", align="C")

    # Línea decorativa
    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(75, 165, 60, 0.5, "F")

    # Tarjetas resumen en portada
    card_data_cover = [
        (f"{total_registros:,}", "Registros"),
        (f"{citados:,}", "Citados"),
        (f"{disponibles:,}", "Disponibles"),
        (f"{bloqueados:,}", "Bloqueados"),
        (f"{completados:,}", "Completados"),
    ]
    card_w = 30
    gap = 4
    total_cards_w = 5 * card_w + 4 * gap
    x_start = (210 - total_cards_w) / 2
    y_cards = 175
    for i, (val, lbl) in enumerate(card_data_cover):
        x = x_start + i * (card_w + gap)
        # Fondo tarjeta con opacidad simulada
        pdf.set_fill_color(255, 255, 255)
        pdf.rect(x, y_cards, card_w, 25, style="F")
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.set_xy(x, y_cards + 3)
        pdf.cell(card_w, 7, val, align="C")
        pdf.set_font("Helvetica", "", 6)
        pdf.set_text_color(100, 100, 100)
        pdf.set_xy(x, y_cards + 13)
        pdf.cell(card_w, 5, lbl, align="C")

    # Semáforo resumen
    y_sem = 210
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*BLANCO)
    pdf.set_xy(20, y_sem)
    pdf.cell(170, 7, "Estado General de Indicadores", align="C")
    pdf.ln(10)

    sem_items = [
        (n_verde, "Optimos", VERDE),
        (n_amarillo, "En Observacion", AMARILLO),
        (n_rojo, "Criticos", ROJO),
    ]
    box_w = 45
    gap_s = 8
    total_s = 3 * box_w + 2 * gap_s
    x_s = (210 - total_s) / 2
    for i, (count, label, color) in enumerate(sem_items):
        x = x_s + i * (box_w + gap_s)
        pdf.set_fill_color(*color)
        pdf.rect(x, y_sem + 12, box_w, 20, style="F")
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(*BLANCO)
        pdf.set_xy(x, y_sem + 13)
        pdf.cell(box_w, 10, str(count), align="C")
        pdf.set_font("Helvetica", "", 8)
        pdf.set_xy(x, y_sem + 23)
        pdf.cell(box_w, 6, label, align="C")

    # Pie de portada
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*AZUL_CLARO)
    pdf.set_xy(20, 255)
    pdf.cell(170, 5, "Servicio de Salud Metropolitano Central", align="C")
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_xy(20, 262)
    pdf.cell(170, 5, f"Generado: {fecha_gen}", align="C")

    # Barra decorativa inferior
    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(30, 255 - 5, 150, 0.5, "F")

    pdf._is_cover = False

    # ══════════════════════════════════════════════════════════════════════════
    # GENERAR GRÁFICOS PNG
    # ══════════════════════════════════════════════════════════════════════════
    charts_png = {}

    fig1 = chart_estado_cupos(df_centro)
    fig1.update_layout(height=400)
    charts_png["cupos"] = _fig_to_png(fig1, 850, 400)

    df_meses_c = _kpis_por_mes_centro(df_centro)
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig2 = chart_evolucion_mensual(df_meses_c, "ocupacion", "Tasa de Ocupacion",
                                        umbral_ok=65, umbral_alerta=50)
        charts_png["ocu_mensual"] = _fig_to_png(fig2, 850, 400)

    if not df_inst_c.empty:
        df_plot = df_inst_c.sort_values("ocupacion")
        colors_ocu = ["#27AE60" if v >= 65 else "#F39C12" if v >= 50 else "#E74C3C"
                      for v in df_plot["ocupacion"]]
        fig3 = go.Figure(go.Bar(
            x=df_plot["ocupacion"], y=df_plot["instrumento"].str[:30],
            orientation="h", marker_color=colors_ocu,
            text=[f"{v:.1f}%" for v in df_plot["ocupacion"]], textposition="outside",
        ))
        fig3.add_vline(x=65, line_dash="dash", line_color="#27AE60", annotation_text="Meta 65%")
        fig3.update_layout(title="Ocupacion por Instrumento", template="plotly_white",
                           xaxis=dict(title="Ocupacion (%)", range=[0, 105]), yaxis=dict(title=""))
        h3 = max(400, len(df_plot) * 40 + 100)
        charts_png["ocu_inst"] = _fig_to_png(fig3, 850, h3)

    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig4 = chart_noshow_vs_umbral(df_meses_c)
        charts_png["noshow"] = _fig_to_png(fig4, 850, 400)

        fig5 = chart_evolucion_mensual(df_meses_c, "bloqueo", "Tasa de Bloqueo",
                                        umbral_ok=10, umbral_alerta=15)
        charts_png["bloqueo"] = _fig_to_png(fig5, 850, 400)

        fig6 = chart_evolucion_mensual(df_meses_c, "efectividad", "Efectividad de Cita",
                                        umbral_ok=88, umbral_alerta=80)
        charts_png["efectividad"] = _fig_to_png(fig6, 850, 400)

    fig7 = chart_rendimiento_instrumento(df_centro)
    h7 = max(400, len(df_inst_c) * 40 + 100) if not df_inst_c.empty else 400
    charts_png["rendimiento"] = _fig_to_png(fig7, 850, h7)

    fig8 = chart_sector(df_centro)
    charts_png["sector"] = _fig_to_png(fig8, 850, 400)

    fig9 = chart_tipo_atencion(df_centro, top_n=15)
    # Agregar etiquetas de datos para PDF
    for trace in fig9.data:
        if trace.type == "bar" and trace.text is None:
            trace.text = [f"{v:,.0f}" for v in (trace.x if trace.orientation == "h" else trace.y)]
            trace.textposition = "outside"
    charts_png["tipo_atencion"] = _fig_to_png(fig9, 850, 480)

    if not df_meses_c.empty and len(df_meses_c) >= 2:
        fig10 = chart_multi_kpi(df_meses_c)
        # Agregar etiquetas de datos a cada serie para PDF
        for trace in fig10.data:
            if trace.type == "scatter" and trace.y is not None:
                trace.mode = "lines+markers+text"
                trace.text = [f"{v:.1f}%" for v in trace.y]
                trace.textposition = "top center"
                trace.textfont = dict(size=9)
        charts_png["multi_kpi"] = _fig_to_png(fig10, 850, 430)

    if "MES_NUM" in df_centro.columns and "INSTRUMENTO" in df_centro.columns:
        fig11 = chart_heatmap_instrumento_mes(df_centro)
        n_i = df_centro["INSTRUMENTO"].nunique()
        charts_png["heatmap"] = _fig_to_png(fig11, 850, max(450, n_i * 38 + 120))

    # ══════════════════════════════════════════════════════════════════════════
    # HELPER: tabla genérica
    # ══════════════════════════════════════════════════════════════════════════
    def _draw_table_header(headers, col_widths):
        """Dibuja encabezado azul de tabla (reutilizable en saltos de página)."""
        pdf.set_fill_color(*AZUL_OSCURO)
        pdf.set_text_color(*BLANCO)
        pdf.set_font("Helvetica", "B", 7)
        for i, h in enumerate(headers):
            pdf.cell(col_widths[i], 7, h, border=1, align="C", fill=True)
        pdf.ln()

    def _draw_table(headers, rows, col_widths=None, align_cols=None):
        """Dibuja una tabla profesional con encabezado azul que se repite en cada página."""
        n_cols = len(headers)
        if col_widths is None:
            col_widths = [190 / n_cols] * n_cols
        if align_cols is None:
            align_cols = ["C"] * n_cols

        row_h = 7
        # Margen inferior de la página (20mm de auto_page_break + algo de holgura)
        bottom_limit = 297 - 20  # A4 height=297mm, margin=20mm

        # Encabezado inicial
        _draw_table_header(headers, col_widths)

        # Filas
        pdf.set_font("Helvetica", "", 7)
        for row_idx, row in enumerate(rows):
            # Verificar si queda espacio; si no, salto de página + re-dibujar encabezado
            if pdf.get_y() + row_h > bottom_limit:
                pdf.add_page()
                _draw_table_header(headers, col_widths)
                pdf.set_font("Helvetica", "", 7)

            bg = GRIS_CLARO if row_idx % 2 == 0 else BLANCO
            pdf.set_fill_color(*bg)
            pdf.set_text_color(*GRIS_TEXTO)
            for i, val in enumerate(row):
                pdf.cell(col_widths[i], row_h, str(val), border=1, align=align_cols[i], fill=True)
            pdf.ln()

    # ══════════════════════════════════════════════════════════════════════════
    # CONTENIDO DEL INFORME
    # ══════════════════════════════════════════════════════════════════════════

    # ── Sección 1: Resumen Ejecutivo ──────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(1, "Resumen Ejecutivo")
    pdf.body_text(
        f"El presente informe analiza la productividad del centro {centro_sel} "
        f"durante el periodo {rango_meses} ({n_meses} meses), abarcando un total de "
        f"{total_registros:,} registros de cupos programados en IRIS. De estos, "
        f"{citados:,} corresponden a cupos citados, {disponibles:,} permanecieron disponibles, "
        f"{bloqueados:,} fueron bloqueados administrativamente y {completados:,} registraron cita completada."
    )

    pdf.kpi_card_row([
        (f"{total_registros:,}", "Total Registros"),
        (f"{citados:,}", "Citados"),
        (f"{disponibles:,}", "Disponibles"),
        (f"{bloqueados:,}", "Bloqueados"),
        (f"{completados:,}", "Completados"),
    ])

    # ── Sección 2: Semáforo de Indicadores ────────────────────────────────────
    pdf.section_title(2, "Semaforo de Indicadores")
    pdf.body_text(
        "Estado de los 10 indicadores clave del modelo de productividad APS. "
        "Verde: dentro de meta. Amarillo: en observacion. Rojo: brecha critica."
    )

    kpi_order = [
        ("ocupacion", "Tasa de Ocupacion"), ("no_show", "Tasa de No-Show"),
        ("bloqueo", "Tasa de Bloqueo"), ("efectividad", "Efectividad de Cita"),
        ("rendimiento", "Rendimiento Promedio"), ("sobrecupo", "Cupos Sobrecupo"),
        ("cobertura_sectorial", "Cobertura Sectorial"), ("agendamiento_remoto", "Agendamiento Remoto"),
        ("variacion_mensual", "Variacion Mensual"), ("ocupacion_extendida", "Ocupacion Horario Extendido"),
    ]
    sem_headers = ["Estado", "Indicador", "Valor", "Meta", "Alerta", "Diagnostico"]
    sem_rows = []
    for key, label in kpi_order:
        k = kpis.get(key, {})
        valor = k.get("valor", 0)
        unidad = k.get("unidad", "%")
        sem = k.get("semaforo", "gris")
        icon = {"verde": "OK", "amarillo": "OBS", "rojo": "CRIT"}.get(sem, "-")
        meta = k.get("umbral_ok", "-")
        alerta = k.get("umbral_alerta", "-")
        sem_rows.append([icon, label, f"{valor:.1f}{unidad}", str(meta), str(alerta), _sem_text(valor, key)])

    _draw_table(sem_headers, sem_rows,
                col_widths=[14, 42, 22, 18, 18, 76],
                align_cols=["C", "L", "C", "C", "C", "L"])

    # ── Sección 3: Estado de Cupos ────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(3, "Distribucion de Estado de Cupos")
    pdf.body_text(
        "Composicion de cupos segun estado final (Citado, Disponible, Bloqueado). "
        "Muestra que proporcion de la oferta programada fue efectivamente utilizada."
    )
    pdf.add_chart(charts_png.get("cupos"), title_hint="Estado de Cupos")

    # ── Sección 4: Tasa de Ocupación ──────────────────────────────────────────
    v_ocu = kpis.get("ocupacion", {}).get("valor", 0)
    pdf.section_title(4, "Analisis de Tasa de Ocupacion")
    pdf.body_text(
        f"La Tasa de Ocupacion mide el porcentaje de cupos asignados respecto del total "
        f"disponible: Citados / (Citados + Disponibles) x 100. El centro registra una "
        f"ocupacion de {v_ocu:.1f}% ({_sem_text(v_ocu, 'ocupacion')}). Meta >= 65%, alerta < 50%."
    )
    pdf.add_chart(charts_png.get("ocu_mensual"), title_hint="Ocupacion Mensual")

    if pdf.get_y() > 160:
        pdf.add_page()
    pdf.add_chart(charts_png.get("ocu_inst"), title_hint="Ocupacion por Instrumento")

    # ── Sección 5: No-Show ────────────────────────────────────────────────────
    v_ns = kpis.get("no_show", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(5, "Analisis de Tasa de No-Show")
    pdf.body_text(
        f"La Tasa de No-Show representa el porcentaje de pacientes citados que no asistieron: "
        f"(Citados - Completados) / Citados x 100. El centro presenta un No-Show de "
        f"{v_ns:.1f}% ({_sem_text(v_ns, 'no_show')}). Meta <= 10%, alerta > 15%."
    )
    pdf.add_chart(charts_png.get("noshow"), title_hint="No-Show Mensual")

    # ── Sección 6: Bloqueo ────────────────────────────────────────────────────
    v_bloq = kpis.get("bloqueo", {}).get("valor", 0)
    pdf.section_title(6, "Analisis de Tasa de Bloqueo")
    pdf.body_text(
        f"La Tasa de Bloqueo mide cupos bloqueados administrativamente: "
        f"Bloqueados / Total x 100. El centro registra {v_bloq:.1f}% "
        f"({_sem_text(v_bloq, 'bloqueo')}). Meta <= 10%, alerta > 15%."
    )
    pdf.add_chart(charts_png.get("bloqueo"), title_hint="Tasa de Bloqueo")

    # ── Sección 7: Efectividad ────────────────────────────────────────────────
    v_efec = kpis.get("efectividad", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(7, "Analisis de Efectividad de Cita")
    pdf.body_text(
        f"La Efectividad de Cita mide citas completadas exitosamente: "
        f"Completados / Citados x 100. El centro alcanza {v_efec:.1f}% "
        f"({_sem_text(v_efec, 'efectividad')}). Meta >= 88%, alerta < 80%."
    )
    pdf.add_chart(charts_png.get("efectividad"), title_hint="Efectividad de Cita")

    # ── Sección 8: Rendimiento ────────────────────────────────────────────────
    v_rend = kpis.get("rendimiento", {}).get("valor", 0)
    pdf.section_title(8, "Rendimiento Promedio por Instrumento")
    pdf.body_text(
        f"El Rendimiento Promedio indica los minutos promedio por atencion: "
        f"Promedio(RENDIMIENTO). El centro presenta {v_rend:.1f} min/atencion."
    )
    if pdf.get_y() > 140:
        pdf.add_page()
    pdf.add_chart(charts_png.get("rendimiento"), title_hint="Rendimiento por Instrumento")

    # ── Sección 9: Sobrecupo ─────────────────────────────────────────────────
    v_sobre = kpis.get("sobrecupo", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(9, "Analisis de Cupos Sobrecupo")
    pdf.body_text(
        f"El Sobrecupo mide atenciones sobre la capacidad programada: "
        f"Sobrecupos / Total x 100. El centro registra {v_sobre:.1f}% "
        f"({_sem_text(v_sobre, 'sobrecupo')}). Meta <= 5%, alerta > 10%."
    )

    # ── Sección 10: Cobertura Sectorial ───────────────────────────────────────
    v_cob = kpis.get("cobertura_sectorial", {}).get("valor", 0)
    pdf.section_title(10, "Cobertura Sectorial")
    pdf.body_text(
        f"La Cobertura Sectorial mide registros con sector territorial informado: "
        f"Con sector / Total x 100. Cobertura: {v_cob:.1f}% "
        f"({_sem_text(v_cob, 'cobertura_sectorial')}). Meta >= 80%, alerta < 60%."
    )
    pdf.add_chart(charts_png.get("sector"), title_hint="Distribucion Sectorial")

    # ── Sección 11: Agendamiento Remoto ───────────────────────────────────────
    v_ag = kpis.get("agendamiento_remoto", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(11, "Agendamiento Remoto")
    pdf.body_text(
        f"Mide citas gestionadas por canales no presenciales: "
        f"(Telefonico + Telesalud) / Total x 100. Resultado: {v_ag:.1f}% "
        f"({_sem_text(v_ag, 'agendamiento_remoto')}). Meta >= 20%, alerta < 5%."
    )

    # ── Sección 12: Horario Extendido + Apertura Sabatina ────────────────────
    v_ext = kpis.get("ocupacion_extendida", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(12, "Horario Extendido y Apertura Sabatina")
    pdf.body_text(
        f"Uso de cupos a partir de las 18:00 hrs (jornada extendida): "
        f"Citados >=18h / (Citados + Disponibles >=18h) x 100. Resultado: {v_ext:.1f}% "
        f"({_sem_text(v_ext, 'ocupacion_extendida')}). Meta >= 50%, alerta < 30%."
    )

    # Tabla segmentada Normal / Extendido / Sábado
    from src.kpis import (
        kpis_horario_segmentado as _kpis_hseg,
        kpis_profesional_sabatino as _kpis_prof_sab,
        kpis_profesional_extendido as _kpis_prof_ext,
    )
    _df_seg = _kpis_hseg(df_centro)
    if not _df_seg.empty:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, "Comparativa por Segmento Horario")
        pdf.ln(8)
        seg_h = ["Segmento", "Total", "Citados", "Disp.", "Bloq.", "Complet.", "Ocup.%", "NoShow%", "Efect.%"]
        seg_r = []
        for _, r in _df_seg.iterrows():
            seg_r.append([
                str(r["segmento"])[:28], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                f'{r["disponibles"]:,.0f}', f'{r["bloqueados"]:,.0f}', f'{r["completados"]:,.0f}',
                f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}',
            ])
        _draw_table(seg_h, seg_r, col_widths=[50, 18, 18, 14, 14, 16, 16, 16, 16])

    # Tabla profesionales Apertura Sabatina
    _df_ps = _kpis_prof_sab(df_centro)
    if not _df_ps.empty:
        if pdf.get_y() > 180:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, f"Profesionales en Apertura Sabatina ({len(_df_ps)})")
        pdf.ln(8)
        ps_h = ["Profesional", "Total", "Citados", "Complet.", "Ocup.%", "NoShow%", "Efect.%"]
        ps_r = []
        for _, r in _df_ps.iterrows():
            ps_r.append([
                str(r["profesional"])[:30], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                f'{r["completados"]:,.0f}', f'{r["ocupacion"]:.1f}',
                f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}',
            ])
        _draw_table(ps_h, ps_r, col_widths=[55, 16, 16, 16, 16, 16, 16])

    # Tabla profesionales Horario Extendido
    _df_pe = _kpis_prof_ext(df_centro)
    if not _df_pe.empty:
        if pdf.get_y() > 180:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, f"Profesionales en Horario Extendido Lun-Vie ({len(_df_pe)})")
        pdf.ln(8)
        pe_h = ["Profesional", "Total", "Citados", "Complet.", "Ocup.%", "NoShow%", "Efect.%"]
        pe_r = []
        for _, r in _df_pe.iterrows():
            pe_r.append([
                str(r["profesional"])[:30], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                f'{r["completados"]:,.0f}', f'{r["ocupacion"]:.1f}',
                f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}',
            ])
        _draw_table(pe_h, pe_r, col_widths=[55, 16, 16, 16, 16, 16, 16])

    # ── Sección 13: Tipo de Atención ──────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(13, "Distribucion por Tipo de Atencion")
    pdf.body_text(
        "Volumen de cupos por tipo de atencion (Morbilidad, Control, Urgencia, etc.). "
        "Identifica la composicion de la cartera de servicios del centro."
    )
    pdf.add_chart(charts_png.get("tipo_atencion"), title_hint="Tipo de Atencion")

    # Tabla tipo atención
    if not df_kpis_ta.empty:
        if pdf.get_y() > 180:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, "Tabla: KPIs por Tipo de Atencion")
        pdf.ln(8)
        ta_headers = ["Tipo Atencion", "Total", "Citados", "Disp.", "Bloq.", "Complet.", "Ocup.%", "NoShow%", "Efect.%", "Rend."]
        ta_rows = []
        for _, r in df_kpis_ta.iterrows():
            ta_rows.append([
                str(r["tipo_atencion"])[:25], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                f'{r["disponibles"]:,.0f}', f'{r["bloqueados"]:,.0f}', f'{r["completados"]:,.0f}',
                f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}',
                f'{r["rendimiento"]:.1f}',
            ])
        _draw_table(ta_headers, ta_rows,
                    col_widths=[38, 18, 18, 16, 16, 18, 16, 18, 16, 16],
                    align_cols=["L", "R", "R", "R", "R", "R", "C", "C", "C", "C"])

    # ── Sección 14: KPIs por Instrumento ──────────────────────────────────────
    pdf.add_page()
    pdf.section_title(14, "KPIs por Instrumento / Profesional")
    pdf.body_text("Resumen de indicadores por profesional del centro.")

    if not df_inst_c.empty:
        inst_headers = ["Instrumento", "Total", "Citados", "Disp.", "Bloq.", "Complet.", "Ocup.%", "NoShow%", "Efect.%", "Rend."]
        inst_rows = []
        for _, r in df_inst_c.iterrows():
            inst_rows.append([
                str(r["instrumento"])[:25], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                f'{r["disponibles"]:,.0f}', f'{r["bloqueados"]:,.0f}', f'{r["completados"]:,.0f}',
                f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}',
                f'{r["rendimiento"]:.1f}',
            ])
        _draw_table(inst_headers, inst_rows,
                    col_widths=[38, 18, 18, 16, 16, 18, 16, 18, 16, 16],
                    align_cols=["L", "R", "R", "R", "R", "R", "C", "C", "C", "C"])

    # ── Sección 15: Multi-KPI ─────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(15, "Evolucion Conjunta de KPIs Principales")
    pdf.body_text(
        "Ocupacion, No-Show y Bloqueo mes a mes. Visualiza la interaccion: un aumento "
        "de bloqueo tipicamente reduce la ocupacion; un No-Show elevado reduce la efectividad."
    )
    pdf.add_chart(charts_png.get("multi_kpi"), title_hint="Multi-KPI Mensual")

    # ── Sección 16: Heatmap ───────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(16, "Mapa de Calor: Ocupacion por Instrumento y Mes")
    pdf.body_text(
        "Cruza cada instrumento (fila) con cada mes (columna), coloreando segun "
        "la tasa de ocupacion. Tonos verdes >= 65%, amarillos zona intermedia, rojos criticos."
    )
    pdf.add_chart(charts_png.get("heatmap"), title_hint="Heatmap Instrumento-Mes")

    # ── Sección 17: Alertas ───────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(17, "Alertas y Brechas del Centro")

    if not alertas_centro:
        pdf.set_fill_color(213, 245, 227)
        pdf.set_draw_color(*VERDE)
        pdf.set_line_width(0.8)
        y_a = pdf.get_y()
        pdf.rect(10, y_a, 190, 12, "FD")
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*VERDE)
        pdf.set_xy(14, y_a + 2)
        pdf.cell(0, 8, "Sin brechas detectadas. Todos los indicadores dentro de umbrales aceptables.")
        pdf.ln(16)
    else:
        for a in alertas_centro:
            sem_a = a.get("semaforo", "gris")
            bg_c = (253, 237, 236) if sem_a == "rojo" else (254, 249, 231)
            brd_c = ROJO if sem_a == "rojo" else AMARILLO
            pdf.set_fill_color(*bg_c)
            pdf.set_draw_color(*brd_c)
            pdf.set_line_width(0.8)
            y_a = pdf.get_y()
            if y_a > 265:
                pdf.add_page()
                y_a = pdf.get_y()
            pdf.rect(10, y_a, 190, 12, "FD")
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_text_color(*brd_c)
            pdf.set_xy(14, y_a + 1)
            tipo = a.get("tipo", "")
            valor_a = a.get("valor", 0)
            unidad_a = a.get("unidad", "")
            desc_a = a.get("descripcion", "")
            pdf.cell(0, 5, f"{tipo}: {valor_a:.1f} {unidad_a}")
            pdf.set_font("Helvetica", "", 7)
            pdf.set_text_color(*GRIS_TEXTO)
            pdf.set_xy(14, y_a + 6)
            pdf.cell(0, 5, desc_a[:120])
            pdf.set_y(y_a + 14)

    # ── Sección 18: Conclusión ────────────────────────────────────────────────
    pdf.section_title(18, "Conclusion del Informe")
    pdf.body_text(
        f"El centro {centro_sel} presenta {n_verde} indicadores en estado optimo, "
        f"{n_amarillo} en zona de observacion y {n_rojo} en brecha critica "
        f"durante el periodo analizado ({rango_meses})."
    )
    if n_rojo > 0:
        kpis_rojos = [k.get("nombre", key) for key, k in kpis.items()
                      if isinstance(k, dict) and k.get("semaforo") == "rojo"]
        pdf.body_text(f"Indicadores criticos: {', '.join(kpis_rojos)}. Se recomienda intervencion inmediata.")
    if n_amarillo > 0:
        kpis_amarillos = [k.get("nombre", key) for key, k in kpis.items()
                          if isinstance(k, dict) and k.get("semaforo") == "amarillo"]
        pdf.body_text(f"Indicadores en observacion: {', '.join(kpis_amarillos)}. Se sugiere monitoreo continuo.")
    if n_rojo == 0 and n_amarillo == 0:
        pdf.body_text("Todos los indicadores se encuentran dentro de los umbrales. Se recomienda mantener las estrategias actuales.")

    # ── Pie final ─────────────────────────────────────────────────────────────
    pdf.ln(10)
    pdf.set_draw_color(*AZUL_MEDIO)
    pdf.set_line_width(0.5)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(3)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 5, "Informe generado automaticamente por el Sistema de Analisis de Productividad APS", align="C")
    pdf.ln(5)
    pdf.cell(0, 5, f"Servicio de Salud Metropolitano Central  |  {centro_sel}  |  {rango_meses}", align="C")
    pdf.ln(5)
    pdf.cell(0, 5, f"Fecha de generacion: {fecha_gen}", align="C")

    # ── Exportar ──────────────────────────────────────────────────────────────
    return bytes(pdf.output())


def _kpis_por_mes_centro(df_centro: pd.DataFrame) -> pd.DataFrame:
    """Calcula KPIs por mes para un centro específico (sin caché, dato ya filtrado)."""
    from src.kpis import (
        calc_ocupacion, calc_no_show, calc_bloqueo, calc_efectividad,
        calc_rendimiento, calc_agendamiento_remoto, calc_sobrecupo,
        calc_cobertura_sectorial,
    )
    if "MES_NUM" not in df_centro.columns or df_centro.empty:
        return pd.DataFrame()

    MESES_ES = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
                7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}
    rows = []
    for mes, grp in df_centro.groupby("MES_NUM", observed=True):
        rows.append({
            "mes": mes,
            "mes_nombre": MESES_ES.get(int(mes), str(mes)),
            "ocupacion": calc_ocupacion(grp),
            "no_show": calc_no_show(grp),
            "bloqueo": calc_bloqueo(grp),
            "efectividad": calc_efectividad(grp),
            "rendimiento": calc_rendimiento(grp),
            "agendamiento_remoto": calc_agendamiento_remoto(grp),
            "sobrecupo": calc_sobrecupo(grp),
            "cobertura_sectorial": calc_cobertura_sectorial(grp),
            "total_registros": len(grp),
            "citados": (grp["ESTADO CUPO"] == "CITADO").sum(),
            "disponibles": (grp["ESTADO CUPO"] == "DISPONIBLE").sum(),
            "bloqueados": (grp["ESTADO CUPO"] == "BLOQUEADO").sum(),
        })
    return pd.DataFrame(rows).sort_values("mes")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    nav, filtros = render_sidebar()

    if has_df():
        dff = apply_filters(st.session_state.df, filtros)
    else:
        dff = pd.DataFrame()

    # Mensaje guía cuando BQ tiene datos pero aún no se ha cargado el subconjunto
    _bq_sin_cargar = (
        bq.bq_configured()
        and st.session_state.get("bq_total_registros", 0) > 0
        and not has_df()
        and not st.session_state.demo_loaded
    )

    if nav == "🏠 Inicio y Carga":
        page_inicio()
    elif nav == "📊 Dashboard KPIs":
        if _bq_sin_cargar:
            st.info("🗄️ Usa el botón **📥 Cargar datos filtrados** en el panel lateral para analizar los datos de BigQuery.", icon="ℹ️")
        elif not has_df():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_dashboard(dff)
    elif nav == "📈 Evolución Temporal":
        if _bq_sin_cargar:
            st.info("🗄️ Usa el botón **📥 Cargar datos filtrados** en el panel lateral para analizar los datos de BigQuery.", icon="ℹ️")
        elif not has_df():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_evolucion(dff)
    elif nav == "🔍 Análisis Detallado":
        if _bq_sin_cargar:
            st.info("🗄️ Usa el botón **📥 Cargar datos filtrados** en el panel lateral para analizar los datos de BigQuery.", icon="ℹ️")
        elif not has_df():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_analisis(dff)
    elif nav == "⚠️ Alertas y Brechas":
        if _bq_sin_cargar:
            st.info("🗄️ Usa el botón **📥 Cargar datos filtrados** en el panel lateral para analizar los datos de BigQuery.", icon="ℹ️")
        elif not has_df():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_alertas(dff)
    elif nav == "📋 Informe por Centro":
        if _bq_sin_cargar:
            st.info("🗄️ Usa el botón **📥 Cargar datos filtrados** en el panel lateral para analizar los datos de BigQuery.", icon="ℹ️")
        elif not has_df():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_informe_centro(dff)

    # Footer + estado almacenamiento
    st.sidebar.markdown("---")
    bq_st = bq.bq_status()
    if bq_st["configurado"]:
        n_bq = st.session_state.get("bq_total_registros", 0)
        st.sidebar.caption(f"🗄️ BigQuery: `{bq_st['dataset']}.{bq_st['table']}`")
        if n_bq:
            st.sidebar.caption(f"📊 {n_bq:,} registros almacenados")
    else:
        status = storage_status()
        if status["github_configurado"]:
            st.sidebar.caption(f"💾 GitHub: `{status['repo']}`")
        else:
            st.sidebar.caption("⚠️ Sin BigQuery ni GitHub — solo sesión activa")
    st.sidebar.caption(
        "Sistema de Análisis de Productividad APS · v1.2  \n"
        "SSMC · Modelo de Análisis de Productividad · 2026"
    )


if __name__ == "__main__":
    main()
