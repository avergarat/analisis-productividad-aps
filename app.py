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
             "🔍 Análisis Detallado", "⚠️ Alertas y Brechas"],
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
                             width="stretch", key="btn_bq_load"):
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
                    if st.button("🔄 Recargar desde BigQuery", width="stretch",
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
                if st.button("⚙️ Procesar archivos", type="primary", width="stretch"):
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
                        if bq.bq_configured():
                            with st.spinner("Guardando en BigQuery..."):
                                ok_bq, msg_bq = bq.insert_data(df_nuevos)
                            if ok_bq:
                                # Actualizar metadatos BQ en sesión
                                st.session_state.bq_filter_options = bq.get_filter_options()
                                st.session_state.bq_total_registros = bq.get_record_count()
                                st.info(msg_bq, icon="🗄️")
                            else:
                                st.warning(f"BigQuery: {msg_bq}", icon="⚠️")

                        # ── Mantener en sesión para análisis inmediato ───────
                        if st.session_state.df is not None and not st.session_state.df.empty and not st.session_state.demo_loaded:
                            df_final = consolidate_files([st.session_state.df, df_nuevos])
                            del df_nuevos
                        else:
                            df_final = df_nuevos
                        gc.collect()
                        st.session_state.df = df_final
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
                        st.success(f"✅ {n_archivos} archivo(s) procesados · **{n_nuevos:,}** nuevos registros · **{len(df_final):,}** en sesión actual")
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
                if st.button("📥 Restaurar datos guardados", type="primary", width="stretch"):
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
            if st.button("🎲 Cargar datos demo", type="secondary", width="stretch"):
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
            if st.button("⬇️ Preparar descarga completa desde BigQuery", width="stretch"):
                with st.spinner("Exportando desde BigQuery... (puede tomar unos segundos)"):
                    csv_bq = bq.export_csv_bytes()
                if csv_bq:
                    st.download_button(
                        "💾 Descargar datos_consolidados_aps.csv",
                        data=csv_bq,
                        file_name="datos_consolidados_aps.csv",
                        mime="text/csv",
                        width="stretch",
                    )
                else:
                    st.error("No se pudo exportar desde BigQuery.")

            if st.button("🗑️ Limpiar TODOS los datos de BigQuery", type="secondary", width="stretch"):
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
                if st.button("🗑️ Limpiar todos los datos", type="secondary", width="stretch"):
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
                st.dataframe(
                    df_disp[["instrumento","Total","Citados","Ocupación","No-Show","Efectividad","Rendim. (min)"]].rename(columns={"instrumento":"Instrumento"}),
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
                df_kpis_disp["Bloqueados"] = df_kpis_disp["bloqueados"].apply(lambda v: f"{v:,}")

                cols_show = [
                    "tipo_atencion", "Total", "Citados", "Disponibles", "Bloqueados",
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
        df_c_display.columns = [
            "Centro", "Ocupación %", "No-Show %", "Bloqueo %",
            "Efectividad %", "Rendimiento (min)", "Total Registros"
        ]
        def color_ocupacion(val):
            if isinstance(val, float):
                if val >= 65:
                    return "color: #27AE60; font-weight: bold"
                elif val >= 50:
                    return "color: #F39C12; font-weight: bold"
                else:
                    return "color: #E74C3C; font-weight: bold"
            return ""

        styled_c = df_c_display.style.map(color_ocupacion, subset=["Ocupación %"]).format({
            "Ocupación %": "{:.1f}",
            "No-Show %": "{:.1f}",
            "Bloqueo %": "{:.1f}",
            "Efectividad %": "{:.1f}",
            "Rendimiento (min)": "{:.1f}",
            "Total Registros": "{:,.0f}",
        })
        st.dataframe(styled_c, width="stretch", hide_index=True)


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
