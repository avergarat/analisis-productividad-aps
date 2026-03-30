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
    kpis_por_tipo_atencion, kpis_tipo_atencion_mes
)
from src.charts import (
    chart_ranking_centros, chart_evolucion_mensual, chart_heatmap_instrumento_mes,
    chart_tipo_atencion, chart_sector, chart_noshow_vs_umbral,
    chart_rendimiento_instrumento, chart_estado_cupos, chart_multi_kpi,
    build_semaforo_table
)
from src.demo_data import generate_demo_data, get_demo_metadata
from src.storage import save_data, load_data, delete_data, github_configured, storage_status
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
    """Guarda df en GitHub + /tmp. Solo para datos reales (no demo)."""
    if st.session_state.get("df") is None or st.session_state.get("demo_loaded", False):
        return
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
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_session()

# Auto-cargar datos desde /tmp o GitHub si la sesión está vacía
if st.session_state.df is None and not st.session_state.demo_loaded:
    if _load_session():
        st.toast(
            f"💾 Datos recuperados · {len(st.session_state.df):,} registros",
            icon="✅"
        )


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def has_data() -> bool:
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
        if has_data():
            df = st.session_state.df
            st.markdown("**Filtros**")

            # Centros
            centros_disp = sorted(df["ESTABLECIMIENTO"].dropna().unique().tolist())
            centros_sel = st.multiselect("Centro de Salud", centros_disp,
                                          default=centros_disp, key="filt_centros")
            filtros["centros"] = centros_sel if centros_sel else centros_disp

            # Meses
            meses_disp = sorted(df["MES_NUM"].dropna().unique().tolist())
            MESES_N = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
                       7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}
            meses_labels = {m: f"{MESES_N.get(int(m), str(m))} ({int(m)})" for m in meses_disp}
            meses_sel_labels = st.multiselect(
                "Meses", options=list(meses_labels.values()),
                default=list(meses_labels.values()), key="filt_meses"
            )
            meses_sel = [m for m, lbl in meses_labels.items() if lbl in meses_sel_labels]
            filtros["meses"] = meses_sel if meses_sel else meses_disp

            # Instrumento
            inst_disp = sorted(df["INSTRUMENTO"].dropna().unique().tolist())
            inst_sel = st.multiselect("Instrumento/Profesional", inst_disp,
                                       default=inst_disp, key="filt_inst")
            filtros["instrumentos"] = inst_sel if inst_sel else inst_disp

            # Sector
            sect_disp = sorted(df["SECTOR"].dropna().unique().tolist())
            sect_sel = st.multiselect("Sector Territorial", sect_disp,
                                       default=sect_disp, key="filt_sect")
            filtros["sectores"] = sect_sel if sect_sel else sect_disp

            # Tipo cupo
            tc_disp = sorted(df["TIPO CUPO"].dropna().unique().tolist())
            tc_sel = st.multiselect("Tipo Cupo", tc_disp,
                                     default=tc_disp, key="filt_tc")
            filtros["tipo_cupo"] = tc_sel if tc_sel else tc_disp

            st.divider()
            dff = apply_filters(df, filtros)
            st.caption(f"📋 **{len(dff):,}** registros seleccionados")
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
                        del dfs  # liberar lista de DataFrames intermedios
                        gc.collect()
                        n_nuevos = len(df_nuevos)
                        # Carga incremental: acumular sobre datos existentes
                        if st.session_state.df is not None and not st.session_state.df.empty and not st.session_state.demo_loaded:
                            df_final = consolidate_files([st.session_state.df, df_nuevos])
                            del df_nuevos
                        else:
                            df_final = df_nuevos
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
                        _save_session()
                        st.success(f"✅ {n_archivos} archivo(s) procesados · **{n_nuevos:,}** nuevos registros · **{len(df_final):,}** registros acumulados en total")
                        if github_configured():
                            st.info("💾 Datos guardados en GitHub — cualquier usuario verá estos datos al abrir la app.", icon="✅")
                        else:
                            st.warning("⚠️ GitHub no configurado: datos solo en caché local (se pierden al reiniciar el servidor). Configura `[github_storage]` en Secrets para persistencia real.", icon="⚠️")

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
        df = st.session_state.df
        st.markdown("#### Resumen de datos cargados")

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

        # Estado de cupos global
        st.markdown("##### Composición de cupos")
        col1, col2 = st.columns([2, 1])
        with col1:
            fig_est = chart_estado_cupos(df)
            st.plotly_chart(fig_est)
        with col2:
            st.markdown("**Archivos procesados:**")
            archivos = df["_archivo"].unique()
            for a in archivos:
                n = (df["_archivo"] == a).sum()
                st.write(f"• {a[:40]} ({n:,} reg.)")
            st.divider()
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "💾 Descargar datos consolidados (.csv)",
                data=csv_bytes,
                file_name="datos_consolidados_aps.csv",
                mime="text/csv",
                use_container_width=True,
                help="Guarda los datos acumulados. Re-súbelo en la próxima sesión usando la pestaña 'Cargar datos guardados' para continuar sin recargar los archivos IRIS.",
            )
            if st.button("🗑️ Limpiar todos los datos", type="secondary", use_container_width=True):
                st.session_state.df = None
                st.session_state.metadata_list = []
                st.session_state.archivos_cargados = []
                st.session_state.demo_loaded = False
                st.session_state.registro_cargas = []
                # Borrar de /tmp y GitHub
                delete_data()
                # Limpiar caché de KPIs
                st.cache_data.clear()
                st.rerun()

        if st.session_state.registro_cargas:
            st.markdown("##### 📋 Registro de cargas (sesión actual)")
            st.caption("Historial incremental de archivos procesados. La columna **'Fecha hasta'** indica el último período cargado por archivo; úsala para saber desde qué fecha debes generar el próximo reporte IRIS.")
            df_reg = pd.DataFrame(st.session_state.registro_cargas)
            st.dataframe(df_reg, use_container_width=True, hide_index=True)


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
        st.dataframe(styled, use_container_width=True, hide_index=True,
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

    sub_tab1, sub_tab2, sub_tab3, sub_tab4 = st.tabs([
        "Por Instrumento", "Por Tipo Atención", "Mapa de Calor", "Grupo Etario"
    ])

    with sub_tab1:
        col1, col2 = st.columns(2)
        with col1:
            fig_rend = chart_rendimiento_instrumento(dff)
            st.plotly_chart(fig_rend)
        with col2:
            df_inst = kpis_por_instrumento(dff)
            if not df_inst.empty:
                import plotly.graph_objects as go
                colors = [
                    "#27AE60" if v >= 65 else "#F39C12" if v >= 50 else "#E74C3C"
                    for v in df_inst["ocupacion"]
                ]
                fig_inst = go.Figure(go.Bar(
                    x=df_inst["ocupacion"],
                    y=df_inst["instrumento"].str[:28],
                    orientation="h",
                    marker_color=colors,
                    text=[f"{v:.1f}%" for v in df_inst["ocupacion"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>Ocupación: %{x:.1f}%<extra></extra>",
                ))
                fig_inst.update_layout(
                    title="Ocupación por Instrumento (%)",
                    template="plotly_white",
                    height=max(350, len(df_inst) * 32 + 80),
                    margin=dict(l=40, r=20, t=50, b=40),
                    xaxis=dict(range=[0, 105], title="Ocupación (%)"),
                )
                st.plotly_chart(fig_inst)

        # Tabla resumen por instrumento
        st.markdown("##### Tabla de KPIs por Instrumento")
        df_inst2 = kpis_por_instrumento(dff)
        if not df_inst2.empty:
            df_inst2_disp = df_inst2.copy()
            df_inst2_disp.columns = [
                "Instrumento", "Ocupación %", "No-Show %",
                "Efectividad %", "Rendimiento (min)", "Total Registros", "Citados"
            ]
            df_inst2_disp = df_inst2_disp.round(1)
            st.dataframe(df_inst2_disp, use_container_width=True, hide_index=True)

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
                st.plotly_chart(fig_ta, use_container_width=True)
            with col2:
                from src.kpis import calc_no_show
                ta_noshow = (
                    dff_ta.groupby("TIPO ATENCION")
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
                st.plotly_chart(fig_ns, use_container_width=True)

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
                st.dataframe(df_kpis_disp, use_container_width=True, hide_index=True)

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
                        st.plotly_chart(fig_ocu_ts, use_container_width=True)

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
                        st.plotly_chart(fig_ns_ts, use_container_width=True)

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
                        st.plotly_chart(fig_ef_ts, use_container_width=True)

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
                        st.plotly_chart(fig_vol_ts, use_container_width=True)

                    if len(tipos_sel) > 8:
                        st.caption(f"ℹ️ Series temporales muestran los 8 tipos con mayor volumen. La tabla de KPIs incluye todos los {len(tipos_sel)} tipos seleccionados.")

    with sub_tab3:
        fig_heat = chart_heatmap_instrumento_mes(dff)
        st.plotly_chart(fig_heat)

    with sub_tab4:
        if "GRUPO_ETARIO" in dff.columns:
            col1, col2 = st.columns(2)
            with col1:
                ge_counts = dff["GRUPO_ETARIO"].value_counts().sort_index()
                import plotly.graph_objects as go
                fig_ge = go.Figure(go.Bar(
                    x=ge_counts.index.astype(str),
                    y=ge_counts.values,
                    marker_color="#2E86C1",
                    text=[f"{v:,}" for v in ge_counts.values],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>%{y:,} registros<extra></extra>",
                ))
                fig_ge.update_layout(
                    title="Distribución por Grupo Etario",
                    template="plotly_white",
                    height=380,
                    xaxis_title="Grupo Etario",
                    yaxis_title="Registros",
                    margin=dict(l=40, r=20, t=50, b=40),
                )
                st.plotly_chart(fig_ge)

            with col2:
                # No-Show por grupo etario
                # (GRUPO_ETARIO es atributo del paciente → solo existe en cupos CITADO;
                #  calcular ocupación por edad sería siempre 100% porque los cupos
                #  DISPONIBLE no tienen paciente asignado ni edad)
                from src.kpis import calc_no_show
                ge_noshow = (
                    dff[dff["ESTADO CUPO"] == "CITADO"]
                    .groupby("GRUPO_ETARIO", observed=True)
                    .apply(calc_no_show)
                    .reset_index(name="no_show")
                )
                colors_ge = [
                    "#E74C3C" if v > 15 else "#F39C12" if v > 10 else "#27AE60"
                    for v in ge_noshow["no_show"]
                ]
                fig_ge2 = go.Figure(go.Bar(
                    x=ge_noshow["GRUPO_ETARIO"].astype(str),
                    y=ge_noshow["no_show"],
                    marker_color=colors_ge,
                    text=[f"{v:.1f}%" for v in ge_noshow["no_show"]],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>No-Show: %{y:.1f}%<extra></extra>",
                ))
                fig_ge2.add_hline(y=10, line_dash="dash", line_color="#E74C3C",
                                   annotation_text="Umbral 10%")
                fig_ge2.update_layout(
                    title="No-Show por Grupo Etario",
                    template="plotly_white",
                    height=380,
                    xaxis_title="Grupo Etario",
                    yaxis_title="No-Show (%)",
                    margin=dict(l=40, r=20, t=50, b=40),
                )
                st.plotly_chart(fig_ge2)
        else:
            st.info("Columna de grupo etario no disponible en los datos cargados.")


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

        styled_c = df_c_display.style.applymap(color_ocupacion, subset=["Ocupación %"]).format({
            "Ocupación %": "{:.1f}",
            "No-Show %": "{:.1f}",
            "Bloqueo %": "{:.1f}",
            "Efectividad %": "{:.1f}",
            "Rendimiento (min)": "{:.1f}",
            "Total Registros": "{:,.0f}",
        })
        st.dataframe(styled_c, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    nav, filtros = render_sidebar()

    if has_data():
        dff = apply_filters(st.session_state.df, filtros)
    else:
        dff = pd.DataFrame()

    if nav == "🏠 Inicio y Carga":
        page_inicio()
    elif nav == "📊 Dashboard KPIs":
        if not has_data():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_dashboard(dff)
    elif nav == "📈 Evolución Temporal":
        if not has_data():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_evolucion(dff)
    elif nav == "🔍 Análisis Detallado":
        if not has_data():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_analisis(dff)
    elif nav == "⚠️ Alertas y Brechas":
        if not has_data():
            st.warning("Primero carga datos desde **Inicio y Carga**.")
        else:
            page_alertas(dff)

    # Footer + estado almacenamiento
    st.sidebar.markdown("---")
    status = storage_status()
    if status["github_configurado"]:
        st.sidebar.caption(f"💾 GitHub: `{status['repo']}`")
    else:
        st.sidebar.caption("⚠️ GitHub no configurado — persistencia solo en sesión activa")
    st.sidebar.caption(
        "Sistema de Análisis de Productividad APS · v1.1  \n"
        "SSMC · Modelo de Análisis de Productividad · 2026"
    )


if __name__ == "__main__":
    main()
