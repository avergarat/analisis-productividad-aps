"""
Módulo de visualizaciones Plotly para el dashboard de productividad APS.
"""
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

# Paleta de colores institucional SSMC
COLORS = {
    "primary": "#1B4F72",
    "secondary": "#2E86C1",
    "accent": "#1ABC9C",
    "verde": "#27AE60",
    "amarillo": "#F39C12",
    "rojo": "#E74C3C",
    "gris": "#95A5A6",
    "bg": "#EBF5FB",
}

SEMAFORO_COLORS = {
    "verde": "#27AE60",
    "amarillo": "#F39C12",
    "rojo": "#E74C3C",
    "gris": "#95A5A6",
}

PLOTLY_TEMPLATE = "plotly_white"


def _base_layout(title: str, height: int = 420) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=15, color=COLORS["primary"])),
        template=PLOTLY_TEMPLATE,
        height=height,
        margin=dict(l=40, r=20, t=60, b=40),
        font=dict(family="Arial, sans-serif", size=12),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )


# ──────────────────────────────────────────────
# 1. Gauge / Medidor para KPI individual
# ──────────────────────────────────────────────
def chart_gauge(valor: float, kpi_def: dict, key: str) -> go.Figure:
    nombre = kpi_def.get("nombre", key)
    unidad = kpi_def.get("unidad", "%")
    umbral_ok = kpi_def.get("umbral_ok", 65)
    umbral_alerta = kpi_def.get("umbral_alerta", 50)
    color_s = SEMAFORO_COLORS.get(kpi_def.get("semaforo", "gris"), COLORS["gris"])

    if unidad == "%":
        rng = [0, 100]
    else:
        rng = [0, max(valor * 1.5, 60)]

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=valor,
        number={"suffix": f" {unidad}", "font": {"size": 24, "color": color_s}},
        title={"text": nombre, "font": {"size": 13, "color": COLORS["primary"]}},
        gauge={
            "axis": {"range": rng, "tickcolor": COLORS["primary"]},
            "bar": {"color": color_s},
            "bgcolor": "white",
            "borderwidth": 1,
            "bordercolor": "#ccc",
            "steps": [
                {"range": [rng[0], rng[1]], "color": "#f0f0f0"},
            ],
        },
    ))
    fig.update_layout(height=200, margin=dict(l=20, r=20, t=50, b=10))
    return fig


# ──────────────────────────────────────────────
# 2. Ranking de ocupación por centro
# ──────────────────────────────────────────────
def chart_ranking_centros(df_kpis: pd.DataFrame) -> go.Figure:
    """df_kpis debe tener columnas: centro, ocupacion."""
    if df_kpis.empty:
        return go.Figure()

    df = df_kpis.sort_values("ocupacion")
    colors = [
        COLORS["verde"] if v >= 65 else COLORS["amarillo"] if v >= 50 else COLORS["rojo"]
        for v in df["ocupacion"]
    ]

    fig = go.Figure(go.Bar(
        x=df["ocupacion"],
        y=df["centro"],
        orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}%" for v in df["ocupacion"]],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Ocupación: %{x:.1f}%<extra></extra>",
    ))

    fig.add_vline(x=65, line_dash="dash", line_color=COLORS["verde"],
                  annotation_text="Meta 65%", annotation_position="top right")
    fig.add_vline(x=50, line_dash="dot", line_color=COLORS["rojo"],
                  annotation_text="Alerta 50%", annotation_position="top left")

    fig.update_layout(
        **_base_layout("Ranking de Ocupación por Centro"),
        xaxis=dict(range=[0, 105], title="Tasa de Ocupación (%)"),
        yaxis=dict(title=""),
        showlegend=False,
    )
    return fig


# ──────────────────────────────────────────────
# 3. Evolución mensual de KPIs
# ──────────────────────────────────────────────
def chart_evolucion_mensual(df_meses: pd.DataFrame, kpi_col: str,
                             titulo: str, umbral_ok: float = None,
                             umbral_alerta: float = None,
                             unidad: str = "%") -> go.Figure:
    """df_meses debe tener columnas: mes_nombre, <kpi_col>."""
    if df_meses.empty or kpi_col not in df_meses.columns:
        return go.Figure()

    fig = go.Figure()

    colors_line = [
        COLORS["verde"] if v >= (umbral_ok or 0) else
        COLORS["amarillo"] if v >= (umbral_alerta or 0) else
        COLORS["rojo"]
        for v in df_meses[kpi_col]
    ]

    fig.add_trace(go.Scatter(
        x=df_meses["mes_nombre"],
        y=df_meses[kpi_col],
        mode="lines+markers+text",
        name=titulo,
        line=dict(color=COLORS["secondary"], width=2.5),
        marker=dict(color=colors_line, size=10, line=dict(color="white", width=2)),
        text=[f"{v:.1f}{unidad}" for v in df_meses[kpi_col]],
        textposition="top center",
        hovertemplate=f"<b>%{{x}}</b><br>{titulo}: %{{y:.1f}}{unidad}<extra></extra>",
    ))

    if umbral_ok is not None:
        fig.add_hline(y=umbral_ok, line_dash="dash", line_color=COLORS["verde"],
                      annotation_text=f"Meta ({umbral_ok}{unidad})",
                      annotation_position="right")
    if umbral_alerta is not None:
        fig.add_hline(y=umbral_alerta, line_dash="dot", line_color=COLORS["rojo"],
                      annotation_text=f"Alerta ({umbral_alerta}{unidad})",
                      annotation_position="right")

    fig.update_layout(
        **_base_layout(f"Evolución Mensual — {titulo}"),
        xaxis=dict(title="Mes"),
        yaxis=dict(title=f"{titulo} ({unidad})"),
        showlegend=False,
    )
    return fig


# ──────────────────────────────────────────────
# 4. Mapa de calor: Instrumento × Mes
# ──────────────────────────────────────────────
def chart_heatmap_instrumento_mes(df: pd.DataFrame) -> go.Figure:
    """Mapa de calor de tasa de ocupación por instrumento y mes."""
    if df.empty or "INSTRUMENTO" not in df.columns or "MES_NUM" not in df.columns:
        return go.Figure()

    from src.kpis import calc_ocupacion

    pivot_data = {}
    for (inst, mes), grp in df.groupby(["INSTRUMENTO", "MES_NUM"]):
        if inst not in pivot_data:
            pivot_data[inst] = {}
        pivot_data[inst][mes] = calc_ocupacion(grp)

    if not pivot_data:
        return go.Figure()

    df_pivot = pd.DataFrame(pivot_data).T.fillna(0)
    df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)

    MESES_ES = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}
    col_names = [MESES_ES.get(c, str(c)) for c in df_pivot.columns]

    # Ordenar instrumentos por ocupación media
    df_pivot["_media"] = df_pivot.mean(axis=1)
    df_pivot = df_pivot.sort_values("_media", ascending=True).drop(columns=["_media"])

    # Truncar nombres largos
    y_labels = [str(i)[:28] for i in df_pivot.index]

    fig = go.Figure(go.Heatmap(
        z=df_pivot.values,
        x=col_names,
        y=y_labels,
        colorscale=[
            [0.0, COLORS["rojo"]],
            [0.5, COLORS["amarillo"]],
            [0.65, COLORS["verde"]],
            [1.0, "#0B5345"],
        ],
        zmin=0, zmax=100,
        colorbar=dict(title="Ocupación %", ticksuffix="%"),
        hovertemplate="<b>%{y}</b><br>Mes: %{x}<br>Ocupación: %{z:.1f}%<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout("Ocupación por Instrumento y Mes (%)", height=max(350, len(df_pivot) * 35 + 100)),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Instrumento/Profesional"),
    )
    return fig


# ──────────────────────────────────────────────
# 5. Distribución por tipo de atención
# ──────────────────────────────────────────────
def chart_tipo_atencion(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    if df.empty or "TIPO ATENCION" not in df.columns:
        return go.Figure()

    counts = df["TIPO ATENCION"].value_counts().head(top_n)
    df_plot = counts.reset_index()
    df_plot.columns = ["tipo", "cantidad"]
    df_plot = df_plot.sort_values("cantidad")

    fig = go.Figure(go.Bar(
        x=df_plot["cantidad"],
        y=df_plot["tipo"].str[:35],
        orientation="h",
        marker_color=COLORS["secondary"],
        hovertemplate="<b>%{y}</b><br>Registros: %{x:,}<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout(f"Top {top_n} Tipos de Atención", height=max(350, top_n * 28 + 100)),
        xaxis=dict(title="Cantidad de registros"),
        yaxis=dict(title=""),
    )
    return fig


# ──────────────────────────────────────────────
# 6. Distribución por sector territorial
# ──────────────────────────────────────────────
def chart_sector(df: pd.DataFrame) -> go.Figure:
    if df.empty or "SECTOR" not in df.columns:
        return go.Figure()

    counts = df["SECTOR"].value_counts()
    sector_colors = {
        "VERDE": "#27AE60", "LILA": "#8E44AD", "ROJO": "#E74C3C",
        "NO INFORMADO": "#95A5A6"
    }
    colors = [sector_colors.get(s, COLORS["gris"]) for s in counts.index]

    fig = go.Figure(go.Pie(
        labels=counts.index,
        values=counts.values,
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        hovertemplate="<b>%{label}</b><br>%{value:,} registros (%{percent})<extra></extra>",
        textinfo="label+percent",
    ))

    fig.update_layout(
        **_base_layout("Distribución por Sector Territorial"),
        showlegend=True,
    )
    return fig


# ──────────────────────────────────────────────
# 7. Comparativo No-Show vs Umbral
# ──────────────────────────────────────────────
def chart_noshow_vs_umbral(df_meses: pd.DataFrame) -> go.Figure:
    if df_meses.empty or "no_show" not in df_meses.columns:
        return go.Figure()

    fig = go.Figure()

    colors_bar = [
        COLORS["rojo"] if v > 15 else COLORS["amarillo"] if v > 10 else COLORS["verde"]
        for v in df_meses["no_show"]
    ]

    fig.add_trace(go.Bar(
        x=df_meses["mes_nombre"],
        y=df_meses["no_show"],
        name="No-Show",
        marker_color=colors_bar,
        text=[f"{v:.1f}%" for v in df_meses["no_show"]],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>No-Show: %{y:.1f}%<extra></extra>",
    ))

    fig.add_hline(y=10, line_dash="dash", line_color=COLORS["amarillo"],
                  annotation_text="Umbral OK (10%)", annotation_position="right")
    fig.add_hline(y=15, line_dash="dot", line_color=COLORS["rojo"],
                  annotation_text="Alerta (15%)", annotation_position="right")

    fig.update_layout(
        **_base_layout("Tasa de No-Show Mensual vs Umbrales"),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Tasa No-Show (%)"),
        showlegend=False,
    )
    return fig


# ──────────────────────────────────────────────
# 8. Distribución de rendimiento por instrumento
# ──────────────────────────────────────────────
def chart_rendimiento_instrumento(df: pd.DataFrame) -> go.Figure:
    if df.empty or "INSTRUMENTO" not in df.columns or "RENDIMIENTO" not in df.columns:
        return go.Figure()

    df_clean = df[["INSTRUMENTO", "RENDIMIENTO"]].dropna()
    grouped = df_clean.groupby("INSTRUMENTO")["RENDIMIENTO"].mean().reset_index()
    grouped = grouped.sort_values("RENDIMIENTO")
    grouped["INSTRUMENTO_CORTO"] = grouped["INSTRUMENTO"].str[:28]

    fig = go.Figure(go.Bar(
        x=grouped["RENDIMIENTO"],
        y=grouped["INSTRUMENTO_CORTO"],
        orientation="h",
        marker_color=COLORS["accent"],
        text=[f"{v:.1f} min" for v in grouped["RENDIMIENTO"]],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Rendimiento prom: %{x:.1f} min<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout("Rendimiento Promedio por Instrumento (min/atención)",
                       height=max(350, len(grouped) * 35 + 100)),
        xaxis=dict(title="Minutos por atención"),
        yaxis=dict(title=""),
    )
    return fig


# ──────────────────────────────────────────────
# 9. Composición de estado de cupos
# ──────────────────────────────────────────────
def chart_estado_cupos(df: pd.DataFrame) -> go.Figure:
    if df.empty or "ESTADO CUPO" not in df.columns:
        return go.Figure()

    counts = df["ESTADO CUPO"].value_counts()
    colors_map = {
        "CITADO": COLORS["verde"],
        "DISPONIBLE": COLORS["amarillo"],
        "BLOQUEADO": COLORS["rojo"],
    }
    colors = [colors_map.get(s, COLORS["gris"]) for s in counts.index]

    fig = go.Figure(go.Bar(
        x=counts.index,
        y=counts.values,
        marker_color=colors,
        text=[f"{v:,}<br>({v/len(df)*100:.1f}%)" for v in counts.values],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>%{y:,} registros<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout("Distribución de Estado de Cupos"),
        xaxis=dict(title="Estado"),
        yaxis=dict(title="Cantidad de registros"),
        showlegend=False,
    )
    return fig


# ──────────────────────────────────────────────
# 10. Evolución multi-KPI (ocupación + no-show + bloqueo)
# ──────────────────────────────────────────────
def chart_multi_kpi(df_meses: pd.DataFrame) -> go.Figure:
    if df_meses.empty:
        return go.Figure()

    fig = go.Figure()

    series = [
        ("ocupacion", "Ocupación", COLORS["verde"], "solid"),
        ("no_show", "No-Show", COLORS["rojo"], "dot"),
        ("bloqueo", "Bloqueo", COLORS["amarillo"], "dash"),
    ]

    for col, name, color, dash in series:
        if col in df_meses.columns:
            fig.add_trace(go.Scatter(
                x=df_meses["mes_nombre"],
                y=df_meses[col],
                mode="lines+markers",
                name=name,
                line=dict(color=color, width=2, dash=dash),
                marker=dict(size=7),
                hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:.1f}}%<extra></extra>",
            ))

    fig.update_layout(
        **_base_layout("Evolución de KPIs Principales (%)"),
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Porcentaje (%)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


# ──────────────────────────────────────────────
# 11. Tabla semáforo de KPIs
# ──────────────────────────────────────────────
def build_semaforo_table(kpis: dict) -> pd.DataFrame:
    """Construye DataFrame para la tabla semáforo."""
    rows = []
    icons = {"verde": "🟢", "amarillo": "🟡", "rojo": "🔴", "gris": "⚪"}

    for key, data in kpis.items():
        if not isinstance(data, dict):
            continue
        valor = data.get("valor", 0)
        unidad = data.get("unidad", "%")
        sem = data.get("semaforo", "gris")
        umbral_ok = data.get("umbral_ok")
        umbral_alerta = data.get("umbral_alerta")

        meta_str = ""
        if umbral_ok is not None:
            meta_str = f"{umbral_ok}{unidad}"

        alerta_str = ""
        if umbral_alerta is not None:
            alerta_str = f"{umbral_alerta}{unidad}"

        rows.append({
            "Estado": icons.get(sem, "⚪"),
            "Indicador": data.get("nombre", key),
            "Valor": f"{valor:.1f} {unidad}",
            "Meta": meta_str,
            "Alerta si": alerta_str,
            "Descripción": data.get("descripcion", ""),
            "_semaforo": sem,
        })

    return pd.DataFrame(rows)
