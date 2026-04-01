"""
Generador de informes HTML y PDF para el Sistema de Analisis de Productividad APS.
Informe interactivo HTML con navegacion, animaciones 3D y diseno glassmorphism.
Informe PDF estilo revista analitica con portada Canva, TOC, semaforo y tablas centradas.
"""
from __future__ import annotations
import io
from datetime import datetime

import pandas as pd
import numpy as np
import plotly.graph_objects as go


# ══════════════════════════════════════════════════════════════
#  UTILIDADES COMPARTIDAS
# ══════════════════════════════════════════════════════════════

def _sem_text_safe(val, kpi):
    from src.kpis import semaforo
    s = semaforo(val, kpi)
    return {"verde": "Optimo", "amarillo": "Observacion", "rojo": "Critico"}.get(s, "-")

def _sem_text(val, kpi):
    from src.kpis import semaforo
    s = semaforo(val, kpi)
    return {"verde": "Optimo", "amarillo": "Observacion", "rojo": "Critico"}.get(s, "-")

def _sem_icon(val, kpi):
    from src.kpis import semaforo
    s = semaforo(val, kpi)
    return {"verde": "&#9679;", "amarillo": "&#9679;", "rojo": "&#9679;"}.get(s, "&#9679;")

def _sem_color_hex(val, kpi):
    from src.kpis import semaforo
    s = semaforo(val, kpi)
    return {"verde": "#27AE60", "amarillo": "#F39C12", "rojo": "#E74C3C"}.get(s, "#95A5A6")

KPI_ORDER = [
    ("ocupacion", "Tasa de Ocupacion"),
    ("no_show", "Tasa de No-Show"),
    ("bloqueo", "Tasa de Bloqueo"),
    ("efectividad", "Efectividad de Cita"),
    ("rendimiento", "Rendimiento Promedio"),
    ("sobrecupo", "Cupos Sobrecupo"),
    ("cobertura_sectorial", "Cobertura Sectorial"),
    ("agendamiento_remoto", "Agendamiento Remoto"),
    ("variacion_mensual", "Variacion Mensual"),
    ("ocupacion_extendida", "Ocupacion Horario Extendido"),
]


# ══════════════════════════════════════════════════════════════
#  HTML REPORT
# ══════════════════════════════════════════════════════════════

def generar_html_informe(
    centro_sel, rango_meses, n_meses, total_registros,
    citados, disponibles, bloqueados, completados,
    kpis, df_centro, df_inst_c, df_kpis_ta,
    alertas_centro, n_verde, n_amarillo, n_rojo,
    kpis_por_mes_fn=None,
) -> str:
    from plotly.offline import get_plotlyjs
    from src.kpis import semaforo, KPI_DEFINITIONS
    from src.charts import (
        chart_estado_cupos, chart_evolucion_mensual, chart_noshow_vs_umbral,
        chart_rendimiento_instrumento, chart_sector, chart_tipo_atencion,
        chart_multi_kpi, chart_heatmap_instrumento_mes, chart_heatmap_pivot,
    )
    from src.kpis import (
        kpis_horario_segmentado,
        kpis_profesional_sabatino, kpis_profesional_extendido,
        kpis_sabatino_por_mes, kpis_extendido_por_mes,
        kpis_sabatino_por_instrumento, kpis_extendido_por_instrumento,
        kpis_instrumento_mes, kpis_tipo_atencion_mes,
        calc_ocupacion, calc_no_show, calc_efectividad, calc_bloqueo,
    )

    plotly_js_code = get_plotlyjs()
    fecha_gen = datetime.now().strftime("%d/%m/%Y %H:%M")

    def _fig_html(fig, w=900, h=450):
        fig.update_layout(width=w, height=h)
        return fig.to_html(include_plotlyjs=False, full_html=False,
                           config={"displayModeBar": True, "responsive": True,
                                   "modeBarButtonsToRemove": ["lasso2d", "select2d"]})

    def _si(val, kpi):
        s = semaforo(val, kpi)
        c = {"verde": "#27AE60", "amarillo": "#F39C12", "rojo": "#E74C3C"}.get(s, "#95A5A6")
        return f'<span style="color:{c};font-size:1.2em">&#9679;</span>'

    def _st(val, kpi):
        s = semaforo(val, kpi)
        return {"verde": "Optimo", "amarillo": "Observacion", "rojo": "Critico"}.get(s, "-")

    # ── KPI values ──
    v = {k: kpis.get(k, {}).get("valor", 0) for k, _ in KPI_ORDER}

    # ── Graficos ──
    ch = {}

    fig1 = chart_estado_cupos(df_centro)
    ch["cupos"] = _fig_html(fig1, 900, 420)

    df_meses_c = kpis_por_mes_fn(df_centro) if kpis_por_mes_fn else pd.DataFrame()
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        ch["ocu_mensual"] = _fig_html(chart_evolucion_mensual(
            df_meses_c, "ocupacion", "Tasa de Ocupacion", 65, 50))
        ch["noshow"] = _fig_html(chart_noshow_vs_umbral(df_meses_c))
        ch["bloqueo"] = _fig_html(chart_evolucion_mensual(
            df_meses_c, "bloqueo", "Tasa de Bloqueo", 10, 15))
        ch["efectividad"] = _fig_html(chart_evolucion_mensual(
            df_meses_c, "efectividad", "Efectividad de Cita", 88, 80))
        ch["multi_kpi"] = _fig_html(chart_multi_kpi(df_meses_c), 900, 450)

    if not df_inst_c.empty:
        df_plot = df_inst_c.sort_values("ocupacion")
        colors_ocu = ["#27AE60" if x >= 65 else "#F39C12" if x >= 50 else "#E74C3C" for x in df_plot["ocupacion"]]
        fig3 = go.Figure(go.Bar(
            x=df_plot["ocupacion"], y=df_plot["instrumento"].str[:30],
            orientation="h", marker_color=colors_ocu,
            text=[f"{x:.1f}%" for x in df_plot["ocupacion"]], textposition="outside"))
        fig3.add_vline(x=65, line_dash="dash", line_color="#27AE60", annotation_text="Meta 65%")
        fig3.update_layout(title="Ocupacion por Instrumento", template="plotly_white",
                           xaxis=dict(title="Ocupacion (%)", range=[0, 105]), yaxis=dict(title=""))
        h3 = max(400, len(df_plot) * 40 + 100)
        ch["ocu_inst"] = _fig_html(fig3, 900, h3)

    ch["rendimiento"] = _fig_html(chart_rendimiento_instrumento(df_centro), 900,
                                   max(400, len(df_inst_c) * 40 + 100) if not df_inst_c.empty else 400)
    ch["sector"] = _fig_html(chart_sector(df_centro), 900, 420)
    ch["tipo_atencion"] = _fig_html(chart_tipo_atencion(df_centro, top_n=15), 900, 500)

    if "MES_NUM" in df_centro.columns and "INSTRUMENTO" in df_centro.columns:
        n_i = df_centro["INSTRUMENTO"].nunique()
        ch["heatmap"] = _fig_html(chart_heatmap_instrumento_mes(df_centro), 900, max(450, n_i * 38 + 120))

    # ── Instrument evolution charts ──
    _inst_evo_html = ""
    if not df_inst_c.empty and "MES_NUM" in df_centro.columns and "INSTRUMENTO" in df_centro.columns:
        top_instruments = tuple(df_inst_c.head(10)["instrumento"].tolist())
        df_inst_mes = kpis_instrumento_mes(df_centro, top_instruments)
        if not df_inst_mes.empty and df_inst_mes["mes"].nunique() >= 2:
            for metric, titulo, color in [("ocupacion", "Ocupacion", "#2E86C1"), ("no_show", "No-Show", "#E74C3C"),
                                          ("efectividad", "Efectividad", "#27AE60"), ("rendimiento", "Rendimiento", "#8E44AD")]:
                fig_im = go.Figure()
                for inst in top_instruments:
                    d = df_inst_mes[df_inst_mes["instrumento"] == inst]
                    if not d.empty:
                        fig_im.add_trace(go.Scatter(
                            x=d["mes_label"], y=d[metric], mode="lines+markers",
                            name=str(inst)[:30],
                            hovertemplate=f"<b>{str(inst)[:30]}</b><br>%{{x}}: %{{y:.1f}}<extra></extra>",
                        ))
                suffix = " (%)" if metric != "rendimiento" else " (min)"
                fig_im.update_layout(title=f"{titulo} por Mes e Instrumento", template="plotly_white",
                                     xaxis=dict(title="Mes"), yaxis=dict(title=f"{titulo}{suffix}"),
                                     legend=dict(font=dict(size=9)), height=450, width=900)
                ch[f"inst_evo_{metric}"] = _fig_html(fig_im, 900, 450)

            # Citados stacked bar
            fig_cit = go.Figure()
            for inst in top_instruments:
                d = df_inst_mes[df_inst_mes["instrumento"] == inst]
                if not d.empty:
                    fig_cit.add_trace(go.Bar(x=d["mes_label"], y=d["citados"], name=str(inst)[:30]))
            fig_cit.update_layout(title="Citados por Mes e Instrumento", template="plotly_white", barmode="stack",
                                  xaxis=dict(title="Mes"), yaxis=dict(title="Citados"),
                                  legend=dict(font=dict(size=9)), height=450, width=900)
            ch["inst_evo_citados"] = _fig_html(fig_cit, 900, 450)

    # ── Tipo atencion evolution charts ──
    _ta_evo_html = ""
    if not df_kpis_ta.empty and "MES_NUM" in df_centro.columns and "TIPO ATENCION" in df_centro.columns:
        top_tipos = tuple(df_kpis_ta.head(10)["tipo_atencion"].tolist())
        df_ta_mes = kpis_tipo_atencion_mes(df_centro, top_tipos)
        if not df_ta_mes.empty and df_ta_mes["mes"].nunique() >= 2:
            for metric, titulo in [("ocupacion", "Ocupacion"), ("no_show", "No-Show"),
                                   ("efectividad", "Efectividad")]:
                fig_ta = go.Figure()
                for tipo in top_tipos:
                    d = df_ta_mes[df_ta_mes["tipo_atencion"] == tipo]
                    if not d.empty:
                        fig_ta.add_trace(go.Scatter(
                            x=d["mes_nombre"], y=d[metric], mode="lines+markers",
                            name=str(tipo)[:30],
                            hovertemplate=f"<b>{str(tipo)[:30]}</b><br>%{{x}}: %{{y:.1f}}%<extra></extra>",
                        ))
                fig_ta.update_layout(title=f"{titulo} por Mes y Tipo de Atencion", template="plotly_white",
                                     xaxis=dict(title="Mes"), yaxis=dict(title=f"{titulo} (%)"),
                                     legend=dict(font=dict(size=9)), height=450, width=900)
                ch[f"ta_evo_{metric}"] = _fig_html(fig_ta, 900, 450)

    # ── Additional heatmaps (No-Show, Efectividad, Bloqueo) ──
    MESES_ES = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}
    if "MES_NUM" in df_centro.columns and "INSTRUMENTO" in df_centro.columns:
        for metric_key, metric_fn, label, cscale in [
            ("noshow", calc_no_show, "No-Show", [[0, "#0B5345"], [0.1, "#27AE60"], [0.15, "#F39C12"], [1.0, "#E74C3C"]]),
            ("efectividad", calc_efectividad, "Efectividad", [[0, "#E74C3C"], [0.8, "#F39C12"], [0.88, "#27AE60"], [1.0, "#0B5345"]]),
            ("bloqueo", calc_bloqueo, "Bloqueo", [[0, "#0B5345"], [0.1, "#27AE60"], [0.15, "#F39C12"], [1.0, "#E74C3C"]]),
        ]:
            pivot_data = {}
            for (inst, mes), grp in df_centro.groupby(["INSTRUMENTO", "MES_NUM"], observed=True):
                if inst not in pivot_data:
                    pivot_data[inst] = {}
                pivot_data[inst][mes] = metric_fn(grp)
            if pivot_data:
                df_piv = pd.DataFrame(pivot_data).T.fillna(0)
                df_piv = df_piv.reindex(sorted(df_piv.columns), axis=1)
                df_piv.columns = [MESES_ES.get(c, str(c)) for c in df_piv.columns]
                fig_hm = chart_heatmap_pivot(df_piv, f"{label} por Instrumento y Mes (%)", f"{label} %", cscale)
                ch[f"heatmap_{metric_key}"] = _fig_html(fig_hm, 900, max(450, len(df_piv) * 38 + 120))
    _3d_html = ""
    if not df_meses_c.empty and len(df_meses_c) >= 3:
        kpi_cols = ["ocupacion", "no_show", "bloqueo", "efectividad"]
        kpi_labels = ["Ocupacion", "No-Show", "Bloqueo", "Efectividad"]
        z_data = []
        for c in kpi_cols:
            if c in df_meses_c.columns:
                z_data.append(df_meses_c[c].values.tolist())
        if len(z_data) >= 2:
            fig3d = go.Figure(data=[go.Surface(
                z=z_data,
                x=list(df_meses_c["mes_nombre"]),
                y=kpi_labels[:len(z_data)],
                colorscale="Viridis",
                hovertemplate="Mes: %{x}<br>KPI: %{y}<br>Valor: %{z:.1f}%<extra></extra>",
            )])
            fig3d.update_layout(
                title="Superficie 3D: Evolucion de KPIs Principales",
                scene=dict(
                    xaxis_title="Mes", yaxis_title="Indicador", zaxis_title="Valor (%)",
                    camera=dict(eye=dict(x=1.5, y=-1.5, z=0.8)),
                ),
                template="plotly_white", height=550, width=900,
                margin=dict(l=0, r=0, t=50, b=0),
            )
            _3d_html = _fig_html(fig3d, 900, 550)

    # ── 3D Scatter: Ocupacion x No-Show x Bloqueo by month ──
    _3d_scatter = ""
    if not df_meses_c.empty and len(df_meses_c) >= 3 and all(c in df_meses_c.columns for c in ["ocupacion", "no_show", "bloqueo"]):
        fig3ds = go.Figure(data=[go.Scatter3d(
            x=df_meses_c["ocupacion"], y=df_meses_c["no_show"], z=df_meses_c["bloqueo"],
            mode="markers+text",
            marker=dict(
                size=df_meses_c.get("total_registros", pd.Series([10]*len(df_meses_c))).values / max(df_meses_c.get("total_registros", pd.Series([1])).max(), 1) * 30 + 5,
                color=df_meses_c["ocupacion"], colorscale="RdYlGn", cmin=30, cmax=80,
                colorbar=dict(title="Ocupacion%"),
            ),
            text=df_meses_c["mes_nombre"],
            hovertemplate="<b>%{text}</b><br>Ocupacion: %{x:.1f}%<br>No-Show: %{y:.1f}%<br>Bloqueo: %{z:.1f}%<extra></extra>",
        )])
        fig3ds.update_layout(
            title="Burbuja 3D: Ocupacion × No-Show × Bloqueo por Mes",
            scene=dict(xaxis_title="Ocupacion %", yaxis_title="No-Show %", zaxis_title="Bloqueo %",
                       camera=dict(eye=dict(x=1.8, y=-1.5, z=0.7))),
            template="plotly_white", height=550, width=900, margin=dict(l=0, r=0, t=50, b=0),
        )
        _3d_scatter = _fig_html(fig3ds, 900, 550)

    # ── Horario Extendido data ──
    _he_seg_html = ""
    _df_seg = kpis_horario_segmentado(df_centro)
    if not _df_seg.empty:
        rows_h = ""
        for _, r in _df_seg.iterrows():
            rows_h += f'<tr><td>{r["segmento"]}</td><td>{r["total"]:,.0f}</td><td>{r["citados"]:,.0f}</td><td>{r["disponibles"]:,.0f}</td><td>{r["bloqueados"]:,.0f}</td><td>{r["completados"]:,.0f}</td><td>{r["ocupacion"]:.1f}%</td><td>{r["no_show"]:.1f}%</td><td>{r["efectividad"]:.1f}%</td></tr>'
        _he_seg_html = f'<h3>Comparativa por Segmento Horario</h3><table><tr><th>Segmento</th><th>Total</th><th>Citados</th><th>Disp.</th><th>Bloq.</th><th>Complet.</th><th>Ocupacion</th><th>No-Show</th><th>Efectividad</th></tr>{rows_h}</table>'

    _he_prof_ext_html = ""
    _df_pe = kpis_profesional_extendido(df_centro)
    if not _df_pe.empty:
        rows_h = ""
        for _, r in _df_pe.iterrows():
            rows_h += f'<tr><td>{r["profesional"]}</td><td>{r["total"]:,.0f}</td><td>{r["citados"]:,.0f}</td><td>{r["completados"]:,.0f}</td><td>{r["ocupacion"]:.1f}%</td><td>{r["no_show"]:.1f}%</td><td>{r["efectividad"]:.1f}%</td></tr>'
        _he_prof_ext_html = f'<h3>Profesionales en Horario Extendido Lun-Vie ({len(_df_pe)})</h3><table><tr><th>Profesional</th><th>Total</th><th>Citados</th><th>Complet.</th><th>Ocupacion</th><th>No-Show</th><th>Efectividad</th></tr>{rows_h}</table>'

    _he_prof_sab_html = ""
    _df_ps = kpis_profesional_sabatino(df_centro)
    if not _df_ps.empty:
        rows_h = ""
        for _, r in _df_ps.iterrows():
            rows_h += f'<tr><td>{r["profesional"]}</td><td>{r["total"]:,.0f}</td><td>{r["citados"]:,.0f}</td><td>{r["completados"]:,.0f}</td><td>{r["ocupacion"]:.1f}%</td><td>{r["no_show"]:.1f}%</td><td>{r["efectividad"]:.1f}%</td></tr>'
        _he_prof_sab_html = f'<h3>Profesionales en Apertura Sabatina ({len(_df_ps)})</h3><table><tr><th>Profesional</th><th>Total</th><th>Citados</th><th>Complet.</th><th>Ocupacion</th><th>No-Show</th><th>Efectividad</th></tr>{rows_h}</table>'

    # Evolution charts for extended/sabatino
    _he_ext_evo = ""
    _df_ext_mes = kpis_extendido_por_mes(df_centro)
    if not _df_ext_mes.empty and len(_df_ext_mes) >= 2:
        fig_ext = chart_evolucion_mensual(_df_ext_mes, "ocupacion", "Ocupacion Extendido", 50, 30)
        _he_ext_evo = f'<h3>Evolucion Mensual - Horario Extendido</h3><div class="chart-container">{_fig_html(fig_ext)}</div>'

    _he_sab_evo = ""
    _df_sab_mes = kpis_sabatino_por_mes(df_centro)
    if not _df_sab_mes.empty and len(_df_sab_mes) >= 2:
        fig_sab = chart_evolucion_mensual(_df_sab_mes, "ocupacion", "Ocupacion Sabatina", 50, 30)
        _he_sab_evo = f'<h3>Evolucion Mensual - Apertura Sabatina</h3><div class="chart-container">{_fig_html(fig_sab)}</div>'

    # Inst tables for extended/sabatino
    _he_inst_ext = ""
    _df_ie = kpis_extendido_por_instrumento(df_centro)
    if not _df_ie.empty:
        rows_h = ""
        for _, r in _df_ie.iterrows():
            rows_h += f'<tr><td>{r["instrumento"]}</td><td>{r["total"]:,.0f}</td><td>{r["citados"]:,.0f}</td><td>{r["ocupacion"]:.1f}%</td><td>{r["no_show"]:.1f}%</td><td>{r["efectividad"]:.1f}%</td></tr>'
        _he_inst_ext = f'<h3>KPIs por Instrumento - Horario Extendido ({len(_df_ie)})</h3><table><tr><th>Instrumento</th><th>Total</th><th>Citados</th><th>Ocupacion</th><th>No-Show</th><th>Efectividad</th></tr>{rows_h}</table>'

    _he_inst_sab = ""
    _df_is = kpis_sabatino_por_instrumento(df_centro)
    if not _df_is.empty:
        rows_h = ""
        for _, r in _df_is.iterrows():
            rows_h += f'<tr><td>{r["instrumento"]}</td><td>{r["total"]:,.0f}</td><td>{r["citados"]:,.0f}</td><td>{r["ocupacion"]:.1f}%</td><td>{r["no_show"]:.1f}%</td><td>{r["efectividad"]:.1f}%</td></tr>'
        _he_inst_sab = f'<h3>KPIs por Instrumento - Apertura Sabatina ({len(_df_is)})</h3><table><tr><th>Instrumento</th><th>Total</th><th>Citados</th><th>Ocupacion</th><th>No-Show</th><th>Efectividad</th></tr>{rows_h}</table>'

    # ── Semaforo rows ──
    kpi_rows_html = ""
    for key, label in KPI_ORDER:
        k = kpis.get(key, {})
        valor = k.get("valor", 0)
        unidad = k.get("unidad", "%")
        sem = k.get("semaforo", "gris")
        meta = k.get("umbral_ok")
        alerta = k.get("umbral_alerta")
        desc = k.get("descripcion", "")
        c = {"verde": "#27AE60", "amarillo": "#F39C12", "rojo": "#E74C3C"}.get(sem, "#95A5A6")
        bg = {"rojo": "#FDEDEC", "amarillo": "#FEF9E7", "verde": "#EAFAF1"}.get(sem, "#fff")
        meta_s = f'{meta}{unidad}' if meta is not None else "-"
        alerta_s = f'{alerta}{unidad}' if alerta is not None else "-"
        kpi_rows_html += f'<tr style="background:{bg}"><td style="text-align:center"><span style="color:{c};font-size:1.4em">&#9679;</span></td><td><strong>{label}</strong></td><td style="text-align:center;font-weight:600">{valor:.1f} {unidad}</td><td style="text-align:center">{meta_s}</td><td style="text-align:center">{alerta_s}</td><td style="font-size:0.85em">{desc}</td></tr>\n'

    # ── Instrument table ──
    inst_rows_html = ""
    if not df_inst_c.empty:
        for _, r in df_inst_c.iterrows():
            blq_pct = r["bloqueados"] / r["total"] * 100 if r["total"] > 0 else 0
            inst_rows_html += (
                f'<tr><td>{r["instrumento"]}</td><td style="text-align:right">{r["total"]:,}</td><td style="text-align:right">{r["citados"]:,}</td>'
                f'<td style="text-align:right">{r["disponibles"]:,}</td><td style="text-align:right">{r["bloqueados"]:,}</td><td style="text-align:right">{r["completados"]:,}</td>'
                f'<td style="text-align:center">{_si(r["ocupacion"],"ocupacion")} {r["ocupacion"]:.1f}%</td>'
                f'<td style="text-align:center">{_si(r["no_show"],"no_show")} {r["no_show"]:.1f}%</td>'
                f'<td style="text-align:center">{_si(blq_pct,"bloqueo")} {blq_pct:.1f}%</td>'
                f'<td style="text-align:center">{_si(r["efectividad"],"efectividad")} {r["efectividad"]:.1f}%</td>'
                f'<td style="text-align:center">{r["rendimiento"]:.1f}</td></tr>\n')

    # ── Tipo atencion table ──
    ta_rows_html = ""
    if not df_kpis_ta.empty:
        for _, r in df_kpis_ta.iterrows():
            ta_rows_html += (
                f'<tr><td>{r["tipo_atencion"]}</td><td style="text-align:right">{r["total"]:,}</td><td style="text-align:right">{r["citados"]:,}</td>'
                f'<td style="text-align:right">{r["disponibles"]:,}</td><td style="text-align:right">{r["bloqueados"]:,}</td><td style="text-align:right">{r["completados"]:,}</td>'
                f'<td style="text-align:center">{_si(r["ocupacion"],"ocupacion")} {r["ocupacion"]:.1f}%</td>'
                f'<td style="text-align:center">{_si(r["no_show"],"no_show")} {r["no_show"]:.1f}%</td>'
                f'<td style="text-align:center">{_si(r.get("bloqueo",0),"bloqueo")} {r.get("bloqueo",0):.1f}%</td>'
                f'<td style="text-align:center">{_si(r["efectividad"],"efectividad")} {r["efectividad"]:.1f}%</td>'
                f'<td style="text-align:center">{r["rendimiento"]:.1f}</td>'
                f'<td style="text-align:center">{r.get("sobrecupo",0):.1f}%</td>'
                f'<td style="text-align:center">{r.get("agendamiento_remoto",0):.1f}%</td></tr>\n')

    # ── Alertas ──
    alertas_html = ""
    if not alertas_centro:
        alertas_html = '<div class="alert-box alert-green"><strong>&#9989; Sin brechas detectadas.</strong> Todos los indicadores dentro de umbrales aceptables.</div>'
    else:
        for a in alertas_centro:
            sem_a = a.get("semaforo", "gris")
            cls = "alert-red" if sem_a == "rojo" else "alert-yellow"
            ico = "&#128308;" if sem_a == "rojo" else "&#128993;"
            alertas_html += f'<div class="alert-box {cls}"><strong>{ico} {a["tipo"]}</strong>: {a["valor"]:.1f} {a["unidad"]} &mdash; {a["descripcion"]}</div>\n'

    # ── Conclusion ──
    conclusion_html = (
        f"<p>El centro <strong>{centro_sel}</strong> presenta <strong>{n_verde}</strong> indicadores en estado optimo "
        f"(<span style='color:#27AE60'>&#9679;</span>), <strong>{n_amarillo}</strong> en zona de observacion "
        f"(<span style='color:#F39C12'>&#9679;</span>) y <strong>{n_rojo}</strong> en brecha critica "
        f"(<span style='color:#E74C3C'>&#9679;</span>) durante el periodo analizado ({rango_meses}).</p>")
    if n_rojo > 0:
        kpis_rojos = [k.get("nombre", key) for key, k in kpis.items() if isinstance(k, dict) and k.get("semaforo") == "rojo"]
        conclusion_html += f"<p><strong>Indicadores criticos:</strong> {', '.join(kpis_rojos)}. Se recomienda intervencion inmediata.</p>"
    if n_amarillo > 0:
        kpis_ama = [k.get("nombre", key) for key, k in kpis.items() if isinstance(k, dict) and k.get("semaforo") == "amarillo"]
        conclusion_html += f"<p><strong>Indicadores en observacion:</strong> {', '.join(kpis_ama)}. Se sugiere monitoreo continuo.</p>"
    if n_rojo == 0 and n_amarillo == 0:
        conclusion_html += "<p>Todos los indicadores se encuentran dentro de los umbrales. Se recomienda mantener las estrategias actuales.</p>"

    # ══════════════════════════════════════════════════════════════
    # BUILD HTML
    # ══════════════════════════════════════════════════════════════
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Informe Productividad &mdash; {centro_sel}</title>
<script>{plotly_js_code}</script>
<style>
  :root {{
    --primary: #1B4F72; --secondary: #2E86C1; --accent: #1ABC9C;
    --green: #27AE60; --yellow: #F39C12; --red: #E74C3C;
    --bg: #F0F4F8; --card-bg: rgba(255,255,255,0.75);
    --text: #2C3E50; --text-light: #666;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  @keyframes fadeInUp {{ from {{ opacity: 0; transform: translateY(30px); }} to {{ opacity: 1; transform: translateY(0); }} }}
  @keyframes slideInLeft {{ from {{ opacity: 0; transform: translateX(-40px); }} to {{ opacity: 1; transform: translateX(0); }} }}
  @keyframes pulse {{ 0%,100% {{ transform: scale(1); }} 50% {{ transform: scale(1.05); }} }}
  @keyframes gradientShift {{ 0% {{ background-position: 0% 50%; }} 50% {{ background-position: 100% 50%; }} 100% {{ background-position: 0% 50%; }} }}
  body {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; color: var(--text); background: var(--bg); line-height: 1.7; }}
  .wrapper {{ display: flex; min-height: 100vh; }}

  /* ── Sidebar TOC ── */
  .sidebar {{ position: fixed; top: 0; left: 0; width: 280px; height: 100vh; background: linear-gradient(180deg, #1B4F72 0%, #154360 100%); color: white; overflow-y: auto; padding: 20px 0; z-index: 100; box-shadow: 4px 0 20px rgba(0,0,0,0.15); }}
  .sidebar-header {{ padding: 15px 20px 20px; text-align: center; border-bottom: 1px solid rgba(255,255,255,0.15); margin-bottom: 10px; }}
  .sidebar-header h3 {{ font-size: 0.95rem; font-weight: 600; letter-spacing: 0.5px; }}
  .sidebar-header p {{ font-size: 0.7rem; opacity: 0.7; margin-top: 4px; }}
  .toc {{ list-style: none; padding: 0; }}
  .toc li {{ padding: 0; }}
  .toc a {{ display: block; padding: 9px 20px; color: rgba(255,255,255,0.85); text-decoration: none; font-size: 0.8rem; border-left: 3px solid transparent; transition: all 0.25s; }}
  .toc a:hover, .toc a.active {{ background: rgba(255,255,255,0.1); border-left-color: var(--accent); color: white; }}
  .toc .toc-num {{ display: inline-block; width: 22px; font-weight: 700; color: var(--accent); }}

  /* ── Content ── */
  .content {{ margin-left: 280px; padding: 0; flex: 1; }}

  /* ── Hero / Header ── */
  .hero {{ background: linear-gradient(135deg, #1B4F72 0%, #2E86C1 50%, #1ABC9C 100%); background-size: 300% 300%; animation: gradientShift 8s ease infinite; color: white; padding: 50px 40px 40px; position: relative; overflow: hidden; }}
  .hero::before {{ content: ''; position: absolute; top: -50%; left: -50%; width: 200%; height: 200%; background: radial-gradient(circle at 30% 70%, rgba(255,255,255,0.05) 0%, transparent 60%); }}
  .hero h1 {{ font-size: 2.2rem; font-weight: 700; margin-bottom: 8px; position: relative; }}
  .hero p {{ font-size: 1rem; opacity: 0.9; position: relative; }}

  /* ── KPI Cards ── */
  .cards {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; padding: 30px 40px; margin-top: -30px; position: relative; z-index: 10; }}
  .card {{ background: var(--card-bg); backdrop-filter: blur(10px); border-radius: 16px; padding: 20px; text-align: center; box-shadow: 0 8px 32px rgba(0,0,0,0.08); border: 1px solid rgba(255,255,255,0.6); animation: fadeInUp 0.6s ease both; transition: transform 0.3s, box-shadow 0.3s; }}
  .card:hover {{ transform: translateY(-5px); box-shadow: 0 12px 40px rgba(0,0,0,0.12); }}
  .card:nth-child(1) {{ animation-delay: 0.1s; }} .card:nth-child(2) {{ animation-delay: 0.2s; }} .card:nth-child(3) {{ animation-delay: 0.3s; }}
  .card:nth-child(4) {{ animation-delay: 0.4s; }} .card:nth-child(5) {{ animation-delay: 0.5s; }}
  .card .val {{ font-size: 1.8rem; font-weight: 800; background: linear-gradient(135deg, var(--primary), var(--secondary)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
  .card .lbl {{ font-size: 0.75rem; color: var(--text-light); margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .card .bar {{ height: 4px; background: linear-gradient(90deg, var(--secondary), var(--accent)); border-radius: 2px; margin-top: 10px; }}

  /* ── Sections ── */
  .section {{ padding: 35px 40px; animation: fadeInUp 0.5s ease both; }}
  .section:nth-child(odd) {{ background: white; }}
  .section:nth-child(even) {{ background: #F8FAFB; }}
  .section h2 {{ color: var(--primary); font-size: 1.5rem; margin-bottom: 15px; padding-bottom: 8px; border-bottom: 3px solid var(--secondary); display: flex; align-items: center; gap: 10px; animation: slideInLeft 0.5s ease both; }}
  .section h2 .sec-num {{ display: inline-flex; align-items: center; justify-content: center; width: 32px; height: 32px; background: var(--secondary); color: white; border-radius: 50%; font-size: 0.85rem; flex-shrink: 0; }}
  .section h3 {{ color: var(--primary); font-size: 1.1rem; margin: 20px 0 10px; padding-left: 12px; border-left: 4px solid var(--accent); }}
  .section p {{ margin-bottom: 12px; }}
  .chart-container {{ margin: 20px 0; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.06); background: white; padding: 10px; }}

  /* ── Semaforo summary bar ── */
  .sem-bar {{ display: flex; gap: 20px; justify-content: center; margin: 20px 0; flex-wrap: wrap; }}
  .sem-pill {{ display: flex; align-items: center; gap: 8px; padding: 10px 24px; border-radius: 50px; font-weight: 600; font-size: 1.1rem; animation: pulse 2s ease infinite; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
  .sem-pill.green {{ background: #D5F5E3; color: #1E8449; }} .sem-pill.yellow {{ background: #FEF9E7; color: #9A7D0A; }} .sem-pill.red {{ background: #FDEDEC; color: #922B21; }}

  /* ── Tables ── */
  table {{ border-collapse: collapse; width: 100%; margin: 15px 0; font-size: 0.88rem; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 12px rgba(0,0,0,0.06); }}
  th {{ background: linear-gradient(135deg, #1B4F72, #2E86C1); color: white; padding: 11px 10px; text-align: left; font-weight: 600; font-size: 0.82rem; letter-spacing: 0.3px; }}
  td {{ padding: 9px 10px; border-bottom: 1px solid #E8ECF0; }}
  tr:nth-child(even) {{ background: #F8FAFB; }}
  tr:hover {{ background: #EBF5FB; transition: background 0.2s; }}

  /* ── Alerts ── */
  .alert-box {{ padding: 14px 18px; border-radius: 10px; margin-bottom: 10px; border-left: 5px solid; animation: fadeInUp 0.4s ease both; }}
  .alert-red {{ background: #FDEDEC; border-color: #E74C3C; }} .alert-yellow {{ background: #FEF9E7; border-color: #F39C12; }} .alert-green {{ background: #D5F5E3; border-color: #27AE60; }}

  /* ── Footer ── */
  .footer {{ background: linear-gradient(135deg, #1B4F72, #154360); color: rgba(255,255,255,0.8); padding: 30px 40px; text-align: center; font-size: 0.8rem; }}
  .footer strong {{ color: white; }}

  /* ── 3D badge ── */
  .badge-3d {{ display: inline-block; padding: 3px 10px; border-radius: 15px; font-size: 0.7rem; font-weight: 700; background: linear-gradient(135deg, #8E44AD, #3498DB); color: white; text-transform: uppercase; letter-spacing: 1px; margin-left: 8px; }}

  /* ── Print ── */
  @media print {{ .sidebar {{ display: none; }} .content {{ margin-left: 0; }} .hero {{ background: #1B4F72 !important; animation: none; }} }}
  @media (max-width: 900px) {{ .sidebar {{ display: none; }} .content {{ margin-left: 0; }} .cards {{ grid-template-columns: repeat(2, 1fr); }} }}
</style>
</head>
<body>
<div class="wrapper">

<!-- ═════ SIDEBAR TOC ═════ -->
<nav class="sidebar">
  <div class="sidebar-header">
    <h3>&#127973; SSMC &middot; Productividad APS</h3>
    <p>Servicio de Salud Metropolitano Central</p>
  </div>
  <ul class="toc">
    <li><a href="#sec1"><span class="toc-num">1</span> Resumen Ejecutivo</a></li>
    <li><a href="#sec2"><span class="toc-num">2</span> Semaforo de Indicadores</a></li>
    <li><a href="#sec3"><span class="toc-num">3</span> Estado de Cupos</a></li>
    <li><a href="#sec4"><span class="toc-num">4</span> Tasa de Ocupacion</a></li>
    <li><a href="#sec5"><span class="toc-num">5</span> Tasa de No-Show</a></li>
    <li><a href="#sec6"><span class="toc-num">6</span> Tasa de Bloqueo</a></li>
    <li><a href="#sec7"><span class="toc-num">7</span> Efectividad de Cita</a></li>
    <li><a href="#sec8"><span class="toc-num">8</span> Rendimiento Promedio</a></li>
    <li><a href="#sec9"><span class="toc-num">9</span> Sobrecupo</a></li>
    <li><a href="#sec10"><span class="toc-num">10</span> Cobertura Sectorial</a></li>
    <li><a href="#sec11"><span class="toc-num">11</span> Agendamiento Remoto</a></li>
    <li><a href="#sec12"><span class="toc-num">12</span> Horario Extendido</a></li>
    <li><a href="#sec13"><span class="toc-num">13</span> Tipo de Atencion</a></li>
    <li><a href="#sec14"><span class="toc-num">14</span> KPIs por Instrumento</a></li>
    <li><a href="#sec15"><span class="toc-num">15</span> Multi-KPI Mensual</a></li>
    <li><a href="#sec16"><span class="toc-num">16</span> Mapas de Calor</a></li>
    {"<li><a href='#sec17'><span class='toc-num'>17</span> Visualizacion 3D</a></li>" if _3d_html else ""}
    <li><a href="#sec18"><span class="toc-num">18</span> Alertas y Brechas</a></li>
    <li><a href="#sec19"><span class="toc-num">19</span> Marco Metodologico</a></li>
    <li><a href="#sec20"><span class="toc-num">20</span> Conclusion</a></li>
  </ul>
</nav>

<!-- ═════ MAIN CONTENT ═════ -->
<main class="content">

<!-- HERO -->
<div class="hero">
  <h1>&#128203; Informe Analitico de Productividad</h1>
  <p>{centro_sel} &middot; Periodo: {rango_meses} &middot; {n_meses} meses evaluados</p>
</div>

<!-- KPI CARDS -->
<div class="cards">
  <div class="card"><div class="val">{total_registros:,}</div><div class="lbl">Total Registros</div><div class="bar"></div></div>
  <div class="card"><div class="val">{citados:,}</div><div class="lbl">Citados</div><div class="bar"></div></div>
  <div class="card"><div class="val">{disponibles:,}</div><div class="lbl">Disponibles</div><div class="bar"></div></div>
  <div class="card"><div class="val">{bloqueados:,}</div><div class="lbl">Bloqueados</div><div class="bar"></div></div>
  <div class="card"><div class="val">{completados:,}</div><div class="lbl">Completados</div><div class="bar"></div></div>
</div>

<!-- Semaforo bar -->
<div class="sem-bar">
  <div class="sem-pill green"><span style="font-size:1.3em">&#9679;</span> {n_verde} Optimos</div>
  <div class="sem-pill yellow"><span style="font-size:1.3em">&#9679;</span> {n_amarillo} Observacion</div>
  <div class="sem-pill red"><span style="font-size:1.3em">&#9679;</span> {n_rojo} Criticos</div>
</div>

<!-- SEC 1 -->
<div class="section" id="sec1">
  <h2><span class="sec-num">1</span> Resumen Ejecutivo</h2>
  <p>El presente informe analiza la productividad del centro <strong>{centro_sel}</strong>
  durante el periodo <strong>{rango_meses}</strong> ({n_meses} meses),
  abarcando un total de <strong>{total_registros:,}</strong> registros de cupos programados en el sistema IRIS.
  De estos, <strong>{citados:,}</strong> corresponden a cupos citados, <strong>{disponibles:,}</strong>
  permanecieron disponibles (sin asignar), <strong>{bloqueados:,}</strong> fueron bloqueados administrativamente y
  <strong>{completados:,}</strong> registraron cita completada.</p>
</div>

<!-- SEC 2 -->
<div class="section" id="sec2">
  <h2><span class="sec-num">2</span> Semaforo de Indicadores</h2>
  <p>Estado de los 10 indicadores clave del modelo de productividad APS.
  <span style="color:#27AE60">&#9679;</span> Dentro de meta &middot;
  <span style="color:#F39C12">&#9679;</span> En observacion &middot;
  <span style="color:#E74C3C">&#9679;</span> Brecha critica.</p>
  <table>
  <tr><th style="text-align:center;width:50px">Estado</th><th>Indicador</th><th style="text-align:center">Valor</th><th style="text-align:center">Meta</th><th style="text-align:center">Alerta</th><th>Descripcion</th></tr>
  {kpi_rows_html}
  </table>
</div>

<!-- SEC 3 -->
<div class="section" id="sec3">
  <h2><span class="sec-num">3</span> Distribucion de Estado de Cupos</h2>
  <p>Composicion de cupos segun estado final (Citado, Disponible, Bloqueado).
  Muestra que proporcion de la oferta programada fue efectivamente utilizada.</p>
  <div class="chart-container">{ch.get("cupos", "<p>Sin datos</p>")}</div>
</div>

<!-- SEC 4 -->
<div class="section" id="sec4">
  <h2><span class="sec-num">4</span> Analisis de Tasa de Ocupacion</h2>
  <p>La <strong>Tasa de Ocupacion</strong> mide el porcentaje de cupos asignados a un paciente
  respecto del total disponible: <code>Citados / (Citados + Disponibles) x 100</code>.
  El centro registra una ocupacion de <strong>{v["ocupacion"]:.1f}%</strong> ({_st(v["ocupacion"], "ocupacion")}).
  Meta &ge; 65%, alerta &lt; 50%.</p>
  {"<div class='chart-container'>" + ch["ocu_mensual"] + "</div>" if "ocu_mensual" in ch else ""}
  {"<div class='chart-container'>" + ch["ocu_inst"] + "</div>" if "ocu_inst" in ch else ""}
</div>

<!-- SEC 5 -->
<div class="section" id="sec5">
  <h2><span class="sec-num">5</span> Analisis de Tasa de No-Show</h2>
  <p>La <strong>Tasa de No-Show</strong> representa el porcentaje de pacientes citados que no asistieron:
  <code>(Citados - Completados) / Citados x 100</code>.
  El centro presenta un No-Show de <strong>{v["no_show"]:.1f}%</strong> ({_st(v["no_show"], "no_show")}).
  Meta &le; 10%, alerta &gt; 15%.</p>
  {"<div class='chart-container'>" + ch["noshow"] + "</div>" if "noshow" in ch else ""}
</div>

<!-- SEC 6 -->
<div class="section" id="sec6">
  <h2><span class="sec-num">6</span> Analisis de Tasa de Bloqueo</h2>
  <p>La <strong>Tasa de Bloqueo</strong> mide cupos bloqueados administrativamente:
  <code>Bloqueados / Total x 100</code>.
  El centro registra <strong>{v["bloqueo"]:.1f}%</strong> ({_st(v["bloqueo"], "bloqueo")}).
  Meta &le; 10%, alerta &gt; 15%.</p>
  {"<div class='chart-container'>" + ch["bloqueo"] + "</div>" if "bloqueo" in ch else ""}
</div>

<!-- SEC 7 -->
<div class="section" id="sec7">
  <h2><span class="sec-num">7</span> Analisis de Efectividad de Cita</h2>
  <p>La <strong>Efectividad de Cita</strong> mide citas completadas exitosamente:
  <code>Completados / Citados x 100</code>.
  El centro alcanza <strong>{v["efectividad"]:.1f}%</strong> ({_st(v["efectividad"], "efectividad")}).
  Meta &ge; 88%, alerta &lt; 80%.</p>
  {"<div class='chart-container'>" + ch["efectividad"] + "</div>" if "efectividad" in ch else ""}
</div>

<!-- SEC 8 -->
<div class="section" id="sec8">
  <h2><span class="sec-num">8</span> Rendimiento Promedio por Instrumento</h2>
  <p>El <strong>Rendimiento Promedio</strong> indica los minutos promedio por atencion.
  El centro presenta <strong>{v["rendimiento"]:.1f} min/atencion</strong>.</p>
  <div class="chart-container">{ch.get("rendimiento", "<p>Sin datos</p>")}</div>
</div>

<!-- SEC 9 -->
<div class="section" id="sec9">
  <h2><span class="sec-num">9</span> Analisis de Cupos Sobrecupo</h2>
  <p>El <strong>Sobrecupo</strong> mide atenciones sobre la capacidad programada:
  <code>Sobrecupos / Total x 100</code>.
  El centro registra <strong>{v["sobrecupo"]:.1f}%</strong> ({_st(v["sobrecupo"], "sobrecupo")}).
  Meta &le; 5%, alerta &gt; 10%.</p>
</div>

<!-- SEC 10 -->
<div class="section" id="sec10">
  <h2><span class="sec-num">10</span> Cobertura Sectorial</h2>
  <p>La <strong>Cobertura Sectorial</strong> mide registros con sector territorial informado.
  Cobertura: <strong>{v["cobertura_sectorial"]:.1f}%</strong> ({_st(v["cobertura_sectorial"], "cobertura_sectorial")}).
  Meta &ge; 80%, alerta &lt; 60%.</p>
  <div class="chart-container">{ch.get("sector", "<p>Sin datos</p>")}</div>
</div>

<!-- SEC 11 -->
<div class="section" id="sec11">
  <h2><span class="sec-num">11</span> Agendamiento Remoto</h2>
  <p>Mide citas gestionadas por canales no presenciales:
  <code>(Telefonico + Telesalud) / Total x 100</code>.
  Resultado: <strong>{v["agendamiento_remoto"]:.1f}%</strong> ({_st(v["agendamiento_remoto"], "agendamiento_remoto")}).
  Meta &ge; 20%, alerta &lt; 5%.</p>
</div>

<!-- SEC 12: HORARIO EXTENDIDO (enhanced) -->
<div class="section" id="sec12">
  <h2><span class="sec-num">12</span> Horario Extendido y Apertura Sabatina</h2>
  <p>Uso de cupos a partir de las 18:00 hrs (jornada extendida con costo adicional):
  <code>Citados &ge;18h / (Citados + Disponibles &ge;18h) x 100</code>.
  Resultado: <strong>{v["ocupacion_extendida"]:.1f}%</strong> ({_st(v["ocupacion_extendida"], "ocupacion_extendida")}).
  Meta &ge; 50%, alerta &lt; 30%.</p>
  {_he_seg_html}
  {_he_ext_evo}
  {_he_sab_evo}
  {_he_prof_ext_html}
  {_he_prof_sab_html}
  {_he_inst_ext}
  {_he_inst_sab}
</div>

<!-- SEC 13 -->
<div class="section" id="sec13">
  <h2><span class="sec-num">13</span> Distribucion por Tipo de Atencion</h2>
  <div class="chart-container">{ch.get("tipo_atencion", "<p>Sin datos</p>")}</div>
  {"<h3>KPIs por Tipo de Atencion</h3><table><tr><th>Tipo Atencion</th><th>Total</th><th>Citados</th><th>Disp.</th><th>Bloq.</th><th>Complet.</th><th>Ocupacion</th><th>No-Show</th><th>Bloqueo</th><th>Efectividad</th><th>Rend.</th><th>Sobrecupo</th><th>Ag. Remoto</th></tr>" + ta_rows_html + "</table>" if ta_rows_html else ""}
  {"<h3>Evolucion Mensual por Tipo de Atencion</h3>" if "ta_evo_ocupacion" in ch else ""}
  {"<div class='chart-container'>" + ch["ta_evo_ocupacion"] + "</div>" if "ta_evo_ocupacion" in ch else ""}
  {"<div class='chart-container'>" + ch["ta_evo_no_show"] + "</div>" if "ta_evo_no_show" in ch else ""}
  {"<div class='chart-container'>" + ch["ta_evo_efectividad"] + "</div>" if "ta_evo_efectividad" in ch else ""}
</div>

<!-- SEC 14 -->
<div class="section" id="sec14">
  <h2><span class="sec-num">14</span> KPIs por Instrumento / Profesional</h2>
  {"<table><tr><th>Instrumento</th><th>Total</th><th>Citados</th><th>Disp.</th><th>Bloq.</th><th>Complet.</th><th>Ocupacion</th><th>No-Show</th><th>Bloqueo</th><th>Efectividad</th><th>Rend.</th></tr>" + inst_rows_html + "</table>" if inst_rows_html else "<p>Sin datos.</p>"}
  {"<h3>Evolucion Mensual por Instrumento</h3>" if "inst_evo_ocupacion" in ch else ""}
  {"<div class='chart-container'>" + ch["inst_evo_ocupacion"] + "</div>" if "inst_evo_ocupacion" in ch else ""}
  {"<div class='chart-container'>" + ch["inst_evo_no_show"] + "</div>" if "inst_evo_no_show" in ch else ""}
  {"<div class='chart-container'>" + ch["inst_evo_efectividad"] + "</div>" if "inst_evo_efectividad" in ch else ""}
  {"<div class='chart-container'>" + ch["inst_evo_rendimiento"] + "</div>" if "inst_evo_rendimiento" in ch else ""}
  {"<div class='chart-container'>" + ch["inst_evo_citados"] + "</div>" if "inst_evo_citados" in ch else ""}
</div>

<!-- SEC 15 -->
{"<div class='section' id='sec15'><h2><span class='sec-num'>15</span> Evolucion Conjunta de KPIs</h2><p>Ocupacion, No-Show y Bloqueo mes a mes.</p><div class='chart-container'>" + ch["multi_kpi"] + "</div></div>" if "multi_kpi" in ch else "<div class='section' id='sec15'><h2><span class='sec-num'>15</span> Evolucion Conjunta</h2><p>No hay datos suficientes.</p></div>"}

<!-- SEC 16 -->
{"<div class='section' id='sec16'><h2><span class='sec-num'>16</span> Mapas de Calor: Instrumento x Mes</h2><h3>Ocupacion por Instrumento y Mes</h3><div class='chart-container'>" + ch["heatmap"] + "</div>" + ("<h3>No-Show por Instrumento y Mes</h3><div class='chart-container'>" + ch["heatmap_noshow"] + "</div>" if "heatmap_noshow" in ch else "") + ("<h3>Efectividad por Instrumento y Mes</h3><div class='chart-container'>" + ch["heatmap_efectividad"] + "</div>" if "heatmap_efectividad" in ch else "") + ("<h3>Bloqueo por Instrumento y Mes</h3><div class='chart-container'>" + ch["heatmap_bloqueo"] + "</div>" if "heatmap_bloqueo" in ch else "") + "</div>" if "heatmap" in ch else "<div class='section' id='sec16'><h2><span class='sec-num'>16</span> Mapas de Calor</h2><p>No hay datos suficientes.</p></div>"}

<!-- SEC 17: 3D VISUALIZATIONS -->
{"<div class='section' id='sec17'><h2><span class='sec-num'>17</span> Visualizacion 3D Interactiva <span class='badge-3d'>3D</span></h2><p>Representaciones tridimensionales de las metricas clave. Arrastre con el mouse para rotar, scroll para zoom.</p><h3>Superficie 3D: Evolucion de KPIs</h3><div class='chart-container'>" + _3d_html + "</div>" + ("<h3>Burbuja 3D: Ocupacion x No-Show x Bloqueo</h3><div class='chart-container'>" + _3d_scatter + "</div>" if _3d_scatter else "") + "</div>" if _3d_html else ""}

<!-- SEC 18 -->
<div class="section" id="sec18">
  <h2><span class="sec-num">18</span> Alertas y Brechas del Centro</h2>
  {alertas_html}
</div>

<!-- SEC 19 -->
<div class="section" id="sec19">
  <h2><span class="sec-num">19</span> Marco Metodologico y Referencias</h2>
  <p>Los indicadores utilizados se fundamentan en marcos internacionales de medicion de productividad en APS:</p>
  <h3>Marco OCDE: Health at a Glance</h3>
  <p>La OCDE agrupa indicadores de desempeno sanitario en cinco dimensiones: acceso, calidad, eficiencia, equidad y resultados.
  Ocupacion, bloqueo y disponibilidad se alinean con <em>eficiencia</em>; No-Show y efectividad con <em>acceso efectivo</em>;
  cobertura sectorial y agendamiento remoto con <em>equidad y modernizacion</em> (OECD, 2023).</p>
  <h3>Marco OMS: Atencion Primaria</h3>
  <p>La OMS establece que la productividad en APS debe evaluarse considerando utilizacion optima, continuidad del cuidado
  y capacidad resolutiva del primer nivel (WHO &amp; UNICEF, 2020).</p>
  <h3>Fundamentacion de Umbrales</h3>
  <table>
  <tr><th>Indicador</th><th>Meta</th><th>Fuente</th></tr>
  <tr><td>Tasa de Ocupacion</td><td>&ge; 65%</td><td>Siciliani et al. (2014), OECD</td></tr>
  <tr><td>Tasa de No-Show</td><td>&le; 10%</td><td>Dantas et al. (2018)</td></tr>
  <tr><td>Tasa de Bloqueo</td><td>&le; 10%</td><td>Murray &amp; Berwick (2003)</td></tr>
  <tr><td>Efectividad de Cita</td><td>&ge; 88%</td><td>Starfield et al. (2005)</td></tr>
  <tr><td>Ocupacion H. Extendido</td><td>&ge; 50%</td><td>MINSAL (2023)</td></tr>
  </table>
  <h3>Referencias (APA 7)</h3>
  <p style="font-size:0.82em;line-height:1.6">
  Dantas, L. F., Fleck, J. L., Cyrino Oliveira, F. L., &amp; Hamacher, S. (2018). No-shows in appointment scheduling: A systematic literature review. <em>Health Policy, 122</em>(4), 412-421.<br>
  MINSAL. (2023). <em>Orientaciones tecnicas para la gestion de la atencion primaria de salud</em>. Subsecretaria de Redes Asistenciales.<br>
  Murray, M., &amp; Berwick, D. M. (2003). Advanced access: Reducing waiting and delays in primary care. <em>JAMA, 289</em>(8), 1035-1040.<br>
  OECD. (2023). <em>Health at a Glance 2023: OECD Indicators</em>. OECD Publishing.<br>
  Siciliani, L., Borowitz, M., &amp; Moran, V. (2014). <em>Waiting time policies in the health sector: What works?</em> OECD Publishing.<br>
  Starfield, B., Shi, L., &amp; Macinko, J. (2005). Contribution of primary care to health systems and health. <em>The Milbank Quarterly, 83</em>(3), 457-502.<br>
  WHO &amp; UNICEF. (2020). <em>Operational framework for primary health care</em>. WHO.
  </p>
</div>

<!-- SEC 20 -->
<div class="section" id="sec20">
  <h2><span class="sec-num">20</span> Conclusion del Informe</h2>
  {conclusion_html}
</div>

<!-- FOOTER -->
<div class="footer">
  <p><strong>Informe generado automaticamente por el Sistema de Analisis de Productividad APS</strong><br>
  Servicio de Salud Metropolitano Central &middot; {centro_sel} &middot; Periodo: {rango_meses}<br>
  Fecha de generacion: {fecha_gen}</p>
</div>

</main>
</div>

<!-- Sidebar active-link tracking -->
<script>
document.addEventListener('scroll', function() {{
  const sections = document.querySelectorAll('.section[id]');
  const links = document.querySelectorAll('.toc a');
  let current = '';
  sections.forEach(s => {{ if (s.getBoundingClientRect().top <= 120) current = s.id; }});
  links.forEach(l => {{
    l.classList.remove('active');
    if (l.getAttribute('href') === '#' + current) l.classList.add('active');
  }});
}});
</script>
</body>
</html>"""
    return html


# ══════════════════════════════════════════════════════════════
#  PDF REPORT
# ══════════════════════════════════════════════════════════════

def generar_pdf_informe(
    centro_sel, rango_meses, n_meses, total_registros,
    citados, disponibles, bloqueados, completados,
    kpis, df_centro, df_inst_c, df_kpis_ta,
    alertas_centro, n_verde, n_amarillo, n_rojo,
    kpis_por_mes_fn=None,
) -> bytes:
    from fpdf import FPDF
    from src.kpis import semaforo
    from src.charts import (
        chart_estado_cupos, chart_evolucion_mensual, chart_noshow_vs_umbral,
        chart_rendimiento_instrumento, chart_sector, chart_tipo_atencion,
        chart_multi_kpi, chart_heatmap_instrumento_mes,
    )
    from src.kpis import (
        kpis_horario_segmentado,
        kpis_profesional_sabatino, kpis_profesional_extendido,
        kpis_sabatino_por_mes, kpis_extendido_por_mes,
        kpis_sabatino_por_instrumento, kpis_extendido_por_instrumento,
    )

    fecha_gen = datetime.now().strftime("%d/%m/%Y %H:%M")

    # ── Helpers ──
    _kaleido_ok = True

    def _fig_to_png(fig, w=900, h=450):
        nonlocal _kaleido_ok
        for trace in fig.data:
            tt = trace.type
            if tt in ("bar", "scatter", "waterfall"):
                if trace.text is not None and not getattr(trace, "textposition", None):
                    trace.textposition = "outside"
                if trace.text is not None:
                    trace.textfont = dict(size=11)
            elif tt == "pie":
                trace.textinfo = "label+percent+value"
                trace.textfont = dict(size=11)
        fig.update_layout(width=w, height=h, template="plotly_white",
                          paper_bgcolor="white", plot_bgcolor="white")
        if not _kaleido_ok:
            return None
        try:
            return fig.to_image(format="png", scale=2, engine="kaleido")
        except Exception:
            pass
        try:
            return fig.to_image(format="png", engine="kaleido")
        except Exception:
            pass
        try:
            return fig.to_image(format="png", scale=2)
        except Exception:
            _kaleido_ok = False
            return None

    def _sem_t(val, kpi):
        s = semaforo(val, kpi)
        return {"verde": "Optimo", "amarillo": "Observacion", "rojo": "Critico"}.get(s, "-")

    def _sem_c(val, kpi):
        s = semaforo(val, kpi)
        return {"verde": (39, 174, 96), "amarillo": (243, 156, 18), "rojo": (231, 76, 60)}.get(s, (149, 165, 166))

    # ── Colors ──
    AZUL_OSCURO = (27, 79, 114)
    AZUL_MEDIO = (46, 134, 193)
    AZUL_CLARO = (174, 214, 241)
    BLANCO = (255, 255, 255)
    GRIS_TEXTO = (44, 62, 80)
    GRIS_CLARO = (248, 249, 250)
    VERDE = (39, 174, 96)
    AMARILLO = (243, 156, 18)
    ROJO = (231, 76, 60)

    _PDF_UNICODE_MAP = str.maketrans({
        "\u2265": ">=", "\u2264": "<=", "\u00d7": "x", "\u00f7": "/",
        "\u2013": "-", "\u2014": "--", "\u2018": "'", "\u2019": "'",
        "\u201c": '"', "\u201d": '"', "\u2026": "...", "\u00b7": ".",
        "\u00e1": "a", "\u00e9": "e", "\u00ed": "i", "\u00f3": "o", "\u00fa": "u",
        "\u00c1": "A", "\u00c9": "E", "\u00cd": "I", "\u00d3": "O", "\u00da": "U",
        "\u00f1": "n", "\u00d1": "N",
    })

    def _ps(txt):
        return str(txt).translate(_PDF_UNICODE_MAP)

    class InformePDF(FPDF):
        def __init__(self):
            super().__init__(orientation="P", unit="mm", format="A4")
            self.set_auto_page_break(auto=True, margin=20)
            self._is_cover = False
            self._toc_entries = []

        def header(self):
            if self._is_cover or self.page_no() <= 2:
                return
            self.set_fill_color(*AZUL_OSCURO)
            self.rect(0, 0, 210, 12, "F")
            self.set_font("Helvetica", "B", 7)
            self.set_text_color(*BLANCO)
            self.set_xy(10, 3)
            self.cell(0, 5, _ps(f"Informe de Productividad APS  |  {centro_sel}  |  {rango_meses}"), align="L")
            self.set_xy(0, 3)
            self.cell(200, 5, f"Pag. {self.page_no()}", align="R")
            self.set_draw_color(*AZUL_MEDIO)
            self.set_line_width(0.5)
            self.line(10, 13, 200, 13)
            self.set_y(18)

        def footer(self):
            if self._is_cover or self.page_no() <= 2:
                return
            self.set_y(-15)
            self.set_draw_color(*AZUL_CLARO)
            self.set_line_width(0.3)
            self.line(10, self.get_y(), 200, self.get_y())
            self.set_font("Helvetica", "I", 7)
            self.set_text_color(150, 150, 150)
            self.cell(0, 8, f"Servicio de Salud Metropolitano Central  |  Generado: {fecha_gen}", align="C")

        def section_title(self, num, title):
            self._toc_entries.append((num, title, self.page_no()))
            self.set_font("Helvetica", "B", 13)
            self.set_text_color(*AZUL_OSCURO)
            y0 = self.get_y()
            self.set_fill_color(*AZUL_MEDIO)
            self.rect(10, y0, 3, 8, "F")
            self.set_xy(16, y0)
            self.cell(0, 8, _ps(f"{num}. {title}"))
            self.ln(12)

        def body_text(self, txt):
            self.set_font("Helvetica", "", 9)
            self.set_text_color(*GRIS_TEXTO)
            self.multi_cell(0, 5, _ps(txt))
            self.ln(2)

        def add_chart(self, png_bytes, w=180, title_hint=""):
            if png_bytes is None:
                y = self.get_y()
                self.set_fill_color(245, 245, 245)
                self.set_draw_color(200, 200, 200)
                self.rect(15, y, 180, 25, "FD")
                self.set_font("Helvetica", "I", 9)
                self.set_text_color(130, 130, 130)
                self.set_xy(15, y + 5)
                self.cell(180, 8, f"[Grafico no disponible: {title_hint}]", align="C")
                self.set_y(y + 28)
                return
            img_stream = io.BytesIO(png_bytes)
            x = (210 - w) / 2
            self.image(img_stream, x=x, w=w)
            self.ln(5)

        def kpi_card_row(self, cards):
            n = len(cards)
            card_w = 36
            gap = 2
            total_w = n * card_w + (n - 1) * gap
            x_start = (210 - total_w) / 2
            y_start = self.get_y()
            for i, (val, label) in enumerate(cards):
                x = x_start + i * (card_w + gap)
                self.set_fill_color(220, 220, 220)
                self.rect(x + 0.5, y_start + 0.5, card_w, 22, style="F")
                self.set_fill_color(*BLANCO)
                self.set_draw_color(*AZUL_CLARO)
                self.rect(x, y_start, card_w, 22, style="FD")
                self.set_fill_color(*AZUL_MEDIO)
                self.rect(x, y_start, card_w, 3, "F")
                self.set_font("Helvetica", "B", 12)
                self.set_text_color(*AZUL_OSCURO)
                self.set_xy(x, y_start + 4)
                self.cell(card_w, 7, _ps(str(val)), align="C")
                self.set_font("Helvetica", "", 6)
                self.set_text_color(100, 100, 100)
                self.set_xy(x, y_start + 12)
                self.cell(card_w, 5, _ps(label), align="C")
            self.set_y(y_start + 28)

    # ── Create PDF ──
    pdf = InformePDF()
    pdf.set_left_margin(10)
    pdf.set_right_margin(10)

    # ═══ PORTADA ═══
    pdf._is_cover = True
    pdf.add_page()

    for i in range(297):
        ratio = i / 297
        r = int(AZUL_OSCURO[0] + (AZUL_MEDIO[0] - AZUL_OSCURO[0]) * ratio)
        g = int(AZUL_OSCURO[1] + (AZUL_MEDIO[1] - AZUL_OSCURO[1]) * ratio)
        b = int(AZUL_OSCURO[2] + (AZUL_MEDIO[2] - AZUL_OSCURO[2]) * ratio)
        pdf.set_fill_color(r, g, b)
        pdf.rect(0, i, 210, 1.1, "F")

    pdf.set_draw_color(255, 255, 255)
    pdf.set_line_width(0.15)
    for offset in range(-300, 300, 40):
        pdf.line(offset, 0, offset + 210, 297)

    pdf.rect(15, 20, 180, 257, "D")
    pdf.rect(17, 22, 176, 253, "D")

    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(30, 40, 150, 1.5, "F")

    pdf.set_fill_color(255, 255, 255)
    pdf.rect(80, 30, 50, 18, style="F")
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.set_xy(80, 33)
    pdf.cell(50, 5, "INFORME ANALITICO", align="C")
    pdf.set_font("Helvetica", "", 7)
    pdf.set_xy(80, 39)
    pdf.cell(50, 5, "PRODUCTIVIDAD APS", align="C")

    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(*BLANCO)
    pdf.set_xy(20, 65)
    pdf.multi_cell(170, 14, "Informe Analitico\nde Productividad", align="C")

    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(60, 100, 90, 1, "F")

    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*BLANCO)
    pdf.set_xy(20, 110)
    pdf.multi_cell(170, 10, _ps(centro_sel), align="C")

    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(*AZUL_CLARO)
    pdf.set_xy(20, 140)
    pdf.cell(170, 8, _ps(f"Periodo: {rango_meses}"), align="C")
    pdf.set_xy(20, 150)
    pdf.cell(170, 8, f"{n_meses} meses evaluados", align="C")

    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(75, 165, 60, 0.5, "F")

    # Cover cards
    card_data_cover = [
        (f"{total_registros:,}", "Registros"), (f"{citados:,}", "Citados"),
        (f"{disponibles:,}", "Disponibles"), (f"{bloqueados:,}", "Bloqueados"),
        (f"{completados:,}", "Completados"),
    ]
    card_w = 30; gap = 4
    total_cards_w = 5 * card_w + 4 * gap
    x_start = (210 - total_cards_w) / 2
    y_cards = 175
    for i, (val, lbl) in enumerate(card_data_cover):
        x = x_start + i * (card_w + gap)
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

    # Semaforo on cover
    y_sem = 210
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*BLANCO)
    pdf.set_xy(20, y_sem)
    pdf.cell(170, 7, "Estado General de Indicadores", align="C")
    pdf.ln(10)
    sem_items = [(n_verde, "Optimos", VERDE), (n_amarillo, "En Observacion", AMARILLO), (n_rojo, "Criticos", ROJO)]
    box_w = 45; gap_s = 8
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

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*AZUL_CLARO)
    pdf.set_xy(20, 255)
    pdf.cell(170, 5, "Servicio de Salud Metropolitano Central", align="C")
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_xy(20, 262)
    pdf.cell(170, 5, f"Generado: {fecha_gen}", align="C")
    pdf.set_fill_color(*AZUL_CLARO)
    pdf.rect(30, 250, 150, 0.5, "F")

    pdf._is_cover = False

    # ═══ TABLE OF CONTENTS (placeholder - page 2) ═══
    toc_page_no = 2
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.cell(0, 12, "Indice de Contenidos", align="C")
    pdf.ln(15)
    # We'll fill this after rendering all content

    # ═══ CHARTS ═══
    charts_png = {}
    fig1 = chart_estado_cupos(df_centro)
    fig1.update_layout(height=400)
    charts_png["cupos"] = _fig_to_png(fig1, 850, 400)

    df_meses_c = kpis_por_mes_fn(df_centro) if kpis_por_mes_fn else pd.DataFrame()
    if not df_meses_c.empty and len(df_meses_c) >= 2:
        charts_png["ocu_mensual"] = _fig_to_png(chart_evolucion_mensual(df_meses_c, "ocupacion", "Tasa de Ocupacion", 65, 50), 850, 400)
        charts_png["noshow"] = _fig_to_png(chart_noshow_vs_umbral(df_meses_c), 850, 400)
        charts_png["bloqueo"] = _fig_to_png(chart_evolucion_mensual(df_meses_c, "bloqueo", "Tasa de Bloqueo", 10, 15), 850, 400)
        charts_png["efectividad"] = _fig_to_png(chart_evolucion_mensual(df_meses_c, "efectividad", "Efectividad de Cita", 88, 80), 850, 400)
        fig10 = chart_multi_kpi(df_meses_c)
        for trace in fig10.data:
            if trace.type == "scatter" and trace.y is not None:
                trace.mode = "lines+markers+text"
                trace.text = [f"{v:.1f}%" for v in trace.y]
                trace.textposition = "top center"
                trace.textfont = dict(size=9)
        charts_png["multi_kpi"] = _fig_to_png(fig10, 850, 430)

    if not df_inst_c.empty:
        df_plot = df_inst_c.sort_values("ocupacion")
        colors_ocu = ["#27AE60" if x >= 65 else "#F39C12" if x >= 50 else "#E74C3C" for x in df_plot["ocupacion"]]
        fig3 = go.Figure(go.Bar(
            x=df_plot["ocupacion"], y=df_plot["instrumento"].str[:30],
            orientation="h", marker_color=colors_ocu,
            text=[f"{x:.1f}%" for x in df_plot["ocupacion"]], textposition="outside"))
        fig3.add_vline(x=65, line_dash="dash", line_color="#27AE60", annotation_text="Meta 65%")
        fig3.update_layout(title="Ocupacion por Instrumento", template="plotly_white",
                           xaxis=dict(title="Ocupacion (%)", range=[0, 105]), yaxis=dict(title=""))
        h3 = max(400, len(df_plot) * 40 + 100)
        charts_png["ocu_inst"] = _fig_to_png(fig3, 850, h3)

    charts_png["rendimiento"] = _fig_to_png(chart_rendimiento_instrumento(df_centro), 850,
                                             max(400, len(df_inst_c) * 40 + 100) if not df_inst_c.empty else 400)
    charts_png["sector"] = _fig_to_png(chart_sector(df_centro), 850, 400)

    fig9 = chart_tipo_atencion(df_centro, top_n=15)
    for trace in fig9.data:
        if trace.type == "bar" and trace.text is None:
            trace.text = [f"{v:,.0f}" for v in (trace.x if trace.orientation == "h" else trace.y)]
            trace.textposition = "outside"
    charts_png["tipo_atencion"] = _fig_to_png(fig9, 850, 480)

    if "MES_NUM" in df_centro.columns and "INSTRUMENTO" in df_centro.columns:
        n_i = df_centro["INSTRUMENTO"].nunique()
        charts_png["heatmap"] = _fig_to_png(chart_heatmap_instrumento_mes(df_centro), 850, max(450, n_i * 38 + 120))

    # Extended hour charts
    _df_ext_mes = kpis_extendido_por_mes(df_centro)
    if not _df_ext_mes.empty and len(_df_ext_mes) >= 2:
        charts_png["ext_evo"] = _fig_to_png(chart_evolucion_mensual(_df_ext_mes, "ocupacion", "Ocupacion Extendido", 50, 30), 850, 380)

    _df_sab_mes = kpis_sabatino_por_mes(df_centro)
    if not _df_sab_mes.empty and len(_df_sab_mes) >= 2:
        charts_png["sab_evo"] = _fig_to_png(chart_evolucion_mensual(_df_sab_mes, "ocupacion", "Ocupacion Sabatina", 50, 30), 850, 380)

    # ═══ TABLE HELPER (centered) ═══
    def _draw_table_header(headers, col_widths, x_off):
        pdf.set_fill_color(*AZUL_OSCURO)
        pdf.set_text_color(*BLANCO)
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_x(x_off)
        for i, h in enumerate(headers):
            pdf.cell(col_widths[i], 7, _ps(h), border=1, align="C", fill=True)
        pdf.ln()

    def _draw_table(headers, rows, col_widths=None, align_cols=None):
        n_cols = len(headers)
        if col_widths is None:
            col_widths = [190 / n_cols] * n_cols
        if align_cols is None:
            align_cols = ["C"] * n_cols

        table_w = sum(col_widths)
        x_off = (210 - table_w) / 2  # CENTER the table
        row_h = 7
        bottom_limit = 297 - 20

        _draw_table_header(headers, col_widths, x_off)

        pdf.set_font("Helvetica", "", 7)
        for row_idx, row in enumerate(rows):
            if pdf.get_y() + row_h > bottom_limit:
                pdf.add_page()
                _draw_table_header(headers, col_widths, x_off)
                pdf.set_font("Helvetica", "", 7)

            bg = GRIS_CLARO if row_idx % 2 == 0 else BLANCO
            pdf.set_fill_color(*bg)
            pdf.set_text_color(*GRIS_TEXTO)
            pdf.set_x(x_off)
            for i, val in enumerate(row):
                pdf.cell(col_widths[i], row_h, _ps(str(val)), border=1, align=align_cols[i], fill=True)
            pdf.ln()

    # ═══ SEMAFORO TABLE HELPER (with colored dots) ═══
    _KPI_FORMULA = {
        "ocupacion": "Citados / (Citados + Disponibles) x 100",
        "no_show": "(Citados - Completados) / Citados x 100",
        "bloqueo": "Bloqueados / Total x 100",
        "efectividad": "Completados / Citados x 100",
        "rendimiento": "Promedio minutos por atencion",
        "sobrecupo": "Sobrecupos / Total x 100",
        "cobertura_sectorial": "Registros con sector / Total x 100",
        "agendamiento_remoto": "(Telefonico + Telesalud) / Total x 100",
        "variacion_mensual": "Cambio mes a mes en tasa de ocupacion (pp)",
        "ocupacion_extendida": "Citados >=18h / (Citados+Disp >=18h) x 100",
    }

    def _draw_semaforo_table():
        sem_headers = ["Estado", "Indicador", "Valor", "Meta", "Alerta", "Calculo"]
        col_w = [12, 38, 20, 16, 16, 88]
        table_w = sum(col_w)
        x_off = (210 - table_w) / 2

        # Header
        pdf.set_fill_color(*AZUL_OSCURO)
        pdf.set_text_color(*BLANCO)
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_x(x_off)
        for i, h in enumerate(sem_headers):
            pdf.cell(col_w[i], 7, _ps(h), border=1, align="C", fill=True)
        pdf.ln()

        # Rows
        pdf.set_font("Helvetica", "", 7)
        for row_idx, (key, label) in enumerate(KPI_ORDER):
            k = kpis.get(key, {})
            valor = k.get("valor", 0)
            unidad = k.get("unidad", "%")
            sem = k.get("semaforo", "gris")
            meta = k.get("umbral_ok")
            alerta = k.get("umbral_alerta")
            formula = _KPI_FORMULA.get(key, "")

            # Row background based on semaforo
            if sem == "rojo":
                bg = (253, 237, 236)
            elif sem == "amarillo":
                bg = (254, 249, 231)
            elif sem == "verde":
                bg = (234, 250, 241)
            else:
                bg = BLANCO
            pdf.set_fill_color(*bg)
            pdf.set_text_color(*GRIS_TEXTO)

            y_row = pdf.get_y()
            pdf.set_x(x_off)

            # Estado: colored circle
            sem_color = {"verde": VERDE, "amarillo": AMARILLO, "rojo": ROJO}.get(sem, (149, 165, 166))
            pdf.cell(col_w[0], 7, "", border=1, align="C", fill=True)
            # Draw circle
            cx = x_off + col_w[0] / 2
            cy = y_row + 3.5
            pdf.set_fill_color(*sem_color)
            pdf.ellipse(cx - 2, cy - 2, 4, 4, "F")
            pdf.set_fill_color(*bg)

            pdf.set_xy(x_off + col_w[0], y_row)
            pdf.set_font("Helvetica", "B", 7)
            pdf.cell(col_w[1], 7, _ps(label), border=1, align="L", fill=True)
            pdf.set_font("Helvetica", "", 7)
            pdf.cell(col_w[2], 7, f"{valor:.1f}{unidad}", border=1, align="C", fill=True)
            meta_s = f"{meta}{unidad}" if meta is not None else "-"
            alerta_s = f"{alerta}{unidad}" if alerta is not None else "-"
            pdf.cell(col_w[3], 7, _ps(str(meta_s)), border=1, align="C", fill=True)
            pdf.cell(col_w[4], 7, _ps(str(alerta_s)), border=1, align="C", fill=True)
            pdf.set_font("Helvetica", "", 6)
            pdf.cell(col_w[5], 7, _ps(formula), border=1, align="L", fill=True)
            pdf.set_font("Helvetica", "", 7)
            pdf.ln()

    # ═══ CONTENT ═══

    # ── S1: Resumen Ejecutivo ──
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
        (f"{total_registros:,}", "Total Registros"), (f"{citados:,}", "Citados"),
        (f"{disponibles:,}", "Disponibles"), (f"{bloqueados:,}", "Bloqueados"),
        (f"{completados:,}", "Completados"),
    ])

    # ── S2: Semaforo ──
    pdf.section_title(2, "Semaforo de Indicadores")
    pdf.body_text("Estado de los 10 indicadores clave del modelo de productividad APS. "
                  "Verde: dentro de meta. Amarillo: en observacion. Rojo: brecha critica.")
    _draw_semaforo_table()

    # ── S3: Estado de Cupos ──
    pdf.add_page()
    pdf.section_title(3, "Distribucion de Estado de Cupos")
    pdf.body_text("Composicion de cupos segun estado final (Citado, Disponible, Bloqueado). "
                  "Muestra que proporcion de la oferta programada fue efectivamente utilizada.")
    pdf.add_chart(charts_png.get("cupos"), title_hint="Estado de Cupos")

    # ── S4: Ocupacion ──
    v_ocu = kpis.get("ocupacion", {}).get("valor", 0)
    pdf.section_title(4, "Analisis de Tasa de Ocupacion")
    pdf.body_text(f"La Tasa de Ocupacion mide el porcentaje de cupos asignados respecto del total "
                  f"disponible: Citados / (Citados + Disponibles) x 100. El centro registra una "
                  f"ocupacion de {v_ocu:.1f}% ({_sem_t(v_ocu, 'ocupacion')}). Meta >= 65%, alerta < 50%.")
    pdf.add_chart(charts_png.get("ocu_mensual"), title_hint="Ocupacion Mensual")
    if pdf.get_y() > 160:
        pdf.add_page()
    pdf.add_chart(charts_png.get("ocu_inst"), title_hint="Ocupacion por Instrumento")

    # ── S5: No-Show ──
    v_ns = kpis.get("no_show", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(5, "Analisis de Tasa de No-Show")
    pdf.body_text(f"La Tasa de No-Show representa el porcentaje de pacientes citados que no asistieron: "
                  f"(Citados - Completados) / Citados x 100. El centro presenta un No-Show de "
                  f"{v_ns:.1f}% ({_sem_t(v_ns, 'no_show')}). Meta <= 10%, alerta > 15%.")
    pdf.add_chart(charts_png.get("noshow"), title_hint="No-Show Mensual")

    # ── S6: Bloqueo ──
    v_bloq = kpis.get("bloqueo", {}).get("valor", 0)
    pdf.section_title(6, "Analisis de Tasa de Bloqueo")
    pdf.body_text(f"La Tasa de Bloqueo mide cupos bloqueados administrativamente: "
                  f"Bloqueados / Total x 100. El centro registra {v_bloq:.1f}% "
                  f"({_sem_t(v_bloq, 'bloqueo')}). Meta <= 10%, alerta > 15%.")
    pdf.add_chart(charts_png.get("bloqueo"), title_hint="Tasa de Bloqueo")

    # ── S7: Efectividad ──
    v_efec = kpis.get("efectividad", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(7, "Analisis de Efectividad de Cita")
    pdf.body_text(f"La Efectividad de Cita mide citas completadas exitosamente: "
                  f"Completados / Citados x 100. El centro alcanza {v_efec:.1f}% "
                  f"({_sem_t(v_efec, 'efectividad')}). Meta >= 88%, alerta < 80%.")
    pdf.add_chart(charts_png.get("efectividad"), title_hint="Efectividad de Cita")

    # ── S8: Rendimiento ──
    v_rend = kpis.get("rendimiento", {}).get("valor", 0)
    pdf.section_title(8, "Rendimiento Promedio por Instrumento")
    pdf.body_text(f"El Rendimiento Promedio indica los minutos promedio por atencion. "
                  f"El centro presenta {v_rend:.1f} min/atencion.")
    if pdf.get_y() > 140:
        pdf.add_page()
    pdf.add_chart(charts_png.get("rendimiento"), title_hint="Rendimiento por Instrumento")

    # ── S9: Sobrecupo ──
    v_sobre = kpis.get("sobrecupo", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(9, "Analisis de Cupos Sobrecupo")
    pdf.body_text(f"El Sobrecupo mide atenciones sobre la capacidad programada: "
                  f"Sobrecupos / Total x 100. El centro registra {v_sobre:.1f}% "
                  f"({_sem_t(v_sobre, 'sobrecupo')}). Meta <= 5%, alerta > 10%.")

    # ── S10: Cobertura Sectorial ──
    v_cob = kpis.get("cobertura_sectorial", {}).get("valor", 0)
    pdf.section_title(10, "Cobertura Sectorial")
    pdf.body_text(f"La Cobertura Sectorial mide registros con sector territorial informado: "
                  f"Con sector / Total x 100. Cobertura: {v_cob:.1f}% "
                  f"({_sem_t(v_cob, 'cobertura_sectorial')}). Meta >= 80%, alerta < 60%.")
    pdf.add_chart(charts_png.get("sector"), title_hint="Distribucion Sectorial")

    # ── S11: Agendamiento Remoto ──
    v_ag = kpis.get("agendamiento_remoto", {}).get("valor", 0)
    pdf.section_title(11, "Agendamiento Remoto")
    pdf.body_text(f"Mide citas gestionadas por canales no presenciales: "
                  f"(Telefonico + Telesalud) / Total x 100. Resultado: {v_ag:.1f}% "
                  f"({_sem_t(v_ag, 'agendamiento_remoto')}). Meta >= 20%, alerta < 5%.")

    # ── S12: Horario Extendido (ENHANCED) ──
    v_ext = kpis.get("ocupacion_extendida", {}).get("valor", 0)
    pdf.add_page()
    pdf.section_title(12, "Horario Extendido y Apertura Sabatina")
    pdf.body_text(f"Uso de cupos a partir de las 18:00 hrs (jornada extendida): "
                  f"Citados >=18h / (Citados + Disponibles >=18h) x 100. Resultado: {v_ext:.1f}% "
                  f"({_sem_t(v_ext, 'ocupacion_extendida')}). Meta >= 50%, alerta < 30%.")

    # Segmented table
    _df_seg = kpis_horario_segmentado(df_centro)
    if not _df_seg.empty:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, "Comparativa por Segmento Horario", align="C")
        pdf.ln(8)
        seg_h = ["Segmento", "Total", "Citados", "Disp.", "Bloq.", "Complet.", "Ocup.%", "NoShow%", "Efect.%"]
        seg_r = []
        for _, r in _df_seg.iterrows():
            seg_r.append([str(r["segmento"])[:28], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                          f'{r["disponibles"]:,.0f}', f'{r["bloqueados"]:,.0f}', f'{r["completados"]:,.0f}',
                          f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}'])
        _draw_table(seg_h, seg_r, col_widths=[50, 18, 18, 14, 14, 16, 16, 16, 16])

    # Extended hours evolution
    if charts_png.get("ext_evo"):
        pdf.ln(3)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, "Evolucion Mensual - Horario Extendido", align="C")
        pdf.ln(8)
        pdf.add_chart(charts_png["ext_evo"], title_hint="Evolucion Extendido")

    # Sabatino evolution
    if charts_png.get("sab_evo"):
        if pdf.get_y() > 160:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, "Evolucion Mensual - Apertura Sabatina", align="C")
        pdf.ln(8)
        pdf.add_chart(charts_png["sab_evo"], title_hint="Evolucion Sabatina")

    # Instrumento extendido table
    _df_ie = kpis_extendido_por_instrumento(df_centro)
    if not _df_ie.empty:
        if pdf.get_y() > 180:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, _ps(f"KPIs por Instrumento - Horario Extendido ({len(_df_ie)})"), align="C")
        pdf.ln(8)
        ie_h = ["Instrumento", "Total", "Citados", "Ocup.%", "NoShow%", "Efect.%"]
        ie_r = []
        for _, r in _df_ie.iterrows():
            ie_r.append([str(r["instrumento"])[:30], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                         f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}'])
        _draw_table(ie_h, ie_r, col_widths=[55, 18, 18, 18, 18, 18])

    # Instrumento sabatino table
    _df_is = kpis_sabatino_por_instrumento(df_centro)
    if not _df_is.empty:
        if pdf.get_y() > 180:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, _ps(f"KPIs por Instrumento - Apertura Sabatina ({len(_df_is)})"), align="C")
        pdf.ln(8)
        is_h = ["Instrumento", "Total", "Citados", "Ocup.%", "NoShow%", "Efect.%"]
        is_r = []
        for _, r in _df_is.iterrows():
            is_r.append([str(r["instrumento"])[:30], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                         f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}'])
        _draw_table(is_h, is_r, col_widths=[55, 18, 18, 18, 18, 18])

    # ── S13: Tipo de Atencion ──
    pdf.add_page()
    pdf.section_title(13, "Distribucion por Tipo de Atencion")
    pdf.body_text("Volumen de cupos por tipo de atencion. Identifica la composicion de la cartera de servicios.")
    pdf.add_chart(charts_png.get("tipo_atencion"), title_hint="Tipo de Atencion")

    if not df_kpis_ta.empty:
        if pdf.get_y() > 180:
            pdf.add_page()
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_OSCURO)
        pdf.cell(0, 7, "KPIs por Tipo de Atencion", align="C")
        pdf.ln(8)
        ta_h = ["Tipo Atencion", "Total", "Citados", "Disp.", "Bloq.", "Complet.", "Ocup.%", "NoShow%", "Efect.%", "Rend."]
        ta_r = []
        for _, r in df_kpis_ta.iterrows():
            ta_r.append([str(r["tipo_atencion"])[:25], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                         f'{r["disponibles"]:,.0f}', f'{r["bloqueados"]:,.0f}', f'{r["completados"]:,.0f}',
                         f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}',
                         f'{r["rendimiento"]:.1f}'])
        _draw_table(ta_h, ta_r, col_widths=[38, 18, 18, 16, 16, 18, 16, 18, 16, 16],
                    align_cols=["L", "R", "R", "R", "R", "R", "C", "C", "C", "C"])

    # ── S14: KPIs por Instrumento ──
    pdf.add_page()
    pdf.section_title(14, "KPIs por Instrumento / Profesional")
    pdf.body_text("Resumen de indicadores por profesional del centro.")
    if not df_inst_c.empty:
        inst_h = ["Instrumento", "Total", "Citados", "Disp.", "Bloq.", "Complet.", "Ocup.%", "NoShow%", "Efect.%", "Rend."]
        inst_r = []
        for _, r in df_inst_c.iterrows():
            inst_r.append([str(r["instrumento"])[:25], f'{r["total"]:,.0f}', f'{r["citados"]:,.0f}',
                           f'{r["disponibles"]:,.0f}', f'{r["bloqueados"]:,.0f}', f'{r["completados"]:,.0f}',
                           f'{r["ocupacion"]:.1f}', f'{r["no_show"]:.1f}', f'{r["efectividad"]:.1f}',
                           f'{r["rendimiento"]:.1f}'])
        _draw_table(inst_h, inst_r, col_widths=[38, 18, 18, 16, 16, 18, 16, 18, 16, 16],
                    align_cols=["L", "R", "R", "R", "R", "R", "C", "C", "C", "C"])

    # ── S15: Multi-KPI ──
    pdf.add_page()
    pdf.section_title(15, "Evolucion Conjunta de KPIs Principales")
    pdf.body_text("Ocupacion, No-Show y Bloqueo mes a mes. Un aumento de bloqueo tipicamente reduce la ocupacion.")
    pdf.add_chart(charts_png.get("multi_kpi"), title_hint="Multi-KPI Mensual")

    # ── S16: Heatmap ──
    pdf.add_page()
    pdf.section_title(16, "Mapa de Calor: Ocupacion por Instrumento y Mes")
    pdf.body_text("Cruza cada instrumento con cada mes, coloreando segun tasa de ocupacion.")
    pdf.add_chart(charts_png.get("heatmap"), title_hint="Heatmap Instrumento-Mes")

    # ── S17: Alertas ──
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
            pdf.cell(0, 5, _ps(f"{a.get('tipo', '')}: {a.get('valor', 0):.1f} {a.get('unidad', '')}"))
            pdf.set_font("Helvetica", "", 7)
            pdf.set_text_color(*GRIS_TEXTO)
            pdf.set_xy(14, y_a + 6)
            pdf.cell(0, 5, _ps(a.get("descripcion", "")[:120]))
            pdf.set_y(y_a + 14)

    # ── S18: Marco Metodologico ──
    pdf.add_page()
    pdf.section_title(18, "Marco Metodologico y Referencias")
    pdf.body_text(
        "Los indicadores de seguimiento utilizados en este informe se fundamentan en marcos de "
        "referencia internacionales para la medicion de productividad y desempeno en atencion primaria "
        "de salud. A continuacion se presentan las bases conceptuales y las fuentes bibliograficas "
        "que sustentan el modelo de evaluacion aplicado."
    )

    # Sub-section: OECD Framework
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.cell(0, 7, "Marco OCDE: Health at a Glance")
    pdf.ln(8)
    pdf.body_text(
        "La Organizacion para la Cooperacion y el Desarrollo Economicos (OCDE) propone un conjunto "
        "de indicadores de desempeno sanitario agrupados en cinco dimensiones: acceso, calidad, "
        "eficiencia, equidad y resultados en salud. Los indicadores de ocupacion, bloqueo y "
        "disponibilidad de cupos empleados en este informe se alinean con la dimension de eficiencia, "
        "mientras que la tasa de no-show y la efectividad de cita abordan la dimension de acceso "
        "efectivo. La cobertura sectorial y el agendamiento remoto corresponden a indicadores de "
        "equidad y modernizacion del acceso (OECD, 2023)."
    )

    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.cell(0, 7, "Marco OMS: Atencion Primaria de Salud")
    pdf.ln(8)
    pdf.body_text(
        "La Organizacion Mundial de la Salud, en su marco operacional para la atencion primaria "
        "de salud, establece que la productividad en APS debe evaluarse considerando la utilizacion "
        "optima de los recursos disponibles, la continuidad del cuidado y la capacidad resolutiva "
        "del primer nivel. Los indicadores de rendimiento por instrumento y la ocupacion por "
        "horario extendido permiten evaluar la intensidad de uso de los recursos humanos "
        "en diferentes modalidades de atencion (WHO & UNICEF, 2020)."
    )

    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.cell(0, 7, "Fundamentacion de Umbrales")
    pdf.ln(8)
    pdf.body_text(
        "Los umbrales operativos aplicados a cada indicador se basan en evidencia publicada y "
        "estandares sectoriales:\n"
        "- Tasa de Ocupacion (meta >= 65%): alineada con el benchmark OCDE para utilizacion de "
        "capacidad instalada en APS, donde valores inferiores al 50% senalan subutilizacion "
        "critica (Siciliani et al., 2014).\n"
        "- Tasa de No-Show (meta <= 10%): la literatura situa el ausentismo promedio en APS "
        "entre 15-30%, considerando optimas las tasas bajo 10% mediante estrategias de "
        "confirmacion y recordatorio (Dantas et al., 2018).\n"
        "- Tasa de Bloqueo (meta <= 10%): un bloqueo superior reduce la capacidad ofertada "
        "y afecta el acceso oportuno. El umbral se establece segun recomendaciones de gestion "
        "de agenda (Murray & Berwick, 2003).\n"
        "- Efectividad de Cita (meta >= 88%): refleja la tasa de resolucion, consistente con "
        "indicadores de continuidad asistencial del modelo de Starfield (Starfield et al., 2005).\n"
        "- Ocupacion Horario Extendido (meta >= 50%): basado en la necesidad de optimizar la "
        "inversion adicional en jornadas vespertinas y sabatinas (MINSAL, 2023)."
    )

    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*AZUL_OSCURO)
    pdf.cell(0, 7, "Referencias Bibliograficas (APA 7)")
    pdf.ln(8)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(*GRIS_TEXTO)

    _refs = [
        "Dantas, L. F., Fleck, J. L., Cyrino Oliveira, F. L., & Hamacher, S. (2018). No-shows in "
        "appointment scheduling: A systematic literature review. Health Policy, 122(4), 412-421. "
        "https://doi.org/10.1016/j.healthpol.2018.02.002",
        "Ministerio de Salud de Chile [MINSAL]. (2023). Orientaciones tecnicas para la gestion de "
        "la atencion primaria de salud. Subsecretaria de Redes Asistenciales.",
        "Murray, M., & Berwick, D. M. (2003). Advanced access: Reducing waiting and delays in "
        "primary care. JAMA, 289(8), 1035-1040. https://doi.org/10.1001/jama.289.8.1035",
        "OECD. (2023). Health at a Glance 2023: OECD Indicators. OECD Publishing. "
        "https://doi.org/10.1787/7a7afb35-en",
        "Siciliani, L., Borowitz, M., & Moran, V. (2014). Waiting time policies in the health "
        "sector: What works? OECD Health Policy Studies. OECD Publishing. "
        "https://doi.org/10.1787/9789264179080-en",
        "Starfield, B., Shi, L., & Macinko, J. (2005). Contribution of primary care to health "
        "systems and health. The Milbank Quarterly, 83(3), 457-502. "
        "https://doi.org/10.1111/j.1468-0009.2005.00409.x",
        "World Health Organization & United Nations Children's Fund [WHO & UNICEF]. (2020). "
        "Operational framework for primary health care: Transforming vision into action. WHO. "
        "https://www.who.int/publications/i/item/9789240017832",
    ]
    for ref in _refs:
        pdf.multi_cell(0, 4.5, _ps(ref))
        pdf.ln(2)

    # ── S19: Conclusion ──
    pdf.add_page()
    pdf.section_title(19, "Conclusion del Informe")
    pdf.body_text(
        f"El centro {centro_sel} presenta {n_verde} indicadores en estado optimo, "
        f"{n_amarillo} en zona de observacion y {n_rojo} en brecha critica "
        f"durante el periodo analizado ({rango_meses}).")
    if n_rojo > 0:
        kpis_rojos = [k.get("nombre", key) for key, k in kpis.items()
                      if isinstance(k, dict) and k.get("semaforo") == "rojo"]
        pdf.body_text(f"Indicadores criticos: {', '.join(kpis_rojos)}. Se recomienda intervencion inmediata.")
    if n_amarillo > 0:
        kpis_ama = [k.get("nombre", key) for key, k in kpis.items()
                    if isinstance(k, dict) and k.get("semaforo") == "amarillo"]
        pdf.body_text(f"Indicadores en observacion: {', '.join(kpis_ama)}. Se sugiere monitoreo continuo.")
    if n_rojo == 0 and n_amarillo == 0:
        pdf.body_text("Todos los indicadores se encuentran dentro de los umbrales. Se recomienda mantener las estrategias actuales.")

    # Footer
    pdf.ln(10)
    pdf.set_draw_color(*AZUL_MEDIO)
    pdf.set_line_width(0.5)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(3)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 5, "Informe generado automaticamente por el Sistema de Analisis de Productividad APS", align="C")
    pdf.ln(5)
    pdf.cell(0, 5, _ps(f"Servicio de Salud Metropolitano Central  |  {centro_sel}  |  {rango_meses}"), align="C")
    pdf.ln(5)
    pdf.cell(0, 5, f"Fecha de generacion: {fecha_gen}", align="C")

    # ═══ FILL TABLE OF CONTENTS (page 2) ═══
    # Save current page, go to page 2
    total_pages = pdf.page_no()
    pdf.page = toc_page_no
    pdf.set_y(35)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*GRIS_TEXTO)

    # Decorative line
    pdf.set_draw_color(*AZUL_MEDIO)
    pdf.set_line_width(0.5)
    pdf.line(30, 33, 180, 33)
    pdf.ln(2)

    for num, title, page in pdf._toc_entries:
        y0 = pdf.get_y()
        # Number circle
        pdf.set_fill_color(*AZUL_MEDIO)
        pdf.ellipse(18, y0 + 0.5, 6, 6, "F")
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*BLANCO)
        pdf.set_xy(18, y0 + 0.8)
        pdf.cell(6, 5, str(num), align="C")

        # Title
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRIS_TEXTO)
        pdf.set_xy(28, y0)
        pdf.cell(140, 7, _ps(title))

        # Page number
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*AZUL_MEDIO)
        pdf.set_xy(170, y0)
        pdf.cell(20, 7, str(page), align="R")
        pdf.set_y(y0 + 8)

    # Restore to last page
    pdf.page = total_pages

    return bytes(pdf.output())
