# Sistema de Análisis de Productividad APS

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://sistema-analisis-productividad-aps.streamlit.app)

Aplicación de análisis de productividad para centros de Atención Primaria de Salud (APS) del **Servicio de Salud Metropolitano Central (SSMC)**, Chile.

## ¿Qué hace?

- Carga y procesa archivos `.xlsx` exportados desde **IRIS** ("Cantidad de Cupos por Citas")
- Consolida múltiples centros de salud (CESFAM) en una tabla maestra
- Calcula automáticamente **10 KPIs de productividad** con semáforo de alertas
- Genera visualizaciones interactivas (evolución mensual, mapas de calor, rankings)
- Detecta brechas críticas según el modelo APS-SSMC
- **Elimina automáticamente datos personales** (RUT, nombre, teléfono) — cumple Ley 19.628

## KPIs calculados

| # | Indicador | Meta | Alerta |
|---|-----------|------|--------|
| 1 | Tasa de Ocupación | ≥ 65% | < 50% |
| 2 | Tasa de No-Show | ≤ 10% | > 15% |
| 3 | Tasa de Bloqueo | ≤ 10% | > 15% |
| 4 | Efectividad de Cita | ≥ 88% | < 80% |
| 5 | Rendimiento Promedio | Referencia | Desv. > 30% |
| 6 | Cupos Sobrecupo | ≤ 5% | > 10% |
| 7 | Cobertura Sectorial | ≥ 80% | < 60% |
| 8 | Agendamiento Remoto | > 20% | < 5% |
| 9 | Variación Mensual | ± 5pp | > 10pp |
| 10 | Ocupación Hora Extendida | ≥ 50% | < 30% |

## Instalación local

```bash
# Clonar repositorio
git clone https://github.com/avergarat/sistema-analisis-productividad-aps.git
cd sistema-analisis-productividad-aps

# Crear entorno virtual
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar
streamlit run app.py
```

## Estructura del proyecto

```
sistema-analisis-productividad-aps/
├── app.py                  # Aplicación principal Streamlit
├── requirements.txt
├── .streamlit/
│   └── config.toml         # Tema y configuración
├── src/
│   ├── processor.py        # Procesamiento de archivos IRIS
│   ├── kpis.py             # Cálculo de 10 KPIs
│   ├── charts.py           # Visualizaciones Plotly
│   └── demo_data.py        # Datos sintéticos para demo
└── README.md
```

## Formato esperado de archivos IRIS

- **Archivo**: `.xlsx` exportado desde IRIS → "Cantidad de Cupos por Citas"
- **Filas 1-8**: Metadatos (SS, Establecimiento, Período, Estado Cupo)
- **Fila 10**: Encabezados de columnas (40 columnas)
- **Fila 11+**: Datos (aprox. 400,000 registros por año por centro)

## Privacidad

Los siguientes campos se eliminan automáticamente al procesar:
`NOMBRE`, `NOMBRE SOCIAL`, `NÚMERO TIPO IDENTIFICACIÓN`, `FECHA DE NACIMIENTO`, `TELÉFONOS`, `DETALLE CUPO`, `OBSERVACIONES`

---

**Versión**: 1.0 · **Fecha**: Marzo 2026
**Desarrollado por**: Análisis de Datos con IA — Servicio de Salud Metropolitano Central
