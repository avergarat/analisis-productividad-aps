"""
Generador de datos sintéticos para demo pública.
Basado en estadísticas reales del CESFAM N°5 - SSMC 2025.
"""
import pandas as pd
import numpy as np


def generate_demo_data(n_records: int = 80_000, seed: int = 42) -> pd.DataFrame:
    """
    Genera dataset sintético que replica la estructura y distribuciones
    del CESFAM N°5 (398,055 registros reales, enero-diciembre 2025).
    """
    rng = np.random.default_rng(seed)

    # Distribución mensual (más citas en meses de mayor actividad)
    monthly_weights = np.array([0.07, 0.08, 0.09, 0.09, 0.07, 0.08,
                                 0.09, 0.09, 0.09, 0.09, 0.08, 0.08])
    monthly_weights /= monthly_weights.sum()
    meses = rng.choice(range(1, 13), size=n_records, p=monthly_weights)

    centros = [
        "Centro de Salud N° 1", "Centro de Salud N° 2", "Centro de Salud N° 3",
        "Centro de Salud N° 4", "Centro de Salud N° 5", "Centro de Salud N° 6",
        "Centro de Salud N° 7",
    ]
    centro_weights = [0.12, 0.15, 0.14, 0.13, 0.18, 0.15, 0.13]
    establecimientos = rng.choice(centros, size=n_records, p=centro_weights)

    instrumentos = [
        "Médico", "Enfermero(a)", "Nutricionista", "Matrón(a)",
        "Técnico Paramédico", "Odontólogo(a)", "Psicólogo(a)",
        "Kinesiólogo(a)", "Trabajador(a) Social", "Técnico en nivel superior en Salud",
        "Profesor(a) de Educación Física",
    ]
    instr_weights = [0.18, 0.15, 0.10, 0.12, 0.14, 0.08, 0.07, 0.06, 0.04, 0.04, 0.02]
    instrumento = rng.choice(instrumentos, size=n_records, p=instr_weights)

    tipos_atencion = [
        "Consulta Morbilidad", "Control Cardiovascular", "Toma de Muestras",
        "Pesquisa HTA", "Consulta Salud Mental", "Sección Rehabilitación",
        "Control Embarazo", "Toma de PAP", "Consulta Lactancia Materna",
        "Trabajo administrativo", "Administración de tto. Inyectable",
        "Rescate Telefónico", "Control Crónico Adulto Mayor", "Electrocardiograma",
        "Actividad de gestión y promoción PEVS",
    ]
    tipo_atencion = rng.choice(tipos_atencion, size=n_records)

    tipo_cupo_choices = ["Cupo Programado", "Sobrecupo", "Cupo de Ajuste"]
    tipo_cupo_weights = [0.88, 0.07, 0.05]
    tipo_cupo = rng.choice(tipo_cupo_choices, size=n_records, p=tipo_cupo_weights)

    # Estado de cupo: distribución realista (ocupación ~41%)
    estado_cupo_choices = ["CITADO", "DISPONIBLE", "BLOQUEADO"]
    estado_cupo_weights = [0.41, 0.44, 0.15]
    estado_cupo = rng.choice(estado_cupo_choices, size=n_records, p=estado_cupo_weights)

    # Estado cita: solo aplica a CITADOS
    estado_cita = np.where(
        estado_cupo == "CITADO",
        rng.choice(
            ["Completado", "Pendiente", "Agendado", "Iniciado"],
            size=n_records,
            p=[0.85, 0.08, 0.05, 0.02]
        ),
        np.where(estado_cupo == "BLOQUEADO", "Bloqueado", "Disponible")
    )

    # Tipo de agendamiento
    agendamiento_choices = ["Personalmente", "Telefónicamente", "Telesalud", "Otro"]
    agendamiento_weights = [0.82, 0.10, 0.05, 0.03]
    tipo_agendamiento = rng.choice(agendamiento_choices, size=n_records, p=agendamiento_weights)

    # Sector territorial
    sector_choices = ["NO INFORMADO", "VERDE", "LILA", "ROJO"]
    sector_weights = [0.50, 0.18, 0.17, 0.15]
    sector = rng.choice(sector_choices, size=n_records, p=sector_weights)

    # Rendimiento: promedio ~23 min, rango 5-186
    rendimiento_base = {
        "Médico": (25, 8), "Enfermero(a)": (20, 6), "Nutricionista": (31, 9),
        "Matrón(a)": (25, 7), "Técnico Paramédico": (16, 5), "Odontólogo(a)": (30, 10),
        "Psicólogo(a)": (45, 12), "Kinesiólogo(a)": (40, 10),
        "Trabajador(a) Social": (30, 8), "Técnico en nivel superior en Salud": (15, 4),
        "Profesor(a) de Educación Física": (60, 15),
    }
    rendimiento = np.array([
        max(5, min(186, rng.normal(*rendimiento_base.get(inst, (23, 8)))))
        for inst in instrumento
    ]).astype(int)

    # Horas
    hora_dist = [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    hora_weights = [0.02, 0.12, 0.14, 0.13, 0.12, 0.08, 0.10, 0.10, 0.09, 0.07, 0.05, 0.04, 0.03, 0.01]
    hora_num = rng.choice(hora_dist, size=n_records, p=hora_weights)

    # Fechas (días hábiles del año 2025)
    dias_por_mes = {
        1: 23, 2: 20, 3: 21, 4: 22, 5: 21, 6: 21,
        7: 23, 8: 21, 9: 22, 10: 23, 11: 20, 12: 19
    }
    fechas = []
    for mes in meses:
        n_dias = dias_por_mes.get(mes, 21)
        dia = rng.integers(1, n_dias + 1)
        try:
            f = pd.Timestamp(year=2025, month=int(mes), day=int(dia))
        except Exception:
            f = pd.Timestamp(year=2025, month=int(mes), day=1)
        fechas.append(f)

    # Cupos utilizados
    cupos_utilizados = np.where(estado_cupo == "CITADO",
                                rng.choice([1, 1, 1, 2], size=n_records),
                                0)

    # Día de semana
    dia_semana = [f.weekday() + 1 for f in fechas]

    df = pd.DataFrame({
        "SS": "S.S. Metropolitano Central",
        "ESTABLECIMIENTO": establecimientos,
        "FECHA": fechas,
        "TIPO ATENCION": tipo_atencion,
        "ESPECIALIDAD": "Sin Especialidad",
        "INSTRUMENTO": instrumento,
        "TIPO CUPO": tipo_cupo,
        "HORA INICIO": [f"{h:02d}:00" for h in hora_num],
        "HORA TERMINO": [f"{h+1:02d}:00" for h in hora_num],
        "ESTADO CUPO": estado_cupo,
        "HABILITADO": "SI",
        "TIPO DE AGENDAMIENTO": tipo_agendamiento,
        "TELECONSULTA": "NO",
        "MOTIVO CUPO": "-",
        "SECTOR": sector,
        "PROFESIONAL": "Profesional Demo",
        "MOTIVO BLOQUEO": "",
        "OBSERVACION DE BLOQUEO": "",
        "FUNCIONARIO REALIZA BLOQUEO": "",
        "FECHA BLOQUEO": "",
        "HORA BLOQUEO": "",
        "ESTADO CITA": estado_cita,
        "RENDIMIENTO": rendimiento,
        "CUPOS UTILIZADOS": cupos_utilizados,
        "HORA_NUM": hora_num,
        "DIA_SEMANA": dia_semana,
        "MES_NUM": meses,
        "EDAD_ANO": rng.integers(0, 90, size=n_records).astype(float),
    })

    # Columnas derivadas (mismas que processor.py)
    df["TRIMESTRE"] = df["MES_NUM"].apply(lambda m: f"Q{int((m - 1) // 3) + 1}")
    MESES_ES = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    df["MES_NOMBRE"] = df["MES_NUM"].map(MESES_ES)
    df["HORARIO_EXTENDIDO"] = df["HORA_NUM"].apply(
        lambda h: "Extendido" if h >= 18 else "Normal"
    )
    bins = [-1, 5, 14, 29, 64, 200]
    labels = ["0-5", "6-14", "15-29", "30-64", "65+"]
    df["GRUPO_ETARIO"] = pd.cut(df["EDAD_ANO"], bins=bins, labels=labels)
    df["AGENDAMIENTO_REMOTO"] = df["TIPO DE AGENDAMIENTO"].apply(
        lambda x: "Remoto" if x in {"Telefónicamente", "Telesalud"} else "Presencial/Otro"
    )
    df["CUPOS_UTIL_BIN"] = (df["CUPOS UTILIZADOS"] >= 1).astype(int)
    df["_archivo"] = "Demo Sintético - 7 CESFAM"

    return df


def get_demo_metadata() -> dict:
    return {
        "servicio_salud": "S.S. Metropolitano Central",
        "establecimiento": "7 CESFAM (datos demo)",
        "fecha_desde": "2025-01-01",
        "fecha_hasta": "2025-12-31",
        "archivo": "DEMO - datos sintéticos basados en CESFAM N°5",
    }
