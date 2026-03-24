#!/usr/bin/env python3
"""
parser_semanal_tv.py
────────────────────
Genera programa_S{semana}_{año}.json a partir de:
  - Programa_Diurno-Nocturno_S{N}_TV.xlsx      (Líneas convencionales: L1,L2,L4,L4A,L5)
  - Acta_Semanal_Mtto_{año}_L3_S{N}_TKE.xlsx   (Línea automática L3)
  - Acta_Semanal_Mtto_{año}_L6_S{N}_TKE.xlsx   (Línea automática L6)
  - Estaciones_Linea_Sigla.xlsx                 (Catálogo de siglas)

Uso:
  python parser_semanal_tv.py \\
      --programa  Programa_Diurno-Nocturno_S13_TV.xlsx \\
      --acta_l3   Acta_Semanal_Mtto__2026_L3_S13_TKE.xlsx \\
      --acta_l6   Acta_Semanal_Mtto__2026_L6_S13_TKE.xlsx \\
      --catalogo  Estaciones_Linea_Sigla.xlsx \\
      --salida    programa_S13_2026.json
"""

import sys, json, re, argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

# ─── CONSTANTES ──────────────────────────────────────────────────────────────

DIAS_HOJA_A_FECHA = {
    "LUNES":     0, "MARTES":    1, "MIERCOLES": 2,
    "JUEVES":    3, "VIERNES":   4, "SABADO":    5, "DOMINGO":   6,
}

DIAS_ABREV = {
    "Lu": "Lunes",   "Ma": "Martes",  "Mi": "Miércoles",
    "Ju": "Jueves",  "Vi": "Viernes", "Sa": "Sábado",    "Do": "Domingo",
}

# ─── CATÁLOGO SIGLAS ──────────────────────────────────────────────────────────

def cargar_catalogo(path: str) -> tuple[dict, dict]:
    """Retorna (sigla→estacion, sigla→linea)."""
    df = pd.read_excel(path, header=None)
    df.columns = ["Linea", "Estacion", "Sigla"]
    df = df[df["Linea"] != "Linea"].dropna(subset=["Sigla"])
    sig2est = dict(zip(df["Sigla"].str.strip(), df["Estacion"].str.strip()))
    sig2lin = dict(zip(df["Sigla"].str.strip(), df["Linea"].str.strip()))
    return sig2est, sig2lin

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def normalizar_linea(raw: str) -> str:
    """'LINEA 4A' → 'L4A', 'LINEA 1' → 'L1'"""
    return raw.strip().replace("LINEA ", "L").replace("LINEA", "L")

def extraer_num(texto: str) -> int:
    m = re.search(r"(\d+)", texto)
    return int(m.group(1)) if m else 1

def construir_ubicacion_bi(linea: str, sigla: str, sistema: str, num: int) -> str:
    """Produce la clave de cruce con Power BI: 'L1-NP-ASC-01'"""
    return f"{linea}-{sigla}-{sistema}-{num:02d}"

def construir_id(linea: str, sigla: str, sistema: str, num: int, fecha: str) -> str:
    """ID único de actividad: 'L1-NP-ASC-01-2026-03-23'"""
    return f"{linea}-{sigla}-{sistema}-{num:02d}-{fecha}"

def clasificar_sistema_acta(actividad: str) -> str:
    """
    En actas de L3/L6 la actividad dice:
      'Mant. Preventivo escalas y asc EM-001'  → ESC (Escalera Mecánica)
      'Mant. Preventivo escalas y asc EL-001'  → ASC (ELevador = ascensor)
    """
    act_upper = actividad.upper()
    if re.search(r"\bEM-?\d+", act_upper):
        return "ESC"
    if re.search(r"\bEL-?\d+", act_upper):
        return "ASC"
    return "ASC"  # fallback conservador

def extraer_codigo_acta(actividad: str) -> str:
    """'Mant. Preventivo escalas y asc EM-001' → 'EM-001'"""
    m = re.search(r"(E[ML]-?\d+|ASC-?\d+)", actividad, re.IGNORECASE)
    return m.group(1).upper() if m else actividad.strip()[-6:]

def limpiar_texto(val) -> str:
    s = str(val).strip() if pd.notna(val) else ""
    return "" if s in ("nan", "None") else s

# ─── PARSER PROGRAMA ─────────────────────────────────────────────────────────

def parsear_programa(path: str, sig2est: dict, sig2lin: dict) -> tuple[dict, list]:
    """
    Lee la hoja 'Nocturno' del Programa y retorna (meta, [actividades]).
    """
    df = pd.read_excel(path, sheet_name="Nocturno", header=None)

    # Extraer semana
    semana = int(df.iloc[2, 2]) if pd.notna(df.iloc[2, 2]) else 0

    # Construir mapa columna → (linea, dia_nombre, fecha_iso)
    linea_row  = df.iloc[1]
    dias_row   = df.iloc[2]
    fechas_row = df.iloc[3]

    col_map: dict[int, dict] = {}
    current_linea = None
    fecha_inicio = fecha_fin = None

    for col in range(len(df.columns)):
        lin = limpiar_texto(linea_row[col])
        if lin.startswith("LINEA"):
            current_linea = normalizar_linea(lin)
        dia_abrev = limpiar_texto(dias_row[col])
        fec = fechas_row[col]
        if current_linea and dia_abrev in DIAS_ABREV and hasattr(fec, "strftime"):
            fecha_iso = fec.strftime("%Y-%m-%d")
            col_map[col] = {
                "linea":      current_linea,
                "dia_semana": DIAS_ABREV[dia_abrev],
                "fecha":      fecha_iso,
            }
            if fecha_inicio is None or fecha_iso < fecha_inicio:
                fecha_inicio = fecha_iso
            if fecha_fin is None or fecha_iso > fecha_fin:
                fecha_fin = fecha_iso

    meta = {
        "semana":       semana,
        "año":          int(fecha_inicio[:4]) if fecha_inicio else datetime.now().year,
        "fecha_inicio": fecha_inicio or "",
        "fecha_fin":    fecha_fin or "",
        "contrato":     "MN-115-2022-G",
        "generado_el":  datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

    actividades = []
    for _, row in df.iterrows():
        sistema = limpiar_texto(row[3]).upper()
        if sistema not in ("ASC", "ESC", "PLA"):
            continue
        codigo_raw = limpiar_texto(row[1])
        if not codigo_raw:
            continue

        partes    = codigo_raw.split()
        sigla     = partes[0]
        num       = extraer_num(partes[1]) if len(partes) > 1 else 1
        om        = limpiar_texto(row[0])
        tipo_mp   = limpiar_texto(row[4])
        encargado = limpiar_texto(row[46])
        fono      = limpiar_texto(row[47])
        obs       = limpiar_texto(row[45])
        contrato  = limpiar_texto(row[44])

        estacion  = sig2est.get(sigla, sigla)

        # Encontrar la columna con 'X'
        for col, info in col_map.items():
            celda = limpiar_texto(row[col]).upper()
            if celda != "X":
                continue

            linea       = info["linea"]
            ubicacion_bi = construir_ubicacion_bi(linea, sigla, sistema, num)
            actividades.append({
                "id":             construir_id(linea, sigla, sistema, num, info["fecha"]),
                "linea":          linea,
                "estacion":       estacion,
                "sigla_estacion": sigla,
                "ubicacion_bi":   ubicacion_bi,
                "sistema":        sistema,
                "codigo_equipo":  codigo_raw,
                "numero_equipo":  num,
                "om_sap":         om,
                "tipo_mp":        tipo_mp,
                "dia_semana":     info["dia_semana"],
                "fecha":          info["fecha"],
                "turno":          "NOCTURNO",
                "fuente":         "Programa",
                "empresa":        "TKELEVADORES",
                "encargado":      encargado,
                "fono":           fono,
                "observaciones":  obs,
                "contrato":       contrato or "MN-115-2022-G",
            })

    return meta, actividades

# ─── PARSER ACTAS ─────────────────────────────────────────────────────────────

def parsear_acta(path: str, linea: str, sig2est: dict, fecha_inicio_semana: str) -> list:
    """
    Lee todas las hojas de días de un acta (L3 o L6) y retorna lista de actividades.
    """
    xl = pd.ExcelFile(path)
    actividades = []

    # Calcular fecha base (lunes de la semana)
    try:
        fecha_base = datetime.strptime(fecha_inicio_semana, "%Y-%m-%d")
    except ValueError:
        fecha_base = datetime.now()

    for hoja in xl.sheet_names:
        hoja_up = hoja.upper().strip()
        if hoja_up not in DIAS_HOJA_A_FECHA:
            continue

        offset_dias = DIAS_HOJA_A_FECHA[hoja_up]
        from datetime import timedelta
        fecha_dia = (fecha_base + timedelta(days=offset_dias)).strftime("%Y-%m-%d")
        dia_nombre = list(DIAS_ABREV.values())[offset_dias]

        df = pd.read_excel(path, sheet_name=hoja, header=None)

        for i, row in df.iterrows():
            if i < 2:
                continue

            actividad = limpiar_texto(row[16])
            if not actividad or "Ingreso a permanencia" in actividad or "ACTIVIDAD" in actividad:
                continue

            lugar    = limpiar_texto(row[6])
            om       = limpiar_texto(row[26])
            turno    = limpiar_texto(row[4]).upper() or "NOCTURNO"
            pers_raw = limpiar_texto(row[18])
            fono_raw = limpiar_texto(row[19])
            obs      = limpiar_texto(row[25])

            # Primer encargado (lista separada por comas)
            encargado = pers_raw.split(",")[0].strip() if pers_raw else ""
            fono      = fono_raw.split(",")[0].strip() if fono_raw else ""

            sistema      = clasificar_sistema_acta(actividad)
            codigo_eq    = extraer_codigo_acta(actividad)
            num          = extraer_num(codigo_eq)
            estacion     = sig2est.get(lugar, lugar)
            ubicacion_bi = construir_ubicacion_bi(linea, lugar, sistema, num)

            actividades.append({
                "id":             construir_id(linea, lugar, sistema, num, fecha_dia),
                "linea":          linea,
                "estacion":       estacion,
                "sigla_estacion": lugar,
                "ubicacion_bi":   ubicacion_bi,
                "sistema":        sistema,
                "codigo_equipo":  codigo_eq,
                "numero_equipo":  num,
                "om_sap":         om,
                "tipo_mp":        "N1",
                "dia_semana":     dia_nombre,
                "fecha":          fecha_dia,
                "turno":          turno,
                "fuente":         f"Acta_{linea}",
                "empresa":        "TKELEVADORES",
                "encargado":      encargado,
                "fono":           fono,
                "observaciones":  obs,
                "contrato":       "MN-115-2022-G",
            })

    return actividades

# ─── PASO 2: VALIDACIÓN CONTRA BI ────────────────────────────────────────────

def enriquecer_con_bi(actividades: list, bi_data: dict) -> list:
    """
    Cruza cada actividad contra el diccionario de datos del BI.
    Agrega campos: en_bi, modelo, marca, fallas_2025, criticos_2025, anos_operacion.
    Los equipos ESC siempre tienen en_bi=False (no están en la tabla de ascensores).
    """
    enriquecidas = []
    sin_match = []

    for act in actividades:
        a = dict(act)
        clave = a["ubicacion_bi"]

        if a["sistema"] == "ESC":
            a["en_bi"]          = False
            a["modelo"]         = None
            a["marca"]          = None
            a["fallas_2025"]    = None
            a["criticos_2025"]  = None
            a["anos_operacion"] = None
        elif clave in bi_data:
            bi = bi_data[clave]
            a["en_bi"]          = True
            a["modelo"]         = bi.get("modelo")
            a["marca"]          = bi.get("marca")
            a["fallas_2025"]    = bi.get("fallas")
            a["criticos_2025"]  = bi.get("criticos")
            a["anos_operacion"] = bi.get("anos_op")
        else:
            a["en_bi"]          = False
            a["modelo"]         = None
            a["marca"]          = None
            a["fallas_2025"]    = None
            a["criticos_2025"]  = None
            a["anos_operacion"] = None
            sin_match.append(clave)

        enriquecidas.append(a)

    if sin_match:
        print(f"  ⚠  {len(sin_match)} equipos ASC/PLA sin match en BI:")
        for c in sorted(set(sin_match)):
            print(f"      {c}")

    return enriquecidas

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Parser unificado de planificación semanal TV Metro")
    ap.add_argument("--programa",  required=True)
    ap.add_argument("--acta_l3",   required=True)
    ap.add_argument("--acta_l6",   required=True)
    ap.add_argument("--catalogo",  required=True)
    ap.add_argument("--salida",    default=None)
    ap.add_argument("--bi_json",   default=None,
                    help="JSON con datos BI pre-exportados (opcional, para enriquecimiento offline)")
    args = ap.parse_args()

    print("╔══════════════════════════════════════════════════╗")
    print("║  Parser Semanal TV — Metro de Santiago           ║")
    print("╚══════════════════════════════════════════════════╝\n")

    # Cargar catálogo
    print("→ Cargando catálogo de siglas...")
    sig2est, sig2lin = cargar_catalogo(args.catalogo)
    print(f"  {len(sig2est)} estaciones cargadas\n")

    # Paso 1a: parsear programa
    print("→ Parseando Programa (L1, L2, L4, L4A, L5)...")
    meta, actividades_prog = parsear_programa(args.programa, sig2est, sig2lin)
    print(f"  Semana {meta['semana']} | {meta['fecha_inicio']} → {meta['fecha_fin']}")
    print(f"  {len(actividades_prog)} actividades extraídas del Programa\n")

    # Paso 1b: parsear actas
    print("→ Parseando Acta L3...")
    actividades_l3 = parsear_acta(args.acta_l3, "L3", sig2est, meta["fecha_inicio"])
    print(f"  {len(actividades_l3)} actividades extraídas de L3\n")

    print("→ Parseando Acta L6...")
    actividades_l6 = parsear_acta(args.acta_l6, "L6", sig2est, meta["fecha_inicio"])
    print(f"  {len(actividades_l6)} actividades extraídas de L6\n")

    todas = actividades_prog + actividades_l3 + actividades_l6
    print(f"→ Total actividades combinadas: {len(todas)}\n")

    # Paso 2: enriquecer con BI (si se pasa archivo BI)
    if args.bi_json and Path(args.bi_json).exists():
        print("→ Enriqueciendo con datos BI...")
        with open(args.bi_json) as f:
            bi_data = json.load(f)
        todas = enriquecer_con_bi(todas, bi_data)
        en_bi = sum(1 for a in todas if a.get("en_bi"))
        sin_bi = sum(1 for a in todas if not a.get("en_bi") and a["sistema"] != "ESC")
        print(f"  {en_bi} equipos cruzados con BI | {sin_bi} sin match\n")
    else:
        # Agregar campos BI vacíos para mantener esquema consistente
        for a in todas:
            if a["sistema"] == "ESC":
                a.update({"en_bi": False, "modelo": None, "marca": None,
                           "fallas_2025": None, "criticos_2025": None, "anos_operacion": None})
            else:
                a.update({"en_bi": None, "modelo": None, "marca": None,
                           "fallas_2025": None, "criticos_2025": None, "anos_operacion": None})

    # Resumen por día
    from collections import Counter
    por_dia = Counter(a["fecha"] for a in todas)
    print("→ Distribución por día:")
    for fecha in sorted(por_dia):
        dt = datetime.strptime(fecha, "%Y-%m-%d")
        label = dt.strftime("%a %d %b").capitalize()
        n = por_dia[fecha]
        bar = "█" * (n // 2)
        print(f"  {label}  {n:3d}  {bar}")

    por_fuente = Counter(a["fuente"] for a in todas)
    print(f"\n→ Por fuente: {dict(por_fuente)}")
    por_sistema = Counter(a["sistema"] for a in todas)
    print(f"→ Por sistema: {dict(por_sistema)}\n")

    # Construir JSON final
    output = {
        "meta":        meta,
        "actividades": todas,
    }

    # Determinar nombre de salida
    if args.salida:
        salida_path = Path(args.salida)
    else:
        salida_path = Path(f"programa_S{meta['semana']}_{meta['año']}.json")

    salida_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"✓ JSON guardado en: {salida_path}")
    print(f"  Tamaño: {salida_path.stat().st_size / 1024:.1f} KB")
    print(f"  Actividades totales: {len(todas)}")

if __name__ == "__main__":
    main()
