# =============================================================================
# SCRIPT: 01_generate_data.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Genera datos sintéticos bancarios chilenos y los sube a GCS (Bronze).
#   Simula clientes, cuentas y transacciones con volumen variable por hora,
#   día de semana y eventos especiales (quincena, viernes, feriados).
#
# DATOS QUE GENERA:
#   - Directos   : RUT, nombre, email, teléfono, tarjeta, cuenta
#   - Indirectos : fecha nacimiento, región, comuna, ocupación, género
#   - Sensibles  : saldo, deuda, score crediticio, categoría de gasto
#
# CONCEPTOS CLAVE:
#   - Bronze Layer: datos crudos en JSON, nunca se modifican
#   - Particionado: fecha=YYYY-MM-DD/hora=HH — permite leer solo lo necesario
#   - Volumen variable: simula comportamiento real bancario con peaks
#   - Ruido intencional: PII en campos incorrectos para que DLP lo detecte
#
# USO:
#   python data_generator/01_generate_data.py
# =============================================================================

import json
import random
import math
import os
from datetime import datetime, date
import pytz
import pandas as pd
from faker import Faker
from google.cloud import storage
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# CONFIGURACIÓN INICIAL
# load_dotenv() lee el archivo .env y carga todas las variables como
# variables de entorno accesibles con os.getenv()
# -----------------------------------------------------------------------------
load_dotenv()

PROJECT_ID  = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TIMEZONE    = pytz.timezone(os.getenv("DAG_TIMEZONE", "America/Santiago"))

# Faker con locale chileno para nombres y datos más realistas
fake = Faker("es_CL")
Faker.seed(42)
random.seed(42)

# -----------------------------------------------------------------------------
# FERIADOS CHILE 2026
# En feriados el banco no opera — volumen = 0
# -----------------------------------------------------------------------------
FERIADOS_CHILE_2026 = {
    date(2026, 1, 1),   # Año Nuevo
    date(2026, 4, 3),   # Viernes Santo
    date(2026, 4, 4),   # Sábado Santo
    date(2026, 5, 1),   # Día del Trabajo
    date(2026, 5, 21),  # Glorias Navales
    date(2026, 6, 29),  # San Pedro y San Pablo
    date(2026, 7, 16),  # Virgen del Carmen
    date(2026, 8, 15),  # Asunción de la Virgen
    date(2026, 9, 18),  # Independencia
    date(2026, 9, 19),  # Glorias del Ejército
    date(2026, 10, 12), # Día de la Raza
    date(2026, 10, 31), # Día de las Iglesias Evangélicas
    date(2026, 11, 1),  # Día de Todos los Santos
    date(2026, 12, 8),  # Inmaculada Concepción
    date(2026, 12, 25), # Navidad
}

# -----------------------------------------------------------------------------
# DATOS DE REFERENCIA CHILE
# Usamos datos reales de regiones y comunas para mayor realismo
# -----------------------------------------------------------------------------
REGIONES_COMUNAS = {
    "Metropolitana": ["Santiago", "Las Condes", "Providencia", "Maipú", "Puente Alto"],
    "Valparaíso":    ["Valparaíso", "Viña del Mar", "Quilpué", "Villa Alemana"],
    "Biobío":        ["Concepción", "Talcahuano", "Chillán", "Los Ángeles"],
    "La Araucanía":  ["Temuco", "Padre Las Casas", "Angol"],
    "Los Lagos":     ["Puerto Montt", "Osorno", "Castro"],
    "Antofagasta":   ["Antofagasta", "Calama", "Tocopilla"],
    "Coquimbo":      ["La Serena", "Coquimbo", "Ovalle"],
    "O'Higgins":     ["Rancagua", "San Fernando", "Pichilemu"],
    "Maule":         ["Talca", "Curicó", "Linares"],
    "Los Ríos":      ["Valdivia", "La Unión"],
}

OCUPACIONES = [
    "Ingeniero", "Médico", "Profesor", "Comerciante", "Empleado",
    "Abogado", "Contador", "Enfermero", "Técnico", "Independiente",
    "Empresario", "Jubilado", "Estudiante", "Funcionario Público"
]

CATEGORIAS_GASTO = [
    "Supermercado", "Salud", "Educación", "Transporte", "Restaurant",
    "Vestuario", "Tecnología", "Entretenimiento", "Servicios Básicos",
    "Farmacia", "Combustible", "Viajes", "Seguros"
]

TIPOS_CUENTA = ["Cuenta Corriente", "Cuenta Vista", "Cuenta Ahorro", "Cuenta RUT"]
CANALES      = ["APP", "WEB", "CAJERO", "SUCURSAL", "POS"]
TIPOS_TXN    = ["CREDITO", "DEBITO", "TRANSFERENCIA", "PAGO_SERVICIO", "GIRO"]

# -----------------------------------------------------------------------------
# GENERADOR DE RUT CHILENO VÁLIDO
# El RUT chileno tiene formato XX.XXX.XXX-D donde D es el dígito verificador.
# El dígito se calcula con el algoritmo módulo 11.
# Esto es importante porque DLP usa el formato Y el dígito para validar.
# -----------------------------------------------------------------------------
def calcular_dv(rut: int) -> str:
    """Calcula el dígito verificador de un RUT chileno usando módulo 11."""
    suma, multiplo = 0, 2
    while rut:
        suma += (rut % 10) * multiplo
        rut  //= 10
        multiplo = 2 if multiplo == 7 else multiplo + 1
    resultado = 11 - (suma % 11)
    return {10: "K", 11: "0"}.get(resultado, str(resultado))

def generar_rut() -> str:
    """Genera un RUT chileno válido con formato XX.XXX.XXX-D."""
    numero = random.randint(5_000_000, 25_000_000)
    dv     = calcular_dv(numero)
    return f"{numero:,}".replace(",", ".") + f"-{dv}"

# -----------------------------------------------------------------------------
# CÁLCULO DE VOLUMEN VARIABLE
# La función retorna cuántas transacciones generar según:
#   - Hora del día (peaks mañana y tarde)
#   - Día de semana (viernes es el mayor)
#   - Día del mes (quincena y fin de mes duplican volumen)
#   - Feriados (sin operación)
# -----------------------------------------------------------------------------
def calcular_volumen(hora: int, fecha: date) -> int:
    """Retorna el número de transacciones a generar para una hora específica."""

    # Sin operación en feriados
    if fecha in FERIADOS_CHILE_2026:
        return 0

    # Volumen base por hora — simula comportamiento real bancario
    volumen_hora = {
        8:  500,   # Apertura — pocas transacciones
        9:  2000,  # Peak mañana inicia
        10: 2200,  # Peak mañana máximo
        11: 1800,  # Bajando del peak
        12: 1500,  # Normal
        13: 800,   # Almuerzo — baja notoria
        14: 1600,  # Retoma actividad
        15: 2300,  # Peak tarde inicia
        16: 2500,  # Peak tarde máximo
        17: 1800,  # Bajando
        18: 1200,  # Cierre — últimas transacciones
    }.get(hora, 1000)

    # Multiplicador por día de semana
    dia_semana = fecha.weekday()  # 0=Lunes, 4=Viernes
    mult_dia = {0: 1.2, 1: 1.0, 2: 1.0, 3: 1.1, 4: 1.5}.get(dia_semana, 1.0)

    # Multiplicador por día del mes (quincena y fin de mes)
    ultimo_dia = 28  # simplificado
    if fecha.day in [15, 16, ultimo_dia, ultimo_dia - 1]:
        mult_mes = 2.0
    else:
        mult_mes = 1.0

    # Ruido aleatorio ±10% para que no sea perfectamente predecible
    ruido = random.uniform(0.9, 1.1)

    return int(volumen_hora * mult_dia * mult_mes * ruido)

# -----------------------------------------------------------------------------
# GENERADORES DE ENTIDADES
# -----------------------------------------------------------------------------
def generar_cliente(id_cliente: str) -> dict:
    """
    Genera un cliente con datos directos, indirectos y sensibles.
    DIRECTO   : rut, nombre, email, telefono, num_tarjeta, num_cuenta
    INDIRECTO : fecha_nacimiento, region, comuna, ocupacion, genero
    SENSIBLE  : saldo, deuda_total, score_crediticio
    """
    region  = random.choice(list(REGIONES_COMUNAS.keys()))
    comuna  = random.choice(REGIONES_COMUNAS[region])
    genero  = random.choice(["M", "F"])
    nombre  = fake.name_male() if genero == "M" else fake.name_female()

    return {
        # --- DATOS DIRECTOS (DLP los detectará y enmascarará) ---
        "id_cliente":       id_cliente,
        "rut":              generar_rut(),
        "nombre_completo":  nombre,
        "email":            fake.email(),
        "telefono":         f"+569{random.randint(10000000, 99999999)}",
        "num_tarjeta":      fake.credit_card_number(),
        "num_cuenta":       f"{random.randint(10000000, 99999999)}",

        # --- DATOS INDIRECTOS (combinados pueden identificar) ---
        "fecha_nacimiento": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "genero":           genero,
        "region":           region,
        "comuna":           comuna,
        "ocupacion":        random.choice(OCUPACIONES),
        "estado_civil":     random.choice(["Soltero", "Casado", "Divorciado", "Viudo"]),
        "nivel_educacion":  random.choice(["Básica", "Media", "Técnica", "Universitaria", "Postgrado"]),

        # --- DATOS SENSIBLES (protegidos por Column Security en Gold) ---
        "saldo_cuenta":         round(random.uniform(0, 50_000_000), 2),
        "deuda_total":          round(random.uniform(0, 20_000_000), 2),
        "score_crediticio":     random.randint(300, 900),
        "renta_mensual":        round(random.uniform(400_000, 10_000_000), 2),
        "tiene_credito":        random.choice([True, False]),
        "categoria_riesgo":     random.choice(["BAJO", "MEDIO", "ALTO"]),

        # --- METADATOS ---
        "fecha_creacion":   fake.date_between(start_date="-5y", end_date="today").isoformat(),
        "arcop_estado":     random.choices(
                                ["ACTIVO", "BLOQUEADO", "ELIMINADO"],
                                weights=[95, 3, 2]
                            )[0],
        "consentimiento_ley19628": True,
        "fecha_consentimiento":    fake.date_between(start_date="-2y", end_date="today").isoformat(),
    }

def generar_transaccion(id_txn: str, clientes: list, hora: int, fecha: date) -> dict:
    """
    Genera una transacción bancaria con volumen y características variables.
    Incluye:
      - 5% duplicadas (error de sistema)
      - 3% con campos nulos (datos incompletos)
      - 2% con valores anómalos (posible fraude)
      - 1% con PII en descripción (para que DLP Discovery lo detecte)
    """
    cliente  = random.choice(clientes)
    es_peak  = hora in [9, 10, 15, 16]
    anomalia = random.random() < 0.02

    # Monto variable: en peaks los montos son más altos
    if anomalia:
        monto = round(random.uniform(5_000_000, 50_000_000), 2)  # Monto sospechoso
    elif es_peak:
        monto = round(random.uniform(10_000, 500_000), 2)
    else:
        monto = round(random.uniform(1_000, 200_000), 2)

    txn = {
        "id_transaccion":   id_txn,
        "id_cliente":       cliente["id_cliente"],
        "num_cuenta":       cliente["num_cuenta"],
        "num_tarjeta_mask": cliente["num_tarjeta"][-4:],  # Solo últimos 4 dígitos
        "monto_clp":        monto,
        "tipo_transaccion": random.choice(TIPOS_TXN),
        "canal":            random.choice(CANALES),
        "categoria_gasto":  random.choice(CATEGORIAS_GASTO),
        "region":           cliente["region"],
        "comuna":           cliente["comuna"],
        "fecha_hora":       datetime.now(TIMEZONE).replace(
                                hour=hora,
                                minute=random.randint(0, 59),
                                second=random.randint(0, 59)
                            ).isoformat(),
        "es_anomalia_flag": anomalia,
        "score_anomalia":   round(random.uniform(0.7, 1.0), 4) if anomalia else round(random.uniform(0.0, 0.3), 4),
        "estado":           "COMPLETADA",
    }

    # --- RUIDO INTENCIONAL ---

    # 3% de registros con campos nulos (datos incompletos reales)
    if random.random() < 0.03:
        txn["categoria_gasto"] = None
        txn["comuna"]          = None

    # 1% con PII en descripción — para que DLP Discovery lo detecte
    # Esto simula cuando un operador escribe datos sensibles en campo libre
    if random.random() < 0.01:
        txn["descripcion"] = f"Transferencia a {cliente['nombre_completo']} RUT {cliente['rut']} tel {cliente['telefono']}"
    else:
        txn["descripcion"] = fake.sentence(nb_words=6)

    # 5% duplicadas (simula error de sistema o retry)
    txn["es_duplicado"] = random.random() < 0.05

    return txn

# -----------------------------------------------------------------------------
# SUBIDA A GCS — BRONZE LAYER
# Estructura de particionado:
#   gs://bucket/bronze/clientes/fecha=YYYY-MM-DD/clientes.json
#   gs://bucket/bronze/transacciones/fecha=YYYY-MM-DD/hora=HH/txn.json
# -----------------------------------------------------------------------------
def subir_a_gcs(bucket_name: str, blob_path: str, data: list) -> None:
    """Sube una lista de diccionarios como JSON Lines a GCS."""
    client  = storage.Client()
    bucket  = client.bucket(bucket_name)
    blob    = bucket.blob(blob_path)
    content = "\n".join(json.dumps(r, ensure_ascii=False) for r in data)
    blob.upload_from_string(content, content_type="application/json")
    print(f"   ✅ Subido: gs://{bucket_name}/{blob_path} ({len(data)} registros)")

# -----------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    ahora    = datetime.now(TIMEZONE)
    fecha    = ahora.date()
    hora     = (ahora - __import__("datetime").timedelta(hours=1)).hour

    print("=" * 60)
    print(" Chilean Bank — Generador de Datos Bronze")
    print(f" Fecha  : {fecha}")
    print(f" Hora   : {hora}:00 (America/Santiago)")
    print(f" Bucket : gs://{BUCKET_NAME}")
    print("=" * 60)

    # --- GENERAR CLIENTES ---
    # Solo generamos clientes una vez al día (primera hora del día)
    # Las ejecuciones siguientes solo generan transacciones nuevas
    NUM_CLIENTES = 5_000
    print(f"\n👥 Generando {NUM_CLIENTES} clientes...")
    clientes = [generar_cliente(f"C{str(i).zfill(6)}") for i in range(1, NUM_CLIENTES + 1)]

    fecha_str   = fecha.strftime("%Y-%m-%d")
    blob_clientes = f"bronze/clientes/fecha={fecha_str}/clientes.json"
    subir_a_gcs(BUCKET_NAME, blob_clientes, clientes)

    # --- GENERAR TRANSACCIONES ---
    num_txn = calcular_volumen(hora, fecha)
    print(f"\n💳 Generando {num_txn} transacciones para hora {hora}:00...")

    if num_txn == 0:
        print("   ⚠️  Feriado o fuera de horario — sin transacciones")
        return

    transacciones = [
        generar_transaccion(f"T{fecha_str.replace('-','')}{hora:02d}{str(i).zfill(6)}", clientes, hora, fecha)
        for i in range(1, num_txn + 1)
    ]

    blob_txn = f"bronze/transacciones/fecha={fecha_str}/hora={hora:02d}/transacciones.json"
    subir_a_gcs(BUCKET_NAME, blob_txn, transacciones)

    print(f"\n{'=' * 60}")
    print(f" ✅ Bronze actualizado exitosamente")
    print(f" Clientes    : {NUM_CLIENTES}")
    print(f" Transacciones: {num_txn}")
    print(f" Hora CL     : {ahora.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"\n PRÓXIMO PASO: python dlp/02_dlp_inspect.py")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
