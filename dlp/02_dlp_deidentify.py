# =============================================================================
# SCRIPT: 02_dlp_deidentify.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Lee datos Bronze desde GCS, aplica transformaciones DLP por campo
#   y carga los datos desidentificados en BigQuery Silver.
#
# TRANSFORMACIONES POR CAMPO:
#   Pseudonimización → rut (correlacionable, reversible con llave KMS)
#   FPE              → num_tarjeta, num_cuenta (mismo formato, correlacionable)
#   Masking          → nombre_completo, email, telefono
#   Bucketing        → fecha_nacimiento → rango edad, saldo, renta, deuda
#   Generalización   → comuna → solo region
#   Replace InfoType → descripcion, comentario_operador
#   Column Security  → score_crediticio, saldo exacto (protegido en Gold via DBT)
#   Sin cambio       → ocupacion, genero, estado_civil, categoria_riesgo
#
# FLUJO:
#   GCS Bronze → DLP API → BigQuery Silver
#
# TABLAS SILVER QUE CREA/ACTUALIZA:
#   silver.clientes_deidentified
#   silver.transacciones_deidentified
#   silver.arcop_solicitudes
#
# CONCEPTOS CLAVE:
#   - Pseudonimización: hash determinístico — mismo RUT siempre mismo hash
#   - FPE: cifrado que preserva formato — tarjeta cifrada parece tarjeta real
#   - Bucketing: valor exacto → rango — útil para analítica sin exponer dato
#   - Replace InfoType: PII en texto libre → [TIPO] preservando contexto
#   - Column Security: aplicado en Gold via Policy Tags, no aquí
#
# USO:
#   python dlp/02_dlp_deidentify.py
# =============================================================================

import json
import os
import hashlib
import re
from datetime import datetime, timedelta, date
from typing import Any
import pytz
from google.cloud import dlp_v2, storage, bigquery
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------
load_dotenv()

PROJECT_ID      = os.getenv("PROJECT_ID")
BUCKET_NAME     = os.getenv("BUCKET_NAME")
DATASET_SILVER  = os.getenv("DATASET_SILVER", "silver")
TIMEZONE        = pytz.timezone(os.getenv("DAG_TIMEZONE", "America/Santiago"))

# -----------------------------------------------------------------------------
# BUCKETING — rangos para datos sensibles
#
# Bucketing convierte un valor exacto en un rango.
# Útil para analítica sin exponer el dato preciso.
# Ejemplo: saldo 1.250.000 → "1M-5M"
# -----------------------------------------------------------------------------
RANGOS_EDAD = [
    (0,  17,  "menor_18"),
    (18, 25,  "18-25"),
    (26, 35,  "26-35"),
    (36, 45,  "36-45"),
    (46, 55,  "46-55"),
    (56, 65,  "56-65"),
    (66, 999, "66+"),
]

RANGOS_SALDO = [
    (0,          500_000,    "0-500K"),
    (500_000,    1_000_000,  "500K-1M"),
    (1_000_000,  5_000_000,  "1M-5M"),
    (5_000_000,  10_000_000, "5M-10M"),
    (10_000_000, 50_000_000, "10M-50M"),
    (50_000_000, float("inf"), "50M+"),
]

RANGOS_RENTA = [
    (0,          400_000,   "0-400K"),
    (400_000,    800_000,   "400K-800K"),
    (800_000,    1_500_000, "800K-1.5M"),
    (1_500_000,  3_000_000, "1.5M-3M"),
    (3_000_000,  6_000_000, "3M-6M"),
    (6_000_000,  float("inf"), "6M+"),
]

RANGOS_DEUDA = [
    (0,          0,          "sin_deuda"),
    (1,          500_000,    "0-500K"),
    (500_000,    2_000_000,  "500K-2M"),
    (2_000_000,  5_000_000,  "2M-5M"),
    (5_000_000,  10_000_000, "5M-10M"),
    (10_000_000, float("inf"), "10M+"),
]

# -----------------------------------------------------------------------------
# FUNCIONES DE TRANSFORMACIÓN
# Cada función implementa una técnica de desidentificación específica.
# -----------------------------------------------------------------------------

def pseudonimizar(valor: str, salt: str = "chb-datagov-2026") -> str:
    """
    Pseudonimización con SHA-256 + salt.

    Por qué SHA-256 + salt:
    - Determinístico: mismo RUT → siempre mismo hash
    - No reversible sin la llave (salt)
    - El salt actúa como llave — sin él no puedes reconstruir el original
    - En producción el salt vendría de Cloud KMS, no hardcodeado

    Ejemplo:
      12.456.789-K → a3f8b2c1d4e5f6a7b8c9d0e1f2a3b4c5
    """
    if not valor:
        return ""
    texto  = f"{salt}:{valor}"
    return hashlib.sha256(texto.encode()).hexdigest()[:16]

def aplicar_masking(valor: str, char: str = "*", visible_inicio: int = 0, visible_fin: int = 0) -> str:
    """
    Masking — reemplaza caracteres con char preservando inicio y fin.

    Ejemplos:
      email    j***@***.cl  (visible_inicio=1, visible_fin=3)
      telefono +569****4321 (visible_inicio=4, visible_fin=4)
      nombre   J*** P****   (visible_inicio=1 por palabra)
    """
    if not valor:
        return ""
    if len(valor) <= visible_inicio + visible_fin:
        return char * len(valor)
    medio  = char * (len(valor) - visible_inicio - visible_fin)
    return valor[:visible_inicio] + medio + (valor[-visible_fin:] if visible_fin else "")

def maskear_email(email: str) -> str:
    """j*** @*****.cl"""
    if not email or "@" not in email:
        return "***@***.***"
    usuario, dominio = email.split("@", 1)
    partes_dominio   = dominio.split(".")
    usuario_mask     = usuario[0] + "*" * (len(usuario) - 1) if len(usuario) > 1 else "*"
    dominio_mask     = "*" * len(partes_dominio[0])
    extension        = partes_dominio[-1] if len(partes_dominio) > 1 else "***"
    return f"{usuario_mask}@{dominio_mask}.{extension}"

def maskear_telefono(telefono: str) -> str:
    """+569****4321"""
    if not telefono:
        return ""
    # Preservar prefijo +569 y últimos 4 dígitos
    digitos = re.sub(r'\D', '', telefono)
    if len(digitos) >= 8:
        return f"+569{'*' * (len(digitos) - 7)}{digitos[-4:]}"
    return "*" * len(telefono)

def maskear_nombre(nombre: str) -> str:
    """Juan González → J*** G*******"""
    if not nombre:
        return ""
    palabras = nombre.split()
    return " ".join(
        p[0] + "*" * (len(p) - 1) if len(p) > 1 else p
        for p in palabras
    )

def aplicar_bucketing(valor: float, rangos: list) -> str:
    """
    Bucketing — convierte valor numérico en rango categórico.
    Itera sobre los rangos hasta encontrar el que corresponde.
    """
    if valor is None:
        return "desconocido"
    for min_val, max_val, etiqueta in rangos:
        if min_val <= valor < max_val:
            return etiqueta
    return rangos[-1][2]  # último rango si excede todos

def calcular_rango_edad(fecha_nacimiento_str: str) -> str:
    """Convierte fecha_nacimiento en rango de edad."""
    if not fecha_nacimiento_str:
        return "desconocido"
    try:
        nacimiento = date.fromisoformat(fecha_nacimiento_str)
        hoy        = date.today()
        edad       = (hoy - nacimiento).days // 365
        return aplicar_bucketing(edad, RANGOS_EDAD)
    except Exception:
        return "desconocido"

def fpe_numero(numero: str) -> str:
    """
    Format Preserving Encryption simulado.

    En producción real usaría DLP API con crypto_replace_ffx_fpe_config
    y una llave KMS. Aquí simulamos con hash que preserva longitud y formato.

    El número resultante:
    - Tiene exactamente los mismos dígitos de cantidad
    - Empieza con los mismos 4 dígitos (BIN de la tarjeta)
    - Es determinístico para el mismo input
    - No es el número real

    Ejemplo:
      4532 1234 5678 9012 → 4532 8743 2198 7654
    """
    if not numero:
        return ""
    digitos = re.sub(r'\D', '', numero)
    if not digitos:
        return numero
    # Preservar primeros 4 dígitos (BIN identifica el banco emisor)
    bin_card = digitos[:4]
    resto    = digitos[4:]
    # Hash determinístico del resto
    hash_val = hashlib.sha256(f"fpe:{numero}:chb".encode()).hexdigest()
    # Extraer dígitos del hash
    hash_digits = re.sub(r'\D', '', hash_val)[:len(resto)]
    # Si no hay suficientes dígitos en el hash, rellenar
    while len(hash_digits) < len(resto):
        hash_digits += hash_digits
    hash_digits = hash_digits[:len(resto)]
    return (bin_card + hash_digits)[:16]

def replace_infotype_en_texto(texto: str) -> str:
    """
    Reemplaza PII conocido en texto libre con [TIPO].
    Aplica regex básicos para los tipos más comunes.
    En producción el reemplazo vendría directamente de DLP API.

    Ejemplo:
      "Llamar a Juan RUT 12.456.789-K" → "Llamar a [PERSON_NAME] RUT [CHILE_RUT]"
    """
    if not texto:
        return ""
    # RUT chileno
    texto = re.sub(r'\b\d{1,2}\.?\d{3}\.?\d{3}-[\dkK]\b', '[CHILE_RUT]', texto)
    # Teléfono chileno
    texto = re.sub(r'\+?56\s?9\s?\d{4}\s?\d{4}', '[PHONE_NUMBER]', texto)
    # Email
    texto = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_ADDRESS]', texto)
    # Número de tarjeta (16 dígitos con espacios)
    texto = re.sub(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', '[CREDIT_CARD_NUMBER]', texto)
    return texto

# -----------------------------------------------------------------------------
# DESIDENTIFICACIÓN DE REGISTROS
# -----------------------------------------------------------------------------
def deidentify_cliente(cliente: dict) -> dict:
    """
    Aplica todas las transformaciones a un registro de cliente.
    Retorna el registro Silver — listo para BigQuery.
    """
    return {
        # ID — no se toca, es surrogate key
        "id_cliente":           cliente.get("id_cliente"),

        # PSEUDONIMIZACIÓN — correlacionable, no reversible sin salt
        "rut_pseudo":           pseudonimizar(cliente.get("rut", "")),

        # MASKING — canales de contacto, no necesitas correlacionar
        "nombre_masked":        maskear_nombre(cliente.get("nombre_completo", "")),
        "email_masked":         maskear_email(cliente.get("email", "")),
        "telefono_masked":      maskear_telefono(cliente.get("telefono", "")),

        # FPE — correlacionable entre sistemas, mismo formato
        "num_tarjeta_fpe":      fpe_numero(cliente.get("num_tarjeta", "")),
        "num_cuenta_fpe":       fpe_numero(cliente.get("num_cuenta", "")),

        # INDIRECTOS — bucketing y generalización
        "rango_edad":           calcular_rango_edad(cliente.get("fecha_nacimiento")),
        "genero":               cliente.get("genero"),           # no es PII directo
        "region":               cliente.get("region"),           # generalización natural
        # comuna eliminada — demasiado granular combinada con edad y género
        "ocupacion":            cliente.get("ocupacion"),
        "estado_civil":         cliente.get("estado_civil"),
        "nivel_educacion":      cliente.get("nivel_educacion"),
        "tipo_cuenta":          cliente.get("tipo_cuenta"),

        # SENSIBLES — bucketing (valor exacto protegido por Column Security en Gold)
        "rango_saldo":          aplicar_bucketing(cliente.get("saldo_cuenta", 0), RANGOS_SALDO),
        "rango_renta":          aplicar_bucketing(cliente.get("renta_mensual", 0), RANGOS_RENTA),
        "rango_deuda":          aplicar_bucketing(cliente.get("deuda_total", 0), RANGOS_DEUDA),
        # score y categoría sin cambio en Silver — Column Security en Gold
        "score_crediticio":     cliente.get("score_crediticio"),
        "categoria_riesgo":     cliente.get("categoria_riesgo"),
        "tiene_credito":        cliente.get("tiene_credito"),

        # TEXTO LIBRE — replace InfoType
        "comentario_operador":  replace_infotype_en_texto(cliente.get("comentario_operador", "")),

        # ARCOP — sin cambio, son flags de estado no PII
        "fecha_creacion":               cliente.get("fecha_creacion"),
        "arcop_estado":                 cliente.get("arcop_estado"),
        "arcop_acceso_solicitado":      cliente.get("arcop_acceso_solicitado"),
        "arcop_rectificacion_pending":  cliente.get("arcop_rectificacion_pending"),
        "arcop_cancelacion_solicitado": cliente.get("arcop_cancelacion_solicitado"),
        "arcop_oposicion_activa":       cliente.get("arcop_oposicion_activa"),
        "arcop_portabilidad_entregada": cliente.get("arcop_portabilidad_entregada"),
        "consentimiento_ley19628":      cliente.get("consentimiento_ley19628"),
        "fecha_consentimiento":         cliente.get("fecha_consentimiento"),

        # METADATOS DE PROCESAMIENTO
        "fecha_deidentify":     datetime.now(TIMEZONE).isoformat(),
        "version_dlp":          "1.0",
    }

def deidentify_transaccion(txn: dict) -> dict:
    """
    Aplica transformaciones a una transacción.
    Preserva campos de negocio necesarios para analítica.
    """
    return {
        # IDs — no se tocan
        "id_transaccion":   txn.get("id_transaccion"),
        "id_cliente":       txn.get("id_cliente"),

        # FPE — correlacionable entre sistemas
        "num_cuenta_fpe":       fpe_numero(txn.get("num_cuenta", "")),
        "num_tarjeta_mask":     txn.get("num_tarjeta_mask"),  # ya son últimos 4 dígitos

        # NEGOCIO — sin cambio, necesarios para analítica
        "monto_clp":            txn.get("monto_clp"),
        "tipo_transaccion":     txn.get("tipo_transaccion"),
        "canal":                txn.get("canal"),
        "categoria_gasto":      txn.get("categoria_gasto"),
        "region":               txn.get("region"),
        # comuna eliminada — demasiado granular
        "fecha_hora":           txn.get("fecha_hora"),
        "estado":               txn.get("estado"),

        # TEXTO LIBRE — replace InfoType
        "descripcion":          replace_infotype_en_texto(txn.get("descripcion", "")),

        # FLAGS ML — necesarios para modelo de anomalías
        "es_anomalia_flag":     txn.get("es_anomalia_flag"),
        "score_anomalia":       txn.get("score_anomalia"),
        "es_duplicado":         txn.get("es_duplicado"),

        # METADATOS
        "fecha_deidentify":     datetime.now(TIMEZONE).isoformat(),
    }

# -----------------------------------------------------------------------------
# CARGA A BIGQUERY SILVER
#
# BigQuery acepta JSON Lines directamente.
# Usamos load_table_from_json para inserción batch eficiente.
# write_disposition WRITE_APPEND agrega sin borrar datos anteriores.
# -----------------------------------------------------------------------------
def cargar_a_bigquery(bq_client: bigquery.Client, tabla_id: str, registros: list, schema: list, truncate: bool = False) -> None:
    """
    Carga registros en BigQuery Silver.

    WRITE_APPEND: agrega registros sin borrar los existentes.
    Idempotencia: el id_transaccion/id_cliente previene duplicados en Gold via DBT.
    """
    if not registros:
        print(f"   ⚠️  Sin registros para cargar en {tabla_id}")
        return

    job_config = bigquery.LoadJobConfig(
        schema            = schema,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if truncate else bigquery.WriteDisposition.WRITE_APPEND,
        source_format     = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    job = bq_client.load_table_from_json(registros, tabla_id, job_config=job_config)
    job.result()  # esperar a que termine

    tabla = bq_client.get_table(tabla_id)
    print(f"   ✅ {tabla_id} → {len(registros):,} registros cargados ({tabla.num_rows:,} total en tabla)")

# -----------------------------------------------------------------------------
# SCHEMAS BIGQUERY SILVER
# Definimos el schema explícitamente para control total de tipos.
# -----------------------------------------------------------------------------
SCHEMA_CLIENTES = [
    bigquery.SchemaField("id_cliente",              "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("rut_pseudo",              "STRING"),
    bigquery.SchemaField("nombre_masked",           "STRING"),
    bigquery.SchemaField("email_masked",            "STRING"),
    bigquery.SchemaField("telefono_masked",         "STRING"),
    bigquery.SchemaField("num_tarjeta_fpe",         "STRING"),
    bigquery.SchemaField("num_cuenta_fpe",          "STRING"),
    bigquery.SchemaField("rango_edad",              "STRING"),
    bigquery.SchemaField("genero",                  "STRING"),
    bigquery.SchemaField("region",                  "STRING"),
    bigquery.SchemaField("ocupacion",               "STRING"),
    bigquery.SchemaField("estado_civil",            "STRING"),
    bigquery.SchemaField("nivel_educacion",         "STRING"),
    bigquery.SchemaField("tipo_cuenta",             "STRING"),
    bigquery.SchemaField("rango_saldo",             "STRING"),
    bigquery.SchemaField("rango_renta",             "STRING"),
    bigquery.SchemaField("rango_deuda",             "STRING"),
    bigquery.SchemaField("score_crediticio",        "INTEGER"),
    bigquery.SchemaField("categoria_riesgo",        "STRING"),
    bigquery.SchemaField("tiene_credito",           "BOOLEAN"),
    bigquery.SchemaField("comentario_operador",     "STRING"),
    bigquery.SchemaField("fecha_creacion",          "STRING"),
    bigquery.SchemaField("arcop_estado",            "STRING"),
    bigquery.SchemaField("arcop_acceso_solicitado",      "BOOLEAN"),
    bigquery.SchemaField("arcop_rectificacion_pending",  "BOOLEAN"),
    bigquery.SchemaField("arcop_cancelacion_solicitado", "BOOLEAN"),
    bigquery.SchemaField("arcop_oposicion_activa",       "BOOLEAN"),
    bigquery.SchemaField("arcop_portabilidad_entregada", "BOOLEAN"),
    bigquery.SchemaField("consentimiento_ley19628", "BOOLEAN"),
    bigquery.SchemaField("fecha_consentimiento",    "STRING"),
    bigquery.SchemaField("fecha_deidentify",        "STRING"),
    bigquery.SchemaField("version_dlp",             "STRING"),
]

SCHEMA_TRANSACCIONES = [
    bigquery.SchemaField("id_transaccion",      "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("id_cliente",          "STRING"),
    bigquery.SchemaField("num_cuenta_fpe",      "STRING"),
    bigquery.SchemaField("num_tarjeta_mask",    "STRING"),
    bigquery.SchemaField("monto_clp",           "FLOAT"),
    bigquery.SchemaField("tipo_transaccion",    "STRING"),
    bigquery.SchemaField("canal",               "STRING"),
    bigquery.SchemaField("categoria_gasto",     "STRING"),
    bigquery.SchemaField("region",              "STRING"),
    bigquery.SchemaField("fecha_hora",          "STRING"),
    bigquery.SchemaField("estado",              "STRING"),
    bigquery.SchemaField("descripcion",         "STRING"),
    bigquery.SchemaField("es_anomalia_flag",    "BOOLEAN"),
    bigquery.SchemaField("score_anomalia",      "FLOAT"),
    bigquery.SchemaField("es_duplicado",        "BOOLEAN"),
    bigquery.SchemaField("fecha_deidentify",    "STRING"),
]

SCHEMA_ARCOP = [
    bigquery.SchemaField("id_solicitud",                 "STRING", mode="REQUIRED"),
    bigquery.SchemaField("id_cliente",                   "STRING"),
    bigquery.SchemaField("tipo_derecho",                 "STRING"),
    bigquery.SchemaField("descripcion_tipo",             "STRING"),
    bigquery.SchemaField("fecha_solicitud",              "STRING"),
    bigquery.SchemaField("fecha_limite",                 "STRING"),
    bigquery.SchemaField("fecha_resolucion",             "STRING"),
    bigquery.SchemaField("estado",                       "STRING"),
    bigquery.SchemaField("canal",                        "STRING"),
    bigquery.SchemaField("dias_habiles_max",             "INTEGER"),
    bigquery.SchemaField("dias_resolucion_estimados",    "INTEGER"),
    bigquery.SchemaField("vence_en_dias",                "INTEGER"),
    bigquery.SchemaField("es_critico",                   "BOOLEAN"),
    bigquery.SchemaField("responsable",                  "STRING"),
]

# -----------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    ahora     = datetime.now(TIMEZONE)
    hora      = int(os.getenv("FORCE_HORA", (ahora - timedelta(hours=1)).hour))
    fecha     = ahora.date()
    fecha_str = fecha.strftime("%Y-%m-%d")

    print("=" * 65)
    print(" Chilean Bank — Cloud DLP DeIdentify → Silver")
    print(f" Fecha   : {fecha_str}")
    print(f" Hora    : {hora:02d}:00")
    print(f" Proyecto: {PROJECT_ID}")
    print(f" Destino : BigQuery {DATASET_SILVER}")
    print("=" * 65)
    print()
    print(" Transformaciones aplicadas:")
    print("   rut            → Pseudonimización (SHA-256 + salt)")
    print("   num_tarjeta    → FPE (formato preservado)")
    print("   num_cuenta     → FPE (formato preservado)")
    print("   nombre         → Masking")
    print("   email          → Masking")
    print("   telefono       → Masking")
    print("   fecha_nac      → Bucketing (rango edad)")
    print("   saldo/renta    → Bucketing (rangos CLP)")
    print("   descripcion    → Replace InfoType")
    print("=" * 65)

    gcs_client = storage.Client()
    bq_client  = bigquery.Client(project=PROJECT_ID)
    bucket     = gcs_client.bucket(BUCKET_NAME)

    # -------------------------------------------------------------------------
    # CLIENTES
    # -------------------------------------------------------------------------
    print(f"\n👥 Procesando clientes...")
    blob_clientes = bucket.blob("bronze/clientes/maestro/clientes_maestro.json")

    if blob_clientes.exists():
        content  = blob_clientes.download_as_text()
        clientes = [json.loads(line) for line in content.strip().split("\n")]
        print(f"   Registros Bronze : {len(clientes):,}")

        clientes_silver = [deidentify_cliente(c) for c in clientes]

        # Verificar transformaciones en muestra
        muestra = clientes_silver[0]
        print(f"\n   🔍 Muestra de transformaciones:")
        print(f"   rut_pseudo     : {muestra['rut_pseudo']}")
        print(f"   nombre_masked  : {muestra['nombre_masked']}")
        print(f"   email_masked   : {muestra['email_masked']}")
        print(f"   telefono_masked: {muestra['telefono_masked']}")
        print(f"   num_tarjeta_fpe: {muestra['num_tarjeta_fpe']}")
        print(f"   rango_edad     : {muestra['rango_edad']}")
        print(f"   rango_saldo    : {muestra['rango_saldo']}")
        print(f"   rango_renta    : {muestra['rango_renta']}")

        tabla_clientes = f"{PROJECT_ID}.{DATASET_SILVER}.clientes_deidentified"
        cargar_a_bigquery(bq_client, tabla_clientes, clientes_silver, SCHEMA_CLIENTES, truncate=True)
    else:
        print("   ⚠️  Maestro no encontrado")

    # -------------------------------------------------------------------------
    # TRANSACCIONES
    # -------------------------------------------------------------------------
    print(f"\n💳 Procesando transacciones hora {hora:02d}:00...")
    blob_txn = bucket.blob(
        f"bronze/transacciones/fecha={fecha_str}/hora={hora:02d}/transacciones.json"
    )

    if blob_txn.exists():
        content       = blob_txn.download_as_text()
        transacciones = [json.loads(line) for line in content.strip().split("\n")]
        print(f"   Registros Bronze : {len(transacciones):,}")

        txn_silver = [deidentify_transaccion(t) for t in transacciones]

        # Verificar replace en descripción
        con_replace = [t for t in txn_silver if "[" in t.get("descripcion", "")]
        if con_replace:
            print(f"\n   🔍 Descripciones con PII reemplazado:")
            for t in con_replace[:3]:
                print(f"   → {t['descripcion']}")

        tabla_txn = f"{PROJECT_ID}.{DATASET_SILVER}.transacciones_deidentified"
        cargar_a_bigquery(bq_client, tabla_txn, txn_silver, SCHEMA_TRANSACCIONES)
    else:
        print(f"   ⚠️  Transacciones hora {hora:02d} no encontradas")

    # -------------------------------------------------------------------------
    # ARCOP
    # -------------------------------------------------------------------------
    print(f"\n⚖️  Procesando solicitudes ARCOP...")
    blob_arcop = bucket.blob(f"bronze/arcop/fecha={fecha_str}/solicitudes.json")

    if blob_arcop.exists():
        content     = blob_arcop.download_as_text()
        solicitudes = [json.loads(line) for line in content.strip().split("\n")]
        print(f"   Registros Bronze : {len(solicitudes):,}")

        # ARCOP no tiene PII directo — se carga tal cual
        tabla_arcop = f"{PROJECT_ID}.{DATASET_SILVER}.arcop_solicitudes"
        cargar_a_bigquery(bq_client, tabla_arcop, solicitudes, SCHEMA_ARCOP)
    else:
        print(f"   ⚠️  ARCOP no encontrado para {fecha_str}")

    # -------------------------------------------------------------------------
    # RESUMEN
    # -------------------------------------------------------------------------
    print(f"\n{'=' * 65}")
    print(f" ✅ DeIdentify completado — Bronze → Silver")
    print(f"{'=' * 65}")
    print(f" Tablas Silver actualizadas:")
    print(f"   {DATASET_SILVER}.clientes_deidentified")
    print(f"   {DATASET_SILVER}.transacciones_deidentified")
    print(f"   {DATASET_SILVER}.arcop_solicitudes")
    print(f" Hora CL : {ahora.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"\n PRÓXIMO PASO: cd dbt && dbt run")
    print(f"{'=' * 65}")

if __name__ == "__main__":
    main()
