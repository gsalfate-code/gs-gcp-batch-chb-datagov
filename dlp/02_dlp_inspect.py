# =============================================================================
# SCRIPT: 02_dlp_inspect.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Inspecciona los datos Bronze con Cloud DLP.
#   NO modifica nada — solo detecta y reporta PII encontrado.
#
# QUÉ HACE:
#   1. Lee clientes y transacciones desde GCS Bronze
#   2. Llama a la DLP API con InfoTypes predefinidos + custom CHILE_RUT
#   3. Reporta todos los findings por campo y tipo
#   4. Guarda el reporte en GCS para auditoría
#   5. Alerta si encuentra PII en campos donde NO debería estar
#
# INFOTYPES QUE BUSCA:
#   CHILE_RUT (custom) → RUT formato XX.XXX.XXX-X
#   PERSON_NAME        → nombres de personas
#   EMAIL_ADDRESS      → correos electrónicos
#   PHONE_NUMBER       → teléfonos
#   CREDIT_CARD_NUMBER → números de tarjeta
#
# THRESHOLD: LIKELY
#   Solo reporta findings con probabilidad LIKELY o VERY_LIKELY.
#   Evita falsos positivos sin perder PII real.
#
# CAMPOS ESPERADOS vs INESPERADOS:
#   Esperado   → PII en campos como rut, email, telefono (normal)
#   Inesperado → PII en campos como descripcion, comentario (alerta)
#
# ESTRUCTURA GCS OUTPUT:
#   bronze/dlp_reports/fecha=YYYY-MM-DD/hora=HH/inspect_report.json
#
# USO:
#   python dlp/02_dlp_inspect.py
# =============================================================================

import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
from google.cloud import dlp_v2, storage
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------
load_dotenv()

PROJECT_ID  = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TIMEZONE    = pytz.timezone(os.getenv("DAG_TIMEZONE", "America/Santiago"))

# -----------------------------------------------------------------------------
# CONFIGURACIÓN DLP
#
# CAMPOS SENSIBLES ESPERADOS — PII aquí es normal, se enmascara en deidentify
# CAMPOS SENSIBLES INESPERADOS — PII aquí es una alerta, no debería estar
# -----------------------------------------------------------------------------
CAMPOS_ESPERADOS = {
    "rut", "nombre_completo", "email", "telefono",
    "num_tarjeta", "num_cuenta"
}

CAMPOS_INESPERADOS = {
    "descripcion", "comentario_operador", "categoria_gasto",
    "region", "comuna", "ocupacion"
}

# Likelihood mínimo para reportar un finding
# LIKELIHOOD_UNSPECIFIED < VERY_UNLIKELY < UNLIKELY < POSSIBLE < LIKELY < VERY_LIKELY
MIN_LIKELIHOOD = "LIKELY"

# -----------------------------------------------------------------------------
# INFOTYPES
#
# Combinamos InfoTypes nativos de Google con uno custom para RUT chileno.
# El InfoType custom usa regex + contexto de palabras cercanas para
# aumentar la precisión y reducir falsos positivos.
# -----------------------------------------------------------------------------
def get_infotypes() -> list:
    """
    Retorna la lista de InfoTypes a detectar.
    CHILE_RUT es custom — Google no lo tiene nativo.
    """
    return [
        # InfoTypes nativos Google
        {"name": "PERSON_NAME"},
        {"name": "EMAIL_ADDRESS"},
        {"name": "PHONE_NUMBER"},
        {"name": "CREDIT_CARD_NUMBER"},
        # InfoType custom Chile RUT
        # Se define inline como CustomInfoType con regex
    ]

def get_custom_infotypes() -> list:
    """
    InfoTypes personalizados para el contexto bancario chileno.

    CHILE_RUT:
      Regex: detecta formato XX.XXX.XXX-X y XXXXXXXX-X
      Contexto: palabras como "rut", "cédula", "identificación"
                aumentan el likelihood cuando están cerca
    """
    return [
        {
            "info_type": {"name": "CHILE_RUT"},
            "regex": {
                # Formato con puntos: 12.456.789-K
                # Formato sin puntos: 12456789-K
                "pattern": r"\b\d{1,2}\.?\d{3}\.?\d{3}-[\dkK]\b"
            },
            "likelihood": "LIKELY",
            "detection_rules": [
                {
                    "hotword_rule": {
                        # Si cerca del número hay estas palabras,
                        # aumenta el likelihood a VERY_LIKELY
                        "hotword_regex": {
                            "pattern": r"(?i)(rut|r\.u\.t|cédula|cedula|identificación|id)"
                        },
                        "proximity": {"window_before": 10},
                        "likelihood_adjustment": {
                            "fixed_likelihood": "VERY_LIKELY"
                        }
                    }
                }
            ]
        },
        {
            # Teléfono chileno — más específico que el nativo
            "info_type": {"name": "CHILE_PHONE"},
            "regex": {
                "pattern": r"\+?56\s?9\s?\d{4}\s?\d{4}"
            },
            "likelihood": "LIKELY",
        }
    ]

# -----------------------------------------------------------------------------
# INSPECCIÓN DLP
#
# DLP puede inspeccionar:
#   - Texto plano (content_item con value)
#   - Tabla estructurada (content_item con table)
#   - Archivos en GCS (storage_config)
#
# Usamos tabla estructurada porque tenemos JSON con campos definidos.
# Así DLP nos dice exactamente en QUÉ campo encontró el PII.
# -----------------------------------------------------------------------------
def inspect_tabla(client: dlp_v2.DlpServiceClient, registros: list, nombre_tabla: str) -> dict:
    """
    Inspecciona una lista de registros JSON con DLP.
    Retorna un resumen de findings por campo y tipo de InfoType.

    Lógica:
    1. Convierte los registros a formato Table de DLP
       (headers + rows)
    2. Llama a inspect_content
    3. Procesa los findings y los agrupa por campo
    """
    if not registros:
        return {}

    # Tomar muestra representativa — DLP tiene límite de 500KB por request
    # Para tablas grandes tomamos muestra de 100 registros
    muestra = registros[:100] if len(registros) > 100 else registros

    # Obtener todos los campos del primer registro
    headers = list(muestra[0].keys())

    # Construir tabla DLP
    # Cada celda es un Value con string_value
    rows = []
    for registro in muestra:
        cells = []
        for header in headers:
            valor = registro.get(header, "")
            cells.append({"string_value": str(valor) if valor is not None else ""})
        rows.append({"values": cells})

    # Configuración de inspección
    inspect_config = {
        "info_types":        get_infotypes(),
        "custom_info_types": get_custom_infotypes(),
        "min_likelihood":    MIN_LIKELIHOOD,
        "include_quote":     True,   # incluir el valor encontrado en el finding
        "limits": {
            "max_findings_per_request": 1000
        }
    }

    content_item = {
        "table": {
            "headers": [{"name": h} for h in headers],
            "rows":    rows
        }
    }

    parent   = f"projects/{PROJECT_ID}/locations/global"
    response = client.inspect_content(
        request={
            "parent":         parent,
            "inspect_config": inspect_config,
            "item":           content_item,
        }
    )

    # Procesar findings
    # Agrupar por campo y tipo de InfoType
    findings_por_campo = defaultdict(lambda: defaultdict(int))
    alertas            = []

    for finding in response.result.findings:
        # Obtener el campo donde se encontró el PII
        campo = "desconocido"
        if finding.location.content_locations:
            loc = finding.location.content_locations[0]
            if loc.record_location.field_id.name:
                campo = loc.record_location.field_id.name

        info_type  = finding.info_type.name
        likelihood = finding.likelihood.name

        findings_por_campo[campo][info_type] += 1

        # Alerta si PII está en campo inesperado
        if campo in CAMPOS_INESPERADOS:
            alertas.append({
                "campo":      campo,
                "info_type":  info_type,
                "likelihood": likelihood,
                "muestra":    finding.quote[:50] if finding.quote else "",
                "tabla":      nombre_tabla,
            })

    return {
        "tabla":             nombre_tabla,
        "registros_muestra": len(muestra),
        "total_findings":    len(response.result.findings),
        "findings_por_campo": dict(findings_por_campo),
        "alertas_pii_inesperado": alertas,
    }

# -----------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    ahora     = datetime.now(TIMEZONE)
    hora      = (ahora - timedelta(hours=1)).hour
    fecha     = ahora.date()
    fecha_str = fecha.strftime("%Y-%m-%d")

    print("=" * 65)
    print(" Chilean Bank — Cloud DLP Inspect")
    print(f" Fecha   : {fecha_str}")
    print(f" Hora    : {hora:02d}:00")
    print(f" Proyecto: {PROJECT_ID}")
    print(f" Threshold: {MIN_LIKELIHOOD}")
    print("=" * 65)

    # Inicializar clientes
    dlp_client = dlp_v2.DlpServiceClient()
    gcs_client = storage.Client()
    bucket     = gcs_client.bucket(BUCKET_NAME)

    reporte_completo = {
        "fecha":      fecha_str,
        "hora":       hora,
        "ejecutado":  ahora.isoformat(),
        "threshold":  MIN_LIKELIHOOD,
        "resultados": [],
        "resumen": {
            "total_findings":         0,
            "total_alertas":          0,
            "campos_con_pii":         [],
            "campos_con_pii_inesperado": [],
        }
    }

    # -------------------------------------------------------------------------
    # INSPECCIONAR CLIENTES
    # -------------------------------------------------------------------------
    print(f"\n🔍 Inspeccionando clientes...")
    blob_clientes = bucket.blob("bronze/clientes/maestro/clientes_maestro.json")

    if blob_clientes.exists():
        content  = blob_clientes.download_as_text()
        # Tomar muestra de 200 registros para inspección
        clientes = [json.loads(line) for line in content.strip().split("\n")][:200]
        print(f"   Muestra: {len(clientes):,} registros")

        resultado_clientes = inspect_tabla(dlp_client, clientes, "clientes_maestro")
        reporte_completo["resultados"].append(resultado_clientes)

        print(f"\n   📊 Findings en clientes:")
        for campo, tipos in resultado_clientes["findings_por_campo"].items():
            estado = "⚠️  INESPERADO" if campo in CAMPOS_INESPERADOS else "✅ esperado"
            for tipo, count in tipos.items():
                print(f"      {estado} | {campo:<30} | {tipo:<25} | {count:>4} findings")

        if resultado_clientes["alertas_pii_inesperado"]:
            print(f"\n   🚨 ALERTAS — PII en campos inesperados:")
            for alerta in resultado_clientes["alertas_pii_inesperado"]:
                print(f"      Campo: {alerta['campo']} | Tipo: {alerta['info_type']} | Muestra: {alerta['muestra']}")
    else:
        print("   ⚠️  Maestro de clientes no encontrado")

    # -------------------------------------------------------------------------
    # INSPECCIONAR TRANSACCIONES
    # -------------------------------------------------------------------------
    print(f"\n🔍 Inspeccionando transacciones hora {hora:02d}:00...")
    blob_txn = bucket.blob(
        f"bronze/transacciones/fecha={fecha_str}/hora={hora:02d}/transacciones.json"
    )

    if blob_txn.exists():
        content      = blob_txn.download_as_text()
        transacciones = [json.loads(line) for line in content.strip().split("\n")][:200]
        print(f"   Muestra: {len(transacciones):,} registros")

        resultado_txn = inspect_tabla(dlp_client, transacciones, "transacciones")
        reporte_completo["resultados"].append(resultado_txn)

        print(f"\n   📊 Findings en transacciones:")
        for campo, tipos in resultado_txn["findings_por_campo"].items():
            estado = "⚠️  INESPERADO" if campo in CAMPOS_INESPERADOS else "✅ esperado"
            for tipo, count in tipos.items():
                print(f"      {estado} | {campo:<30} | {tipo:<25} | {count:>4} findings")

        if resultado_txn["alertas_pii_inesperado"]:
            print(f"\n   🚨 ALERTAS — PII en campos inesperados:")
            for alerta in resultado_txn["alertas_pii_inesperado"]:
                print(f"      Campo: {alerta['campo']} | Tipo: {alerta['info_type']} | Muestra: {alerta['muestra']}")
    else:
        print(f"   ⚠️  Transacciones hora {hora:02d} no encontradas")

    # -------------------------------------------------------------------------
    # INSPECCIONAR ARCOP
    # -------------------------------------------------------------------------
    print(f"\n🔍 Inspeccionando solicitudes ARCOP...")
    blob_arcop = bucket.blob(f"bronze/arcop/fecha={fecha_str}/solicitudes.json")

    if blob_arcop.exists():
        content    = blob_arcop.download_as_text()
        solicitudes = [json.loads(line) for line in content.strip().split("\n")]
        print(f"   Muestra: {len(solicitudes):,} registros")

        resultado_arcop = inspect_tabla(dlp_client, solicitudes, "arcop_solicitudes")
        reporte_completo["resultados"].append(resultado_arcop)

        print(f"\n   📊 Findings en ARCOP:")
        if resultado_arcop["findings_por_campo"]:
            for campo, tipos in resultado_arcop["findings_por_campo"].items():
                for tipo, count in tipos.items():
                    print(f"      {campo:<30} | {tipo:<25} | {count:>4} findings")
        else:
            print("      ✅ Sin PII directo en solicitudes ARCOP")
    else:
        print(f"   ⚠️  Solicitudes ARCOP no encontradas")

    # -------------------------------------------------------------------------
    # RESUMEN Y GUARDADO DEL REPORTE
    # -------------------------------------------------------------------------
    total_findings = sum(r.get("total_findings", 0) for r in reporte_completo["resultados"])
    total_alertas  = sum(len(r.get("alertas_pii_inesperado", [])) for r in reporte_completo["resultados"])

    campos_con_pii = list({
        campo
        for r in reporte_completo["resultados"]
        for campo in r.get("findings_por_campo", {}).keys()
    })

    campos_inesperados_encontrados = list({
        alerta["campo"]
        for r in reporte_completo["resultados"]
        for alerta in r.get("alertas_pii_inesperado", [])
    })

    reporte_completo["resumen"] = {
        "total_findings":              total_findings,
        "total_alertas":               total_alertas,
        "campos_con_pii":              campos_con_pii,
        "campos_con_pii_inesperado":   campos_inesperados_encontrados,
    }

    # Guardar reporte en GCS para auditoría CMF
    blob_reporte = bucket.blob(
        f"bronze/dlp_reports/fecha={fecha_str}/hora={hora:02d}/inspect_report.json"
    )
    blob_reporte.upload_from_string(
        json.dumps(reporte_completo, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

    print(f"\n{'=' * 65}")
    print(f" ✅ Inspección completada")
    print(f"{'=' * 65}")
    print(f" Total findings    : {total_findings:,}")
    print(f" Alertas PII       : {total_alertas:,} {'🚨' if total_alertas > 0 else '✅'}")
    print(f" Campos con PII    : {', '.join(campos_con_pii) if campos_con_pii else 'ninguno'}")
    if campos_inesperados_encontrados:
        print(f" ⚠️  PII inesperado en: {', '.join(campos_inesperados_encontrados)}")
    print(f" Reporte guardado  : bronze/dlp_reports/fecha={fecha_str}/hora={hora:02d}/")
    print(f"\n PRÓXIMO PASO: python dlp/02_dlp_deidentify.py")
    print(f"{'=' * 65}")

if __name__ == "__main__":
    main()
