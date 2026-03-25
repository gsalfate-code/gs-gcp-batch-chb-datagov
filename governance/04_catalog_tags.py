# =============================================================================
# SCRIPT: 04_catalog_tags.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Aplica Aspects de sensibilidad a tablas Silver y Gold.
#   Usa entry names directos desde gcloud dataplex entries list
#   en vez de lookup (que requiere permisos adicionales).
# =============================================================================

import os
import json
import subprocess
from datetime import datetime
import pytz
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID     = os.getenv("PROJECT_ID")
REGION         = os.getenv("REGION", "us-central1")
TIMEZONE       = pytz.timezone(os.getenv("DAG_TIMEZONE", "America/Santiago"))
ASPECT_TYPE_ID = "chb-sensitivity"

def run(cmd: str) -> tuple:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == 0, result.stdout.strip(), result.stderr.strip()

def get_entry_name(tabla: str, dataset: str) -> str:
    """Obtiene el entry name completo desde gcloud dataplex entries list."""
    success, out, err = run(
        f"gcloud dataplex entries list "
        f"--location={REGION} "
        f"--project={PROJECT_ID} "
        f"--entry-group='@bigquery' "
        f"--filter='name:{tabla}' "
        f"--format='value(name)' "
        f"--limit=1"
    )
    if success and out:
        # Construir el full entry name
        return (
            f"projects/{PROJECT_ID}/locations/{REGION}"
            f"/entryGroups/@bigquery/entries/"
            f"//bigquery.googleapis.com/projects/{PROJECT_ID}"
            f"/datasets/{dataset}/tables/{tabla}"
        )
    return ""

def aplicar_aspect(tabla_fqn: str, sensitivity: str, pii: bool,
                   ley19628: bool, owner: str, retention: int, arcop: bool) -> None:
    proyecto, dataset, tabla = tabla_fqn.split(".")

    # Entry name directo — formato conocido para BigQuery
    entry_name = (
        f"projects/{PROJECT_ID}/locations/{REGION}"
        f"/entryGroups/@bigquery/entries/"
        f"bigquery.googleapis.com/projects/{PROJECT_ID}"
        f"/datasets/{dataset}/tables/{tabla}"
    )

    aspect_file = f"/tmp/aspect_{dataset}_{tabla}.json"
    aspect_key  = f"{PROJECT_ID}.{REGION}.{ASPECT_TYPE_ID}"

    with open(aspect_file, "w") as f:
        json.dump({
            aspect_key: {
                "data": {
                    "sensitivity_level": sensitivity,
                    "pii_present":       pii,
                    "ley_19628_applies": ley19628,
                    "data_owner":        owner,
                    "retention_years":   retention,
                    "arcop_relevant":    arcop,
                }
            }
        }, f)

    success, out, err = run(
        f"gcloud dataplex entries update '{entry_name}' "
        f"--update-aspects={aspect_file} "
        f"--project={PROJECT_ID} --quiet"
    )

    if success:
        print(f"   ✅ {tabla:<35} → {sensitivity}")
    else:
        print(f"   ⚠️  {tabla}: {(out+err)[:150]}")

def main():
    ahora = datetime.now(TIMEZONE)
    print("=" * 65)
    print(" Chilean Bank — Dataplex Catalog Aspects")
    print(f" Proyecto  : {PROJECT_ID}")
    print(f" Ejecutado : {ahora.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 65)

    print(f"\n🥈 Aplicando aspects a Silver...")
    for t, s, p, l, o, r, a in [
        ("clientes_deidentified",     "CONFIDENTIAL", True,  True,  "Data Engineering", 7, True),
        ("transacciones_deidentified","CONFIDENTIAL", True,  True,  "Data Engineering", 7, True),
        ("arcop_solicitudes",         "CONFIDENTIAL", False, True,  "Compliance Team",  7, True),
    ]:
        aplicar_aspect(f"{PROJECT_ID}.silver.{t}", s, p, l, o, r, a)

    print(f"\n🥇 Aplicando aspects a Gold...")
    for t, s, p, l, o, r, a in [
        ("dim_cliente",            "INTERNAL", False, True,  "Analytics Team",   7, True),
        ("fact_transacciones",     "INTERNAL", False, True,  "Analytics Team",   7, True),
        ("fact_arcop_solicitudes", "INTERNAL", False, True,  "Compliance Team",  7, True),
        ("mart_calidad_datos",     "INTERNAL", False, False, "Data Engineering", 3, False),
        ("mart_arcop_compliance",  "INTERNAL", False, True,  "Compliance Team",  7, True),
        ("mart_anomalias",         "INTERNAL", False, True,  "Risk Team",        7, True),
    ]:
        aplicar_aspect(f"{PROJECT_ID}.gold.{t}", s, p, l, o, r, a)

    print(f"\n{'=' * 65}")
    print(f" ✅ Aspects aplicados")
    print(f" Ver: https://console.cloud.google.com/dataplex/catalog")
    print(f"\n PRÓXIMO PASO: python security/05_policy_tags.py")
    print(f"{'=' * 65}")

if __name__ == "__main__":
    main()
