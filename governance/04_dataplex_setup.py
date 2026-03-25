# =============================================================================
# SCRIPT: 04_dataplex_setup.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Configura el Data Lake en Dataplex con zonas y assets.
#   Usa gcloud CLI en vez de Python SDK para evitar quota exhaustion.
#   La CLI hace una sola llamada por operación vs múltiples del SDK.
#
# JERARQUÍA:
#   Lake: chb-datagov-lake
#     Zona RAW     → GCS Bronze
#     Zona CURATED → BigQuery Silver
#     Zona CURATED → BigQuery Gold
# =============================================================================

import os
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
REGION     = os.getenv("REGION", "us-central1")
BUCKET     = os.getenv("BUCKET_NAME")
LAKE_ID    = "chb-datagov-lake"

def run(cmd: str, descripcion: str) -> bool:
    """Ejecuta un comando gcloud y maneja errores gracefully."""
    print(f"   → {descripcion}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"   ✅ OK")
        return True
    elif "already exists" in result.stderr.lower() or "in use" in result.stderr.lower():
        print(f"   ℹ️  Ya existe — continuando")
        return True
    else:
        print(f"   ⚠️  {result.stderr.strip()[:200]}")
        return False

def main():
    print("=" * 65)
    print(" Chilean Bank — Dataplex Setup (via gcloud CLI)")
    print(f" Proyecto : {PROJECT_ID}")
    print(f" Región   : {REGION}")
    print(f" Lake     : {LAKE_ID}")
    print("=" * 65)

    # LAGO
    print(f"\n🌊 Lake...")
    run(
        f"gcloud dataplex lakes create {LAKE_ID} "
        f"--project={PROJECT_ID} "
        f"--location={REGION} "
        f"--display-name='Chilean Bank DataGov Lake' "
        f"--quiet",
        "Creando lake"
    )
    time.sleep(10)

    # ZONAS
    print(f"\n📁 Zonas...")
    run(
        f"gcloud dataplex zones create bronze-zone "
        f"--lake={LAKE_ID} "
        f"--project={PROJECT_ID} "
        f"--location={REGION} "
        f"--type=RAW "
        f"--resource-location-type=SINGLE_REGION "
        f"--display-name='Bronze Zone — Datos Crudos' "
        f"--quiet",
        "Creando bronze-zone"
    )
    time.sleep(10)

    run(
        f"gcloud dataplex zones create silver-zone "
        f"--lake={LAKE_ID} "
        f"--project={PROJECT_ID} "
        f"--location={REGION} "
        f"--type=CURATED "
        f"--resource-location-type=SINGLE_REGION "
        f"--display-name='Silver Zone — Datos Desidentificados' "
        f"--quiet",
        "Creando silver-zone"
    )
    time.sleep(10)

    run(
        f"gcloud dataplex zones create gold-zone "
        f"--lake={LAKE_ID} "
        f"--project={PROJECT_ID} "
        f"--location={REGION} "
        f"--type=CURATED "
        f"--resource-location-type=SINGLE_REGION "
        f"--display-name='Gold Zone — Datos de Negocio' "
        f"--quiet",
        "Creando gold-zone"
    )
    time.sleep(15)

    # ASSETS
    print(f"\n🪣 Assets...")
    run(
        f"gcloud dataplex assets create bronze-bucket "
        f"--lake={LAKE_ID} "
        f"--zone=bronze-zone "
        f"--project={PROJECT_ID} "
        f"--location={REGION} "
        f"--resource-type=STORAGE_BUCKET "
        f"--resource-name=projects/{PROJECT_ID}/buckets/{BUCKET} "
        f"--display-name='Bronze GCS Bucket' "
        f"--quiet",
        "Registrando GCS Bronze"
    )
    time.sleep(15)

    run(
        f"gcloud dataplex assets create silver-dataset "
        f"--lake={LAKE_ID} "
        f"--zone=silver-zone "
        f"--project={PROJECT_ID} "
        f"--location={REGION} "
        f"--resource-type=BIGQUERY_DATASET "
        f"--resource-name=projects/{PROJECT_ID}/datasets/silver "
        f"--display-name='Silver BigQuery Dataset' "
        f"--quiet",
        "Registrando BigQuery Silver"
    )
    time.sleep(15)

    run(
        f"gcloud dataplex assets create gold-dataset "
        f"--lake={LAKE_ID} "
        f"--zone=gold-zone "
        f"--project={PROJECT_ID} "
        f"--location={REGION} "
        f"--resource-type=BIGQUERY_DATASET "
        f"--resource-name=projects/{PROJECT_ID}/datasets/gold "
        f"--display-name='Gold BigQuery Dataset' "
        f"--quiet",
        "Registrando BigQuery Gold"
    )

    print(f"\n{'=' * 65}")
    print(f" ✅ Dataplex configurado")
    print(f"{'=' * 65}")
    print(f" https://console.cloud.google.com/dataplex/lakes")
    print(f"\n PRÓXIMO PASO: python governance/04_catalog_tags.py")
    print(f"{'=' * 65}")

if __name__ == "__main__":
    main()
