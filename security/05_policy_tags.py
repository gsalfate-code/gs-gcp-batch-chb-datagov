# =============================================================================
# SCRIPT: 05_policy_tags.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Implementa Column-Level Security en BigQuery usando Policy Tags.
#
# CONCEPTOS CLAVE:
#   Taxonomy        : contenedor de Policy Tags (jerarquía de permisos)
#   Policy Tag      : etiqueta que se asigna a una columna en BigQuery
#   Fine-Grained    : cuando una columna tiene Policy Tag, solo usuarios con
#                     rol datacatalog.categoryFineGrainedReader pueden verla.
#                     El resto ve NULL aunque hagan SELECT *
#
# COLUMNAS PROTEGIDAS:
#   CONFIDENTIAL → rut_pseudo, score_crediticio
#   INTERNAL     → num_tarjeta_fpe, num_cuenta_fpe, rango_saldo, rango_renta
#
# TODO SE HACE CON PYTHON SDK — no gcloud CLI
# Usamos datacatalog_v1.PolicyTagManagerClient() para taxonomy y tags
# Usamos bigquery.Client() para aplicar los tags al schema
#
# USO:
#   python security/05_policy_tags.py
# =============================================================================

import os
import json
from dotenv import load_dotenv
from google.cloud import datacatalog_v1, bigquery
from google.api_core.exceptions import AlreadyExists

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
REGION     = os.getenv("REGION", "us-central1")
DATASET    = "gold"

def crear_taxonomy(client: datacatalog_v1.PolicyTagManagerClient) -> str:
    """
    Crea la Taxonomy — contenedor de todos los Policy Tags.
    Si ya existe la retorna sin recrear.
    """
    print(f"\n📋 Creando Taxonomy...")
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"

    for tax in client.list_taxonomies(parent=parent):
        if tax.display_name == "CHB Data Classification Taxonomy":
            print(f"   ℹ️  Ya existe: {tax.name}")
            return tax.name

    taxonomy = datacatalog_v1.Taxonomy(
        display_name = "CHB Data Classification Taxonomy",
        description  = "Taxonomía de clasificación para Chilean Bank — Ley 19.628",
        activated_policy_types = [
            datacatalog_v1.Taxonomy.PolicyType.FINE_GRAINED_ACCESS_CONTROL
        ]
    )
    result = client.create_taxonomy(parent=parent, taxonomy=taxonomy)
    print(f"   ✅ Taxonomy creada: {result.name}")
    return result.name

def crear_policy_tag(client: datacatalog_v1.PolicyTagManagerClient,
                     taxonomy_name: str, display_name: str, description: str) -> str:
    """
    Crea un Policy Tag dentro de la Taxonomy.
    Si ya existe lo retorna sin recrear.
    """
    print(f"\n🏷️  Creando Policy Tag: {display_name}...")

    for tag in client.list_policy_tags(parent=taxonomy_name):
        if tag.display_name == display_name:
            print(f"   ℹ️  Ya existe: {tag.name}")
            return tag.name

    policy_tag = datacatalog_v1.PolicyTag(
        display_name = display_name,
        description  = description
    )
    result = client.create_policy_tag(parent=taxonomy_name, policy_tag=policy_tag)
    print(f"   ✅ Policy Tag creado: {result.name}")
    return result.name

def aplicar_policy_tag_columna(bq_client: bigquery.Client,
                                tabla: str, columna: str,
                                policy_tag_name: str) -> None:
    """
    Aplica un Policy Tag a una columna de BigQuery.
    Actualiza el schema de la tabla agregando policyTags a la columna.
    Después de esto, la columna requiere rol Fine-Grained Reader para ser vista.
    """
    tabla_ref = f"{PROJECT_ID}.{DATASET}.{tabla}"

    # Obtener tabla y su schema actual
    bq_tabla  = bq_client.get_table(tabla_ref)
    schema    = bq_tabla.schema
    nuevo_schema = []

    columna_encontrada = False
    for field in schema:
        if field.name == columna:
            # Aplicar Policy Tag a esta columna
            nuevo_campo = bigquery.SchemaField(
                name         = field.name,
                field_type   = field.field_type,
                mode         = field.mode,
                description  = field.description,
                policy_tags  = bigquery.PolicyTagList(names=[policy_tag_name])
            )
            nuevo_schema.append(nuevo_campo)
            columna_encontrada = True
        else:
            nuevo_schema.append(field)

    if not columna_encontrada:
        print(f"   ⚠️  Columna {columna} no encontrada en {tabla}")
        return

    # Actualizar schema de la tabla
    bq_tabla.schema = nuevo_schema
    bq_client.update_table(bq_tabla, ["schema"])
    print(f"   ✅ {tabla}.{columna} → {policy_tag_name.split('/')[-1]}")

def main():
    print("=" * 65)
    print(" Chilean Bank — Column Security (Policy Tags)")
    print(f" Proyecto : {PROJECT_ID}")
    print(f" Dataset  : {DATASET}")
    print(f" Región   : {REGION}")
    print("=" * 65)

    ptm_client = datacatalog_v1.PolicyTagManagerClient()
    bq_client  = bigquery.Client(project=PROJECT_ID)

    # PASO 1: Crear Taxonomy
    taxonomy_name = crear_taxonomy(ptm_client)

    # PASO 2: Crear Policy Tags
    tag_confidential = crear_policy_tag(
        ptm_client, taxonomy_name,
        "CONFIDENTIAL",
        "Datos altamente sensibles — RUT pseudonimizado, score crediticio. Solo roles autorizados."
    )

    tag_internal = crear_policy_tag(
        ptm_client, taxonomy_name,
        "INTERNAL",
        "Datos internos — tarjetas FPE, cuentas FPE, rangos financieros."
    )

    # PASO 3: Aplicar a columnas de dim_cliente
    print(f"\n🔐 Aplicando a gold.dim_cliente...")
    for col in ["rut_pseudo", "score_crediticio"]:
        aplicar_policy_tag_columna(bq_client, "dim_cliente", col, tag_confidential)
    for col in ["num_tarjeta_fpe", "num_cuenta_fpe", "rango_saldo", "rango_renta"]:
        aplicar_policy_tag_columna(bq_client, "dim_cliente", col, tag_internal)

    # PASO 4: Aplicar a columnas de fact_transacciones
    print(f"\n🔐 Aplicando a gold.fact_transacciones...")
    for col in ["rut_pseudo", "score_crediticio"]:
        aplicar_policy_tag_columna(bq_client, "fact_transacciones", col, tag_confidential)
    for col in ["num_cuenta_fpe", "num_tarjeta_mask"]:
        aplicar_policy_tag_columna(bq_client, "fact_transacciones", col, tag_internal)

    # PASO 5: Dar acceso a tu usuario como Fine-Grained Reader
    # Esto permite que tu cuenta vea las columnas protegidas
    print(f"\n👤 Otorgando acceso Fine-Grained Reader a tu usuario...")
    import subprocess
    result = subprocess.run(
        f"gcloud data-catalog taxonomies add-iam-policy-binding "
        f"{taxonomy_name} "
        f"--location={REGION} "
        f"--member='user:gsalfate.gcp@gmail.com' "
        f"--role='roles/datacatalog.categoryFineGrainedReader' "
        f"--quiet",
        shell=True, capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"   ✅ gsalfate.gcp@gmail.com puede ver columnas protegidas")
    else:
        print(f"   ⚠️  {result.stderr[:150]}")

    print(f"\n{'=' * 65}")
    print(f" ✅ Column Security configurado")
    print(f"{'=' * 65}")
    print(f" Taxonomy    : CHB Data Classification Taxonomy")
    print(f" CONFIDENTIAL: rut_pseudo, score_crediticio")
    print(f" INTERNAL    : num_tarjeta_fpe, num_cuenta_fpe, rango_saldo, rango_renta")
    print(f"")
    print(f" Demostración en BigQuery Console:")
    print(f" SELECT rut_pseudo, score_crediticio FROM gold.dim_cliente LIMIT 5")
    print(f" → Tu usuario ve los valores reales")
    print(f" → Un usuario sin rol ve NULL")
    print(f"")
    print(f" PRÓXIMO PASO: python ai/06_train_model.py")
    print(f"{'=' * 65}")

if __name__ == "__main__":
    main()
