#!/bin/bash
# =============================================================================
# SCRIPT: 00_setup.sh
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Script de configuración inicial del proyecto GCP.
#   Se ejecuta UNA SOLA VEZ antes de comenzar el pipeline.
#   Activa todas las APIs necesarias, crea el Service Account,
#   asigna permisos y crea la infraestructura base (bucket + datasets).
#
# CONCEPTOS CLAVE:
#   - APIs GCP: cada servicio debe activarse explícitamente antes de usarse
#   - Service Account (SA): identidad que usa el DAG para operar en GCP
#     sin credenciales personales. Principio de mínimo privilegio.
#   - IAM Roles: definen qué puede hacer el SA. Solo los necesarios.
#   - GCS Bucket: almacenamiento de objetos. Aquí vivirá la capa Bronze.
#   - BigQuery Datasets: contenedores de tablas. Uno por capa Medallion.
#
# USO:
#   bash setup/00_setup.sh
# =============================================================================

set -e

# Cargar variables desde .env
set -a; source .env; set +a

echo "=================================================="
echo " Chilean Bank — Data Governance Setup"
echo " Proyecto : $PROJECT_ID"
echo " Región   : $REGION"
echo " Fecha    : $(TZ=America/Santiago date)"
echo "=================================================="

echo ""
echo "📌 PASO 1: Configurando proyecto activo..."
gcloud config set project $PROJECT_ID

echo ""
echo "🔌 PASO 2: Activando APIs de GCP..."
echo "   Esto puede tardar 1-2 minutos..."
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  dlp.googleapis.com \
  datacatalog.googleapis.com \
  dataplex.googleapis.com \
  composer.googleapis.com \
  aiplatform.googleapis.com \
  cloudkms.googleapis.com \
  --quiet
echo "   ✅ APIs activadas"

echo ""
echo "👤 PASO 3: Creando Service Account..."
gcloud iam service-accounts create $SA_NAME \
  --display-name="Chilean Bank DataGov SA" \
  --quiet
echo "   ✅ SA creado: ${SA_EMAIL}"

echo ""
echo "🔐 PASO 4: Asignando roles IAM..."
for ROLE in \
  roles/bigquery.admin \
  roles/storage.admin \
  roles/dlp.admin \
  roles/datacatalog.admin \
  roles/dataplex.admin
do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --quiet
  echo "   ✅ Rol asignado: $ROLE"
done

echo ""
echo "🪣 PASO 5: Creando bucket GCS..."
gcloud storage buckets create gs://$BUCKET_NAME \
  --location=$REGION \
  --quiet
echo "   ✅ Bucket creado: gs://$BUCKET_NAME"

echo ""
echo "📊 PASO 6: Creando datasets BigQuery..."
bq --location=$LOCATION mk --dataset ${PROJECT_ID}:${DATASET_BRONZE}
echo "   ✅ Dataset creado: $DATASET_BRONZE"
bq --location=$LOCATION mk --dataset ${PROJECT_ID}:${DATASET_SILVER}
echo "   ✅ Dataset creado: $DATASET_SILVER"
bq --location=$LOCATION mk --dataset ${PROJECT_ID}:${DATASET_GOLD}
echo "   ✅ Dataset creado: $DATASET_GOLD"

echo ""
echo "=================================================="
echo " 🎉 Setup completado exitosamente"
echo "=================================================="
echo " Bucket  : gs://$BUCKET_NAME"
echo " Datasets: $DATASET_BRONZE | $DATASET_SILVER | $DATASET_GOLD"
echo " SA      : $SA_EMAIL"
echo " Hora CL : $(TZ=America/Santiago date)"
echo ""
echo " PRÓXIMO PASO: python data_generator/01_generate_data.py"
echo "=================================================="
