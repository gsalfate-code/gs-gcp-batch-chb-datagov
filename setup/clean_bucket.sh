#!/bin/bash
# =============================================================================
# SCRIPT: clean_bucket.sh
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Limpia el bucket GCS completamente y lo recrea vacío.
#   Útil para reiniciar la demo desde cero.
#
# ⚠️  ADVERTENCIA: Elimina TODOS los datos del bucket sin recuperación.
#
# USO:
#   bash setup/clean_bucket.sh
# =============================================================================

set -e
set -a; source .env; set +a

echo "=================================================="
echo " Chilean Bank — Limpieza de Bucket GCS"
echo " Bucket: gs://$BUCKET_NAME"
echo "=================================================="
echo ""
echo "⚠️  ADVERTENCIA: Se eliminarán TODOS los datos."
read -p "   ¿Confirmas? (escribe 'si' para continuar): " confirmacion

if [ "$confirmacion" != "si" ]; then
    echo "   ❌ Operación cancelada."
    exit 0
fi

echo ""
echo "🗑️  Eliminando contenido del bucket..."
gcloud storage rm -r gs://$BUCKET_NAME/** 2>/dev/null || echo "   Bucket ya estaba vacío"

echo ""
echo "✅ Bucket limpio: gs://$BUCKET_NAME"
echo "   Listo para nueva carga inicial."
echo ""
echo " PRÓXIMO PASO: python data_generator/00_load_initial_data.py"
echo "=================================================="
