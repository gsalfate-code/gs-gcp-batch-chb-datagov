#!/bin/bash
set -e
cd ~/gs-gcp-batch-chb-datagov

echo "🔄 Generando y procesando día completo..."
for HORA in 08 09 10 11 12 13 14 15 16 17 18 19; do
  echo "  → Hora $HORA"
  FORCE_HORA=$HORA python data_generator/01_generate_data.py 2>&1 | grep -E "transacciones.json"
  FORCE_HORA=$HORA python dlp/02_dlp_deidentify.py 2>&1 | grep -E "total en tabla"
done

echo "🔧 DBT..."
cd dbt/chb_datagov && dbt run --full-refresh --profiles-dir ~/.dbt 2>&1 | grep -E "OK|ERROR|Finished"
cd ~/gs-gcp-batch-chb-datagov

echo "🤖 ML..."
python ai/06_train_model.py 2>&1 | grep -E "✅|Registros|ANOMALA|NORMAL"

echo "✅ Backfill completado"
