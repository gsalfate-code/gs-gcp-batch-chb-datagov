# 🏦 Chilean Bank — Data Governance GCP

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=google-bigquery&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)
![Looker](https://img.shields.io/badge/Looker-4285F4?style=for-the-badge&logo=looker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

Pipeline completo de gobierno de datos en Google Cloud Platform para un banco chileno, implementando la arquitectura Medallion con cumplimiento de la **Ley 19.628** de Protección de Datos Personales.

---

## 🎯 Problema

Un banco chileno enfrenta estos desafíos críticos:

- **Datos personales sin protección** — RUTs, tarjetas y cuentas circulan en texto claro
- **Sin trazabilidad** — Imposible responder quién accedió a qué dato y cuándo
- **Compliance deficiente** — Solicitudes ARCOP sin seguimiento ni alertas de vencimiento
- **Calidad desconocida** — Sin métricas de salud de datos en tiempo real
- **Anomalías sin detectar** — Transacciones fraudulentas no identificadas a tiempo
- **Sin gobierno de datos** — No existe catálogo de PII, dueños ni retención definida

---

## ✅ Solución

Plataforma end-to-end en GCP que protege PII automáticamente, rastrea linaje de columnas, monitorea ARCOP, mide calidad, detecta anomalías con ML y orquesta todo cada hora en horario laboral chileno.

---

## 🏗️ Arquitectura Medallion
```
┌─────────────────────────────────────────────────────┐
│                    GCS BRONZE                       │
│  Clientes (maestro) │ Transacciones │ ARCOP         │
│  PII completo — RUT, tarjeta, cuenta, nombre        │
└──────────────────────┬──────────────────────────────┘
                       │ Cloud DLP (SHA-256, FPE,
                       │ Masking, Bucketing)
                       ▼
┌─────────────────────────────────────────────────────┐
│                  BIGQUERY SILVER                    │
│  clientes_deidentified │ transacciones_deidentified │
│  rut_pseudo │ num_tarjeta_fpe │ rango_edad          │
│  PII enmascarado — acceso para ingenieros           │
└──────────────────────┬──────────────────────────────┘
                       │ DBT (staging + marts)
                       ▼
┌─────────────────────────────────────────────────────┐
│                   BIGQUERY GOLD                     │
│  dim_cliente (SCD2) │ fact_transacciones            │
│  mart_arcop_compliance │ mart_calidad_datos         │
│  mart_anomalias │ fact_arcop_solicitudes            │
│  Datos de negocio — acceso para analistas           │
└──────────────────────┬──────────────────────────────┘
                       │ BigQuery ML (K-Means)
                       ▼
┌─────────────────────────────────────────────────────┐
│              GOLD — ML RESULTS                      │
│  fact_transacciones_ml                              │
│  344 anomalías │ 22 alertas UAF                     │
└─────────────────────────────────────────────────────┘
```

### Flujo del DAG (Composer 3 — cada hora 8-18h L-V)
```
generate_data → dlp_deidentify → dbt_run → bqml_apply → catalog_tags → notify_success
```

### Gobierno de Datos (Dataplex)
```
Lake: chb-datagov-lake
├── bronze-zone (RAW)     → GCS Bucket
├── silver-zone (CURATED) → BigQuery silver
└── gold-zone   (CURATED) → BigQuery gold

Aspects en 9 tablas:
  sensitivity_level │ pii_present │ ley_19628_applies
  data_owner │ retention_years │ arcop_relevant
```

### Linaje Column-Level
```
silver.clientes_deidentified.rut_pseudo
              ▼ DBT
gold.dim_cliente.rut_pseudo
       ├──► fact_transacciones.rut_pseudo
       ├──► fact_arcop_solicitudes.rut_pseudo
       └──► mart_anomalias.rut_pseudo
```

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología | Propósito |
|---|---|---|
| Bronze | Google Cloud Storage | Datos crudos con PII |
| Silver / Gold | BigQuery | Data Warehouse |
| Protección PII | Cloud DLP | Pseudonimización, masking, FPE |
| Transformación | DBT + dbt-bigquery | Silver → Gold, SCD2, 15 tests |
| Gobierno | Dataplex Universal Catalog | Lake, Aspects, Linaje |
| Seguridad columnar | BigQuery Policy Tags | Column-Level Security |
| ML Anomalías | BigQuery ML K-Means | Detección de fraude |
| Orquestación | Cloud Composer 3 | Pipeline horario con alertas |
| Visualización | Looker Studio + Metabase + Power BI | Dashboards |
| CI/CD | GitHub | Control de versiones |

---

## 📁 Estructura del Proyecto
```
gs-gcp-batch-chb-datagov/
├── setup/
│   └── 00_setup.sh                    # Setup inicial GCP
├── data_generator/
│   └── 01_generate_data.py            # Genera datos Bronze (horario)
├── dlp/
│   ├── 02_dlp_inspect.py              # Detección PII
│   └── 02_dlp_deidentify.py           # Bronze → Silver
├── dbt/chb_datagov/
│   └── models/
│       ├── staging/                   # Views validación Silver
│       │   ├── stg_clientes.sql
│       │   ├── stg_transacciones.sql
│       │   └── stg_arcop.sql
│       └── marts/                     # Tablas Gold
│           ├── dim_cliente.sql        # SCD2
│           ├── fact_transacciones.sql
│           ├── fact_arcop_solicitudes.sql
│           ├── mart_calidad_datos.sql
│           ├── mart_arcop_compliance.sql
│           └── mart_anomalias.sql
├── governance/
│   ├── 04_dataplex_setup.py           # Lake + zonas + assets
│   └── 04_catalog_tags.py             # Aspects chb-sensitivity
├── security/
│   └── 05_policy_tags.py              # Column Security
├── ai/
│   └── 06_train_model.py              # BigQuery ML K-Means
├── dags/
│   └── chilean_bank.py               # Composer DAG
└── requirements.txt
```

---

## 🔒 Seguridad y Compliance

### Técnicas DLP aplicadas

| Campo | Técnica | Resultado |
|---|---|---|
| `rut` | SHA-256 + salt | `rut_pseudo` (16 chars hex) |
| `num_tarjeta` | FPE 16 dígitos | `num_tarjeta_fpe` |
| `nombre` | Masking | `M**** D****` |
| `email` | Masking | `c****@*****.com` |
| `fecha_nacimiento` | Bucketing | `rango_edad: 46-55` |
| `saldo` | Bucketing | `rango_saldo: 10M-50M` |
| `descripcion` | Replace InfoType | `[CHILE_RUT]` |

### Defensa en Profundidad
```
Capa 1: Cloud DLP       → pseudonimiza PII en ingesta
Capa 2: Policy Tags     → bloquea columnas en BigQuery (403)
Capa 3: Dataplex Aspects→ documenta sensibilidad de cada tabla
Capa 4: Data Lineage    → audita flujo de datos column-level
```

### Ley 19.628 — Derechos ARCOP

| Derecho | Plazo Legal | Monitoreo |
|---|---|---|
| Acceso | 15 días | mart_arcop_compliance |
| Rectificación | 15 días | semáforo VERDE/AMARILLO/ROJO |
| Cancelación | 5 días | prioridad CRITICA si vence |
| Oposición | 15 días | alertas automáticas |
| Portabilidad | 15 días | fact_arcop_solicitudes |

---

## 🚀 Uso

### Pipeline manual
```bash
python data_generator/01_generate_data.py
python dlp/02_dlp_deidentify.py
cd dbt/chb_datagov && dbt run && dbt test
python ai/06_train_model.py
python governance/04_catalog_tags.py
```

### Comandos útiles
```bash
# Estado Composer
gcloud composer environments describe chb-datagov-composer \
  --location=us-central1 --project=gs-gcp-batch-chb-datagov \
  --format='value(state)'

# Triggerear DAG
gcloud composer environments run chb-datagov-composer \
  --location=us-central1 --project=gs-gcp-batch-chb-datagov \
  dags trigger -- chilean_bank

# Tablas en Dataplex
gcloud dataplex entries list \
  --location=us-central1 --project=gs-gcp-batch-chb-datagov \
  --entry-group='@bigquery' --format='value(name)'
```

---

## 📊 Resultados

| Métrica | Valor |
|---|---|
| Clientes activos | 32.664 |
| Modelos DBT | 9/9 ✅ |
| Tests DBT | 15/15 ✅ |
| Tablas con Aspects | 9/9 ✅ |
| Transacciones ANOMALA | 344 |
| Alertas UAF | 22 |
| Monto promedio anomalía | $2.371.759 CLP |
| Duration DAG | ~7 minutos |

---

## 🌐 Links del Proyecto

| Recurso | URL |
|---|---|
| Dashboard público | https://storage.googleapis.com/chb-datagov-dashboard/index.html |
| Cloud Function API | https://us-central1-gs-gcp-batch-chb-datagov.cloudfunctions.net/dashboard-api |
| Repositorio GitHub | https://github.com/gsalfate-code/gs-gcp-batch-chb-datagov |

---

## 👤 Autor

**Gonzalo Salfate**
- GitHub: [@gsalfate-code](https://github.com/gsalfate-code)
- Email: gsalfate.gcp@gmail.com
