# =============================================================================
# SCRIPT: 01_generate_data.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Generador horario de datos bancarios chilenos.
#   Corre cada hora en horario laboral (08:00-18:00) de lunes a viernes.
#
# LÓGICA DE CLIENTES:
#   Primera ejecución del día (08:00):
#     - Carga maestro existente desde GCS (si existe)
#     - Agrega 50-200 clientes nuevos
#     - Elimina 2-10 clientes (cancelación, fallecimiento, etc.)
#     - Bloquea 5-15 clientes (fraude, mora, ARCOP cancelación)
#     - Reactiva 1-5 clientes (resolución de bloqueo)
#     - Guarda maestro actualizado + delta del día
#   Ejecuciones siguientes:
#     - Solo carga maestro y genera transacciones
#
# LÓGICA DE TRANSACCIONES:
#   - Volumen variable por curva gaussiana (peaks 10h y 15h)
#   - Factor día derivado de fecha (días buenos y malos)
#   - Procesa siempre la hora ANTERIOR a la actual
#
# ERRORES INTENCIONALES REALES:
#   DLP detecta  → PII en descripción, tarjeta expuesta, email en texto
#   DBT detecta  → email mal formado, monto negativo, fecha futura,
#                  edad inválida, género inválido, score inconsistente,
#                  región/comuna inconsistente, saldo negativo
#
# SOLICITUDES ARCOP:
#   - 0.05% clientes activos por día normal
#   - 0.08% en quincena/fin de mes
#   - Ciclo de vida completo con estados y plazos reales
#
# ESTRUCTURA GCS:
#   bronze/clientes/maestro/clientes_maestro.json
#   bronze/clientes/delta/fecha=YYYY-MM-DD/delta.json
#   bronze/transacciones/fecha=YYYY-MM-DD/hora=HH/transacciones.json
#   bronze/arcop/fecha=YYYY-MM-DD/solicitudes.json
#
# USO:
#   python data_generator/01_generate_data.py
# =============================================================================

import json
import math
import random
import os
from datetime import datetime, date, timedelta
import pytz
from faker import Faker
from google.cloud import storage
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------
load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
TIMEZONE    = pytz.timezone(os.getenv("DAG_TIMEZONE", "America/Santiago"))
fake        = Faker("es_CL")

# -----------------------------------------------------------------------------
# CONSTANTES
# -----------------------------------------------------------------------------
VOLUMEN_DIARIO_BASE = 200_000
VOLUMEN_MINIMO_HORA = 200
HORARIO_INICIO      = 8
HORARIO_FIN         = 18

MULT_DIA = {0: 1.2, 1: 1.0, 2: 1.0, 3: 1.1, 4: 1.5}

FERIADOS_CHILE_2026 = {
    date(2026, 1, 1),  date(2026, 4, 3),  date(2026, 4, 4),
    date(2026, 5, 1),  date(2026, 5, 21), date(2026, 6, 29),
    date(2026, 7, 16), date(2026, 8, 15), date(2026, 9, 18),
    date(2026, 9, 19), date(2026, 10, 12),date(2026, 10, 31),
    date(2026, 11, 1), date(2026, 12, 8), date(2026, 12, 25),
}

REGIONES_COMUNAS = {
    "Metropolitana": ["Santiago", "Las Condes", "Providencia", "Maipú", "Puente Alto", "Ñuñoa", "La Florida"],
    "Valparaíso":    ["Valparaíso", "Viña del Mar", "Quilpué", "Villa Alemana", "San Antonio"],
    "Biobío":        ["Concepción", "Talcahuano", "Chillán", "Los Ángeles", "Coronel"],
    "La Araucanía":  ["Temuco", "Padre Las Casas", "Angol", "Victoria"],
    "Los Lagos":     ["Puerto Montt", "Osorno", "Castro", "Puerto Varas"],
    "Antofagasta":   ["Antofagasta", "Calama", "Tocopilla", "Mejillones"],
    "Coquimbo":      ["La Serena", "Coquimbo", "Ovalle", "Illapel"],
    "O'Higgins":     ["Rancagua", "San Fernando", "Pichilemu", "Rengo"],
    "Maule":         ["Talca", "Curicó", "Linares", "Cauquenes"],
    "Los Ríos":      ["Valdivia", "La Unión", "Panguipulli"],
}

OCUPACIONES = [
    "Ingeniero", "Médico", "Profesor", "Comerciante", "Empleado",
    "Abogado", "Contador", "Enfermero", "Técnico", "Independiente",
    "Empresario", "Jubilado", "Estudiante", "Funcionario Público",
    "Operador", "Vendedor", "Administrativo", "Conductor"
]

CATEGORIAS_GASTO = [
    "Supermercado", "Salud", "Educación", "Transporte", "Restaurant",
    "Vestuario", "Tecnología", "Entretenimiento", "Servicios Básicos",
    "Farmacia", "Combustible", "Viajes", "Seguros", "Retail"
]

TIPOS_CUENTA = ["Cuenta Corriente", "Cuenta Vista", "Cuenta Ahorro", "Cuenta RUT"]
CANALES      = ["APP", "WEB", "CAJERO", "SUCURSAL", "POS"]
TIPOS_TXN    = ["CREDITO", "DEBITO", "TRANSFERENCIA", "PAGO_SERVICIO", "GIRO"]

# ARCOP
ARCOP_TIPOS        = ["A", "R", "P", "O", "C"]
ARCOP_PESOS        = [45, 25, 15, 10, 5]
ARCOP_CANALES      = ["WEB", "APP", "SUCURSAL", "EMAIL"]
ARCOP_PESOS_CANAL  = [50, 30, 15, 5]
ARCOP_RESOLUCION   = {"A": (3,5), "R": (5,10), "P": (5,8), "O": (2,3), "C": (10,15)}

# -----------------------------------------------------------------------------
# UTILIDADES
# -----------------------------------------------------------------------------
def es_dia_habil(fecha: date) -> bool:
    return fecha.weekday() < 5 and fecha not in FERIADOS_CHILE_2026

def dias_habiles_desde(fecha_inicio: date, dias: int) -> date:
    fecha, contados = fecha_inicio, 0
    while contados < dias:
        fecha += timedelta(days=1)
        if es_dia_habil(fecha):
            contados += 1
    return fecha

def calcular_dv(rut: int) -> str:
    suma, multiplo = 0, 2
    while rut:
        suma     += (rut % 10) * multiplo
        rut      //= 10
        multiplo  = 2 if multiplo == 7 else multiplo + 1
    resultado = 11 - (suma % 11)
    return {10: "K", 11: "0"}.get(resultado, str(resultado))

def generar_rut() -> str:
    numero = random.randint(5_000_000, 25_000_000)
    return f"{numero:,}".replace(",", ".") + f"-{calcular_dv(numero)}"

def gaussian(hora: float, peak: float, amplitud: float) -> float:
    return math.exp(-((hora - peak) ** 2) / (2 * amplitud ** 2))

def calcular_volumen_hora(hora: int, fecha: date, factor_dia: float) -> int:
    if fecha in FERIADOS_CHILE_2026:
        return 0
    if hora < HORARIO_INICIO or hora > HORARIO_FIN:
        # Volumen nocturno único por hora — usa hora como seed adicional
        # garantiza que ninguna hora nocturna tenga el mismo volumen
        base   = VOLUMEN_MINIMO_HORA + (hora * 17)  # desplazamiento único por hora
        ruido  = 0.7 + (((hora * 7 + int(factor_dia * 100)) % 100) / 150)
        return int(base * ruido)
    curva = (
        0.5 * gaussian(hora, peak=10, amplitud=1.2) +
        0.5 * gaussian(hora, peak=15, amplitud=1.8)
    )
    suma_curva = sum(
        0.5 * gaussian(h, 10, 1.2) + 0.5 * gaussian(h, 15, 1.8)
        for h in range(HORARIO_INICIO, HORARIO_FIN + 1)
    )
    horas_lab    = HORARIO_FIN - HORARIO_INICIO + 1
    volumen_hora = VOLUMEN_MINIMO_HORA + (curva / suma_curva) * (
        VOLUMEN_DIARIO_BASE - VOLUMEN_MINIMO_HORA * horas_lab
    )
    mult_dia = MULT_DIA.get(fecha.weekday(), 1.0)
    mult_mes = 2.0 if fecha.day in [15, 16, 28, 29, 30, 31] else 1.0
    ruido    = random.uniform(0.94, 1.06)
    return max(VOLUMEN_MINIMO_HORA, int(volumen_hora * mult_dia * mult_mes * factor_dia * ruido))

# -----------------------------------------------------------------------------
# ERRORES INTENCIONALES REALES
# -----------------------------------------------------------------------------
def inyectar_errores_cliente(cliente: dict) -> dict:
    # DBT: email mal formado (migración legacy)
    if random.random() < 0.005:
        cliente["email"] = cliente["email"].replace("@", " ").replace(".", " ")
    # DBT: fecha nacimiento futura (bug formulario web)
    if random.random() < 0.002:
        cliente["fecha_nacimiento"] = (date.today() + timedelta(days=random.randint(1, 365))).isoformat()
    # DBT: edad menor 18 (error digitación)
    if random.random() < 0.002:
        cliente["fecha_nacimiento"] = (date.today() - timedelta(days=random.randint(1, 17*365))).isoformat()
    # DBT: género inválido (sistema legacy)
    if random.random() < 0.002:
        cliente["genero"] = random.choice(["X", "N/A", "U", ""])
    # DBT: saldo negativo (error integración contable)
    if random.random() < 0.003:
        cliente["saldo_cuenta"] = round(random.uniform(-500_000, -1), 2)
    # DBT: score fuera de rango (bug sistema scoring)
    if random.random() < 0.002:
        cliente["score_crediticio"] = random.choice([
            random.randint(-100, 299),
            random.randint(901, 1200),
        ])
    # DBT: score vs categoría inconsistente
    if random.random() < 0.003:
        cliente["score_crediticio"] = random.randint(750, 900)
        cliente["categoria_riesgo"] = "ALTO"
    # DBT: comuna no pertenece a región
    if random.random() < 0.003:
        regiones_otras    = [r for r in REGIONES_COMUNAS if r != cliente["region"]]
        cliente["comuna"] = random.choice(REGIONES_COMUNAS[random.choice(regiones_otras)])
    # DLP Discovery: RUT en comentario operador
    if random.random() < 0.001:
        cliente["comentario_operador"] = (
            f"Cliente verificado presencialmente RUT {cliente['rut']} "
            f"en sucursal {random.choice(['Santiago Centro','Providencia','Las Condes'])}"
        )
    return cliente

def inyectar_errores_transaccion(txn: dict, cliente: dict) -> dict:
    # DBT: monto negativo (reversión mal codificada)
    if random.random() < 0.003:
        txn["monto_clp"] = round(random.uniform(-500_000, -1), 2)
    # DBT: fecha futura (bug scheduler pagos programados)
    if random.random() < 0.002:
        fecha_futura   = date.today() + timedelta(days=random.randint(1, 180))
        txn["fecha_hora"] = datetime(
            fecha_futura.year, fecha_futura.month, fecha_futura.day,
            random.randint(8, 18), random.randint(0, 59),
            tzinfo=TIMEZONE
        ).isoformat()
    # DLP: PII en descripción (operador escribe manualmente)
    if random.random() < 0.001:
        txn["descripcion"] = (
            f"Transferencia solicitada por {cliente['nombre_completo']} "
            f"RUT {cliente['rut']} fono {cliente['telefono']}"
        )
    # DLP: tarjeta completa en descripción
    if random.random() < 0.001:
        txn["descripcion"] = (
            f"Pago con tarjeta {cliente['num_tarjeta']} autorizado por cliente"
        )
    # DLP: email en descripción
    if random.random() < 0.001:
        txn["descripcion"] = (
            f"Notificación enviada a {cliente['email']} por transacción completada"
        )
    return txn

# -----------------------------------------------------------------------------
# GENERADORES
# -----------------------------------------------------------------------------
def generar_cliente(id_cliente: str, fecha_creacion: date) -> dict:
    region = random.choice(list(REGIONES_COMUNAS.keys()))
    comuna = random.choice(REGIONES_COMUNAS[region])
    genero = random.choice(["M", "F"])
    nombre = fake.name_male() if genero == "M" else fake.name_female()
    hoy    = date.today()
    dias   = max(0, (hoy - fecha_creacion).days)

    cliente = {
        # DIRECTOS
        "id_cliente":              id_cliente,
        "rut":                     generar_rut(),
        "nombre_completo":         nombre,
        "email":                   fake.email(),
        "telefono":                f"+569{random.randint(10000000, 99999999)}",
        "num_tarjeta":             fake.credit_card_number(),
        "num_cuenta":              f"{random.randint(10000000, 99999999)}",
        # INDIRECTOS
        "fecha_nacimiento":        fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "genero":                  genero,
        "region":                  region,
        "comuna":                  comuna,
        "ocupacion":               random.choice(OCUPACIONES),
        "estado_civil":            random.choice(["Soltero","Casado","Divorciado","Viudo"]),
        "nivel_educacion":         random.choice(["Básica","Media","Técnica","Universitaria","Postgrado"]),
        "tipo_cuenta":             random.choice(TIPOS_CUENTA),
        # SENSIBLES
        "saldo_cuenta":            round(random.uniform(0, 50_000_000), 2),
        "deuda_total":             round(random.uniform(0, 20_000_000), 2),
        "score_crediticio":        random.randint(300, 900),
        "renta_mensual":           round(random.uniform(400_000, 10_000_000), 2),
        "tiene_credito":           random.choice([True, False]),
        "categoria_riesgo":        random.choice(["BAJO","MEDIO","ALTO"]),
        "comentario_operador":     "",
        # ARCOP
        "fecha_creacion":               fecha_creacion.isoformat(),
        "arcop_estado":                 "ACTIVO",
        "arcop_acceso_solicitado":      False,
        "arcop_rectificacion_pending":  False,
        "arcop_cancelacion_solicitado": False,
        "arcop_oposicion_activa":       False,
        "arcop_portabilidad_entregada": False,
        "consentimiento_ley19628":      True,
        "fecha_consentimiento":         (
            fecha_creacion + timedelta(days=random.randint(0, min(30, dias)))
        ).isoformat(),
    }
    return inyectar_errores_cliente(cliente)

def generar_transaccion(id_txn: str, cliente: dict, hora: int, fecha: date) -> dict:
    es_anomalia = random.random() < 0.003
    es_peak     = hora in [9, 10, 15, 16]

    if es_anomalia:
        monto = round(random.uniform(5_000_000, 50_000_000), 2)
    elif es_peak:
        monto = round(random.uniform(10_000, 500_000), 2)
    else:
        monto = round(random.uniform(1_000, 200_000), 2)

    txn = {
        "id_transaccion":   id_txn,
        "id_cliente":       cliente["id_cliente"],
        "num_cuenta":       cliente["num_cuenta"],
        "num_tarjeta_mask": cliente["num_tarjeta"][-4:],
        "monto_clp":        monto,
        "tipo_transaccion": random.choice(TIPOS_TXN),
        "canal":            random.choice(CANALES),
        "categoria_gasto":  random.choice(CATEGORIAS_GASTO),
        "region":           cliente["region"],
        "comuna":           cliente["comuna"],
        "fecha_hora":       datetime(
                                fecha.year, fecha.month, fecha.day,
                                hora,
                                random.randint(0, 59),
                                random.randint(0, 59),
                                random.randint(0, 999999),
                                tzinfo=TIMEZONE
                            ).isoformat(),
        "descripcion":      fake.sentence(nb_words=6),
        "es_anomalia_flag": es_anomalia,
        "score_anomalia":   round(random.uniform(0.7, 1.0), 4) if es_anomalia
                            else round(random.uniform(0.0, 0.3), 4),
        "estado":           "COMPLETADA",
        "es_duplicado":     random.random() < 0.008,
    }

    if random.random() < 0.012:
        txn["categoria_gasto"] = None
        txn["comuna"]          = None

    return inyectar_errores_transaccion(txn, cliente)

def generar_solicitudes_arcop(clientes: list, fecha: date) -> list:
    """
    Genera solicitudes ARCOP con ciclo de vida completo.
    0.05% clientes activos en día normal, 0.08% en quincena.
    """
    hoy          = date.today()
    es_quincena  = fecha.day in [15, 16, 28, 29, 30, 31]
    pct          = 0.0008 if es_quincena else 0.0005
    activos      = [c for c in clientes if c.get("arcop_estado") == "ACTIVO"]
    num          = max(1, int(len(activos) * pct))
    muestra      = random.sample(activos, min(num, len(activos)))
    solicitudes  = []

    for cliente in muestra:
        tipo             = random.choices(ARCOP_TIPOS, weights=ARCOP_PESOS)[0]
        canal            = random.choices(ARCOP_CANALES, weights=ARCOP_PESOS_CANAL)[0]
        min_d, max_d     = ARCOP_RESOLUCION[tipo]
        dias_res         = random.randint(min_d, max_d)
        fecha_limite     = dias_habiles_desde(fecha, 15)
        fecha_resolucion = dias_habiles_desde(fecha, dias_res)
        dias_trans       = (hoy - fecha).days

        if dias_trans == 0:
            estado = "PENDIENTE"
        elif dias_trans <= 2:
            estado = "EN_PROCESO"
        elif dias_trans >= dias_res:
            estado = "VENCIDO" if fecha_resolucion > fecha_limite else "COMPLETADO"
        else:
            estado = "EN_PROCESO"

        solicitudes.append({
            "id_solicitud":              f"ARCOP{fecha.strftime('%Y%m%d')}{cliente['id_cliente']}",
            "id_cliente":                cliente["id_cliente"],
            "tipo_derecho":              tipo,
            "descripcion_tipo":          {"A":"Acceso","R":"Rectificación","C":"Cancelación","O":"Oposición","P":"Portabilidad"}[tipo],
            "fecha_solicitud":           fecha.isoformat(),
            "fecha_limite":              fecha_limite.isoformat(),
            "fecha_resolucion":          fecha_resolucion.isoformat() if estado == "COMPLETADO" else None,
            "estado":                    estado,
            "canal":                     canal,
            "dias_habiles_max":          15,
            "dias_resolucion_estimados": dias_res,
            "vence_en_dias":             max(0, (fecha_limite - hoy).days),
            "es_critico":                tipo == "C",
            "responsable":               random.choice(["Data Steward Norte","Data Steward Sur","Data Steward RM"]),
        })

    return solicitudes

# -----------------------------------------------------------------------------
# GCS OPERATIONS
# -----------------------------------------------------------------------------
def subir_a_gcs(bucket_name: str, blob_path: str, data: list) -> None:
    client  = storage.Client()
    bucket  = client.bucket(bucket_name)
    blob    = bucket.blob(blob_path)
    content = "\n".join(json.dumps(r, ensure_ascii=False) for r in data)
    blob.upload_from_string(content, content_type="application/json")
    print(f"   ✅ gs://{bucket_name}/{blob_path} → {len(data):,} registros")

def blob_existe(bucket_name: str, blob_path: str) -> bool:
    client = storage.Client()
    return client.bucket(bucket_name).blob(blob_path).exists()

def cargar_desde_gcs(bucket_name: str, blob_path: str) -> list:
    client  = storage.Client()
    content = client.bucket(bucket_name).blob(blob_path).download_as_text()
    return [json.loads(line) for line in content.strip().split("\n")]

# -----------------------------------------------------------------------------
# GESTIÓN MAESTRO DE CLIENTES
# El maestro es el archivo vivo que crece día a día.
# Cada día se agregan nuevos, se eliminan algunos y se bloquean otros.
# El delta registra exactamente qué cambió — DBT lo usa para SCD2.
# -----------------------------------------------------------------------------
def actualizar_maestro(clientes: list, fecha: date, fecha_str: str) -> tuple:
    """
    Actualiza el maestro de clientes con movimientos del día.
    Retorna (maestro_actualizado, delta_del_dia, stats)
    """
    hoy = date.today()

    # Nuevos clientes del día (crecimiento orgánico)
    num_nuevos = random.randint(50, 200)
    nuevos     = [
        generar_cliente(
            f"N{fecha_str.replace('-','')}{str(i).zfill(4)}",
            fecha
        )
        for i in range(1, num_nuevos + 1)
    ]

    # Clientes eliminados (cancelación cuenta, fallecimiento, migración)
    activos      = [c for c in clientes if c["arcop_estado"] == "ACTIVO"]
    num_eliminados = random.randint(2, 10)
    eliminados   = random.sample(activos, min(num_eliminados, len(activos)))
    ids_eliminados = {c["id_cliente"] for c in eliminados}

    # Clientes bloqueados (fraude detectado, mora, solicitud ARCOP cancelación)
    num_bloqueados = random.randint(5, 15)
    candidatos_bloqueo = [
        c for c in activos
        if c["id_cliente"] not in ids_eliminados
    ]
    bloqueados     = random.sample(candidatos_bloqueo, min(num_bloqueados, len(candidatos_bloqueo)))
    ids_bloqueados = {c["id_cliente"] for c in bloqueados}

    # Clientes reactivados (resolución de bloqueo previo)
    bloqueados_prev = [c for c in clientes if c["arcop_estado"] == "BLOQUEADO"]
    num_reactivados = random.randint(1, 5)
    reactivados     = random.sample(bloqueados_prev, min(num_reactivados, len(bloqueados_prev)))
    ids_reactivados = {c["id_cliente"] for c in reactivados}

    # Aplicar cambios al maestro
    maestro_actualizado = []
    for cliente in clientes:
        cid = cliente["id_cliente"]
        if cid in ids_eliminados:
            cliente["arcop_estado"] = "ELIMINADO"
        elif cid in ids_bloqueados:
            cliente["arcop_estado"] = "BLOQUEADO"
        elif cid in ids_reactivados:
            cliente["arcop_estado"] = "ACTIVO"
        maestro_actualizado.append(cliente)

    # Agregar nuevos al maestro
    maestro_actualizado.extend(nuevos)

    # Delta del día — registro de todos los cambios
    delta = {
        "fecha":         fecha_str,
        "nuevos":        [c["id_cliente"] for c in nuevos],
        "eliminados":    [c["id_cliente"] for c in eliminados],
        "bloqueados":    [c["id_cliente"] for c in bloqueados],
        "reactivados":   [c["id_cliente"] for c in reactivados],
        "total_maestro": len(maestro_actualizado),
        "total_activos": sum(1 for c in maestro_actualizado if c["arcop_estado"] == "ACTIVO"),
    }

    stats = {
        "nuevos":      num_nuevos,
        "eliminados":  len(eliminados),
        "bloqueados":  len(bloqueados),
        "reactivados": len(reactivados),
        "total":       len(maestro_actualizado),
    }

    return maestro_actualizado, delta, stats

# -----------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    ahora     = datetime.now(TIMEZONE)
    hora      = (ahora - timedelta(hours=1)).hour
    fecha     = ahora.date()
    fecha_str = fecha.strftime("%Y-%m-%d")

    # Factor único y consistente por día
    random.seed(int(fecha_str.replace("-", "")))
    factor_dia = random.uniform(0.75, 1.25)
    random.seed()

    print("=" * 65)
    print(" Chilean Bank — Generador Horario Bronze")
    print(f" Fecha       : {fecha_str}")
    print(f" Hora proceso: {hora:02d}:00 (hora anterior)")
    print(f" Hora actual : {ahora.strftime('%H:%M:%S')} (America/Santiago)")
    print(f" Factor día  : {factor_dia:.2f} ({'activo' if factor_dia >= 1 else 'tranquilo'})")
    print("=" * 65)

    # Curva del día
    print("\n📈 Curva proyectada:")
    for h in range(0, 24):
        vol  = calcular_volumen_hora(h, fecha, factor_dia)
        bars = "█" * (vol // 5000)
        mark = " ◄" if h == hora else ""
        print(f"   {h:02d}h → {vol:>7,} txn  {bars}{mark}")

    # -------------------------------------------------------------------------
    # GESTIÓN MAESTRO DE CLIENTES
    # Solo en la primera hora del día (hora == HORARIO_INICIO - 1 = 7)
    # -------------------------------------------------------------------------
    blob_maestro = "bronze/clientes/maestro/clientes_maestro.json"
    es_primera_hora = hora == (HORARIO_INICIO - 1) or not blob_existe(BUCKET_NAME, blob_maestro)

    if es_primera_hora:
        print(f"\n👥 Primera hora del día — actualizando maestro de clientes...")

        if blob_existe(BUCKET_NAME, blob_maestro):
            print(f"   Cargando maestro existente...")
            clientes = cargar_desde_gcs(BUCKET_NAME, blob_maestro)
            print(f"   ✅ {len(clientes):,} clientes cargados")
        else:
            print(f"   Maestro no existe — creando base inicial...")
            num_base = random.randint(10_000, 50_000)
            print(f"   Generando {num_base:,} clientes base + movimientos del día...")
            clientes = [
                generar_cliente(
                    f"C{str(i).zfill(7)}",
                    fecha - timedelta(days=random.randint(1, 365*2))
                )
                for i in range(1, num_base + 1)
            ]

        # Actualizar maestro con movimientos del día
        clientes, delta, stats = actualizar_maestro(clientes, fecha, fecha_str)

        # Subir maestro actualizado
        subir_a_gcs(BUCKET_NAME, blob_maestro, clientes)

        # Subir delta del día
        blob_delta = f"bronze/clientes/delta/fecha={fecha_str}/delta.json"
        subir_a_gcs(BUCKET_NAME, blob_delta, [delta])

        print(f"\n   📊 Movimientos del día:")
        print(f"   └ Nuevos      : +{stats['nuevos']:,}")
        print(f"   └ Eliminados  : -{stats['eliminados']:,}")
        print(f"   └ Bloqueados  : ~{stats['bloqueados']:,}")
        print(f"   └ Reactivados : +{stats['reactivados']:,}")
        print(f"   └ Total maestro: {stats['total']:,}")

    else:
        print(f"\n👥 Cargando maestro de clientes...")
        clientes = cargar_desde_gcs(BUCKET_NAME, blob_maestro)
        print(f"   ✅ {len(clientes):,} clientes")

    # -------------------------------------------------------------------------
    # TRANSACCIONES DE LA HORA
    # Solo con clientes activos
    # -------------------------------------------------------------------------
    clientes_activos = [c for c in clientes if c["arcop_estado"] == "ACTIVO"]
    num_txn          = calcular_volumen_hora(hora, fecha, factor_dia)

    print(f"\n💳 Generando {num_txn:,} transacciones para hora {hora:02d}:00...")

    if num_txn == 0:
        print("   ⚠️  Feriado o fuera de horario")
        return

    transacciones = [
        generar_transaccion(
            f"T{fecha_str.replace('-','')}{hora:02d}{str(i).zfill(7)}",
            random.choice(clientes_activos),
            hora,
            fecha
        )
        for i in range(1, num_txn + 1)
    ]

    duplicados  = sum(1 for t in transacciones if t["es_duplicado"])
    anomalias   = sum(1 for t in transacciones if t["es_anomalia_flag"])
    nulos       = sum(1 for t in transacciones if t["categoria_gasto"] is None)
    pii_en_desc = sum(1 for t in transacciones if any(
        x in t.get("descripcion", "")
        for x in ["RUT", "tarjeta", "@"]
    ))

    blob_txn = f"bronze/transacciones/fecha={fecha_str}/hora={hora:02d}/transacciones.json"
    subir_a_gcs(BUCKET_NAME, blob_txn, transacciones)

    # -------------------------------------------------------------------------
    # SOLICITUDES ARCOP DE LA HORA (solo primera hora del día)
    # -------------------------------------------------------------------------
    if es_primera_hora:
        blob_arcop = f"bronze/arcop/fecha={fecha_str}/solicitudes.json"
        if not blob_existe(BUCKET_NAME, blob_arcop):
            solicitudes = generar_solicitudes_arcop(clientes_activos, fecha)
            subir_a_gcs(BUCKET_NAME, blob_arcop, solicitudes)
            print(f"\n⚖️  ARCOP: {len(solicitudes):,} solicitudes generadas")

    # -------------------------------------------------------------------------
    # RESUMEN
    # -------------------------------------------------------------------------
    print(f"\n{'=' * 65}")
    print(f" ✅ Bronze actualizado — {ahora.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"{'=' * 65}")
    print(f" Clientes activos  : {len(clientes_activos):,}")
    print(f" Transacciones     : {num_txn:,}")
    print(f" └ Duplicados      : {duplicados:,} ({duplicados/num_txn*100:.1f}%)")
    print(f" └ Anómalas        : {anomalias:,} ({anomalias/num_txn*100:.1f}%)")
    print(f" └ Con nulos       : {nulos:,} ({nulos/num_txn*100:.1f}%)")
    print(f" └ PII en desc     : {pii_en_desc:,} ({pii_en_desc/num_txn*100:.2f}%)")
    print(f"\n PRÓXIMO PASO: python dlp/02_dlp_inspect.py")
    print(f"{'=' * 65}")

if __name__ == "__main__":
    main()
