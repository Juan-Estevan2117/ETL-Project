"""
Producer de Kafka — Streaming de métricas derivadas de la fact table.

Componente de streaming de la entrega final. A diferencia de un producer que
lee un CSV, este consulta la tabla de hechos `fact_educacion_superior` del
Data Warehouse y publica 3 métricas de negocio (equidad y cobertura) como
eventos JSON al topic de Kafka.

La fact table es estática, por lo que la "continuidad" exigida por el enunciado
se simula re-consultando y re-publicando las métricas en bucle cada
STREAM_DELAY_SECONDS, emulando un panel de monitoreo vivo. Se detiene con Ctrl+C.

Las 3 métricas (reutilizan la lógica de sql/bi_queries.sql):
  1. tasa_cobertura_credito         -> por departamento (dim_ubicacion)
  2. beneficiarios_por_estrato      -> por estrato (dim_estrato, excluye estrato 0)
  3. matriculados_por_sector +      -> por sector_ies (dim_sector_ies, excluye 'desconocido')
     beneficiarios_por_sector

Ejecución (desde la raíz del proyecto, con el DW poblado y Kafka levantado):
    python kafka/producer_metrics.py
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Permitir importar el módulo `config` de src/ (mismo patrón que el resto del repo)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, MYSQL_URL, STREAM_DELAY_SECONDS


# --- Consultas SQL de las métricas derivadas de la fact table ---
# Cada query agrega la fact table por una dimensión, reutilizando la lógica
# de sql/bi_queries.sql. Todas devuelven columnas homogéneas para normalizar.

QUERY_COBERTURA_DEPTO = text("""
    SELECT
        UPPER(du.departamento) AS dimension_value,
        ROUND(
            (SUM(fe.nuevos_beneficiarios_credito) / NULLIF(SUM(fe.total_matriculados), 0)) * 100,
            2
        ) AS metric_value
    FROM fact_educacion_superior fe
    JOIN dim_ubicacion du ON fe.sk_ubicacion = du.sk_ubicacion
    GROUP BY du.departamento
    HAVING SUM(fe.total_matriculados) > 0
    ORDER BY metric_value DESC
""")

QUERY_BENEFICIARIOS_ESTRATO = text("""
    SELECT
        de.descripcion_estrato AS dimension_value,
        SUM(fe.nuevos_beneficiarios_credito) AS metric_value
    FROM fact_educacion_superior fe
    JOIN dim_estrato de ON fe.sk_estrato = de.sk_estrato
    WHERE de.estrato > 0
    GROUP BY de.descripcion_estrato, de.estrato
    ORDER BY de.estrato ASC
""")

QUERY_SECTOR = text("""
    SELECT
        dsi.sector_ies AS dimension_value,
        SUM(fe.total_matriculados) AS total_matriculados,
        SUM(fe.nuevos_beneficiarios_credito) AS total_beneficiarios
    FROM fact_educacion_superior fe
    JOIN dim_sector_ies dsi ON fe.sk_sector_ies = dsi.sk_sector_ies
    WHERE dsi.sector_ies != 'desconocido'
    GROUP BY dsi.sector_ies
    ORDER BY dsi.sector_ies
""")


def get_db_engine() -> Engine:
    """Crea el engine SQLAlchemy contra el Data Warehouse (reutiliza MYSQL_URL)."""
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    # Validar conexión temprano para fallar con un mensaje claro.
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("🔌 Conexión al Data Warehouse establecida.")
    return engine


def _now_iso() -> str:
    """Timestamp ISO-8601 en UTC, asignado por el producer al publicar el evento."""
    return datetime.now(timezone.utc).isoformat()


def build_metric_events(engine: Engine) -> list[dict]:
    """
    Consulta la fact table y construye la lista de eventos de métricas.

    Cada evento sigue el esquema común que espera el consumer:
        {metric_name, dimension_key, dimension_value, metric_value, event_timestamp}
    """
    events: list[dict] = []
    ts = _now_iso()

    with engine.connect() as conn:
        # Métrica 1: tasa de cobertura de crédito por departamento.
        for row in conn.execute(QUERY_COBERTURA_DEPTO).mappings():
            if row["metric_value"] is None:
                continue
            events.append({
                "metric_name": "tasa_cobertura_credito",
                "dimension_key": "departamento",
                "dimension_value": str(row["dimension_value"]),
                "metric_value": float(row["metric_value"]),
                "event_timestamp": ts,
            })

        # Métrica 2: beneficiarios de crédito por estrato socioeconómico.
        for row in conn.execute(QUERY_BENEFICIARIOS_ESTRATO).mappings():
            events.append({
                "metric_name": "beneficiarios_por_estrato",
                "dimension_key": "estrato",
                "dimension_value": str(row["dimension_value"]),
                "metric_value": float(row["metric_value"] or 0),
                "event_timestamp": ts,
            })

        # Métrica 3: matriculados y beneficiarios por sector de IES (dos eventos por sector).
        for row in conn.execute(QUERY_SECTOR).mappings():
            sector = str(row["dimension_value"])
            events.append({
                "metric_name": "matriculados_por_sector",
                "dimension_key": "sector_ies",
                "dimension_value": sector,
                "metric_value": float(row["total_matriculados"] or 0),
                "event_timestamp": ts,
            })
            events.append({
                "metric_name": "beneficiarios_por_sector",
                "dimension_key": "sector_ies",
                "dimension_value": sector,
                "metric_value": float(row["total_beneficiarios"] or 0),
                "event_timestamp": ts,
            })

    return events


def create_producer() -> KafkaProducer:
    """Crea el KafkaProducer con serialización JSON y confirmación total (acks=all)."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
    except NoBrokersAvailable:
        sys.exit(
            f"❌ No hay broker Kafka en {KAFKA_BOOTSTRAP}. "
            "Levanta Kafka con: docker compose -f kafka/docker-compose.kafka.yaml up -d"
        )
    print(f"📡 Producer conectado a Kafka en {KAFKA_BOOTSTRAP}.")
    return producer


def main() -> None:
    """Bucle principal: re-consulta la fact table y publica las métricas cada N segundos."""
    print("🚀 Iniciando producer de métricas (Ctrl+C para detener)...")
    engine = get_db_engine()
    producer = create_producer()

    ciclo = 0
    try:
        while True:
            ciclo += 1
            events = build_metric_events(engine)

            if not events:
                print(
                    "⚠️  La fact table no devolvió métricas. "
                    "¿Ejecutaste el pipeline batch (python src/main.py)?"
                )

            for event in events:
                producer.send(KAFKA_TOPIC, value=event)
            producer.flush()

            print(
                f"✅ Ciclo {ciclo}: {len(events)} métricas publicadas en '{KAFKA_TOPIC}'. "
                f"Próximo envío en {STREAM_DELAY_SECONDS}s."
            )
            time.sleep(STREAM_DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n🛑 Detenido por el usuario. Cerrando producer...")
    finally:
        producer.flush()
        producer.close()
        engine.dispose()
        print("👋 Producer cerrado.")


if __name__ == "__main__":
    main()
