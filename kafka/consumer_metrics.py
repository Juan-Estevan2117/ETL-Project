"""
Consumer de Kafka — Monitoreo en tiempo real de métricas del Data Warehouse.

Componente de streaming de la entrega final. Se suscribe al topic donde el
producer publica las métricas derivadas de la fact table y, por cada evento:
  1. Muestra una línea de monitoreo legible en consola (salida en tiempo real).
  2. Persiste el evento en la tabla `stream_metrics_log` del Data Warehouse.

Cumple las dos opciones del enunciado ("display OR persist"): muestra Y persiste.
Se detiene con Ctrl+C.

Ejecución (desde la raíz del proyecto, con Kafka levantado y el DW disponible):
    python kafka/consumer_metrics.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Permitir importar el módulo `config` de src/ (mismo patrón que el resto del repo)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, MYSQL_URL


# Campos obligatorios de un evento de métrica válido.
REQUIRED_FIELDS = ("metric_name", "dimension_key", "dimension_value", "metric_value", "event_timestamp")

# Sentencia de inserción en la tabla de persistencia del stream.
INSERT_STMT = text("""
    INSERT INTO stream_metrics_log
        (metric_name, dimension_key, dimension_value, metric_value,
         event_timestamp, kafka_offset, kafka_partition)
    VALUES
        (:metric_name, :dimension_key, :dimension_value, :metric_value,
         :event_timestamp, :kafka_offset, :kafka_partition)
""")


def get_db_engine() -> Engine:
    """Crea el engine SQLAlchemy contra el Data Warehouse (reutiliza MYSQL_URL)."""
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("🔌 Conexión al Data Warehouse establecida.")
    return engine


def create_consumer() -> KafkaConsumer:
    """
    Crea el KafkaConsumer.

    - auto_offset_reset='earliest': al reconectar sin offset previo, reproduce
      el histórico del topic (decisión de diseño para no perder mensajes).
    - group_id fijo: permite reanudar desde el último offset confirmado.
    """
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="dw-metrics-monitor",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
    except NoBrokersAvailable:
        sys.exit(
            f"❌ No hay broker Kafka en {KAFKA_BOOTSTRAP}. "
            "Levanta Kafka con: docker compose -f kafka/docker-compose.kafka.yaml up -d"
        )
    print(f"📡 Consumer suscrito al topic '{KAFKA_TOPIC}' en {KAFKA_BOOTSTRAP}.")
    return consumer


def _parse_event_timestamp(value: str) -> datetime:
    """Convierte el timestamp ISO del producer a datetime para MySQL."""
    return datetime.fromisoformat(value)


def persist_event(engine: Engine, event: dict, offset: int, partition: int) -> None:
    """Inserta el evento de métrica en stream_metrics_log."""
    params = {
        "metric_name": str(event["metric_name"]),
        "dimension_key": str(event["dimension_key"]),
        "dimension_value": str(event["dimension_value"]),
        "metric_value": float(event["metric_value"]),
        "event_timestamp": _parse_event_timestamp(event["event_timestamp"]),
        "kafka_offset": offset,
        "kafka_partition": partition,
    }
    with engine.begin() as conn:
        conn.execute(INSERT_STMT, params)


def main() -> None:
    """Bucle principal: consume eventos, los muestra y los persiste."""
    print("🚀 Iniciando consumer de monitoreo (Ctrl+C para detener)...")
    engine = get_db_engine()
    consumer = create_consumer()

    procesados = 0
    descartados = 0
    try:
        for message in consumer:
            event = message.value

            # Validación mínima: descartar eventos malformados sin abortar el monitor.
            if not isinstance(event, dict) or any(f not in event for f in REQUIRED_FIELDS):
                descartados += 1
                print(f"⚠️  [offset={message.offset}] evento descartado (esquema inválido): {event}")
                continue

            try:
                persist_event(engine, event, message.offset, message.partition)
            except Exception as exc:  # noqa: BLE001 — el monitor no debe caerse por un evento
                descartados += 1
                print(f"⚠️  [offset={message.offset}] no se pudo persistir: {exc}")
                continue

            procesados += 1
            # Salida de monitoreo en tiempo real.
            print(
                f"📥 [offset={message.offset}] "
                f"metric={event['metric_name']} "
                f"{event['dimension_key']}={event['dimension_value']} "
                f"value={event['metric_value']}"
            )

    except KeyboardInterrupt:
        print(
            f"\n🛑 Detenido por el usuario. "
            f"Eventos persistidos: {procesados} | descartados: {descartados}."
        )
    finally:
        consumer.close()
        engine.dispose()
        print("👋 Consumer cerrado.")


if __name__ == "__main__":
    main()
