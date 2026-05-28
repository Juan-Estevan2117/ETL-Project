"""
Panel en vivo del stream — lee stream_metrics_log y redibuja cada pocos segundos.

Componente opcional de demostración: muestra dos gráficas que se actualizan en
tiempo real mientras corren el producer y el consumer, para evidenciar el flujo
del streaming frente a la audiencia.

  • Gráfica 1 (PULSO): eventos consumidos por segundo en la última ventana.
    Se mueve en cada ciclo → prueba de que el stream está vivo.
  • Gráfica 2 (NEGOCIO): última lectura de la tasa de cobertura de crédito por
    departamento (Top 10). Refleja el snapshot más reciente del DW.

Ejecución (con producer + consumer corriendo y el DW poblado):
    python kafka/live_charts.py
"""

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from sqlalchemy import create_engine, text

# Permitir importar el módulo `config` de src/ (mismo patrón que el resto del repo)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from config import MYSQL_URL

# Segundos entre refrescos del panel.
REFRESH_SECONDS = 3

engine = create_engine(MYSQL_URL, pool_pre_ping=True)

# --- Consultas (idénticas a las documentadas) ---
QUERY_PULSO = """
    SELECT DATE_FORMAT(received_at, '%H:%i:%s') AS t, COUNT(*) AS eventos
    FROM stream_metrics_log
    WHERE received_at >= NOW() - INTERVAL 2 MINUTE
    GROUP BY t
    ORDER BY t
"""

QUERY_COBERTURA = """
    SELECT s.dimension_value AS departamento, s.metric_value AS cobertura_pct
    FROM stream_metrics_log s
    JOIN (
        SELECT dimension_value, MAX(received_at) AS last_seen
        FROM stream_metrics_log
        WHERE metric_name = 'tasa_cobertura_credito'
        GROUP BY dimension_value
    ) ult ON s.dimension_value = ult.dimension_value
         AND s.received_at = ult.last_seen
    WHERE s.metric_name = 'tasa_cobertura_credito'
    ORDER BY cobertura_pct DESC
    LIMIT 10
"""

fig, (ax_pulso, ax_cob) = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("Monitoreo en vivo — stream_metrics_log", fontweight="bold")


def refrescar(_frame):
    """Re-consulta la BD y redibuja ambas gráficas."""
    # --- Gráfica 1: pulso de ingesta ---
    # text() evita que pandas/pymysql interpreten el '%' de DATE_FORMAT como parámetro
    df_pulso = pd.read_sql(text(QUERY_PULSO), engine)
    ax_pulso.clear()
    if not df_pulso.empty:
        ax_pulso.fill_between(df_pulso["t"], df_pulso["eventos"], alpha=0.4, color="tab:blue")
        ax_pulso.plot(df_pulso["t"], df_pulso["eventos"], color="tab:blue", marker="o")
    ax_pulso.set_title("Pulso del stream (eventos/seg, últimos 2 min)")
    ax_pulso.set_xlabel("Hora (received_at)")
    ax_pulso.set_ylabel("Eventos consumidos")
    ax_pulso.tick_params(axis="x", rotation=45, labelsize=7)

    # --- Gráfica 2: cobertura por departamento (última lectura) ---
    df_cob = pd.read_sql(text(QUERY_COBERTURA), engine)
    ax_cob.clear()
    if not df_cob.empty:
        ax_cob.barh(df_cob["departamento"], df_cob["cobertura_pct"], color="tab:green")
        ax_cob.invert_yaxis()  # el mayor arriba
    ax_cob.set_title("Cobertura de crédito por depto. (Top 10, última lectura)")
    ax_cob.set_xlabel("Tasa de cobertura (%)")
    fig.tight_layout(rect=[0, 0, 1, 0.96])


# FuncAnimation llama a refrescar() cada REFRESH_SECONDS milisegundos*1000
ani = FuncAnimation(fig, refrescar, interval=REFRESH_SECONDS * 1000, cache_frame_data=False)

if __name__ == "__main__":
    print("📊 Panel en vivo iniciado. Cierra la ventana para terminar.")
    plt.show()
    engine.dispose()
