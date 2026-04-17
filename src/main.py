import sys
from pathlib import Path

# Añadir el directorio 'src' al sys.path para permitir importaciones modulares
# Esto asegura que el script se puede ejecutar como 'python src/main.py'
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root))

from src.config import PRIMARY_CSV, PROCESSED_DIR
from src.extract import extract_data, extract_icetex_api
from src.transform import clean_primary, aggregate_primary, clean_icetex, aggregate_icetex
from src.integrate import integrate_sources
from src.load import load_data

def main():
    """
    Función principal que orquesta la ejecución del pipeline ETL completo.
    1. Carga variables de entorno (manejado en config.py).
    2. Valida la existencia del archivo de datos primario.
    3. Ejecuta secuencialmente las fases de Extracción, Transformación, Integración y Carga.
    """
    print("\n=============================================================")
    print("🚀 INICIANDO PIPELINE ETL - ODS4 EDUCACIÓN SUPERIOR COLOMBIA 🚀")
    print("=============================================================\n")

    # --- Validación de Prerrequisitos ---
    if not PRIMARY_CSV.exists():
        print(f"❌ ERROR CRÍTICO: No se encontró el dataset primario en la ruta esperada:")
        print(f"   {PRIMARY_CSV}")
        print("   Por favor, descarga el dataset y colócalo en 'airflow/data/raw/educacionCol.csv'")
        return

    try:
        # --- FASE 1: EXTRACCIÓN (E) ---
        print("--- FASE 1: EXTRACCIÓN (E) ---")
        # Fuente 1: Dataset primario SNIES
        df_primary_raw = extract_data(str(PRIMARY_CSV))
        # Fuente 2: API de ICETEX
        df_icetex_raw = extract_icetex_api()

        # --- FASE 2: TRANSFORMACIÓN (T) ---
        print("--- FASE 2: TRANSFORMACIÓN (T) ---")
        
        # Procesamiento del dataset primario
        df_primary_clean = clean_primary(df_primary_raw)
        df_primary_agg = aggregate_primary(df_primary_clean)

        # Procesamiento del dataset de ICETEX
        df_icetex_clean = clean_icetex(df_icetex_raw)
        df_icetex_agg = aggregate_icetex(df_icetex_clean)

        # Exportar los dataframes procesados para validación y auditoría
        print("-> Exportando dataframes procesados a 'airflow/data/processed/'...")
        df_primary_agg.to_csv(PROCESSED_DIR / "primary_aggregated.csv", index=False)
        df_icetex_agg.to_csv(PROCESSED_DIR / "icetex_aggregated.csv", index=False)
        print("      ✅ CSVs exportados exitosamente.")

        # --- FASE 2.5: INTEGRACIÓN ---
        df_integrated = integrate_sources(df_primary_agg, df_icetex_agg)

        # --- FASE 3: CARGA (L) ---
        # El módulo de carga (load.py) se encarga de la conexión y orquestación
        if not df_integrated.empty:
            load_data(df_integrated)
        else:
            print("⚠️ ADVERTENCIA: El dataset integrado está vacío. No se cargará nada a la base de datos.")

    except Exception as e:
        print(f"❌ OCURRIÓ UN ERROR FATAL EN EL PIPELINE:")
        print(f"   Error: {e}")
        print("   La ejecución ha sido abortada.")
    
    finally:
        print("\n=============================================================")
        print("🏁 EJECUCIÓN DEL PIPELINE FINALIZADA 🏁")
        print("=============================================================")

if __name__ == "__main__":
    main()