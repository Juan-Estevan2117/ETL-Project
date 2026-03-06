import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Configurar el PYTHONPATH automáticamente para que encuentre los módulos en 'src/'
# Esto funciona tanto en Windows como en Linux/Mac
current_dir = Path(__file__).resolve().parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

# Importar los módulos del pipeline
from extract import extract_data
from transform import clean_and_transform
from load import get_db_connection, load_data

def main():
    # Cargar variables de entorno desde el archivo .env si existe
    load_dotenv()

    print("==================================================")
    print("🚀 INICIANDO PIPELINE ETL - PROYECTO ODS COLOMBIA 🚀")
    print("==================================================\n")

    # 1. Definir rutas usando pathlib para compatibilidad multiplataforma
    raw_data_path = current_dir / "data" / "raw" / "educacionCol.csv"
    
    if not raw_data_path.exists():
        print(f"❌ ERROR CRÍTICO: No se encontró el dataset crudo en:\n{raw_data_path}")
        print("Asegúrate de haber descargado el CSV y colocarlo en la carpeta data/raw/")
        return

    # 2. Configuración de Base de Datos
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD") 
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")

    engine = get_db_connection(
        user=DB_USER, 
        password=DB_PASSWORD, 
        host=DB_HOST, 
        port=DB_PORT, 
        database=DB_NAME
    )

    if engine is None:
        print("❌ No se pudo conectar a la base de datos. Abortando ETL.")
        print("Asegúrate de que el servicio de MySQL esté corriendo y de haber ejecutado el script SQL en Workbench.")
        return

    try:
        # --- FASE 1: EXTRACCIÓN ---
        print("\n--- FASE 1: EXTRACCIÓN (E) ---")
        df_raw = extract_data(str(raw_data_path))

        # --- FASE 2: TRANSFORMACIÓN ---
        print("\n--- FASE 2: TRANSFORMACIÓN (T) ---")
        df_clean = clean_and_transform(df_raw)
        
        # Guardar en data/processed
        processed_data_path = current_dir / "data" / "processed" / "educacionCol_clean.csv"
        processed_data_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"   -> Exportando datos transformados a {processed_data_path}...")
        df_clean.to_csv(processed_data_path, index=False)
        print("      ✅ CSV exportado exitosamente.")

        # --- FASE 3: CARGA ---
        print("\n--- FASE 3: CARGA (L) ---")
        # Precaución de seguridad: Validamos que la transformación no haya devuelto un DF vacío
        if not df_clean.empty:
            load_data(df_clean, engine)
        else:
            print("⚠️ Advertencia: El dataset limpio está vacío. No se cargará nada a la base de datos.")

    except Exception as e:
        print(f"\n❌ Ocurrió un error fatal durante la ejecución del pipeline:")
        print(e)
    finally:
        # Limpieza de conexiones si es necesario (SQLAlchemy Engine maneja su propio pool, 
        # pero es buena práctica indicarlo visualmente)
        print("\n==================================================")
        print("🏁 EJECUCIÓN DEL PIPELINE FINALIZADA 🏁")
        print("==================================================")

if __name__ == "__main__":
    main()