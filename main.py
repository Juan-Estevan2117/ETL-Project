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
import pymysql

def init_database_if_not_exists(user, password, host, port, database, sql_script_path):
    """
    Verifica si la base de datos existe. Si no, lee el archivo SQL y lo ejecuta
    para crear el esquema y las tablas automáticamente.
    """
    try:
        # Conectar al servidor MySQL sin especificar la base de datos
        connection = pymysql.connect(host=host, user=user, password=password, port=int(port))
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW DATABASES LIKE '{database}'")
            result = cursor.fetchone()
            
            if not result:
                print(f"⚙️ La base de datos '{database}' no existe. Creando e inicializando desde script...")
                with open(sql_script_path, 'r', encoding='utf-8') as f:
                    sql_script = f.read()
                
                # Separar las consultas por ';' y ejecutarlas una por una
                statements = sql_script.split(';')
                for statement in statements:
                    if statement.strip():
                        cursor.execute(statement)
                connection.commit()
                print("   ✅ Base de datos y tablas creadas con éxito.")
        connection.close()
    except Exception as e:
        print(f"⚠️ Error al verificar/inicializar la base de datos: {e}")

def main():
    """
    Función principal que orquesta la ejecución del pipeline ETL.
    1. Carga variables de entorno.
    2. Valida la existencia del archivo de datos y la conexión a la base de datos.
    3. Ejecuta secuencialmente las fases de Extracción, Transformación y Carga.
    """
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

    # Inicialización automática de la base de datos (Ejecuta DDL si no existe)
    sql_script_path = current_dir / "sql" / "init_dw_matriculas_col.sql"
    init_database_if_not_exists(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, str(sql_script_path))

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