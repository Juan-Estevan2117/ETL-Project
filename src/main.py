import sys
from pathlib import Path

# Añadir la raíz del proyecto al sys.path para permitir importaciones como 'from src.X import Y'
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root))

import pymysql
from sqlalchemy import text

from src.config import PRIMARY_CSV, PROCESSED_DIR, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DW_DB
from src.extract import extract_data, extract_icetex_api
from src.transform import clean_primary, aggregate_primary, clean_icetex, aggregate_icetex
from src.integrate import integrate_sources
from src.load import load_data


def init_database_if_not_exists(sql_path: Path) -> None:
    """
    Verifica si la base de datos del DW existe. Si no, ejecuta el DDL completo.
    Esto permite que el pipeline sea autónomo: no requiere que el usuario ejecute
    el DDL manualmente antes de la primera ejecución.

    La comprobación es lazy: si la BD ya existe, no toca nada (no hace drop/recreate).
    Para un re-run limpio se debe eliminar la BD manualmente.
    """
    print("--- Verificando existencia de la base de datos ---")
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=int(MYSQL_PORT),
        charset='utf8mb4'
)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW DATABASES LIKE '{MYSQL_DW_DB}'")
            if cursor.fetchone():
                print(f"   -> Base de datos '{MYSQL_DW_DB}' encontrada. Omitiendo inicialización del DDL.")
                return

            print(f"   -> Base de datos '{MYSQL_DW_DB}' no existe. Ejecutando DDL desde '{sql_path.name}'...")
            sql_content = sql_path.read_text(encoding='utf-8')

            # Filtrar y ejecutar cada sentencia del DDL
            for stmt in sql_content.split(';'):
                stmt = stmt.strip()
                # Ignorar bloques vacíos y comentarios puros
                if not stmt or all(line.startswith('--') for line in stmt.splitlines() if line.strip()):
                    continue
                cursor.execute(stmt)

        conn.commit()
        print(f"   -> DDL ejecutado correctamente. Base de datos '{MYSQL_DW_DB}' inicializada.")
    finally:
        conn.close()


def main():
    """
    Función principal que orquesta la ejecución del pipeline ETL completo.
    1. Carga variables de entorno (manejado en config.py).
    2. Valida la existencia del archivo de datos primario.
    3. Ejecuta secuencialmente las fases de Extracción, Transformación, Integración y Carga.
    """
    print("\n=============================================================")
    print("INICIANDO PIPELINE ETL - ODS4 EDUCACIÓN SUPERIOR COLOMBIA")
    print("=============================================================\n")

    # --- Validación de Prerrequisitos ---
    if not PRIMARY_CSV.exists():
        print(f"ERROR CRITICO: No se encontró el dataset primario en la ruta esperada:")
        print(f"   {PRIMARY_CSV}")
        print("   Por favor, descarga el dataset y colócalo en 'airflow/data/raw/educacionCol.csv'")
        return

    # --- INICIALIZACIÓN DEL SCHEMA (idempotente) ---
    sql_ddl_path = project_root / "sql" / "init_dw_matriculas_col.sql"
    init_database_if_not_exists(sql_ddl_path)

    try:
        # --- FASE 1: EXTRACCIÓN (E) ---
        print("--- FASE 1: EXTRACCIÓN (E) ---")
        df_primary_raw = extract_data(str(PRIMARY_CSV))
        df_icetex_raw = extract_icetex_api()

        # --- FASE 2: TRANSFORMACIÓN (T) ---
        print("--- FASE 2: TRANSFORMACIÓN (T) ---")
        
        # Procesamiento del dataset primario
        df_primary_clean = clean_primary(df_primary_raw)
        
        # Guardar el primario limpio (granularidad fina) para la vista auxiliar y auditoría
        df_primary_clean.to_csv(PROCESSED_DIR / "educacionCol_clean.csv", index=False)
        print("      -> CSV primario limpio exportado: 'educacionCol_clean.csv'.")

        # Cargar a la tabla legacy para la vista auxiliar (solo columnas relevantes)
        legacy_cols = [
            'codigo_ies', 'nombre_ies', 'principal_seccional', 'sector_ies',
            'caracter', 'codigo_snies', 'nombre_programa', 'nivel_formacion',
            'metodologia', 'area_conocimiento', 'nucleo_basico',
            'codigo_municipio', 'municipio', 'codigo_departamento',
            'departamento', 'id_genero', 'anio', 'semestre', 'total_matriculados'
        ]
        try:
            from src.load import get_db_connection
            print("   -> Cargando datos de granularidad fina en 'legacy_matriculas_detalle'...")
            engine = get_db_connection()
            df_primary_clean[legacy_cols].to_sql(
                'legacy_matriculas_detalle', con=engine,
                if_exists='replace', index=False, chunksize=10000
            )
            # Crear/actualizar la vista auxiliar sobre la tabla legacy
            view_sql = (project_root / "sql" / "vw_matriculas_detalle.sql").read_text(encoding='utf-8')
            with engine.begin() as conn:
                for stmt in view_sql.split(';'):
                    stmt = stmt.strip()
                    if stmt and not all(l.startswith('--') for l in stmt.splitlines() if l.strip()):
                        conn.execute(text(stmt))
            print("      Carga a tabla legacy y vista auxiliar completada.")
            engine.dispose()
        except Exception as e:
            print(f"   ADVERTENCIA: No se pudo cargar la tabla legacy/vista. Error: {e}")

        df_primary_agg = aggregate_primary(df_primary_clean)

        # Procesamiento del dataset de ICETEX
        df_icetex_clean = clean_icetex(df_icetex_raw)
        df_icetex_agg = aggregate_icetex(df_icetex_clean)

        # Exportar los datasets procesados para auditoría y validación externa
        print("-> Exportando datasets procesados a 'airflow/data/processed/'...")
        df_primary_agg.to_csv(PROCESSED_DIR / "educacionCol_aggregated.csv", index=False)
        df_icetex_agg.to_csv(PROCESSED_DIR / "creditos_icetex_clean.csv", index=False)
        print("   -> Exportados: 'educacionCol_aggregated.csv', 'creditos_icetex_clean.csv'.")

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