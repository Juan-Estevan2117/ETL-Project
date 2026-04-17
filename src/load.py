import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert as mysql_insert
import gc

from config import MYSQL_URL

def get_db_connection() -> Engine:
    """Establece la conexión con la base de datos usando la URL del config."""
    try:
        engine = create_engine(MYSQL_URL)
        with engine.connect() as connection:
            print("Conexión a la base de datos establecida correctamente.")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        raise


def _insert_ignore(table, conn, keys, data_iter):
    """
    Método personalizado para to_sql que emite INSERT IGNORE en MySQL.
    Permite que el pipeline sea re-ejecutable sin necesidad de borrar la BD:
    los registros ya existentes se omiten silenciosamente en lugar de lanzar
    IntegrityError por la UNIQUE KEY de cada dimensión.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = mysql_insert(table.table).prefix_with('IGNORE')
    conn.execute(stmt, data)


def _load_dimension_and_get_sk(df_source: pd.DataFrame, dim_name: str, business_keys: list, engine: Engine) -> pd.DataFrame:
    """
    Carga una dimensión usando INSERT IGNORE y retorna el DataFrame completo con SKs.
    El INSERT IGNORE garantiza idempotencia: en re-runs, los registros duplicados
    se descartan sin error, preservando las surrogate keys ya asignadas por MySQL.
    """
    df_dim = df_source[business_keys].drop_duplicates().dropna(subset=business_keys)

    # INSERT IGNORE: inserta solo los registros que no violen la UNIQUE KEY
    df_dim.to_sql(dim_name, con=engine, if_exists='append', index=False, method=_insert_ignore)

    # Releer la tabla completa para obtener las SKs generadas por AUTO_INCREMENT
    df_loaded = pd.read_sql_table(dim_name, con=engine)
    print(f"      Dimension '{dim_name}' cargada. {df_loaded.shape[0]} registros totales.")
    return df_loaded

def load_dimensions(df_integrated: pd.DataFrame, engine: Engine) -> dict:
    """Carga todas las dimensiones y retorna un diccionario de DataFrames dimensionales."""
    print("--- Cargando y mapeando dimensiones ---")
    
    dims = {}
    
    # Dim Tiempo
    dims['dim_tiempo'] = _load_dimension_and_get_sk(df_integrated, 'dim_tiempo', ['anio', 'semestre'], engine)
    
    # Dim Ubicacion (Departamento)
    dims['dim_ubicacion'] = _load_dimension_and_get_sk(df_integrated, 'dim_ubicacion', ['departamento'], engine)

    # Dim Demografia (Género) - Enriquecida post-carga
    dims['dim_demografia'] = _load_dimension_and_get_sk(df_integrated, 'dim_demografia', ['id_genero'], engine)
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_demografia SET descripcion_genero = 'Masculino' WHERE id_genero = 1 AND (descripcion_genero IS NULL OR descripcion_genero = '');"))
        conn.execute(text("UPDATE dim_demografia SET descripcion_genero = 'Femenino' WHERE id_genero = 2 AND (descripcion_genero IS NULL OR descripcion_genero = '');"))

    # Dim Nivel Formacion - Enriquecida post-carga
    # Los valores de nivel_formacion están en minúsculas (convención del pipeline)
    dims['dim_nivel_formacion'] = _load_dimension_and_get_sk(df_integrated, 'dim_nivel_formacion', ['nivel_formacion'], engine)
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_nivel_formacion SET tipo_formacion = 'Pregrado' WHERE nivel_formacion IN ('tecnica profesional', 'tecnologica', 'universitaria');"))
        conn.execute(text("UPDATE dim_nivel_formacion SET tipo_formacion = 'Posgrado' WHERE nivel_formacion IN ('especializacion', 'maestria', 'doctorado');"))

    # Dim Sector IES
    dims['dim_sector_ies'] = _load_dimension_and_get_sk(df_integrated, 'dim_sector_ies', ['sector_ies'], engine)

    # Dim Estrato (ya pre-poblada por el DDL, aquí solo se lee)
    dims['dim_estrato'] = pd.read_sql_table('dim_estrato', con=engine)
    print("      ✅ Dimensión 'dim_estrato' releída (pre-poblada).")

    return dims

def map_surrogate_keys(fact_df: pd.DataFrame, dim_df: pd.DataFrame, business_keys: list, sk_name: str) -> pd.DataFrame:
    """Mapea las surrogate keys a la tabla de hechos usando diccionarios para eficiencia."""
    mapping = dim_df.set_index(business_keys)[sk_name].to_dict()
    
    if len(business_keys) == 1:
        fact_df[sk_name] = fact_df[business_keys[0]].map(mapping)
    else:
        # Crear una tupla de las llaves de negocio para el mapeo
        fact_df[sk_name] = pd.Series(list(zip(*[fact_df[c] for c in business_keys]))).map(mapping)
        
    fact_df.drop(columns=business_keys, inplace=True)
    gc.collect()
    return fact_df

def load_fact_table(df_integrated: pd.DataFrame, dims: dict, engine: Engine):
    """Prepara y carga la tabla de hechos."""
    print("--- Preparando y cargando la tabla de hechos ---")
    
    fact_df = df_integrated.copy()

    # Mapeo de todas las surrogate keys
    fact_df = map_surrogate_keys(fact_df, dims['dim_tiempo'], ['anio', 'semestre'], 'sk_tiempo')
    fact_df = map_surrogate_keys(fact_df, dims['dim_ubicacion'], ['departamento'], 'sk_ubicacion')
    fact_df = map_surrogate_keys(fact_df, dims['dim_demografia'], ['id_genero'], 'sk_demografia')
    fact_df = map_surrogate_keys(fact_df, dims['dim_nivel_formacion'], ['nivel_formacion'], 'sk_nivel_formacion')
    fact_df = map_surrogate_keys(fact_df, dims['dim_sector_ies'], ['sector_ies'], 'sk_sector_ies')
    fact_df = map_surrogate_keys(fact_df, dims['dim_estrato'], ['estrato'], 'sk_estrato')
    
    # Seleccionar y ordenar columnas para la tabla de hechos
    fact_cols = [
        'sk_tiempo', 'sk_ubicacion', 'sk_demografia', 'sk_nivel_formacion',
        'sk_sector_ies', 'sk_estrato', 'total_matriculados', 'nuevos_beneficiarios_credito'
    ]
    fact_df = fact_df[fact_cols]
    
    # Validar que no haya nulos en las FKs
    filas_antes = len(fact_df)
    fact_df.dropna(inplace=True)
    filas_despues = len(fact_df)
    if filas_despues < filas_antes:
        print(f"⚠️ Advertencia: Se eliminaron {filas_antes - filas_despues} filas con llaves foráneas nulas.")

    # Carga masiva en la base de datos
    print(f"   -> Cargando {len(fact_df)} registros en 'fact_educacion_superior'...")
    fact_df.to_sql('fact_educacion_superior', con=engine, if_exists='append', index=False, chunksize=10000)
    
    print("      ✅ Carga de la tabla de hechos completada.")

def load_data(df_integrated: pd.DataFrame):
    """
    Orquesta todo el proceso de carga al Data Warehouse.
    """
    print("--- FASE 3: CARGA (L) ---")
    engine = None
    try:
        engine = get_db_connection()
        
        # 1. Cargar dimensiones
        dim_dataframes = load_dimensions(df_integrated, engine)
        
        # 2. Cargar tabla de hechos
        load_fact_table(df_integrated, dim_dataframes, engine)
        
        print("🎉 Proceso de Carga Finalizado con Éxito.")
        
    except Exception as e:
        print(f"\n❌ Ocurrió un error fatal durante la fase de carga: {e}")
        # En un escenario real, aquí iría un rollback o una notificación.
    finally:
        if engine:
            engine.dispose()
            print("🔌 Conexión a la base de datos cerrada.")
