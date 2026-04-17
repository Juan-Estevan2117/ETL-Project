import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import gc

from config import MYSQL_URL


def get_db_connection() -> Engine:
    """Establece la conexión con la base de datos usando la URL del config."""
    try:
        engine = create_engine(MYSQL_URL)
        with engine.connect() as connection:
            print("Conexion a la base de datos establecida correctamente.")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        raise


def _load_dimension_and_get_sk(df_source: pd.DataFrame, dim_name: str, business_keys: list, engine: Engine) -> pd.DataFrame:
    """
    Carga una dimensión con INSERT IGNORE (idempotente) y retorna el DF con SKs.
    Usa engine.begin() para garantizar commit explícito al final del bloque.
    """
    df_dim = df_source[business_keys].drop_duplicates().dropna(subset=business_keys)

    cols = ', '.join(f'`{k}`' for k in business_keys)
    placeholders = ', '.join(f':{k}' for k in business_keys)
    stmt = text(f"INSERT IGNORE INTO `{dim_name}` ({cols}) VALUES ({placeholders})")
    records = df_dim.to_dict(orient='records')

    # engine.begin() abre transacción y commitea al salir del bloque
    with engine.begin() as conn:
        if records:
            conn.execute(stmt, records)

    # Releer la tabla completa para obtener las SKs generadas por AUTO_INCREMENT
    df_loaded = pd.read_sql_table(dim_name, con=engine)
    print(f"      Dimension '{dim_name}': {df_loaded.shape[0]} registros en BD.")
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

    # Dim Nivel Formacion - Enriquecida post-carga (valores en minúsculas)
    dims['dim_nivel_formacion'] = _load_dimension_and_get_sk(df_integrated, 'dim_nivel_formacion', ['nivel_formacion'], engine)
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_nivel_formacion SET tipo_formacion = 'Pregrado' WHERE nivel_formacion IN ('tecnica profesional', 'tecnologica', 'universitaria');"))
        conn.execute(text("UPDATE dim_nivel_formacion SET tipo_formacion = 'Posgrado' WHERE nivel_formacion IN ('especializacion', 'maestria', 'doctorado');"))

    # Dim Sector IES
    dims['dim_sector_ies'] = _load_dimension_and_get_sk(df_integrated, 'dim_sector_ies', ['sector_ies'], engine)

    # Dim Estrato (pre-poblada por el DDL, solo se lee)
    dims['dim_estrato'] = pd.read_sql_table('dim_estrato', con=engine)
    print(f"      Dimension 'dim_estrato': {dims['dim_estrato'].shape[0]} registros en BD (pre-poblada).")

    return dims


def map_surrogate_keys(fact_df: pd.DataFrame, dim_df: pd.DataFrame, business_keys: list, sk_name: str) -> pd.DataFrame:
    """Mapea las surrogate keys a la tabla de hechos usando diccionarios (anti-OOM)."""
    mapping = dim_df.set_index(business_keys)[sk_name].to_dict()

    if len(business_keys) == 1:
        fact_df[sk_name] = fact_df[business_keys[0]].map(mapping)
    else:
        fact_df[sk_name] = pd.Series(list(zip(*[fact_df[c] for c in business_keys]))).map(mapping)

    fact_df.drop(columns=business_keys, inplace=True)
    gc.collect()
    return fact_df


def load_fact_table(df_integrated: pd.DataFrame, dims: dict, engine: Engine):
    """Prepara y carga la tabla de hechos con INSERT IGNORE y commit explícito."""
    print("--- Preparando y cargando la tabla de hechos ---")

    fact_df = df_integrated.copy()

    # Mapeo de todas las surrogate keys
    fact_df = map_surrogate_keys(fact_df, dims['dim_tiempo'], ['anio', 'semestre'], 'sk_tiempo')
    fact_df = map_surrogate_keys(fact_df, dims['dim_ubicacion'], ['departamento'], 'sk_ubicacion')
    fact_df = map_surrogate_keys(fact_df, dims['dim_demografia'], ['id_genero'], 'sk_demografia')
    fact_df = map_surrogate_keys(fact_df, dims['dim_nivel_formacion'], ['nivel_formacion'], 'sk_nivel_formacion')
    fact_df = map_surrogate_keys(fact_df, dims['dim_sector_ies'], ['sector_ies'], 'sk_sector_ies')
    fact_df = map_surrogate_keys(fact_df, dims['dim_estrato'], ['estrato'], 'sk_estrato')

    fact_cols = [
        'sk_tiempo', 'sk_ubicacion', 'sk_demografia', 'sk_nivel_formacion',
        'sk_sector_ies', 'sk_estrato', 'total_matriculados', 'nuevos_beneficiarios_credito'
    ]
    fact_df = fact_df[fact_cols]

    # Validar que no haya nulos en las FKs (indicaría un mapping fallido)
    filas_antes = len(fact_df)
    fact_df.dropna(inplace=True)
    filas_despues = len(fact_df)
    if filas_despues < filas_antes:
        print(f"   ADVERTENCIA: Se eliminaron {filas_antes - filas_despues} filas con llaves foraneas nulas.")

    # Castear FKs a int (el dropna puede dejarlas como float)
    for col in fact_cols[:6]:
        fact_df[col] = fact_df[col].astype(int)

    # Carga con INSERT IGNORE y commit explícito por chunks
    print(f"   -> Cargando {len(fact_df)} registros en 'fact_educacion_superior'...")

    cols = ', '.join(f'`{c}`' for c in fact_cols)
    placeholders = ', '.join(f':{c}' for c in fact_cols)
    stmt = text(f"INSERT IGNORE INTO `fact_educacion_superior` ({cols}) VALUES ({placeholders})")
    records = fact_df.to_dict(orient='records')

    chunk_size = 10000
    with engine.begin() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            conn.execute(stmt, chunk)

    print("      Carga de la tabla de hechos completada.")


def verify_load(engine: Engine):
    """
    Verificación post-carga: consulta la base de datos directamente (no DataFrames)
    para confirmar que los datos fueron persistidos correctamente.
    """
    print("\n--- VERIFICACION POST-CARGA (consultas a la BD) ---")
    tables = [
        'dim_tiempo', 'dim_ubicacion', 'dim_demografia',
        'dim_nivel_formacion', 'dim_sector_ies', 'dim_estrato',
        'fact_educacion_superior', 'legacy_matriculas_detalle'
    ]

    with engine.connect() as conn:
        for table_name in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
            count = result.scalar()
            status = "OK" if count > 0 else "VACIA"
            print(f"      [{status}] {table_name}: {count} registros")

        # Métricas agregadas de la fact table
        result = conn.execute(text(
            "SELECT SUM(total_matriculados) AS matriculados, "
            "SUM(nuevos_beneficiarios_credito) AS beneficiarios "
            "FROM fact_educacion_superior"
        ))
        row = result.fetchone()
        if row and row[0]:
            print(f"\n      Metricas: {int(row[0]):,} matriculados | {int(row[1]):,} beneficiarios ICETEX")
        else:
            print("\n      ALERTA: La fact table no contiene datos agregables.")

    print("--- FIN VERIFICACION ---\n")


def load_data(df_integrated: pd.DataFrame):
    """Orquesta todo el proceso de carga al Data Warehouse."""
    print("--- FASE 3: CARGA (L) ---")
    engine = None
    try:
        engine = get_db_connection()

        # 1. Cargar dimensiones
        dim_dataframes = load_dimensions(df_integrated, engine)

        # 2. Cargar tabla de hechos
        load_fact_table(df_integrated, dim_dataframes, engine)

        # 3. Verificación contra la BD real
        verify_load(engine)

        print("Proceso de Carga Finalizado con Exito.")

    except Exception as e:
        print(f"\nOcurrio un error fatal durante la fase de carga: {e}")
    finally:
        if engine:
            engine.dispose()
            print("Conexion a la base de datos cerrada.")
