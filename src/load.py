import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import gc

def get_db_connection(user='root', password='', host='localhost', port='3306', database='dw_matriculas_col'):
    """
    Establece una conexión con el motor de base de datos MySQL utilizando SQLAlchemy.

    Args:
        user (str): Nombre de usuario de la base de datos.
        password (str): Contraseña del usuario.
        host (str): Dirección IP o hostname del servidor MySQL.
        port (str): Puerto de conexión del servidor.
        database (str): Nombre del esquema o base de datos.

    Returns:
        sqlalchemy.engine.Engine: Objeto engine si la conexión es exitosa, o None si ocurre un error.
    """
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        print("🔌 Conexión a la base de datos establecida correctamente.")
        return engine
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        return None

def load_dimension(df_master: pd.DataFrame, dim_name: str, cols_to_extract: list, rename_dict: dict, engine) -> pd.DataFrame:
    """
    Filtra los valores únicos para una dimensión específica, los carga en la base de datos
    y recupera las Surrogate Keys autogeneradas.

    Args:
        df_master (pd.DataFrame): DataFrame maestro que contiene todos los datos limpios.
        dim_name (str): Nombre de la tabla dimensional en la base de datos.
        cols_to_extract (list): Lista de nombres de columnas a extraer del df_master.
        rename_dict (dict): Diccionario para renombrar columnas antes de la inserción (puede estar vacío).
        engine (sqlalchemy.engine.Engine): Motor de conexión a la base de datos.

    Returns:
        pd.DataFrame: DataFrame de la dimensión recién cargada, incluyendo sus Surrogate Keys (SK).
    """
    print(f"   -> Cargando dimensión: {dim_name}...")
    
    # Se obtienen las combinaciones únicas y se descartan valores nulos
    df_dim = df_master[cols_to_extract].drop_duplicates().dropna()
    
    if rename_dict:
        df_dim = df_dim.rename(columns=rename_dict)
    
    # Volcado de la dimensión en MySQL, respetando el esquema preexistente
    df_dim.to_sql(dim_name, con=engine, if_exists='append', index=False)
    
    # Lectura inversa para obtener los IDs autoincrementales asignados por MySQL
    df_loaded = pd.read_sql_table(dim_name, con=engine)
    print(f"      ✅ {dim_name} cargada. {df_loaded.shape[0]} registros totales.")
    
    return df_loaded

def load_data(df_clean: pd.DataFrame, engine):
    """
    Orquesta el flujo completo de carga al Data Warehouse:
    1. Carga de las 5 tablas dimensionales.
    2. Cruce de llaves foráneas mediante diccionarios.
    3. Carga masiva por lotes de la tabla de hechos.

    Args:
        df_clean (pd.DataFrame): DataFrame principal limpio y transformado.
        engine (sqlalchemy.engine.Engine): Motor de conexión a la base de datos.

    Returns:
        None
    """
    if engine is None:
        print("❌ Operación abortada: No hay conexión a DB.")
        return

    print("🚀 Iniciando carga al Data Warehouse...")

    # ==========================================
    # FASE 1: POBLAR DIMENSIONES
    # ==========================================
    
    dim_demografia = load_dimension(
        df_master=df_clean, dim_name='dim_demografia', cols_to_extract=['id_genero'], rename_dict={}, engine=engine
    )
    # Enriquecimiento post-carga para hacer la dimensión descriptiva
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_demografia SET descripcion_genero = 'Masculino' WHERE id_genero = 1;"))
        conn.execute(text("UPDATE dim_demografia SET descripcion_genero = 'Femenino' WHERE id_genero = 2;"))

    dim_tiempo = load_dimension(
        df_master=df_clean, dim_name='dim_tiempo', cols_to_extract=['anio', 'semestre'], rename_dict={}, engine=engine
    )
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_tiempo SET periodo_academico = CONCAT(anio, '-', semestre);"))

    dim_ubicacion = load_dimension(
        df_master=df_clean, dim_name='dim_ubicacion', 
        cols_to_extract=['codigo_municipio', 'municipio', 'codigo_departamento', 'departamento'], rename_dict={}, engine=engine
    )

    dim_programa = load_dimension(
        df_master=df_clean, dim_name='dim_programa', 
        cols_to_extract=['codigo_snies', 'nombre_programa', 'nivel_formacion', 'metodologia', 'area_conocimiento', 'nucleo_basico'], rename_dict={}, engine=engine
    )

    dim_institucion = load_dimension(
        df_master=df_clean, dim_name='dim_institucion', 
        cols_to_extract=['codigo_ies', 'nombre_ies', 'principal_seccional', 'sector', 'caracter'], rename_dict={}, engine=engine
    )

    # ==========================================
    # FASE 2: PREPARAR TABLA DE HECHOS (Optimización)
    # ==========================================
    print("   -> Preparando fact_matriculas (Mapeo por Diccionarios para bajo consumo de RAM)...")
    fact_df = df_clean.copy()
    
    def map_surrogate_keys(df, dim_df, cols, sk_col):
        """
        Reemplaza columnas descriptivas por llaves sustitutas utilizando diccionarios.
        Es drásticamente más eficiente en memoria que pd.merge().
        """
        if len(cols) == 1:
            mapping = dim_df.set_index(cols[0])[sk_col].to_dict()
            df[sk_col] = df[cols[0]].map(mapping)
        else:
            mapping = dim_df.set_index(cols)[sk_col].to_dict()
            df[sk_col] = pd.Series(list(zip(*[df[c] for c in cols]))).map(mapping)
        
        # Eliminación explícita para evitar saturación de memoria RAM
        df.drop(columns=cols, inplace=True)
        gc.collect()

    map_surrogate_keys(fact_df, dim_demografia, ['id_genero'], 'sk_demografia')
    map_surrogate_keys(fact_df, dim_tiempo, ['anio', 'semestre'], 'sk_tiempo')
    map_surrogate_keys(fact_df, dim_ubicacion, ['codigo_municipio', 'municipio', 'codigo_departamento', 'departamento'], 'sk_ubicacion')
    map_surrogate_keys(fact_df, dim_programa, ['codigo_snies', 'nombre_programa', 'nivel_formacion', 'metodologia', 'area_conocimiento', 'nucleo_basico'], 'sk_programa')
    map_surrogate_keys(fact_df, dim_institucion, ['codigo_ies', 'nombre_ies', 'principal_seccional', 'sector', 'caracter'], 'sk_institucion')

    fact_to_load = fact_df[['sk_institucion', 'sk_programa', 'sk_ubicacion', 'sk_tiempo', 'sk_demografia', 'total_matriculados']]

    # Filtrar registros que no hayan podido cruzarse correctamente
    filas_antes = len(fact_to_load)
    fact_to_load = fact_to_load.dropna()
    if len(fact_to_load) < filas_antes:
        print(f"      ⚠️ Advertencia: Se perdieron {filas_antes - len(fact_to_load)} registros al cruzar las dimensiones.")

    # ==========================================
    # FASE 3: CARGAR HECHOS A MYSQL
    # ==========================================
    print("   -> Cargando fact_matriculas en BD...")
    fact_to_load.to_sql('fact_matriculas', con=engine, if_exists='append', index=False, chunksize=10000)
    
    print(f"      ✅ Hechos cargados. Total de registros insertados: {len(fact_to_load)}")
    print("🎉 Proceso de Carga Finalizado con Éxito.")
