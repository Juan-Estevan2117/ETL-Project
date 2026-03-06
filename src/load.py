import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def get_db_connection(user='root', password='', host='localhost', port='3306', database='dw_matriculas_col'):
    """
    Crea y retorna una conexión engine de SQLAlchemy a MySQL.
    Nota: Requiere tener instalado pymysql (pip install pymysql)
    """
    try:
        # Construimos la cadena de conexión de SQLAlchemy
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        print("🔌 Conexión a la base de datos establecida correctamente.")
        return engine
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        return None

def load_dimension(df_master: pd.DataFrame, dim_name: str, cols_to_extract: list, rename_dict: dict, engine) -> pd.DataFrame:
    """
    Extrae datos únicos para una dimensión, la carga a BD y devuelve el cruce con las Surrogate Keys.
    """
    print(f"   -> Cargando dimensión: {dim_name}...")
    
    # 1. Extraer solo los valores únicos para esta dimensión y quitar nulos
    df_dim = df_master[cols_to_extract].drop_duplicates().dropna()
    
    # 2. Renombrar a como espera la tabla en la BD
    if rename_dict:
        df_dim = df_dim.rename(columns=rename_dict)
    
    # 3. Cargar a la Base de Datos
    # if_exists='append' asume que la tabla ya existe (creada por tu script SQL)
    # index=False evita subir el índice numérico de Pandas
    df_dim.to_sql(dim_name, con=engine, if_exists='append', index=False)
    
    # 4. Recuperar la dimensión con sus llaves primarias autogeneradas (SK)
    # Leemos la tabla completa recién insertada para obtener los SKs generados por MySQL
    df_loaded = pd.read_sql_table(dim_name, con=engine)
    print(f"      ✅ {dim_name} cargada. {df_loaded.shape[0]} registros totales.")
    
    return df_loaded

def load_data(df_clean: pd.DataFrame, engine):
    """
    Orquesta la carga de las dimensiones y luego la tabla de hechos.
    """
    if engine is None:
        print("❌ Operación abortada: No hay conexión a DB.")
        return

    print("🚀 Iniciando carga al Data Warehouse...")

    # --- 1. CARGA DE DIMENSIONES ---
    
    # Dimensión Demografía
    dim_demografia = load_dimension(
        df_master=df_clean,
        dim_name='dim_demografia',
        cols_to_extract=['id_genero'],
        rename_dict={}, 
        engine=engine
    )
    # Lógica de negocio (ETL) para la descripción (Ya que solo venía el número 1 o 2)
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_demografia SET descripcion_genero = 'Masculino' WHERE id_genero = 1;"))
        conn.execute(text("UPDATE dim_demografia SET descripcion_genero = 'Femenino' WHERE id_genero = 2;"))

    # Dimensión Tiempo
    dim_tiempo = load_dimension(
        df_master=df_clean,
        dim_name='dim_tiempo',
        cols_to_extract=['anio', 'semestre'],
        rename_dict={},
        engine=engine
    )
    with engine.begin() as conn:
        conn.execute(text("UPDATE dim_tiempo SET periodo_academico = CONCAT(anio, '-', semestre);"))

    # Dimensión Ubicacion
    dim_ubicacion = load_dimension(
        df_master=df_clean,
        dim_name='dim_ubicacion',
        cols_to_extract=['codigo_municipio', 'municipio', 'codigo_departamento', 'departamento'],
        rename_dict={},
        engine=engine
    )

    # Dimensión Programa
    dim_programa = load_dimension(
        df_master=df_clean,
        dim_name='dim_programa',
        cols_to_extract=['codigo_snies', 'nombre_programa', 'nivel_formacion', 'metodologia', 'area_conocimiento', 'nucleo_basico'],
        rename_dict={},
        engine=engine
    )

    # Dimensión Institucion
    # NOTA: Como la jerarquía está en el archivo crudo, usaremos las columnas 'ies' 
    # Aquí podríamos extraer: codigo_ies, nombre_ies, principal_seccional, sector, caracter
    dim_institucion = load_dimension(
        df_master=df_clean,
        dim_name='dim_institucion',
        cols_to_extract=['codigo_ies', 'nombre_ies', 'principal_seccional', 'sector', 'caracter'],
        rename_dict={},
        engine=engine
    )

    # --- 2. PREPARAR TABLA DE HECHOS ---
    print("   -> Preparando fact_matriculas (Cruzando llaves foráneas)...")
    
    # Hacemos merge (JOIN) del DataFrame limpio con cada dimensión recuperada de la BD
    # para cambiar las llaves naturales (ej. nombre municipio) por las Surrogate Keys (ej. sk_ubicacion)
    
    # Merge con Demografía
    fact_df = df_clean.merge(dim_demografia[['sk_demografia', 'id_genero']], on=['id_genero'], how='left')
    
    # Merge con Tiempo
    fact_df = fact_df.merge(dim_tiempo[['sk_tiempo', 'anio', 'semestre']], on=['anio', 'semestre'], how='left')
    
    # Merge con Ubicacion (cruzamos por todas sus columnas para asegurar match exacto)
    fact_df = fact_df.merge(dim_ubicacion[['sk_ubicacion', 'codigo_municipio', 'municipio', 'codigo_departamento', 'departamento']], 
                            on=['codigo_municipio', 'municipio', 'codigo_departamento', 'departamento'], how='left')
    
    # Merge con Programa
    fact_df = fact_df.merge(dim_programa[['sk_programa', 'codigo_snies', 'nombre_programa', 'nivel_formacion', 'metodologia', 'area_conocimiento', 'nucleo_basico']], 
                            on=['codigo_snies', 'nombre_programa', 'nivel_formacion', 'metodologia', 'area_conocimiento', 'nucleo_basico'], how='left')
    
    # Merge con Institucion
    fact_df = fact_df.merge(dim_institucion[['sk_institucion', 'codigo_ies', 'nombre_ies', 'principal_seccional', 'sector', 'caracter']], 
                            on=['codigo_ies', 'nombre_ies', 'principal_seccional', 'sector', 'caracter'], how='left')

    # Seleccionamos solo las llaves foráneas (SKs) y la métrica
    fact_to_load = fact_df[['sk_institucion', 'sk_programa', 'sk_ubicacion', 'sk_tiempo', 'sk_demografia', 'total_matriculados']]

    # Eliminamos filas que por alguna razón no cruzaron (para mantener integridad referencial)
    filas_antes = len(fact_to_load)
    fact_to_load = fact_to_load.dropna()
    if len(fact_to_load) < filas_antes:
        print(f"      ⚠️ Advertencia: Se perdieron {filas_antes - len(fact_to_load)} registros al cruzar las dimensiones.")

    # --- 3. CARGAR TABLA DE HECHOS ---
    print("   -> Cargando fact_matriculas en BD...")
    # Para la tabla de hechos grande, chunksize=10000 es vital para no colapsar la memoria del servidor MySQL
    fact_to_load.to_sql('fact_matriculas', con=engine, if_exists='append', index=False, chunksize=10000)
    
    print(f"      ✅ Hechos cargados. Total de registros insertados: {len(fact_to_load)}")
    print("🎉 Proceso de Carga Finalizado con Éxito.")

# Para poder usar text() en SQLAlchemy al hacer UPDATES
from sqlalchemy import text
