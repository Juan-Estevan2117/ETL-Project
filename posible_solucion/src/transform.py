import pandas as pd

def transform_data(df: pd.DataFrame) -> dict:
    print("Iniciando transformación de datos...")
    
    # 1. Limpieza de duplicados y nulos críticos
    df_clean = df.drop_duplicates().copy()
    df_clean = df_clean.dropna(subset=['Total Matriculados', 'Código SNIES delprograma'])
    
    # 2. Conversión de tipos 
    # Se agrega los IDs a la lista para que crucen bien con el INT de tu create_tables.sql
    cols_numericas = ['Año', 'Semestre', 'Total Matriculados', 'Id Género', 
                    'Id_Nivel_Formacion', 'Id_Metodologia', 'Id_Area', 
                    'Id_Sector', 'Id_Caracter', 'Código de la Institución', 
                    'Código SNIES delprograma', 'Código del Municipio(Programa)', 
                    'Código del Departamento(Programa)']
    
    for col in cols_numericas:
        if col in df_clean.columns:
            # pd.to_numeric con errors='coerce' convierte los textos inválidos a NaN
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            # Llenamos los NaN con 0 y convertimos a entero
            df_clean[col] = df_clean[col].fillna(0).astype(int)
    
    # 3. Validación 
    # Ahora que 'Total Matriculados' es estrictamente numérico, esta línea funcionará perfecto.
    df_clean = df_clean[df_clean['Total Matriculados'] >= 0]

    
    # 4. Estandarización de textos
    str_cols = ['Institución de Educación Superior (IES)', 'Programa Académico', 'Municipio de oferta del programa']
    for col in str_cols:
        df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()

    # 5. Creación de ID de tiempo
    df_clean['id_tiempo'] = df_clean['Año'].astype(str) + '-' + df_clean['Semestre'].astype(str)

    
    # --- Creación de Dimensiones ---
    
    # En cada dimensión, aseguramos que la PK sea única usando subset
    dim_institucion = df_clean[['Código de la Institución', 'Institución de Educación Superior (IES)', 
                                'Principal oSeccional', 'Id_Sector', 'Id_Caracter']].copy()
    dim_institucion.columns = ['codigo_ies', 'nombre_ies', 'principal_seccional', 'sector', 'caracter']
    # Mantener solo la primera vez que aparezca cada codigo_ies
    dim_institucion = dim_institucion.drop_duplicates(subset=['codigo_ies'], keep='first')

    dim_programa = df_clean[['Código SNIES delprograma', 'Programa Académico', 'Id_Nivel_Formacion', 
                            'Id_Metodologia', 'Id_Area', 'Núcleo Básico del Conocimiento (NBC)']].copy()
    dim_programa.columns = ['codigo_snies', 'nombre_programa', 'id_nivel_formacion', 'id_metodologia', 'id_area', 'nucleo_basico']
    dim_programa = dim_programa.drop_duplicates(subset=['codigo_snies'], keep='first')

    dim_ubicacion = df_clean[['Código del Municipio(Programa)', 'Municipio de oferta del programa', 
                            'Código del Departamento(Programa)', 'Departamento de oferta del programa']].copy()
    dim_ubicacion.columns = ['codigo_municipio', 'municipio', 'codigo_departamento', 'departamento']
    dim_ubicacion = dim_ubicacion.drop_duplicates(subset=['codigo_municipio'], keep='first')

    dim_demografia = df_clean[['Id Género']].drop_duplicates(subset=['Id Género']).copy()
    dim_demografia.columns = ['id_genero']
    dim_demografia['descripcion_genero'] = dim_demografia['id_genero'].apply(
        lambda x: 'Masculino' if x == 1 else ('Femenino' if x == 2 else 'Otro')
    )
    
    # Surrogate Key para Dimensión Tiempo
    dim_tiempo = df_clean[['id_tiempo', 'Año', 'Semestre']].drop_duplicates().reset_index(drop=True)
    # Insertamos una columna autoincremental en la posición 0
    dim_tiempo.insert(0, 'sk_tiempo', range(1, len(dim_tiempo) + 1))
    dim_tiempo.columns = ['sk_tiempo', 'id_tiempo_natural', 'anio', 'semestre']

    # --- Tabla de Hechos ---
    fact_matriculas = df_clean[['Código de la Institución', 'Código SNIES delprograma', 
                                'Código del Municipio(Programa)', 'id_tiempo', 'Id Género', 'Total Matriculados']]
    
    # Reemplazamos el texto 'id_tiempo' por el número 'sk_tiempo' haciendo un JOIN (merge)
    fact_matriculas = fact_matriculas.merge(
        dim_tiempo[['id_tiempo_natural', 'sk_tiempo']], 
        left_on='id_tiempo', 
        right_on='id_tiempo_natural', 
        how='left'
    )

    # Seleccionamos las columnas finales usando sk_tiempo en vez de id_tiempo y renombramos
    fact_matriculas = fact_matriculas[['Código de la Institución', 'Código SNIES delprograma', 
                                    'Código del Municipio(Programa)', 'sk_tiempo', 'Id Género', 'Total Matriculados']]
    fact_matriculas.columns = ['codigo_ies', 'codigo_snies', 'codigo_municipio', 'sk_tiempo', 'id_genero', 'total_matriculados']

    # Surrogate Key para la Tabla de Hechos
    fact_matriculas.insert(0, 'id_hecho', range(1, len(fact_matriculas) + 1))

    print("Transformación completada con Surrogate Keys implementadas.")
    return {
        'dim_institucion': dim_institucion,
        'dim_programa': dim_programa,
        'dim_ubicacion': dim_ubicacion,
        'dim_tiempo': dim_tiempo,
        'dim_demografia': dim_demografia,
        'fact_matriculas': fact_matriculas
    }