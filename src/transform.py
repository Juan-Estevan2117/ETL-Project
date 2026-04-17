import pandas as pd
import unicodedata
import re
from typing import Dict

# --- Funciones de Limpieza y Transformación para el Pipeline ETL ---

def clean_text(text: str) -> str:
    """
    Limpia y estandariza cadenas de texto.
    Elimina tildes, puntuación, espacios extra y convierte a minúsculas.
    """
    if not isinstance(text, str):
        return text
    text = text.lower().strip()
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    text = re.sub(r'[.,]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text

def clean_primary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la limpieza y estandarización al dataset primario (SNIES).
    """
    print("🧹 Limpiando dataset primario (SNIES)...")
    
    # 1. Renombrar columnas
    column_mapping = {
        'Código de la Institución': 'codigo_ies', 'Institución de Educación Superior (IES)': 'nombre_ies',
        'Principal oSeccional': 'principal_seccional', 'Id_Sector': 'sector_ies',
        'Id_Caracter': 'caracter', 'Código SNIES delprograma': 'codigo_snies',
        'Programa Académico': 'nombre_programa', 'Id_Nivel_Formacion': 'nivel_formacion',
        'Id_Metodologia': 'metodologia', 'Id_Area': 'area_conocimiento',
        'Núcleo Básico del Conocimiento (NBC)': 'nucleo_basico', 'Código del Municipio(Programa)': 'codigo_municipio',
        'Municipio de oferta del programa': 'municipio', 'Código del Departamento(Programa)': 'codigo_departamento',
        'Departamento de oferta del programa': 'departamento', 'Id Género': 'id_genero',
        'Año': 'anio', 'Semestre': 'semestre', 'Total Matriculados': 'total_matriculados'
    }
    df = df.rename(columns=column_mapping)

    # 2. Eliminar duplicados exactos
    df = df.drop_duplicates()

    # 3. Limpieza de textos y homologación geográfica
    text_cols = ['municipio', 'departamento']
    for col in text_cols:
        df[col] = df[col].apply(clean_text)
    
    # El profiling reveló inconsistencias específicas
    geo_map = {
        'bogota dc': 'bogota', 'narinio': 'narino', 'guajira': 'la guajira',
        'san andres y provi': 'san andres y providencia',
        'archipielago de san andres providencia y santa catalina': 'san andres y providencia'
    }
    df['departamento'] = df['departamento'].replace(geo_map)
    df['municipio'] = df['municipio'].replace({'bogota dc': 'bogota', 'santafe de bogota': 'bogota'})

    # 4. Mapeo de IDs a textos descriptivos (Nivel y Sector)
    # Convención: todos los valores de dominio en minúsculas para consistencia con la suite GX
    nivel_map = {
        1: 'tecnica profesional',
        2: 'tecnologica',
        3: 'universitaria',
        4: 'especializacion',
        5: 'maestria',
        6: 'doctorado',
        7: 'especializacion', # Especialización Médico Quirúrgica
        8: 'especializacion', # Especialización Tecnológica
        10: 'especializacion' # Especialización Técnico Profesional
    }
    sector_map = {
        1: 'oficial',
        2: 'privado'
    }

    df['nivel_formacion'] = pd.to_numeric(df['nivel_formacion'], errors='coerce').map(nivel_map).fillna('desconocido')
    df['sector_ies'] = pd.to_numeric(df['sector_ies'], errors='coerce').map(sector_map).fillna('desconocido')

    # 5. Casteo de tipos
    df['total_matriculados'] = pd.to_numeric(df['total_matriculados'], errors='coerce').fillna(0).astype(int)
    df = df[df['total_matriculados'] > 0]
    
    print("✅ Limpieza del dataset primario completada.")
    return df

def aggregate_primary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega el dataset primario al grano común del Data Warehouse.
    """
    print("🏗️  Agregando dataset primario al grano común...")
    
    # Llaves para la agregación
    agg_keys = ['anio', 'semestre', 'departamento', 'nivel_formacion', 'sector_ies', 'id_genero']
    
    # Agrupar y sumar los matriculados
    df_agg = df.groupby(agg_keys, as_index=False).agg(total_matriculados=('total_matriculados', 'sum'))
    
    # Añadir columna de estrato por defecto
    df_agg['estrato'] = 0  # Imputar 'Desconocido'
    
    print(f"✅ Agregación primaria completada. Filas resultantes: {df_agg.shape[0]}")
    return df_agg

def clean_icetex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el dataset de la API de ICETEX.
    Basado en los hallazgos del profiling (Fase 0).
    """
    print("🧹 Limpiando dataset de la API de ICETEX...")
    df = df.copy()

    # 1. Renombrar y seleccionar columnas
    rename_map = {
        'vigencia': 'anio',
        'periodo_otorgamiento': 'periodo',
        'sexo_al_nacer': 'genero',
        'estrato_socio_economico': 'estrato',
        'departamento_de_origen': 'departamento',
        'sector_ies': 'sector_ies',
        'nivel_de_formacion': 'nivel_formacion',
        'numero_de_nuevos_beneficiarios': 'nuevos_beneficiarios_credito'
    }
    df = df.rename(columns=rename_map)
    df = df[list(rename_map.values())]

    # 2. Descartar filas según hallazgos del profiling
    df = df[df['genero'] != 'Intersexual']  # 14 filas
    df = df[df['nivel_formacion'] != 'Normalista']  # 7 filas

    # 3. Extraer semestre del 'periodo' (e.g., "2015-1")
    df['semestre'] = df['periodo'].str.split('-').str[1]

    # 4. Mapeo de valores
    df['id_genero'] = df['genero'].map({'Femenino': 2, 'Masculino': 1})
    df['sector_ies'] = df['sector_ies'].map({'OFICIAL': 'oficial', 'PRIVADO': 'privado', 'N/A': 'desconocido'})

    # Homologación de Nivel de Formación (11 valores ICETEX → 7 canónicos, en minúsculas)
    nivel_map = {
        'Formación técnica profesional': 'tecnica profesional', 'Tecnológico': 'tecnologica',
        'Universitario': 'universitaria', 'Especialización universitaria': 'especializacion',
        'Especialización médico quirúrgica': 'especializacion', 'Especialización tecnológica': 'especializacion',
        'Especialización técnico profesional': 'especializacion', 'Maestría': 'maestria',
        'Doctorado': 'doctorado', 'Exterior': 'exterior'
    }
    df['nivel_formacion'] = df['nivel_formacion'].replace(nivel_map)

    # 5. Limpieza de texto y homologación geográfica
    df['departamento'] = df['departamento'].apply(clean_text)
    geo_map = {
        'bogota, d.c.': 'bogota',
        'archipielago de san andres, providencia y santa catalina': 'san andres y providencia'
    }
    df['departamento'] = df['departamento'].replace(geo_map)
    
    # 6. Casteo de tipos
    # id_genero se trata por separado: NO se aplica fillna(0) para evitar introducir
    # valores inválidos (0 no es un género reconocido). Las filas sin mapeo se descartan.
    numeric_cols = ['anio', 'semestre', 'estrato', 'nuevos_beneficiarios_credito']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df['id_genero'] = pd.to_numeric(df['id_genero'], errors='coerce')
    filas_antes = len(df)
    df = df.dropna(subset=['id_genero'])
    descartadas = filas_antes - len(df)
    if descartadas > 0:
        print(f"   -> {descartadas} filas descartadas por género no reconocido en ICETEX.")
    df['id_genero'] = df['id_genero'].astype(int)
    
    print("✅ Limpieza del dataset de ICETEX completada.")
    return df

def aggregate_icetex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega el dataset de ICETEX al grano común del Data Warehouse.
    """
    print("🏗️  Agregando dataset de ICETEX al grano común...")
    
    # Llaves para la agregación (incluye estrato)
    agg_keys = ['anio', 'semestre', 'departamento', 'nivel_formacion', 'sector_ies', 'id_genero', 'estrato']
    
    df_agg = df.groupby(agg_keys, as_index=False).agg(nuevos_beneficiarios_credito=('nuevos_beneficiarios_credito', 'sum'))
    
    print(f"✅ Agregación de ICETEX completada. Filas resultantes: {df_agg.shape[0]}")
    return df_agg
