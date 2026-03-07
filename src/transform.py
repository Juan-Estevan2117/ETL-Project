import pandas as pd
import numpy as np
import unicodedata
import re

# Importamos el módulo de extracción (modularidad)
from extract import extract_data

def clean_text(text: str) -> str:
    """
    Limpia y estandariza cadenas de texto eliminando tildes, puntuación y espacios adicionales.

    Args:
        text (str): Cadena de texto a limpiar. Puede ser de cualquier tipo o NaN.

    Returns:
        str: Cadena de texto en minúsculas, sin tildes ni signos de puntuación.
        Retorna el mismo valor si la entrada es NaN (Not a Number).
    """
    if pd.isna(text):
        return text
    
    # Convierte a minúsculas y elimina espacios al principio y al final
    text = str(text).lower().strip()
    
    # Descompone los caracteres acentuados (NFD) y elimina las marcas diacríticas (Mn = Nonspacing_Mark)
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    
    # Reemplaza signos de puntuación comunes por una cadena vacía
    text = re.sub(r'[.,]', '', text)
    
    # Reduce múltiples espacios consecutivos a un solo espacio
    text = re.sub(r'\s+', ' ', text)
    
    return text

def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las reglas de negocio y limpieza de datos al DataFrame crudo.

    Args:
        df (pd.DataFrame): DataFrame original proveniente de la fase de Extracción.

    Returns:
        pd.DataFrame: DataFrame limpio, estandarizado y agrupado a la granularidad 
        requerida para la Tabla de Hechos.
    """
    print("🧹 Iniciando proceso de transformación y limpieza...")

    # ---------------------------------------------------------
    # 1. Eliminación de Duplicados Exactos (Errores del SNIES)
    # ---------------------------------------------------------
    # Se eliminan filas que son 100% idénticas en todas sus columnas para evitar inflar las métricas.
    filas_antes = df.shape[0]
    df = df.drop_duplicates()
    print(f"   - Se eliminaron {filas_antes - df.shape[0]} filas exactamente duplicadas (clones).")

    # ---------------------------------------------------------
    # 2. Renombrado de Columnas a Formato de Base de Datos
    # ---------------------------------------------------------
    # Mapeo manual para asegurar compatibilidad exacta con el Data Warehouse (SQL)
    column_mapping = {
        'Código de la Institución': 'codigo_ies',
        'Institución de Educación Superior (IES)': 'nombre_ies',
        'Principal oSeccional': 'principal_seccional',
        'Id_Sector': 'sector',
        'Id_Caracter': 'caracter',
        'Código SNIES delprograma': 'codigo_snies',
        'Programa Académico': 'nombre_programa',
        'Id_Nivel_Formacion': 'nivel_formacion',
        'Id_Metodologia': 'metodologia',
        'Id_Area': 'area_conocimiento',
        'Núcleo Básico del Conocimiento (NBC)': 'nucleo_basico',
        'Código del Municipio(Programa)': 'codigo_municipio',
        'Municipio de oferta del programa': 'municipio',
        'Código del Departamento(Programa)': 'codigo_departamento',
        'Departamento de oferta del programa': 'departamento',
        'Id Género': 'id_genero',
        'Año': 'anio',
        'Semestre': 'semestre',
        'Total Matriculados': 'total_matriculados'
    }
    df = df.rename(columns=column_mapping)
    print("   - Columnas renombradas manualmente (Alineadas con el DW).")

    # ---------------------------------------------------------
    # 3. Casteo de Métrica Principal y Filtrado del Grano
    # ---------------------------------------------------------
    # Convierte la columna de métrica a numérico. Valores no numéricos se vuelven NaN.
    df['total_matriculados'] = pd.to_numeric(df['total_matriculados'], errors='coerce')
    filas_antes_nulos = df.shape[0]
    
    # El grano exige al menos 1 estudiante matriculado. Se descartan nulos y ceros.
    df = df.dropna(subset=['total_matriculados'])
    df = df[df['total_matriculados'] > 0]
    df['total_matriculados'] = df['total_matriculados'].astype(int)
    print(f"   - Se eliminaron {filas_antes_nulos - df.shape[0]} filas sin matrículas válidas.")

    # ---------------------------------------------------------
    # 4. Estandarización de Textos y Homologación Geográfica
    # ---------------------------------------------------------
    # Columnas categóricas a normalizar con clean_text()
    columnas_texto = [
        'nombre_ies', 'principal_seccional', 'nombre_programa', 
        'nucleo_basico', 'municipio', 'departamento'
    ]
    
    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
            
    # Homologación manual forzada para reducir los departamentos de 54 a los 33 oficiales
    mapa_deptos = {
        'bogota dc': 'bogota',
        'narinio': 'narino',
        'guajira': 'la guajira',
        'san andres y provi': 'san andres y providencia',
        'archipielago de sa': 'san andres y providencia',
        'archipielago de san andres providencia y santa catalina': 'san andres y providencia'
    }
    
    # Homologación forzada para la capital
    mapa_municipios = {
        'bogota dc': 'bogota',
        'santafe de bogota': 'bogota',
        'bogota d c': 'bogota',
        'bogota': 'bogota'
    }

    # Aplica diccionarios de homologación si las columnas existen en el DataFrame
    for col_dep in ['departamento', 'departamento_ies']:
        if col_dep in df.columns:
            df[col_dep] = df[col_dep].replace(mapa_deptos)
            
    for col_mun in ['municipio', 'municipio_ies']:
        if col_mun in df.columns:
            df[col_mun] = df[col_mun].replace(mapa_municipios)

    print("   - Se estandarizaron los nombres de todos los Departamentos y Municipios.")
    print(f"   - Se normalizaron {len(columnas_texto)} columnas de texto (Tildes y Puntuación eliminadas).")

    # ---------------------------------------------------------
    # 5. Adaptación de Tipos para Carga en MySQL (SQLAlchemy)
    # ---------------------------------------------------------
    # El SNIES puede traer valores "ND" que se reemplazan por -1 en enteros
    df['codigo_snies'] = pd.to_numeric(df['codigo_snies'], errors='coerce').fillna(-1).astype(int)
    
    # Se fuerza a string porque en el SQL DDL están definidos como VARCHAR 
    # y SQLAlchemy fallaría al intentar mapear INT64 con VARCHAR.
    for col in ['nivel_formacion', 'metodologia', 'sector', 'caracter']:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # ---------------------------------------------------------
    # 6. Agrupación Final (Consolidación de la Métrica)
    # ---------------------------------------------------------
    # Llave compuesta por todas las dimensiones. Si existen varios registros para la misma llave,
    # se suman sus estudiantes (ej. reportes separados en distintos meses del mismo semestre).
    llave_negocio = [
        'codigo_ies', 'nombre_ies', 'principal_seccional',
        'sector', 'caracter', 'codigo_snies', 'nombre_programa',
        'nivel_formacion', 'metodologia', 'area_conocimiento', 'nucleo_basico',
        'codigo_municipio', 'municipio',
        'codigo_departamento', 'departamento',
        'anio', 'semestre', 'id_genero'
    ]
    
    df_agrupado = df.groupby(llave_negocio, as_index=False)['total_matriculados'].sum()
    print(f"   - Filas finales consolidadas tras agrupar: {df_agrupado.shape[0]}")

    print("✅ Transformación completada.")
    return df_agrupado