import pandas as pd
import numpy as np
import unicodedata
import re

# Importamos el módulo de extracción (modularidad)
from extract import extract_data

def clean_text(text: str) -> str:
    """Limpia tildes, signos de puntuación y espacios extras."""
    if pd.isna(text):
        return text
    # Convertir a minúsculas y quitar espacios en los bordes
    text = str(text).lower().strip()
    # Quitar tildes (normalización NFD)
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    # Reemplazar puntos y comas por vacío
    text = re.sub(r'[.,]', '', text)
    # Quitar dobles espacios
    text = re.sub(r'\s+', ' ', text)
    return text

def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia los datos y aplica las reglas de negocio."""
    print("🧹 Iniciando proceso de transformación y limpieza...")

    # 1. Manejo de duplicados EXACTOS
    filas_antes = df.shape[0]
    df = df.drop_duplicates()
    print(f"   - Se eliminaron {filas_antes - df.shape[0]} filas exactamente duplicadas (clones).")

    # 2. Renombrar Columnas Manualmente para que coincidan con el Data Warehouse
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

    # 3. Casteo de Tipos y Eliminación de Ceros/Nulos
    df['total_matriculados'] = pd.to_numeric(df['total_matriculados'], errors='coerce')
    filas_antes_nulos = df.shape[0]
    df = df.dropna(subset=['total_matriculados'])
    df = df[df['total_matriculados'] > 0]
    df['total_matriculados'] = df['total_matriculados'].astype(int)
    print(f"   - Se eliminaron {filas_antes_nulos - df.shape[0]} filas sin matrículas válidas.")

    # 4. Normalización Avanzada de Textos (Tildes, Puntuación, Homologación)
    columnas_texto = [
        'nombre_ies', 'principal_seccional', 'nombre_programa', 
        'nucleo_basico', 'municipio', 'departamento'
    ]
    
    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
            
    # --- HOMOLOGACIÓN MANUAL DE DEPARTAMENTOS Y MUNICIPIOS ---
    mapa_deptos = {
        'bogota dc': 'bogota',
        'narinio': 'narino',
        'guajira': 'la guajira',
        'san andres y provi': 'san andres y providencia',
        'archipielago de sa': 'san andres y providencia',
        'archipielago de san andres providencia y santa catalina': 'san andres y providencia'
    }
    
    mapa_municipios = {
        'bogota dc': 'bogota',
        'santafe de bogota': 'bogota',
        'bogota d c': 'bogota',
        'bogota': 'bogota'
    }

    # Aplicar a las columnas de Departamentos
    for col_dep in ['departamento', 'departamento_ies']:
        if col_dep in df.columns:
            df[col_dep] = df[col_dep].replace(mapa_deptos)
            
    # Aplicar a las columnas de Municipios
    for col_mun in ['municipio', 'municipio_ies']:
        if col_mun in df.columns:
            df[col_mun] = df[col_mun].replace(mapa_municipios)

    print("   - Se estandarizaron los nombres de todos los Departamentos y Municipios.")

    print(f"   - Se normalizaron {len(columnas_texto)} columnas de texto (Tildes y Puntuación eliminadas).")

    # 5. Manejo de códigos y casteos forzados para coincidir con la BD
    df['codigo_snies'] = pd.to_numeric(df['codigo_snies'], errors='coerce').fillna(-1).astype(int)
    
    # IMPORTANTE: En el archivo CSV original, nivel_formacion y metodologia vienen como números (IDs), 
    # pero en tu script MySQL los definimos como VARCHAR(150) porque a futuro deberían contener texto.
    # Por ahora, los forzaremos a string para que no choque el pd.merge() con la lectura de SQLAlchemy.
    for col in ['nivel_formacion', 'metodologia']:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # 6. Agrupación Final
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