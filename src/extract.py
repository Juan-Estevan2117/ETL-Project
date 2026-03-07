import pandas as pd
import os

def extract_data(file_path: str) -> pd.DataFrame:
    """
    Carga el conjunto de datos desde un archivo CSV a la memoria principal.

    Args:
        file_path (str): Ruta relativa o absoluta hacia el archivo CSV crudo.

    Returns:
        pd.DataFrame: Un objeto DataFrame de Pandas conteniendo todos los registros sin alterar.

    Raises:
        FileNotFoundError: Si la ruta especificada en file_path no existe.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo no existe en la ruta: {file_path}")
        
    print(f"📥 Extrayendo datos desde: {file_path}...")
    
    # Se desactiva la inferencia inicial de memoria de Pandas para evitar advertencias 
    # causadas por columnas con tipos de datos mixtos (números y letras mezclados).
    df = pd.read_csv(file_path, low_memory=False)
    
    print(f"✅ Extracción completada. Filas recuperadas: {df.shape[0]}")
    return df
