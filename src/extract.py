import pandas as pd
import os

def extract_data(file_path: str) -> pd.DataFrame:
    """
    Extrae los datos crudos del archivo CSV.
    No realiza ninguna transformación, solo la carga a memoria.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo no existe en la ruta: {file_path}")
        
    print(f"📥 Extrayendo datos desde: {file_path}...")
    
    # Se usa low_memory=False para evitar warnings por tipos mixtos en columnas grandes
    df = pd.read_csv(file_path, low_memory=False)
    
    print(f"✅ Extracción completada. Filas recuperadas: {df.shape[0]}")
    return df
