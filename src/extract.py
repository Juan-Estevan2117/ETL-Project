import pandas as pd
import requests
import time
from typing import List, Dict, Any

from config import SOCRATA_ENDPOINT, SOCRATA_APP_TOKEN

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
    try:
        print(f"📥 Extrayendo datos desde: {file_path}...")
        
        # Se desactiva la inferencia inicial de memoria de Pandas para evitar advertencias 
        # causadas por columnas con tipos de datos mixtos (números y letras mezclados).
        df = pd.read_csv(file_path, low_memory=False)
        
        print(f"✅ Extracción completada. Filas recuperadas: {df.shape[0]}")
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo no existe en la ruta: {file_path}")

def extract_icetex_api(
    endpoint: str = SOCRATA_ENDPOINT, 
    app_token: str = SOCRATA_APP_TOKEN, 
    limit: int = 50000) -> pd.DataFrame:
    """
    Extrae datos de créditos otorgados por ICETEX desde la API Socrata de Datos.gov.co.
    Maneja paginación y reintentos simples.

    Args:
        endpoint (str): La URL del endpoint de la API.
        app_token (str, optional): Token de aplicación para evitar throttling.
        limit (int): El número de registros a solicitar por página.

    Returns:
        pd.DataFrame: Un DataFrame con todos los registros recuperados de la API.
    """
    print(f"📥 Extrayendo datos desde la API de ICETEX: {endpoint}...")
    
    offset = 0
    all_data: List[Dict[str, Any]] = []
    retries = 3
    
    headers = {"X-App-Token": app_token} if app_token else {}

    while True:
        try:
            params = {"$limit": limit, "$offset": offset}
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()  # Lanza una excepción para errores HTTP 4xx/5xx

            data = response.json()
            if not data:
                print("   -> No se encontraron más datos. Finalizando paginación.")
                break
            
            all_data.extend(data)
            print(f"   -> Página recuperada. Registros hasta ahora: {len(all_data)}")
            offset += limit

        except requests.exceptions.RequestException as e:
            if retries > 0:
                print(f"⚠️ Error en la petición: {e}. Reintentando en 5 segundos... ({retries} intentos restantes)")
                retries -= 1
                time.sleep(5)
            else:
                print(f"❌ Error fatal: No se pudo conectar a la API después de varios intentos.")
                raise e
    
    df = pd.DataFrame(all_data)
    print(f"✅ Extracción de API completada. Total de filas recuperadas: {df.shape[0]}")
    return df
