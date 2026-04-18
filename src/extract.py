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
        print(f"Extrayendo datos desde: {file_path}...")
        # low_memory=False evita advertencias de tipos mixtos en columnas numéricas del SNIES
        df = pd.read_csv(file_path, low_memory=False)
        print(f"Extracción completada. Filas recuperadas: {df.shape[0]}")
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
    print(f"Extrayendo datos desde la API de ICETEX: {endpoint}...")

    MAX_RETRIES = 3
    offset = 0
    all_data: List[Dict[str, Any]] = []
    headers = {"X-App-Token": app_token} if app_token else {}

    while True:
        params = {"$limit": limit, "$offset": offset}
        data = None

        # Reintento por petición (backoff lineal: 5s, 10s, 15s)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(endpoint, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES:
                    wait = 5 * attempt
                    print(f"   -> Error en petición (intento {attempt}/{MAX_RETRIES}): {e}. Reintentando en {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"   -> Error fatal: no se pudo conectar a la API tras {MAX_RETRIES} intentos.")
                    raise

        if not data:
            print("   -> No se encontraron más datos. Finalizando paginación.")
            break

        all_data.extend(data)
        print(f"   -> Página recuperada (offset={offset}). Registros acumulados: {len(all_data)}")
        offset += limit
    
    df = pd.DataFrame(all_data)
    print(f"Extracción de API completada. Total de filas recuperadas: {df.shape[0]}")
    return df
