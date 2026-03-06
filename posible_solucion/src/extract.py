import pandas as pd

def extract_data(file_path):
    print("Iniciando extracción de datos...")
    try:
        df = pd.read_csv(
            file_path, 
            sep=',', 
            quotechar='"', 
            encoding='utf-8',
            on_bad_lines='warn',
            low_memory=False
        )
        print(f"Datos extraídos exitosamente. Total de filas: {len(df)}")
        return df
    except Exception as e:
        print(f"Error durante la extracción: {e}")
        raise e