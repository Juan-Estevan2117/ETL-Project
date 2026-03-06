import os
from dotenv import load_dotenv
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

load_dotenv()

def main():
    DB_URL = os.getenv("DB_URL")
    CSV_FILE = "data/raw/educacionCol.csv" 

    try:
        # 1. Extraer
        df = extract_data(CSV_FILE)
        
        # 2. Transformar 
        dict_tablas = transform_data(df)
        
        # 3. Cargar
        load_data(dict_tablas, DB_URL)
        
        print("\n¡ETL finalizado con éxito!")
    except Exception as e:
        print(f"Error en el proceso: {e}")

if __name__ == "__main__":
    main()