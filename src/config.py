import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar el .env que está en el directorio de airflow
#PROJECT_ROOT = Path(__file__).resolve().parent.parent
#load_dotenv(PROJECT_ROOT / "airflow" / ".env")

# Se define la ruta del proyecto y se carga el .env
PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / "airflow" / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# --- Rutas del sistema de archivos ---
AIRFLOW_DIR: Path = PROJECT_ROOT / "airflow"
DATA_DIR: Path = AIRFLOW_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
PROCESSED_DIR: Path = DATA_DIR / "processed"
STAGING_DIR: Path = DATA_DIR / "staging" # Directorio para archivos intermedios (pickles)

# Asegurarse de que los directorios existan
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
STAGING_DIR.mkdir(parents=True, exist_ok=True)


# --- Archivos de datos ---
PRIMARY_CSV: Path = RAW_DIR / "educacionCol.csv"


# --- Configuración de la Base de Datos ---
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DW_DB = os.getenv("MYSQL_DW_DB")

# Construir la URL de conexión para SQLAlchemy
MYSQL_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DW_DB}"


# --- Configuración de la API de Socrata (Datos.gov.co) ---
SOCRATA_ENDPOINT = "https://www.datos.gov.co/resource/26bn-e42j.json"
# Token opcional para evitar throttling (límites de tasa de peticiones)
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)


# Validación rápida para asegurar que las variables esenciales están cargadas
if not all([MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DW_DB]):
    raise ValueError(
        "Una o más variables de entorno de MySQL no están definidas. "
        "Asegúrate de que tu archivo 'airflow/.env' está configurado correctamente."
    )
