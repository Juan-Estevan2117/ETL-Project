import sys
sys.path.insert(0, "/opt/airflow/src")

from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"


def _extract_primary():
    from extract import extract_data
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df = extract_data(str(RAW_DIR / "educacionCol.csv"))
    df.to_pickle(STAGING_DIR / "_stage_primary_raw.pkl")


def _extract_icetex():
    from extract import extract_icetex_api
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df = extract_icetex_api()
    df.to_pickle(STAGING_DIR / "_stage_icetex_raw.pkl")


def _clean_primary():
    import pandas as pd
    from transform import clean_primary
    df = pd.read_pickle(STAGING_DIR / "_stage_primary_raw.pkl")
    df_clean = clean_primary(df)
    df_clean.to_pickle(STAGING_DIR / "_stage_primary_clean.pkl")


def _aggregate_primary():
    import pandas as pd
    from transform import aggregate_primary
    df = pd.read_pickle(STAGING_DIR / "_stage_primary_clean.pkl")
    df_agg = aggregate_primary(df)
    df_agg.to_pickle(STAGING_DIR / "_stage_primary_agg.pkl")


def _clean_icetex():
    import pandas as pd
    from transform import clean_icetex
    df = pd.read_pickle(STAGING_DIR / "_stage_icetex_raw.pkl")
    df_clean = clean_icetex(df)
    df_clean.to_pickle(STAGING_DIR / "_stage_icetex_clean.pkl")


def _aggregate_icetex():
    import pandas as pd
    from transform import aggregate_icetex
    df = pd.read_pickle(STAGING_DIR / "_stage_icetex_clean.pkl")
    df_agg = aggregate_icetex(df)
    df_agg.to_pickle(STAGING_DIR / "_stage_icetex_agg.pkl")


def _integrate():
    import pandas as pd
    from integrate import integrate_sources
    df_primary = pd.read_pickle(STAGING_DIR / "_stage_primary_agg.pkl")
    df_icetex = pd.read_pickle(STAGING_DIR / "_stage_icetex_agg.pkl")
    df_integrated = integrate_sources(df_primary, df_icetex)
    df_integrated.to_pickle(STAGING_DIR / "_stage_integrated.pkl")


def _validate_gx():
    import pandas as pd
    from validate import run_validation
    df = pd.read_pickle(STAGING_DIR / "_stage_integrated.pkl")
    critical_passed, report = run_validation(df)
    if not critical_passed:
        raise RuntimeError(
            f"Validaciones criticas GX fallaron: {report['critical_failures']}"
        )
    df.to_pickle(STAGING_DIR / "_stage_validated.pkl")


def _load_dw():
    import pandas as pd
    from load import load_data
    df = pd.read_pickle(STAGING_DIR / "_stage_validated.pkl")
    load_data(df)


with DAG(
    dag_id="etl_ods4",
    description="Pipeline ETL ODS4 - Educacion Superior Colombia (SNIES + ICETEX)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "ods4", "educacion"],
) as dag:

    extract_primary = PythonOperator(task_id="extract_primary", python_callable=_extract_primary)
    extract_icetex = PythonOperator(task_id="extract_icetex", python_callable=_extract_icetex)

    clean_primary = PythonOperator(task_id="clean_primary", python_callable=_clean_primary)
    clean_icetex = PythonOperator(task_id="clean_icetex", python_callable=_clean_icetex)

    aggregate_primary = PythonOperator(task_id="aggregate_primary", python_callable=_aggregate_primary)
    aggregate_icetex = PythonOperator(task_id="aggregate_icetex", python_callable=_aggregate_icetex)

    integrate = PythonOperator(task_id="integrate", python_callable=_integrate)
    validate_gx = PythonOperator(task_id="validate_gx", python_callable=_validate_gx)
    load_dw = PythonOperator(task_id="load_dw", python_callable=_load_dw)

    extract_primary >> clean_primary >> aggregate_primary >> integrate
    extract_icetex >> clean_icetex >> aggregate_icetex >> integrate
    integrate >> validate_gx >> load_dw
