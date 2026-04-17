import os
import great_expectations as gx
from great_expectations.checkpoint import UpdateDataDocsAction
import pandas as pd
from pathlib import Path

# GX_PROJECT_ROOT permite apuntar el directorio contenedor de `gx/` a otra ruta.
# En Docker se redirige a /opt/airflow/data para aprovechar el volumen ya montado
# con permisos del usuario airflow (evita bind-mounts extra y lios de ownership).
PROJECT_ROOT = Path(os.environ.get("GX_PROJECT_ROOT", Path(__file__).resolve().parent.parent))


def _build_critical_expectations() -> list:
    e = gx.expectations
    return [
        e.ExpectColumnValuesToNotBeNull(column="anio"),
        e.ExpectColumnValuesToNotBeNull(column="semestre"),
        e.ExpectColumnValuesToNotBeNull(column="departamento"),
        e.ExpectColumnValuesToNotBeNull(column="nivel_formacion"),
        e.ExpectColumnValuesToNotBeNull(column="sector_ies"),
        e.ExpectColumnValuesToNotBeNull(column="id_genero"),
        e.ExpectColumnValuesToNotBeNull(column="estrato"),
        e.ExpectColumnValuesToBeBetween(column="total_matriculados", min_value=0),
        e.ExpectColumnValuesToBeBetween(column="nuevos_beneficiarios_credito", min_value=0),
        e.ExpectColumnValuesToBeInSet(column="estrato", value_set=[0, 1, 2, 3, 4, 5, 6]),
        e.ExpectColumnValuesToBeInSet(
            column="nivel_formacion",
            value_set=["tecnica profesional", "tecnologica", "universitaria",
                       "especializacion", "maestria", "doctorado", "exterior"],
        ),
        e.ExpectColumnValuesToBeInSet(column="sector_ies", value_set=["oficial", "privado", "desconocido"]),
        e.ExpectColumnValuesToBeInSet(column="id_genero", value_set=[1, 2]),
        e.ExpectColumnValuesToBeBetween(column="anio", min_value=2015, max_value=2025),
    ]


def _build_non_critical_expectations() -> list:
    e = gx.expectations
    return [
        e.ExpectTableRowCountToBeBetween(min_value=5000, max_value=150000),
        e.ExpectColumnValuesToBeInSet(column="semestre", value_set=[1, 2]),
    ]


def run_validation(df: pd.DataFrame) -> tuple:
    print("--- FASE 2.75: VALIDACION (Great Expectations) ---")

    # Asegurar que el contenedor del directorio gx/ existe; GX crea `gx/` dentro.
    PROJECT_ROOT.mkdir(parents=True, exist_ok=True)
    context = gx.get_context(mode="file", project_root_dir=str(PROJECT_ROOT))

    data_source = context.data_sources.add_or_update_pandas(name="integrated_source")
    try:
        data_asset = data_source.get_asset("integrated_df")
    except LookupError:
        data_asset = data_source.add_dataframe_asset(name="integrated_df")
    try:
        batch_def = data_asset.get_batch_definition("full_batch")
    except LookupError:
        batch_def = data_asset.add_batch_definition_whole_dataframe("full_batch")

    # --- Suite critica ---
    critical_suite = gx.ExpectationSuite(name="critical_suite")
    critical_suite.expectations = _build_critical_expectations()
    critical_suite = context.suites.add_or_update(critical_suite)

    critical_vd = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name="critical_validation",
            data=batch_def,
            suite=critical_suite,
        )
    )

    # --- Suite no-critica ---
    non_critical_suite = gx.ExpectationSuite(name="non_critical_suite")
    non_critical_suite.expectations = _build_non_critical_expectations()
    non_critical_suite = context.suites.add_or_update(non_critical_suite)

    non_critical_vd = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name="non_critical_validation",
            data=batch_def,
            suite=non_critical_suite,
        )
    )

    # --- Checkpoint con ambas validaciones ---
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name="etl_checkpoint",
            validation_definitions=[critical_vd, non_critical_vd],
            actions=[UpdateDataDocsAction(name="update_data_docs")],
        )
    )

    checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

    # --- Procesar resultados ---
    critical_failures = []
    critical_passes = 0
    non_critical_failures = []
    non_critical_passes = 0

    for vd_result in checkpoint_result.run_results.values():
        suite_name = vd_result.suite_name
        for exp_result in vd_result.results:
            exp_config = exp_result.expectation_config
            exp_type = exp_config.type
            col = exp_config.kwargs.get("column", "table")

            if suite_name == "critical_suite":
                if exp_result.success:
                    critical_passes += 1
                else:
                    critical_failures.append({"expectation": exp_type, "column": col})
                    print(f"      [FALLO] {exp_type} en '{col}'")
            else:
                if exp_result.success:
                    non_critical_passes += 1
                else:
                    non_critical_failures.append({"expectation": exp_type, "column": col})
                    print(f"      [WARN] {exp_type} en '{col}' (no critica)")

    total_critical = len(_build_critical_expectations())
    total_non_critical = len(_build_non_critical_expectations())

    print(f"   Criticas: {critical_passes}/{total_critical} pasaron.")
    print(f"   No-criticas: {non_critical_passes}/{total_non_critical} pasaron.")

    critical_passed = len(critical_failures) == 0
    report = {
        "critical_total": total_critical,
        "critical_passed": critical_passes,
        "critical_failures": critical_failures,
        "non_critical_total": total_non_critical,
        "non_critical_passed": non_critical_passes,
        "non_critical_failures": non_critical_failures,
    }

    if critical_passed:
        print("   RESULTADO: Todas las validaciones criticas PASARON. Continuando con la carga.")
    else:
        print(f"   RESULTADO: {len(critical_failures)} validacion(es) critica(s) FALLARON.")

    # Data Docs path para el usuario
    docs_path = PROJECT_ROOT / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
    print(f"   Data Docs: file://{docs_path}")
    print("--- FIN VALIDACION ---\n")
    return critical_passed, report
