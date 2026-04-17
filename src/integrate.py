import pandas as pd

def integrate_sources(primary_agg: pd.DataFrame, icetex_agg: pd.DataFrame) -> pd.DataFrame:
    """
    Integra los dos datasets agregados (SNIES y ICETEX) en una única tabla.

    Realiza un FULL OUTER JOIN para asegurar que no se pierda información de ninguna
    de las dos fuentes, incluso si no hay una contraparte en la otra.

    Args:
        primary_agg (pd.DataFrame): DataFrame del SNIES, agregado al grano común.
        icetex_agg (pd.DataFrame): DataFrame de ICETEX, agregado al grano común.

    Returns:
        pd.DataFrame: Un único DataFrame con los datos integrados.
    """
    print("🤝 Integrando las dos fuentes de datos (SNIES e ICETEX)...")

    # Las 7 llaves de negocio que definen el grano del nuevo Data Warehouse
    join_keys = ['anio', 'semestre', 'departamento', 'nivel_formacion', 'sector_ies', 'id_genero', 'estrato']

    # Realizar la unión FULL OUTER
    df_integrated = pd.merge(
        primary_agg,
        icetex_agg,
        on=join_keys,
        how='outer'
    )

    # Rellenar con 0 las métricas donde no hubo correspondencia en el join
    # - Si una fila de SNIES no tiene contraparte en ICETEX, 'nuevos_beneficiarios_credito' será NaN.
    # - Si una fila de ICETEX no tiene contraparte en SNIES, 'total_matriculados' será NaN.
    df_integrated['total_matriculados'] = df_integrated['total_matriculados'].fillna(0)
    df_integrated['nuevos_beneficiarios_credito'] = df_integrated['nuevos_beneficiarios_credito'].fillna(0)

    # Asegurar que las métricas sean de tipo entero
    df_integrated['total_matriculados'] = df_integrated['total_matriculados'].astype(int)
    df_integrated['nuevos_beneficiarios_credito'] = df_integrated['nuevos_beneficiarios_credito'].astype(int)

    print(f"✅ Integración completada. Filas totales en el dataset combinado: {df_integrated.shape[0]}")
    
    # Validar que no haya nulos en las llaves después del merge, lo que indicaría un problema
    null_keys = df_integrated[join_keys].isnull().sum().sum()
    if null_keys > 0:
        print(f"⚠️ Advertencia: Se encontraron {null_keys} valores nulos en las columnas llave después de la integración.")

    return df_integrated
