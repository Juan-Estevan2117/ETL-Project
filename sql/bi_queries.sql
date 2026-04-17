-- ==============================================================================
-- CONSULTAS PARA BUSINESS INTELLIGENCE (BI) Y DASHBOARDS (2da Entrega)
-- Escenario: Analisis de Matriculas (SNIES) y Creditos Educativos (ICETEX)
--
-- Convenciones:
--   - nivel_formacion y sector_ies almacenados en minusculas.
--   - estrato 0 = 'Desconocido' (imputado para datos de SNIES).
--   - La fact table tiene filas solo-SNIES (beneficiarios=0) y solo-ICETEX
--     (matriculados=0); las metricas se agregan con SUM para cruzar fuentes.
-- ==============================================================================

USE dw_matriculas_col;

-- ==============================================================================
-- SECCION 1: CONSULTAS DE INTEGRACION (SNIES + ICETEX)
-- Solo son posibles gracias a la integracion de ambas fuentes.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1.1. Tasa de Cobertura de Credito por Departamento
-- Identifica "desiertos de financiacion": alta matricula, baja cobertura.
-- Visualizacion: Mapa coropletico con doble metrica.
-- ------------------------------------------------------------------------------
SELECT
    UPPER(du.departamento) AS departamento,
    SUM(fe.total_matriculados) AS total_matriculados,
    SUM(fe.nuevos_beneficiarios_credito) AS total_beneficiarios_credito,
    ROUND(
        (SUM(fe.nuevos_beneficiarios_credito) / NULLIF(SUM(fe.total_matriculados), 0)) * 100,
        2
    ) AS tasa_cobertura_credito_pct
FROM fact_educacion_superior fe
JOIN dim_ubicacion du ON fe.sk_ubicacion = du.sk_ubicacion
GROUP BY du.departamento
ORDER BY tasa_cobertura_credito_pct DESC;


-- ------------------------------------------------------------------------------
-- 1.2. Distribucion de Beneficiarios de Credito por Estrato Socioeconomico
-- Mide la equidad: a que estratos llega mas la financiacion de ICETEX?
-- Nota: SNIES no reporta estrato (imputado como 0), por lo que no es posible
-- calcular "tasa de cobertura" por estrato. En su lugar se analiza:
--   (a) volumen absoluto de beneficiarios por estrato
--   (b) porcentaje del total nacional que recibe cada estrato
-- Visualizacion: Barras con doble eje (volumen absoluto y % del total).
-- ------------------------------------------------------------------------------
SELECT
    de.descripcion_estrato,
    SUM(fe.nuevos_beneficiarios_credito) AS total_beneficiarios_credito,
    ROUND(
        SUM(fe.nuevos_beneficiarios_credito) * 100.0 /
        (SELECT SUM(nuevos_beneficiarios_credito)
           FROM fact_educacion_superior fe2
           JOIN dim_estrato de2 ON fe2.sk_estrato = de2.sk_estrato
          WHERE de2.estrato > 0),
        2
    ) AS porcentaje_del_total_pct
FROM fact_educacion_superior fe
JOIN dim_estrato de ON fe.sk_estrato = de.sk_estrato
WHERE de.estrato > 0
GROUP BY de.descripcion_estrato, de.estrato
ORDER BY de.estrato ASC;


-- ------------------------------------------------------------------------------
-- 1.3. Tendencia: Creditos por Sector de IES (Oficial vs Privado) por Anio
-- Analiza si los creditos se dirigen mas a IES publicas o privadas.
-- Se excluye sector 'desconocido'.
-- Visualizacion: Lineas o areas apiladas.
-- ------------------------------------------------------------------------------
SELECT
    dt.anio,
    dsi.sector_ies,
    SUM(fe.total_matriculados) AS total_matriculados,
    SUM(fe.nuevos_beneficiarios_credito) AS total_beneficiarios_credito
FROM fact_educacion_superior fe
JOIN dim_tiempo dt ON fe.sk_tiempo = dt.sk_tiempo
JOIN dim_sector_ies dsi ON fe.sk_sector_ies = dsi.sk_sector_ies
WHERE dsi.sector_ies != 'desconocido'
GROUP BY dt.anio, dsi.sector_ies
ORDER BY dt.anio, dsi.sector_ies;


-- ==============================================================================
-- SECCION 2: CONSULTAS SOBRE EL FACT TABLE AGREGADO
-- Analisis que aprovechan el grano del nuevo modelo dimensional.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 2.1. Evolucion Temporal de Matriculas por Nivel de Formacion
-- Visualizacion: Lineas multiples (una por nivel).
-- ------------------------------------------------------------------------------
SELECT
    dt.anio,
    dnf.nivel_formacion,
    dnf.tipo_formacion,
    SUM(fe.total_matriculados) AS total_matriculados
FROM fact_educacion_superior fe
JOIN dim_tiempo dt ON fe.sk_tiempo = dt.sk_tiempo
JOIN dim_nivel_formacion dnf ON fe.sk_nivel_formacion = dnf.sk_nivel_formacion
WHERE dnf.nivel_formacion != 'exterior'
GROUP BY dt.anio, dnf.nivel_formacion, dnf.tipo_formacion
ORDER BY dt.anio, total_matriculados DESC;


-- ------------------------------------------------------------------------------
-- 2.2. Top 10 Departamentos por Volumen de Matricula
-- Visualizacion: Barras horizontales con porcentaje nacional.
-- ------------------------------------------------------------------------------
SELECT
    UPPER(du.departamento) AS departamento,
    SUM(fe.total_matriculados) AS total_matriculados,
    ROUND(
        SUM(fe.total_matriculados) * 100.0 /
        (SELECT SUM(total_matriculados) FROM fact_educacion_superior),
        2
    ) AS porcentaje_nacional
FROM fact_educacion_superior fe
JOIN dim_ubicacion du ON fe.sk_ubicacion = du.sk_ubicacion
GROUP BY du.departamento
HAVING SUM(fe.total_matriculados) > 0
ORDER BY total_matriculados DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- 2.3. Brecha de Genero por Nivel de Formacion (Pregrado vs Posgrado)
-- Visualizacion: Barras agrupadas (Mujeres vs Hombres por nivel).
-- ------------------------------------------------------------------------------
SELECT
    dnf.nivel_formacion,
    dnf.tipo_formacion,
    SUM(CASE WHEN dd.id_genero = 2 THEN fe.total_matriculados ELSE 0 END) AS total_mujeres,
    SUM(CASE WHEN dd.id_genero = 1 THEN fe.total_matriculados ELSE 0 END) AS total_hombres,
    ROUND(
        SUM(CASE WHEN dd.id_genero = 2 THEN fe.total_matriculados ELSE 0 END) * 100.0 /
        NULLIF(SUM(fe.total_matriculados), 0),
        2
    ) AS porcentaje_mujeres
FROM fact_educacion_superior fe
JOIN dim_nivel_formacion dnf ON fe.sk_nivel_formacion = dnf.sk_nivel_formacion
JOIN dim_demografia dd ON fe.sk_demografia = dd.sk_demografia
WHERE dnf.nivel_formacion NOT IN ('exterior', 'desconocido')
GROUP BY dnf.nivel_formacion, dnf.tipo_formacion
ORDER BY dnf.tipo_formacion, total_mujeres DESC;


-- ==============================================================================
-- SECCION 3: CONSULTAS SOBRE LA VISTA AUXILIAR (GRANULARIDAD FINA)
-- Requieren que la vista vw_matriculas_detalle haya sido creada ejecutando
-- sql/vw_matriculas_detalle.sql manualmente.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 3.1. Paridad de Genero por Nucleo Basico de Conocimiento (Top 20)
-- Visualizacion: Barras divergentes.
-- ------------------------------------------------------------------------------
SELECT
    UPPER(v.nucleo_basico) AS campo_de_estudio,
    SUM(CASE WHEN v.id_genero = 2 THEN v.total_matriculados ELSE 0 END) AS total_mujeres,
    SUM(CASE WHEN v.id_genero = 1 THEN v.total_matriculados ELSE 0 END) AS total_hombres,
    ROUND(
        (SUM(CASE WHEN v.id_genero = 2 THEN v.total_matriculados ELSE 0 END) /
         NULLIF(SUM(CASE WHEN v.id_genero = 1 THEN v.total_matriculados ELSE 0 END), 0)) * 100,
        2
    ) AS indice_paridad
FROM vw_matriculas_detalle v
WHERE v.nucleo_basico IS NOT NULL
GROUP BY v.nucleo_basico
ORDER BY (total_mujeres + total_hombres) DESC
LIMIT 20;


-- ------------------------------------------------------------------------------
-- 3.2. Tipos de Formacion: Grandes Ejes vs Otras Regiones
-- Visualizacion: Barras apiladas por zona.
-- ------------------------------------------------------------------------------
SELECT
    CASE
        WHEN v.departamento IN ('bogota', 'antioquia', 'valle del cauca', 'atlantico', 'santander')
        THEN 'Grandes Ejes'
        ELSE 'Otras Regiones'
    END AS zona_geografica,
    UPPER(v.nivel_formacion) AS nivel_de_formacion,
    SUM(v.total_matriculados) AS volumen_matriculas
FROM vw_matriculas_detalle v
GROUP BY zona_geografica, v.nivel_formacion
ORDER BY zona_geografica, volumen_matriculas DESC;


-- ------------------------------------------------------------------------------
-- 3.3. Top 10 Instituciones Publicas por Matricula Universitaria
-- Visualizacion: Tabla / Barras.
-- ------------------------------------------------------------------------------
SELECT
    UPPER(v.nombre_ies) AS nombre_institucion,
    UPPER(v.departamento) AS departamento_principal,
    SUM(v.total_matriculados) AS total_matriculas_universitarias
FROM vw_matriculas_detalle v
WHERE v.sector_ies = 'oficial'
  AND v.nivel_formacion = 'universitaria'
GROUP BY v.nombre_ies, v.departamento
ORDER BY total_matriculas_universitarias DESC
LIMIT 10;
