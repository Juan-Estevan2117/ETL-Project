-- ==============================================================================
-- CONSULTAS PARA BUSINESS INTELLIGENCE (BI) Y DASHBOARDS (2da Entrega)
-- Escenario: Análisis de Matrículas (SNIES) y Créditos Educativos (ICETEX)
-- ==============================================================================

USE dw_matriculas_col;

-- ==============================================================================
-- SECCIÓN 1: NUEVAS CONSULTAS DE INTEGRACIÓN (SNIES + ICETEX)
-- Estas consultas solo son posibles gracias a la integración de ambas fuentes.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1.1. KPI: Tasa de Cobertura de Crédito por Departamento
-- Propósito: Identificar "desiertos de financiación" donde hay muchos matriculados
--            pero pocos beneficiarios de crédito ICETEX.
-- Visualización Sugerida: Mapa de Calor (Choropleth Map) con doble métrica.
-- ------------------------------------------------------------------------------
SELECT
    UPPER(du.departamento) AS departamento,
    SUM(fe.total_matriculados) AS total_matriculados,
    SUM(fe.nuevos_beneficiarios_credito) AS total_beneficiarios_credito,
    -- Tasa de cobertura: (Beneficiarios / Matriculados) * 100
    -- Se usa NULLIF para evitar división por cero en departamentos sin matriculados.
    ROUND(
        (SUM(fe.nuevos_beneficiarios_credito) / NULLIF(SUM(fe.total_matriculados), 0)) * 100,
        2
    ) AS tasa_cobertura_credito_pct
FROM fact_educacion_superior fe
JOIN dim_ubicacion du ON fe.sk_ubicacion = du.sk_ubicacion
GROUP BY du.departamento
ORDER BY tasa_cobertura_credito_pct DESC;


-- ------------------------------------------------------------------------------
-- 1.2. ANÁLISIS: Brecha de Acceso a Crédito por Estrato Socioeconómico
-- Propósito: Medir la equidad del sistema. ¿A qué estratos llega más la financiación?
-- Visualización Sugerida: Gráfico de Barras con dos ejes (Volumen y Tasa).
-- ------------------------------------------------------------------------------
SELECT
    de.descripcion_estrato,
    SUM(fe.total_matriculados) AS total_matriculados,
    SUM(fe.nuevos_beneficiarios_credito) AS total_beneficiarios_credito,
    ROUND(
        (SUM(fe.nuevos_beneficiarios_credito) / NULLIF(SUM(fe.total_matriculados), 0)) * 100,
        2
    ) AS tasa_cobertura_por_estrato_pct
FROM fact_educacion_superior fe
JOIN dim_estrato de ON fe.sk_estrato = de.sk_estrato
-- Excluimos el estrato 'Desconocido' (0) que solo contiene datos de matriculados
-- y distorsionaría el análisis de cobertura de crédito.
WHERE de.estrato > 0
GROUP BY de.descripcion_estrato, de.estrato
ORDER BY de.estrato ASC;


-- ------------------------------------------------------------------------------
-- 1.3. TENDENCIA: Preferencia de Crédito por Sector de IES (Público vs Privado)
-- Propósito: Analizar si los créditos se dirigen más a IES públicas o privadas.
-- Visualización Sugerida: Gráfico de Líneas o Áreas 100% Apiladas.
-- ------------------------------------------------------------------------------
SELECT
    dt.anio,
    dsi.sector_ies,
    SUM(fe.total_matriculados) AS total_matriculados,
    SUM(fe.nuevos_beneficiarios_credito) AS total_beneficiarios_credito
FROM fact_educacion_superior fe
JOIN dim_tiempo dt ON fe.sk_tiempo = dt.sk_tiempo
JOIN dim_sector_ies dsi ON fe.sk_sector_ies = dsi.sk_sector_ies
WHERE dsi.sector_ies != 'Desconocido' -- Excluimos sectores no identificados
GROUP BY dt.anio, dsi.sector_ies
ORDER BY dt.anio, dsi.sector_ies;


-- ==============================================================================
-- SECCIÓN 2: CONSULTAS ADAPTADAS (Legado de la 1ra Entrega)
-- Consultas originales, ahora adaptadas para usar la nueva VISTA AUXILIAR
-- `vw_matriculas_detalle`, que mantiene la granularidad fina.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 2.1. KPI: Tasa de Paridad de Género por Área de Conocimiento (Foco en STEM)
-- Adaptada para usar la vista `vw_matriculas_detalle`.
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
-- 2.2. ANÁLISIS: Tipos de Formación en la "Periferia" vs "Capitales"
-- Adaptada para usar la vista `vw_matriculas_detalle`.
-- ------------------------------------------------------------------------------
SELECT
    CASE
        WHEN v.departamento IN ('bogota', 'antioquia', 'valle del cauca', 'atlantico', 'santander') THEN 'grandes ejes'
        ELSE 'Otras Regiones'
    END AS zona_geografica,
    UPPER(v.nivel_formacion) AS nivel_de_formacion,
    SUM(v.total_matriculados) AS volumen_matriculas
FROM vw_matriculas_detalle v
GROUP BY zona_geografica, v.nivel_formacion
ORDER BY zona_geografica, volumen_matriculas DESC;


-- ------------------------------------------------------------------------------
-- 2.3. TOP 10: Instituciones Públicas con Mayor Impacto en Nivel Universitario
-- Adaptada para usar la vista `vw_matriculas_detalle`.
-- Nota: los valores de nivel_formacion y sector están en minúsculas (convención del pipeline).
-- ------------------------------------------------------------------------------
SELECT
    UPPER(v.nombre_ies) AS nombre_institucion,
    UPPER(v.departamento) AS departamento_principal,
    SUM(v.total_matriculados) AS total_matriculas_universitarias
FROM vw_matriculas_detalle v
WHERE v.sector = 'oficial'
  AND v.nivel_formacion = 'universitaria'
GROUP BY v.nombre_ies, v.departamento
ORDER BY total_matriculas_universitarias DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- 2.4. EVOLUCIÓN TEMPORAL: Tendencia de Matrículas por Nivel de Formación (2015-2021)
-- Propósito: Analizar cómo ha cambiado la composición de la matrícula por nivel
--            educativo a lo largo del tiempo.
-- Visualización Sugerida: Gráfico de Líneas Múltiples (una línea por nivel).
-- Fuente: fact_educacion_superior (grano agregado)
-- ------------------------------------------------------------------------------
SELECT
    dt.anio,
    dnf.nivel_formacion,
    SUM(fe.total_matriculados) AS total_matriculados
FROM fact_educacion_superior fe
JOIN dim_tiempo dt ON fe.sk_tiempo = dt.sk_tiempo
JOIN dim_nivel_formacion dnf ON fe.sk_nivel_formacion = dnf.sk_nivel_formacion
WHERE dnf.nivel_formacion != 'desconocido'
GROUP BY dt.anio, dnf.nivel_formacion
ORDER BY dt.anio, total_matriculados DESC;


-- ------------------------------------------------------------------------------
-- 2.5. DISTRIBUCIÓN GEOGRÁFICA: Top 10 Departamentos por Volumen de Matrícula
-- Propósito: Identificar las regiones con mayor concentración de estudiantes
--            y medir la brecha entre "grandes ejes" y periferia.
-- Visualización Sugerida: Gráfico de Barras Horizontales.
-- Fuente: fact_educacion_superior (grano agregado)
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
ORDER BY total_matriculados DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- 2.6. PARIDAD DE GÉNERO: Brecha por Nivel de Formación
-- Propósito: Detectar si la brecha de género varía según el nivel educativo
--            (posgrado vs pregrado) a nivel nacional.
-- Visualización Sugerida: Gráfico de Barras Agrupadas (Mujeres vs Hombres).
-- Fuente: fact_educacion_superior (grano agregado)
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
WHERE dnf.nivel_formacion != 'desconocido'
GROUP BY dnf.nivel_formacion, dnf.tipo_formacion
ORDER BY dnf.tipo_formacion, total_mujeres DESC;
