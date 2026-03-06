-- ==============================================================================
-- CONSULTAS PARA BUSINESS INTELLIGENCE (BI) Y DASHBOARDS
-- Escenario de Negocio: Brechas de Acceso a la Educación Superior (ODS 4)
-- ==============================================================================
-- Estas consultas están diseñadas para conectarse directamente desde 
-- herramientas como Power BI, Tableau o Looker Studio hacia el Data Warehouse.

USE dw_matriculas_col;

-- ------------------------------------------------------------------------------
-- 1. KPI: Índice de Concentración de Matrículas (Top Departamentos vs Resto)
-- Propósito: Evidenciar la centralización del acceso a la educación.
-- Visualización Sugerida: Mapa de Calor (Choropleth Map) o Gráfico de Treemap.
-- ------------------------------------------------------------------------------
SELECT
    CASE du.departamento
        WHEN 'bogota' THEN 'Bogotá, Colombia'
        WHEN 'antioquia' THEN 'Antioquia, Colombia'
        WHEN 'valle del cauca' THEN 'Valle del Cauca, Colombia'
        WHEN 'atlantico' THEN 'Atlántico, Colombia'
        WHEN 'santander' THEN 'Santander, Colombia'
        WHEN 'bolivar' THEN 'Bolívar, Colombia'
        WHEN 'cordoba' THEN 'Córdoba, Colombia'
        WHEN 'norte de santander' THEN 'Norte de Santander, Colombia'
        WHEN 'boyaca' THEN 'Boyacá, Colombia'
        WHEN 'narino' THEN 'Nariño, Colombia'
        WHEN 'tolima' THEN 'Tolima, Colombia'
        WHEN 'magdalena' THEN 'Magdalena, Colombia'
        WHEN 'cundinamarca' THEN 'Cundinamarca, Colombia'
        WHEN 'cesar' THEN 'Cesar, Colombia'
        WHEN 'huila' THEN 'Huila, Colombia'
        WHEN 'sucre' THEN 'Sucre, Colombia'
        WHEN 'cauca' THEN 'Cauca, Colombia'
        WHEN 'meta' THEN 'Meta, Colombia'
        WHEN 'caldas' THEN 'Caldas, Colombia'
        WHEN 'risaralda' THEN 'Risaralda, Colombia'
        WHEN 'choco' THEN 'Chocó, Colombia'
        WHEN 'quindio' THEN 'Quindío, Colombia'
        WHEN 'casanare' THEN 'Casanare, Colombia'
        WHEN 'caqueta' THEN 'Caquetá, Colombia'
        WHEN 'putumayo' THEN 'Putumayo, Colombia'
        WHEN 'la guajira' THEN 'La Guajira, Colombia'
        WHEN 'arauca' THEN 'Arauca, Colombia'
        WHEN 'guaviare' THEN 'Guaviare, Colombia'
        WHEN 'amazonas' THEN 'Amazonas, Colombia'
        WHEN 'vichada' THEN 'Vichada, Colombia'
        WHEN 'guainia' THEN 'Guainía, Colombia'
        WHEN 'vaupes' THEN 'Vaupés, Colombia'
        WHEN 'san andres y providencia' THEN 'San Andrés y Providencia, Colombia'
        ELSE CONCAT(du.departamento, ', Colombia')
    END AS departamento_oficial,
    SUM(fm.total_matriculados) AS total_estudiantes,
    ROUND(SUM(fm.total_matriculados) / (SELECT SUM(total_matriculados) FROM fact_matriculas) * 100, 2) AS participacion_nacional_pct
FROM fact_matriculas fm
INNER JOIN dim_ubicacion du ON fm.sk_ubicacion = du.sk_ubicacion
GROUP BY du.departamento
ORDER BY SUM(fm.total_matriculados) DESC;


-- ------------------------------------------------------------------------------
-- 2. KPI: Tasa de Paridad de Género por Área de Conocimiento (Foco en STEM)
-- Propósito: Identificar la brecha de género (ODS 5) en carreras científicas y técnicas.
-- Visualización Sugerida: Gráfico de Barras Apiladas 100% o Gráfico de Pirámide.
-- ------------------------------------------------------------------------------
SELECT 
    UPPER(dp.nucleo_basico) AS campo_de_estudio,
    SUM(CASE WHEN dd.descripcion_genero = 'Femenino' THEN fm.total_matriculados ELSE 0 END) AS total_mujeres,
    SUM(CASE WHEN dd.descripcion_genero = 'Masculino' THEN fm.total_matriculados ELSE 0 END) AS total_hombres,
    ROUND(
        SUM(CASE WHEN dd.descripcion_genero = 'Femenino' THEN fm.total_matriculados ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN dd.descripcion_genero = 'Masculino' THEN fm.total_matriculados ELSE 0 END), 0) * 100
    , 2) AS indice_paridad
FROM fact_matriculas fm
INNER JOIN dim_programa dp ON fm.sk_programa = dp.sk_programa
INNER JOIN dim_demografia dd ON fm.sk_demografia = dd.sk_demografia
GROUP BY dp.nucleo_basico
ORDER BY SUM(fm.total_matriculados) DESC
LIMIT 20;


-- ------------------------------------------------------------------------------
-- 3. TENDENCIA: Crecimiento Histórico de Matrículas por Sector (Público vs Privado)
-- Propósito: Monitorear si el estado está asumiendo la carga de la expansión educativa.
-- Visualización Sugerida: Gráfico de Líneas con dos series (Público y Privado).
-- ------------------------------------------------------------------------------
SELECT 
    -- Convertimos el año entero (ej. 2015) en una fecha válida (2015-01-01) 
    -- para que Looker Studio la reconozca como Dimensión de Tiempo obligatoria.
    STR_TO_DATE(CONCAT(dt.anio, '-01-01'), '%Y-%m-%d') AS fecha_anio,
    UPPER(di.sector) AS sector,
    SUM(fm.total_matriculados) AS total_matriculados
FROM fact_matriculas fm
INNER JOIN dim_tiempo dt ON fm.sk_tiempo = dt.sk_tiempo
GROUP BY dt.anio, di.sector
ORDER BY dt.anio ASC, di.sector ASC;


-- ------------------------------------------------------------------------------
-- 4. ANÁLISIS: Tipos de Formación en la "Periferia" vs "Capitales"
-- Propósito: Descubrir si a las regiones alejadas solo llegan carreras técnicas
--            mientras que las universitarias se quedan en las capitales.
-- Visualización Sugerida: Gráfico de Columnas Agrupadas.
-- ------------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN du.departamento IN ('bogota', 'antioquia', 'valle del cauca', 'atlantico') THEN 'Grandes Ejes'
        ELSE 'Regiones Periféricas' 
    END AS zona_geografica,
    UPPER(dp.nivel_formacion) AS nivel_de_formacion,
    SUM(fm.total_matriculados) AS volumen_matriculas
FROM fact_matriculas fm
INNER JOIN dim_ubicacion du ON fm.sk_ubicacion = du.sk_ubicacion
INNER JOIN dim_programa dp ON fm.sk_programa = dp.sk_programa
GROUP BY 
    CASE 
        WHEN du.departamento IN ('bogota', 'antioquia', 'valle del cauca', 'atlantico') THEN 'Grandes Ejes'
        ELSE 'Regiones Periféricas' 
    END, 
    dp.nivel_formacion
ORDER BY zona_geografica ASC, SUM(fm.total_matriculados) DESC;


-- ------------------------------------------------------------------------------
-- 5. IMPACTO: Expansión de la Metodología Virtual y a Distancia a través de los años
-- Propósito: Evaluar si la virtualidad está siendo el motor para democratizar el acceso.
-- Visualización Sugerida: Gráfico de Áreas Apiladas (Stacked Area Chart).
-- ------------------------------------------------------------------------------
SELECT 
    -- Casteo a fecha oficial para gráficos de líneas de tiempo
    STR_TO_DATE(CONCAT(dt.anio, '-01-01'), '%Y-%m-%d') AS fecha_anio,
    UPPER(dp.metodologia) AS metodologia,
    SUM(fm.total_matriculados) AS total_estudiantes
FROM fact_matriculas fm
INNER JOIN dim_tiempo dt ON fm.sk_tiempo = dt.sk_tiempo
INNER JOIN dim_programa dp ON fm.sk_programa = dp.sk_programa
GROUP BY dt.anio, dp.metodologia
ORDER BY dt.anio ASC, SUM(fm.total_matriculados) DESC;


-- ------------------------------------------------------------------------------
-- 6. TOP 10: Instituciones Públicas con Mayor Impacto en el Nivel Universitario
-- Propósito: Identificar las universidades públicas que más sostienen la demanda 
--            del país para posible asignación de presupuestos.
-- Visualización Sugerida: Tabla Dinámica o Gráfico de Barras Horizontales.
-- ------------------------------------------------------------------------------
SELECT 
    UPPER(di.nombre_ies) AS nombre_institucion,
    UPPER(du.departamento) AS departamento_principal,
    SUM(fm.total_matriculados) AS total_matriculas_universitarias
FROM fact_matriculas fm
INNER JOIN dim_institucion di ON fm.sk_institucion = di.sk_institucion
INNER JOIN dim_programa dp ON fm.sk_programa = dp.sk_programa
INNER JOIN dim_ubicacion du ON fm.sk_ubicacion = du.sk_ubicacion
WHERE di.sector = 'oficial' -- Instituciones Públicas
    AND dp.nivel_formacion LIKE '%universitaria%' -- Solo programas profesionales/universitarios
GROUP BY di.nombre_ies, du.departamento
ORDER BY SUM(fm.total_matriculados) DESC
LIMIT 10;
