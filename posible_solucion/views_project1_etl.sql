-- SCRIPT DE VIEWS  
-- 1. TABLÓN DETALLADO
-- Base principal para el informe.
DROP VIEW IF EXISTS vw_matriculas_maestra CASCADE;
CREATE VIEW vw_matriculas_maestra AS
SELECT 
    t.anio,
    t.semestre,
    i.nombre_ies,
    i.principal_seccional,
    p.nombre_programa,
    p.nucleo_basico,
    u.departamento,
    u.municipio,
    d.descripcion_genero AS genero,
    f.total_matriculados
FROM fact_matriculas f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
JOIN dim_institucion i ON f.codigo_ies = i.codigo_ies
JOIN dim_programa p ON f.codigo_snies = p.codigo_snies
JOIN dim_ubicacion u ON f.codigo_municipio = u.codigo_municipio
JOIN dim_demografia d ON f.id_genero = d.id_genero;


-- 2. VISTA DE BRECHA DE GÉNERO 
-- Para analizar la evolución de hombres vs mujeres.
DROP VIEW IF EXISTS vw_brecha_genero CASCADE;
CREATE VIEW vw_brecha_genero AS
SELECT 
    t.anio,
    t.semestre,
    d.descripcion_genero AS genero,
    SUM(f.total_matriculados) AS total_estudiantes
FROM fact_matriculas f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
JOIN dim_demografia d ON f.id_genero = d.id_genero
GROUP BY t.anio, t.semestre, d.descripcion_genero;


-- 3. VISTA DE COBERTURA REGIONAL
-- Para mapas y análisis por departamento/municipio.
DROP VIEW IF EXISTS vw_cobertura_regional CASCADE;
CREATE VIEW vw_cobertura_regional AS
SELECT 
    t.anio,
    u.departamento,
    u.municipio,
    SUM(f.total_matriculados) AS total_estudiantes
FROM fact_matriculas f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
JOIN dim_ubicacion u ON f.codigo_municipio = u.codigo_municipio
GROUP BY t.anio, u.departamento, u.municipio;


-- 4. VISTA TOP INSTITUCIONES
-- Para rankings de universidades con mayor impacto en el país.
DROP VIEW IF EXISTS vw_top_instituciones CASCADE;
CREATE VIEW vw_top_instituciones AS
SELECT 
    t.anio,
    i.nombre_ies,
    i.principal_seccional,
    SUM(f.total_matriculados) AS total_estudiantes
FROM fact_matriculas f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
JOIN dim_institucion i ON f.codigo_ies = i.codigo_ies
GROUP BY t.anio, i.nombre_ies, i.principal_seccional;


-- 5. VISTA TENDENCIAS ACADÉMICAS (ODS 4)
-- Para analizar qué áreas del conocimiento están creciendo.
DROP VIEW IF EXISTS vw_tendencia_programas CASCADE;
CREATE VIEW vw_tendencia_programas AS
SELECT 
    t.anio,
    p.nucleo_basico,
    p.nombre_programa,
    SUM(f.total_matriculados) AS total_estudiantes
FROM fact_matriculas f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
JOIN dim_programa p ON f.codigo_snies = p.codigo_snies
GROUP BY t.anio, p.nucleo_basico, p.nombre_programa;