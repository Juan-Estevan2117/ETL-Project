-- =====================================================================
-- Vista Auxiliar: vw_matriculas_detalle
--
-- Propósito:
-- Esta vista recrea la granularidad fina del dataset primario (SNIES),
-- permitiendo análisis a nivel de Institución, Programa y Municipio.
--
-- El pipeline ETL se encarga de poblar automáticamente la tabla subyacente
-- 'legacy_matriculas_detalle', por lo que esta vista siempre reflejará
-- los datos de la última ejecución del pipeline.
--
-- Ejecución:
-- Ejecuta este script una vez en tu cliente SQL para crear la vista.
-- Posteriormente, puedes consultarla directamente desde Looker Studio
-- o cualquier otra herramienta de BI.
-- =====================================================================

CREATE OR REPLACE VIEW `vw_matriculas_detalle` AS
SELECT
    `codigo_ies`,
    `nombre_ies`,
    `principal_seccional`,
    `sector_ies` AS `sector`,
    `caracter`,
    `codigo_snies`,
    `nombre_programa`,
    `nivel_formacion`,
    `metodologia`,
    `area_conocimiento`,
    `nucleo_basico`,
    `codigo_municipio`,
    `municipio`,
    `codigo_departamento`,
    `departamento`,
    `id_genero`,
    `anio`,
    `semestre`,
    `total_matriculados`
FROM
    `legacy_matriculas_detalle`;

-- Ejemplo de consulta sobre la vista:
-- SELECT anio, departamento, SUM(total_matriculados)
-- FROM vw_matriculas_detalle
-- WHERE nivel_formacion = 'Universitaria'
-- GROUP BY anio, departamento
-- ORDER BY anio, SUM(total_matriculados) DESC;
