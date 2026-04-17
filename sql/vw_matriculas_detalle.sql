-- =====================================================================
-- Vista Auxiliar: vw_matriculas_detalle
--
-- Propósito:
-- Expone los datos del SNIES con granularidad original (IES, programa,
-- municipio) desde la tabla 'legacy_matriculas_detalle', poblada
-- automáticamente por el pipeline.
--
-- =====================================================================

USE dw_matriculas_col;

CREATE OR REPLACE VIEW `vw_matriculas_detalle` AS
SELECT
    `codigo_ies`,
    `nombre_ies`,
    `principal_seccional`,
    `sector_ies`,
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
