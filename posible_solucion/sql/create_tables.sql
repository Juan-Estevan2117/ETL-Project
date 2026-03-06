-- ==========================================
-- 1. DDL
-- ==========================================
DROP TABLE IF EXISTS fact_matriculas CASCADE;
DROP TABLE IF EXISTS dim_institucion CASCADE;
DROP TABLE IF EXISTS dim_programa CASCADE;
DROP TABLE IF EXISTS dim_ubicacion CASCADE;
DROP TABLE IF EXISTS dim_tiempo CASCADE;
DROP TABLE IF EXISTS dim_demografia CASCADE;

CREATE TABLE dim_institucion (
    codigo_ies INT PRIMARY KEY,
    nombre_ies VARCHAR,
    principal_seccional VARCHAR,
    sector INT,
    caracter INT
);

CREATE TABLE dim_programa (
    codigo_snies INT PRIMARY KEY,
    nombre_programa VARCHAR,
    id_nivel_formacion INT,
    id_metodologia INT,
    id_area INT,
    nucleo_basico VARCHAR
);

CREATE TABLE dim_ubicacion (
    codigo_municipio INT PRIMARY KEY,
    municipio VARCHAR,
    codigo_departamento INT,
    departamento VARCHAR
);

CREATE TABLE dim_tiempo (
    sk_tiempo INT PRIMARY KEY,         -- Nuestra Surrogate Key (1, 2, 3...)
    id_tiempo_natural VARCHAR,         -- La llave natural ('2021-1', '2021-2'...)
    anio INT,
    semestre INT
);

CREATE TABLE dim_demografia (
    id_genero INT PRIMARY KEY,
    descripcion_genero VARCHAR
);

CREATE TABLE fact_matriculas (
    id_hecho INT PRIMARY KEY,                                -- Surrogate Key de Hechos
    codigo_ies INT REFERENCES dim_institucion(codigo_ies),   -- Foreign Key
    codigo_snies INT REFERENCES dim_programa(codigo_snies),  -- Foreign Key
    codigo_municipio INT REFERENCES dim_ubicacion(codigo_municipio), -- Foreign Key
    sk_tiempo INT REFERENCES dim_tiempo(sk_tiempo),          -- Foreign Key (Apunta a la Surrogate Key)
    id_genero INT REFERENCES dim_demografia(id_genero),      -- Foreign Key
    total_matriculados INT
);