-- =====================================================================
-- Data Warehouse para el Proyecto ODS 4 (Educación Superior en Colombia)
-- Segunda Entrega: Integración de API ICETEX y automatización con Airflow.
--
-- Este script SQL define el nuevo esquema dimensional (Star Schema) que
-- integra datos de matrículas (SNIES) y créditos educativos (ICETEX).
--
-- Grano de la Fact Table:
-- Total de matriculados y beneficiarios de crédito por combinación de:
-- anio, semestre, departamento, nivel de formación, sector IES, genero y estrato.
--
-- Ejecución:
-- Este script se ejecuta automáticamente por el pipeline de ETL si la
-- base de datos no existe, o al iniciar el servicio `mysql-dw` en Docker.
-- =====================================================================

-- --- Configuración de la sesión ---
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- --- Creación del Schema (idempotente) ---
CREATE SCHEMA IF NOT EXISTS `dw_matriculas_col` DEFAULT CHARACTER SET utf8mb4;
USE `dw_matriculas_col`;

-- =====================================================================
-- TABLAS DE DIMENSIONES (DIMENSIONS)
-- =====================================================================

-- -----------------------------------------------------
-- Dimensión 1: Tiempo (Conformada, reutilizada)
-- Almacena la jerarquía temporal (Año, Semestre).
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dim_tiempo`;
CREATE TABLE `dim_tiempo` (
  `sk_tiempo` INT NOT NULL AUTO_INCREMENT,
  `anio` INT NOT NULL,
  `semestre` INT NOT NULL,
  `periodo_academico` VARCHAR(10) GENERATED ALWAYS AS (CONCAT(`anio`, '-', `semestre`)) STORED,
  PRIMARY KEY (`sk_tiempo`),
  UNIQUE KEY `uk_dim_tiempo` (`anio`, `semestre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------
-- Dimensión 2: Ubicación (Conformada, modificada)
-- Almacena la jerarquía geográfica a nivel de Departamento.
-- Se asume que el depto. de oferta (SNIES) es proxy del depto. de origen (ICETEX).
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dim_ubicacion`;
CREATE TABLE `dim_ubicacion` (
  `sk_ubicacion` INT NOT NULL AUTO_INCREMENT,
  `codigo_departamento` INT, -- Puede ser nulo si no hay código oficial
  `departamento` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`sk_ubicacion`),
  UNIQUE KEY `uk_dim_ubicacion` (`departamento`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------
-- Dimensión 3: Demografía (Conformada, reutilizada)
-- Almacena la variable demográfica de género.
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dim_demografia`;
CREATE TABLE `dim_demografia` (
  `sk_demografia` INT NOT NULL AUTO_INCREMENT,
  `id_genero` INT NOT NULL,
  `descripcion_genero` VARCHAR(20) NULL,
  PRIMARY KEY (`sk_demografia`),
  UNIQUE KEY `uk_dim_demografia` (`id_genero`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------
-- Dimensión 4: Nivel de Formación (Conformada, nueva)
-- Abstrae el nivel educativo de los programas académicos.
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dim_nivel_formacion`;
CREATE TABLE `dim_nivel_formacion` (
  `sk_nivel_formacion` INT NOT NULL AUTO_INCREMENT,
  `nivel_formacion` VARCHAR(60) NOT NULL,
  `tipo_formacion` VARCHAR(20) NULL, -- 'Pregrado', 'Posgrado'
  PRIMARY KEY (`sk_nivel_formacion`),
  UNIQUE KEY `uk_dim_nivel_formacion` (`nivel_formacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------
-- Dimensión 5: Sector IES (Conformada, nueva)
-- Abstrae el sector de la Institución de Educación Superior (IES).
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dim_sector_ies`;
CREATE TABLE `dim_sector_ies` (
  `sk_sector_ies` INT NOT NULL AUTO_INCREMENT,
  `sector_ies` VARCHAR(20) NOT NULL, -- 'Oficial', 'Privado', 'Desconocido'
  PRIMARY KEY (`sk_sector_ies`),
  UNIQUE KEY `uk_dim_sector_ies` (`sector_ies`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------
-- Dimensión 6: Estrato (Conformada, nueva)
-- Dimensión nueva que proviene de la fuente de datos de ICETEX.
-- '0' representa valor desconocido (imputado para datos de SNIES).
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dim_estrato`;
CREATE TABLE `dim_estrato` (
  `sk_estrato` INT NOT NULL AUTO_INCREMENT,
  `estrato` INT NOT NULL,
  `descripcion_estrato` VARCHAR(30) NULL,
  PRIMARY KEY (`sk_estrato`),
  UNIQUE KEY `uk_dim_estrato` (`estrato`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- TABLA DE HECHOS (FACT TABLE)
-- =====================================================================

-- -----------------------------------------------------
-- Tabla de Hechos: fact_educacion_superior
-- Contiene las métricas numéricas aditivas del proceso de negocio.
-- -----------------------------------------------------
DROP TABLE IF EXISTS `fact_educacion_superior`;
CREATE TABLE `fact_educacion_superior` (
  `sk_fact` INT NOT NULL AUTO_INCREMENT,
  `sk_tiempo` INT NOT NULL,
  `sk_ubicacion` INT NOT NULL,
  `sk_demografia` INT NOT NULL,
  `sk_nivel_formacion` INT NOT NULL,
  `sk_sector_ies` INT NOT NULL,
  `sk_estrato` INT NOT NULL,
  `total_matriculados` INT NOT NULL DEFAULT 0,
  `nuevos_beneficiarios_credito` INT NOT NULL DEFAULT 0,
  PRIMARY KEY (`sk_fact`),
  UNIQUE KEY `uk_grain` (`sk_tiempo`, `sk_ubicacion`, `sk_demografia`, `sk_nivel_formacion`, `sk_sector_ies`, `sk_estrato`),
  FOREIGN KEY (`sk_tiempo`) REFERENCES `dim_tiempo`(`sk_tiempo`),
  FOREIGN KEY (`sk_ubicacion`) REFERENCES `dim_ubicacion`(`sk_ubicacion`),
  FOREIGN KEY (`sk_demografia`) REFERENCES `dim_demografia`(`sk_demografia`),
  FOREIGN KEY (`sk_nivel_formacion`) REFERENCES `dim_nivel_formacion`(`sk_nivel_formacion`),
  FOREIGN KEY (`sk_sector_ies`) REFERENCES `dim_sector_ies`(`sk_sector_ies`),
  FOREIGN KEY (`sk_estrato`) REFERENCES `dim_estrato`(`sk_estrato`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- INSERCIONES INICIALES (SEEDING)
-- =====================================================================
-- Pre-poblar la dimensión de estrato con los valores conocidos, incluyendo
-- el valor '0' para 'Desconocido'.
-- ---------------------------------------------------------------------
INSERT INTO `dim_estrato` (`estrato`, `descripcion_estrato`) VALUES
(0, 'Desconocido'),
(1, 'Estrato 1'),
(2, 'Estrato 2'),
(3, 'Estrato 3'),
(4, 'Estrato 4'),
(5, 'Estrato 5'),
(6, 'Estrato 6')
ON DUPLICATE KEY UPDATE `descripcion_estrato` = VALUES(`descripcion_estrato`);

-- --- Restauración de la configuración de la sesión ---
SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
