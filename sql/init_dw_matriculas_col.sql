SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema dw_matriculas_col
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `dw_matriculas_col` ;

-- -----------------------------------------------------
-- Schema dw_matriculas_col
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `dw_matriculas_col` DEFAULT CHARACTER SET utf8mb4 ;
USE `dw_matriculas_col` ;

-- -----------------------------------------------------
-- Table `dw_matriculas_col`.`dim_demografia`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dw_matriculas_col`.`dim_demografia` ;

CREATE TABLE IF NOT EXISTS `dw_matriculas_col`.`dim_demografia` (
  `sk_demografia` INT NOT NULL AUTO_INCREMENT,
  `id_genero` INT NULL DEFAULT NULL,
  `descripcion_genero` VARCHAR(50) NULL DEFAULT NULL,
  PRIMARY KEY (`sk_demografia`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `dw_matriculas_col`.`dim_institucion`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dw_matriculas_col`.`dim_institucion` ;

CREATE TABLE IF NOT EXISTS `dw_matriculas_col`.`dim_institucion` (
  `sk_institucion` INT NOT NULL AUTO_INCREMENT,
  `codigo_ies` INT NULL DEFAULT NULL,
  `nombre_ies` VARCHAR(255) NULL DEFAULT NULL,
  `principal_seccional` VARCHAR(100) NULL DEFAULT NULL,
  `sector` VARCHAR(100) NULL DEFAULT NULL,
  `caracter` VARCHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`sk_institucion`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `dw_matriculas_col`.`dim_programa`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dw_matriculas_col`.`dim_programa` ;

CREATE TABLE IF NOT EXISTS `dw_matriculas_col`.`dim_programa` (
  `sk_programa` INT NOT NULL AUTO_INCREMENT,
  `codigo_snies` INT NULL DEFAULT NULL,
  `nombre_programa` VARCHAR(255) NULL DEFAULT NULL,
  `nivel_formacion` VARCHAR(150) NULL DEFAULT NULL,
  `metodologia` VARCHAR(100) NULL DEFAULT NULL,
  `area_conocimiento` VARCHAR(255) NULL DEFAULT NULL,
  `nucleo_basico` VARCHAR(255) NULL DEFAULT NULL,
  PRIMARY KEY (`sk_programa`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `dw_matriculas_col`.`dim_tiempo`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dw_matriculas_col`.`dim_tiempo` ;

CREATE TABLE IF NOT EXISTS `dw_matriculas_col`.`dim_tiempo` (
  `sk_tiempo` INT NOT NULL AUTO_INCREMENT,
  `anio` INT NULL DEFAULT NULL,
  `semestre` INT NULL DEFAULT NULL,
  `periodo_academico` VARCHAR(20) NULL DEFAULT NULL,
  PRIMARY KEY (`sk_tiempo`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `dw_matriculas_col`.`dim_ubicacion`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dw_matriculas_col`.`dim_ubicacion` ;

CREATE TABLE IF NOT EXISTS `dw_matriculas_col`.`dim_ubicacion` (
  `sk_ubicacion` INT NOT NULL AUTO_INCREMENT,
  `codigo_municipio` INT NULL DEFAULT NULL,
  `municipio` VARCHAR(150) NULL DEFAULT NULL,
  `codigo_departamento` INT NULL DEFAULT NULL,
  `departamento` VARCHAR(150) NULL DEFAULT NULL,
  PRIMARY KEY (`sk_ubicacion`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `dw_matriculas_col`.`fact_matriculas`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `dw_matriculas_col`.`fact_matriculas` ;

CREATE TABLE IF NOT EXISTS `dw_matriculas_col`.`fact_matriculas` (
  `sk_fact` INT NOT NULL AUTO_INCREMENT,
  `total_matriculados` INT NULL DEFAULT NULL,
  `sk_institucion` INT NULL DEFAULT NULL,
  `sk_programa` INT NULL DEFAULT NULL,
  `sk_ubicacion` INT NULL DEFAULT NULL,
  `sk_tiempo` INT NULL DEFAULT NULL,
  `sk_demografia` INT NULL DEFAULT NULL,
  INDEX `sk_institucion` (`sk_institucion` ASC),
  INDEX `sk_programa` (`sk_programa` ASC),
  INDEX `sk_ubicacion` (`sk_ubicacion` ASC),
  INDEX `sk_tiempo` (`sk_tiempo` ASC),
  INDEX `sk_demografia` (`sk_demografia` ASC),
  PRIMARY KEY (`sk_fact`),
  CONSTRAINT `fact_matriculas_ibfk_1`
    FOREIGN KEY (`sk_institucion`)
    REFERENCES `dw_matriculas_col`.`dim_institucion` (`sk_institucion`),
  CONSTRAINT `fact_matriculas_ibfk_2`
    FOREIGN KEY (`sk_programa`)
    REFERENCES `dw_matriculas_col`.`dim_programa` (`sk_programa`),
  CONSTRAINT `fact_matriculas_ibfk_3`
    FOREIGN KEY (`sk_ubicacion`)
    REFERENCES `dw_matriculas_col`.`dim_ubicacion` (`sk_ubicacion`),
  CONSTRAINT `fact_matriculas_ibfk_4`
    FOREIGN KEY (`sk_tiempo`)
    REFERENCES `dw_matriculas_col`.`dim_tiempo` (`sk_tiempo`),
  CONSTRAINT `fact_matriculas_ibfk_5`
    FOREIGN KEY (`sk_demografia`)
    REFERENCES `dw_matriculas_col`.`dim_demografia` (`sk_demografia`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
