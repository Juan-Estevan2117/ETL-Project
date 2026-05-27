# Observatorio de Acceso y Financiación a la Educación Superior (ODS 4)
**Proyecto ETL — Entrega Final | Ingeniería de Datos e Inteligencia Artificial**

> La entrega final integra todo lo construido en las dos primeras entregas (pipeline batch
> con Airflow, Data Warehouse dimensional en MySQL, validación con Great Expectations y
> dashboard estático en Looker Studio) y **añade un componente de streaming con Apache Kafka**
> que publica métricas derivadas de la *fact table* y las monitorea/persiste en tiempo real.

---

## Tabla de Contenido

1. [Objetivos de Negocio Refinados](#1-objetivos-de-negocio-refinados)
2. [Fuentes de Datos](#2-fuentes-de-datos)
3. [Resumen de Perfilado de Datos](#3-resumen-de-perfilado-de-datos)
4. [Modelo Dimensional](#4-modelo-dimensional-star-schema)
5. [Estrategia de Integración y Supuestos](#5-estrategia-de-integración-y-supuestos)
6. [Explicación del Pipeline ETL](#6-explicación-del-pipeline-etl)
7. [Estrategia de Validación (Great Expectations)](#7-estrategia-de-validación-great-expectations)
8. [Consultas BI y Dashboard](#8-consultas-bi-y-dashboard)
9. [Estructura del Proyecto](#9-estructura-del-proyecto)
10. [Instrucciones de Ejecución](#10-instrucciones-de-ejecución-local)
11. [Diseño del DAG de Airflow](#11-diseño-del-dag-de-airflow)
12. [Componente de Streaming con Apache Kafka](#12-componente-de-streaming-con-apache-kafka)
13. [Monitoreo en Tiempo Real e Interpretación](#13-monitoreo-en-tiempo-real-e-interpretación)
14. [Business Objectives Achievement](#14-business-objectives-achievement)
15. [Supuestos, Limitaciones y Mejoras Futuras](#15-supuestos-limitaciones-y-mejoras-futuras)

---

## 1. Objetivos de Negocio Refinados

La primera entrega construyó un Data Warehouse sobre las **matrículas** de educación superior (dataset SNIES del MEN), permitiendo analizar la distribución de la oferta educativa a nivel departamental, institucional y por programa.

En esta segunda entrega el objetivo evoluciona hacia una visión más profunda del **acceso efectivo a la educación superior**, integrando los **créditos educativos otorgados por el ICETEX**.

> "Analizar la cobertura de la financiación estatal (ICETEX) y su correlación con la oferta educativa (SNIES), identificando dónde están los estudiantes, quiénes pueden financiar sus estudios, en qué tipo de institución y desde qué estrato socioeconómico. Esto permite medir la equidad del sistema y la efectividad de las políticas de crédito respecto al ODS 4."

Preguntas analíticas que habilita la integración:

- ¿Cuál es la tasa de cobertura de crédito por departamento? ¿Existen "desiertos de financiación"?
- ¿La financiación del ICETEX llega equitativamente a todos los estratos socioeconómicos?
- ¿Hay preferencia de créditos hacia el sector público o privado? ¿Ha cambiado en el tiempo?
- ¿Qué brecha de género existe en el acceso al crédito según el nivel de formación?

---

## 2. Fuentes de Datos

### 2.1. Fuente Primaria — Matrículas SNIES (CSV)

| Atributo | Valor |
|---|---|
| Origen | `datos.gov.co` — Estadísticas de Matrículas en Educación Superior |
| Ruta local | `airflow/data/raw/educacionCol.csv` |
| Volumen | ~390,000 registros (2015–2021) |
| Granularidad original | IES × Programa × Municipio × Año × Semestre × Género |
| Métricas aportadas | `total_matriculados` |
| Dimensiones aportadas | Institución, Programa, Municipio, Departamento, Nivel, Sector, Género |

### 2.2. Fuente Secundaria — Créditos ICETEX (API Socrata)

| Atributo | Valor |
|---|---|
| Origen | Portal `datos.gov.co` — API Socrata |
| Endpoint | `https://www.datos.gov.co/resource/26bn-e42j.json` |
| Autenticación | Header `X-App-Token` (opcional, aumenta rate limit) |
| Paginación | `$limit` / `$offset` (páginas de 50,000 registros) |
| Volumen | ~107,000 registros (2015–2025) |
| Granularidad original | Año × Semestre × Departamento de origen × Nivel × Sector IES × Género × Estrato × Modalidad |
| Métricas aportadas | `nuevos_beneficiarios_credito` |
| Dimensión exclusiva | **Estrato socioeconómico** (no disponible en SNIES) |

**Justificación de elección:** El estrato socioeconómico es la variable de equidad clave para el ODS 4. Ninguna otra fuente pública disponible en `datos.gov.co` combina estrato + nivel de formación + departamento + género con cobertura nacional, lo que hace a ICETEX la única opción que enriquece cualitativamente el modelo.

---

## 3. Resumen de Perfilado de Datos

### 3.1. Dataset Primario (SNIES)

Realizado en la primera entrega. Hallazgos principales:

- **Nulos**: columna `Total Matriculados` presenta ~0.3% de nulos; se imputan con 0 y se filtran registros con matrícula ≤ 0.
- **Duplicados**: presencia de duplicados exactos (~2%); eliminados con `drop_duplicates()`.
- **Tipos**: `Id_Nivel_Formacion` e `Id_Sector` son enteros numéricos que requieren mapeo a etiquetas de texto.
- **Inconsistencias geográficas**: múltiples variantes del mismo departamento (`bogota dc`, `bogota, d.c.`, `narinio` por `narino`, etc.); corregidas con diccionario de homologación.
- **Niveles de formación**: IDs 1–10, donde 4/7/8/10 corresponden a variantes de Especialización.

### 3.2. Dataset API (ICETEX)

Realizado en la Fase 0 (notebook `notebooks/eda.ipynb`). Hallazgos que impactan el pipeline:

| Problema identificado | Impacto | Solución en `clean_icetex` |
|---|---|---|
| `VIGENCIA` como string limpio (sin coma de miles) | Casteo directo a int | `pd.to_numeric()` |
| `PERIODO OTORGAMIENTO` formato `"YYYY-[1\|2]"` | Extraer semestre | `str.split('-').str[1]` |
| `SEXO AL NACER` incluye `Intersexual` (14 filas, 0.01%) | Descartar para FK válida | Filtro previo al mapeo |
| `Bogotá, D.C.` con coma y mayúsculas en depto | Desalineación con SNIES | `clean_text()` + reemplazo `'bogota, d.c.' → 'bogota'` |
| `Archipiélago de San Andrés...` nombre completo | Mismo problema | Reemplazo a `'san andres y providencia'` |
| 11 valores distintos de nivel de formación | No coinciden con los 6 del primario | Diccionario de 11→7 canónicos |
| `NIVEL DE FORMACIÓN = 'Exterior'` (9,154 filas, 8.6%) | Nivel sin equivalente en SNIES | Se conserva como valor `'exterior'`; filas con solo beneficiarios |
| `NIVEL = 'Normalista'` (7 filas) | Fuera del alcance del DW | Descartado |
| `SECTOR IES = 'N/A'` (9,184 filas, 8.5%) | FK no resolvible | Mapeado a `'desconocido'` |
| `ESTRATO ∈ {1..6}` sin outliers, sin nulos | Puede integrarse directamente | Cast a int, sin imputación |
| Rango de años: 2015–2025 | 4 años extra vs primario (2015–2021) | Expectation GX ajustada a 2015–2025 |

#### Re-perfilado en la entrega final (refresh de la API ICETEX)

Al re-extraer la API para la entrega final, el dataset trae más vigencias y aparecen **variantes de `nivel_formacion` que el mapeo original (basado en claves exactas con tildes) no cubría**, lo que hacía fallar la expectativa crítica `ExpectColumnValuesToBeInSet` sobre `nivel_formacion` y abortaba el pipeline. Hallazgos y solución:

| Valor nuevo encontrado | Causa | Impacto | Solución en `clean_icetex` |
|---|---|---|---|
| `Especialización médico quirurgica` (~2,063 filas) | La API escribe `quirurgica` **sin tilde**; la clave del mapa la tenía con tilde → no coincidía | Quedaba sin homologar → fuera del set canónico | Se **normaliza con `clean_text` antes de mapear** (las tildes dejan de importar) → `especializacion` |
| `Formació técnica profesional` (1 fila) | Nombre **truncado** por la API (falta la `n` de "Formación") | No coincidía con la clave exacta | Clave explícita para la variante truncada → `tecnica profesional` |
| `Educación continuada` (~143 filas) | Modalidad de formación continua, no es un nivel académico del DW | Sin equivalente canónico | **Descartado** (mismo criterio que `Normalista`); se reporta el conteo en logs |

**Cambio de diseño aplicado:** el mapeo de `nivel_formacion` en `clean_icetex` pasó de `.replace()` con claves exactas a `clean_text()` + `.map()` con **claves normalizadas** (minúsculas, sin tildes ni puntuación). Las filas cuyo nivel no tiene equivalente canónico se descartan explícitamente. Esto hace la homologación **robusta ante refreshes futuros de la API** (variaciones de tildes/casing) y restaura el paso de la validación crítica.

**Convención de casing:** todos los valores de dominio (`nivel_formacion`, `sector_ies`) se almacenan en **minúsculas** en todo el pipeline (transform → fact table → queries BI).

---

## 4. Modelo Dimensional (Star Schema)

### 4.1. Decisión de diseño: grano agregado

El grano de la primera entrega (IES × Programa × Municipio) es incompatible con ICETEX (que solo provee departamento, no municipio ni programa). Para integrar ambas fuentes se redujo el grano al **denominador común**:

> Un registro por `(anio, semestre, departamento, nivel_formacion, sector_ies, genero, estrato)`

Sacrificio documentado: se pierde la granularidad de IES, Programa, Municipio, Metodología y Área del primario a nivel del star schema. Esta reducción fue necesaria para habilitar la integración con ICETEX y enfocar el análisis en las preguntas de equidad y cobertura que motivan el proyecto.

![Modelo dimensional — Star Schema](diagrams/star_schemma_dw_matriculas_colV2.png)

### 4.2. Tabla de Hechos: `fact_educacion_superior`

| Columna | Tipo | Descripción |
|---|---|---|
| `sk_fact` | INT PK | Surrogate key autoincrementable |
| `sk_tiempo` | INT FK | → `dim_tiempo` |
| `sk_ubicacion` | INT FK | → `dim_ubicacion` |
| `sk_demografia` | INT FK | → `dim_demografia` |
| `sk_nivel_formacion` | INT FK | → `dim_nivel_formacion` |
| `sk_sector_ies` | INT FK | → `dim_sector_ies` |
| `sk_estrato` | INT FK | → `dim_estrato` |
| `total_matriculados` | INT | Métrica SNIES (0 si la combinación no existe en SNIES) |
| `nuevos_beneficiarios_credito` | INT | Métrica ICETEX (0 si la combinación no existe en ICETEX) |

Restricción de unicidad: `UNIQUE KEY uk_grain (sk_tiempo, sk_ubicacion, sk_demografia, sk_nivel_formacion, sk_sector_ies, sk_estrato)`.

### 4.3. Dimensiones conformadas (6)

| Dimensión | Campos clave | Notas |
|---|---|---|
| `dim_tiempo` | `anio`, `semestre`, `periodo_academico` (columna generada) | Reutilizada |
| `dim_ubicacion` | `departamento`, `codigo_departamento` (nullable) | Reducida de municipio a depto |
| `dim_demografia` | `id_genero`, `descripcion_genero` | Reutilizada; enriquecida post-carga |
| `dim_nivel_formacion` | `nivel_formacion`, `tipo_formacion` (Pregrado/Posgrado) | Nueva; derivada de dim_programa |
| `dim_sector_ies` | `sector_ies` (oficial/privado/desconocido) | Nueva; derivada de dim_institucion |
| `dim_estrato` | `estrato` (0..6), `descripcion_estrato` | Nueva; pre-poblada por DDL |

---

## 5. Estrategia de Integración y Supuestos

### Mecanismo de integración

1. Ambos datasets se limpian y agregan independientemente al grano común (7 llaves).
2. Se realiza un `FULL OUTER JOIN` para preservar todos los registros de ambas fuentes.
3. `total_matriculados = 0` donde ICETEX no tiene contraparte en SNIES (ej. créditos al exterior).
4. `nuevos_beneficiarios_credito = 0` donde SNIES no tiene contraparte en ICETEX (mayoría de combinaciones del primario).

### Supuesto crítico documentado

La dimensión `dim_ubicacion` se trata a nivel **departamento** como proxy conformado entre las dos fuentes:
- En SNIES: departamento de **oferta del programa** (dónde está la IES).
- En ICETEX: departamento de **origen del estudiante** (dónde nació o reside).

Se asume que, a nivel agregado, estos son intercambiables para análisis estratégicos (la mayoría de los estudiantes estudian cerca de su lugar de origen). Esta aproximación introduce ruido en regiones con alta migración estudiantil (Bogotá, Medellín), documentado explícitamente para el consumidor del dashboard.

---

## 6. Explicación del Pipeline ETL

![Arquitectura del pipeline ETL](diagrams/architecture_diagramV3.svg)

### Módulos del pipeline

| Módulo | Función principal | Responsabilidad |
|---|---|---|
| `config.py` | — | Lee `airflow/.env`; expone rutas y URLs como constantes |
| `extract.py` | `extract_data()`, `extract_icetex_api()` | CSV + API con paginación y reintentos por petición |
| `transform.py` | `clean_primary()`, `aggregate_primary()`, `clean_icetex()`, `aggregate_icetex()` | Limpieza, homologación y agregación de ambas fuentes |
| `integrate.py` | `integrate_sources()` | FULL OUTER JOIN + fillna(0) sobre las 7 llaves |
| `validate.py` | `run_validation()` | Suite Great Expectations (Fase B) |
| `load.py` | `load_data()` | 6 dims + fact con dict-mapping anti-OOM |
| `main.py` | `main()`, `init_database_if_not_exists()` | Orquestador; auto-crea el schema si no existe |

### Decisión de diseño: surrogate keys con diccionarios

El mapeo de llaves foráneas se realiza con `set_index().to_dict()` + `Series.map()` en lugar de `pd.merge()`. Esto evita explosiones de memoria en datasets grandes (anti-OOM pattern), a costa de mayor verbosidad en el código. Cada dimensión se carga, se relee para obtener sus SKs generadas, y el dict resultante se usa para mapear la columna correspondiente en el fact DataFrame.

---

## 7. Estrategia de Validación (Great Expectations)

La suite `fact_educacion_superior_suite` se ejecuta sobre el DataFrame integrado **antes** de la carga a MySQL. Un fallo en cualquier expectativa crítica aborta el pipeline con `sys.exit(1)`.

### Expectativas críticas (14 — abortan el pipeline)

| # | Tipo | Columna | Justificación |
|---|---|---|---|
| 1–7 | `ExpectColumnValuesToNotBeNull` | `anio`, `semestre`, `departamento`, `nivel_formacion`, `sector_ies`, `id_genero`, `estrato` | Claves de negocio obligatorias |
| 8–9 | `ExpectColumnValuesToBeBetween(min=0)` | `total_matriculados`, `nuevos_beneficiarios_credito` | Métricas aditivas, no negativas |
| 10 | `ExpectColumnValuesToBeInSet([0..6])` | `estrato` | `0=Desconocido` (SNIES), `1..6` (ICETEX) |
| 11 | `ExpectColumnValuesToBeInSet(7 valores)` | `nivel_formacion` | Dominio canónico en minúsculas |
| 12 | `ExpectColumnValuesToBeInSet(['oficial','privado','desconocido'])` | `sector_ies` | `N/A` ICETEX → `desconocido` |
| 13 | `ExpectColumnValuesToBeInSet([1,2])` | `id_genero` | `Intersexual` descartado en clean_icetex |
| 14 | `ExpectColumnValuesToBeBetween(2015, 2025)` | `anio` | Rango real: SNIES 2015–2021, ICETEX 2015–2025 |

### Expectativas no críticas (2 — solo warning)

| # | Tipo | Justificación |
|---|---|---|
| 15 | `ExpectTableRowCountToBeBetween(5000, 150000)` | Sanity check de volumen |
| 16 | `ExpectColumnValuesToBeInSet(semestre, [1, 2])` | Semestre académico válido |

### Implementación técnica

- **GX v1.16.1** con Fluent API y contexto de archivo (`mode="file"`, raíz en `gx/`).
- Dos suites (`critical_suite`, `non_critical_suite`) ejecutadas por un único checkpoint (`etl_checkpoint`).
- `UpdateDataDocsAction` genera reportes HTML navegables en `gx/uncommitted/data_docs/local_site/`.
- Para visualizar los resultados: `xdg-open gx/uncommitted/data_docs/local_site/index.html`.

---

## 8. Consultas BI y Dashboard

Las queries que alimentan el dashboard están en `sql/bi_queries.sql` y se agrupan en dos bloques que corresponden uno a uno con los gráficos publicados en Looker Studio.

### 8.1. Consultas de Integración (SNIES + ICETEX)

Estas consultas son el valor diferencial de la segunda entrega: solo son posibles gracias a la integración de ambas fuentes en una única fact table. Cruzan las métricas `total_matriculados` (SNIES) y `nuevos_beneficiarios_credito` (ICETEX) para responder preguntas de equidad y cobertura.

| Query | Descripción | Agregación / Métrica | Visualización |
|---|---|---|---|
| 1.1 | **Tasa de cobertura de crédito por departamento** — identifica "desiertos de financiación" donde hay alta matrícula pero baja penetración de créditos ICETEX. | `SUM(beneficiarios) / NULLIF(SUM(matriculados), 0) * 100` → `tasa_cobertura_credito_pct` por `dim_ubicacion.departamento` | Mapa coroplético de Colombia |
| 1.2 | **Distribución de beneficiarios de crédito por estrato socioeconómico** — mide la equidad: ¿a qué estratos llega más la financiación? Se reporta volumen absoluto y porcentaje del total nacional (excluyendo `estrato=0` imputado para SNIES). | `SUM(beneficiarios)` y `% del total` por `dim_estrato.descripcion_estrato` | Barras horizontales |
| 1.3 | **Tendencia de créditos por sector de IES (oficial vs privado) por año** — analiza si los créditos se dirigen más a IES públicas o privadas a lo largo del tiempo. Excluye `sector_ies='desconocido'`. | `SUM(matriculados)` y `SUM(beneficiarios)` por `dim_tiempo.anio` × `dim_sector_ies.sector_ies` | Líneas / áreas apiladas |

### 8.2. Consultas sobre el Modelo Dimensional Agregado

Aprovechan el grano del star schema (`fact_educacion_superior` + dimensiones conformadas).

| Query | Descripción | Agregación / Métrica | Visualización |
|---|---|---|---|
| 2.1 | **Evolución temporal de matrículas por nivel de formación** — excluye `nivel_formacion='exterior'`. | `SUM(matriculados)` por `dim_tiempo.anio` × `dim_nivel_formacion.nivel_formacion` | Líneas múltiples |
| 2.2 | **Top 10 departamentos por volumen de matrícula** — incluye `porcentaje_nacional` respecto al total del país. | `SUM(matriculados)` y `% nacional` por `dim_ubicacion.departamento`, ordenado DESC, `LIMIT 10` | Barras horizontales |
| 2.3 | **Brecha de género por nivel de formación** — compara hombres vs mujeres por nivel (pregrado/posgrado), con `porcentaje_mujeres`. Excluye `exterior` y `desconocido`. | `SUM(CASE ... id_genero ...)` por `dim_nivel_formacion` | Barras agrupadas |

### 8.3. Dashboard (Looker Studio)

El dashboard interactivo fue construido en **Google Looker Studio**, conectado directamente al Data Warehouse MySQL (`dw_matriculas_col`) mediante el conector oficial de MySQL. Cada gráfico se alimenta de una **tabla personalizada** con la query correspondiente de `sql/bi_queries.sql`; los KPIs superiores se calculan con campos agregados directamente sobre la fact table.

**KPIs principales (tarjetas de resumen):**

- **Total Matriculados (SNIES 2015–2021)** = `SUM(total_matriculados)`
- **Beneficiarios ICETEX (2015–2025)** = `SUM(nuevos_beneficiarios_credito)`
- **Cobertura Nacional de Crédito (%)** = `SUM(nuevos_beneficiarios_credito) / SUM(total_matriculados) * 100`

**Filtros interactivos:** `anio`, `departamento`, `sector_ies` (aplicados globalmente mediante grupos de filtros de Looker).

**Referencia rápida — fuente de cada componente del dashboard:**

| Componente del dashboard | Fuente en el DW | Query |
|---|---|---|
| KPIs superiores | `fact_educacion_superior` (tabla completa) | — |
| Mapa de cobertura por departamento | Tabla personalizada | Query 1.1 |
| Beneficiarios por estrato | Tabla personalizada | Query 1.2 |
| Tendencia por sector de IES | Tabla personalizada | Query 1.3 |
| Evolución por nivel de formación | Tabla personalizada | Query 2.1 |
| Top 10 departamentos | Tabla personalizada | Query 2.2 |
| Brecha de género por nivel | Tabla personalizada | Query 2.3 |

![Dashboard Looker Studio](diagrams/dashboard_lookerV2.png)

---

## 9. Estructura del Proyecto

Estructura plana, sin packaging: `main.py` se ejecuta directamente desde la raíz y el DAG de Airflow inserta `/opt/airflow/src` en `sys.path`. Los directorios marcados como *gitignored* existen en disco pero no se versionan; los archivos bajo `gx/` se generan automáticamente en el primer run del pipeline.

```
project_delivery_2/
├── airflow/                                        # infra Airflow + Docker + datos
│   ├── docker-compose.yaml                         # servicios airflow + mysql-dw
│   ├── .env                                        # credenciales reales (gitignored)
│   ├── .env.example                                # plantilla de credenciales
│   ├── requirements.txt                            # deps de los contenedores Airflow
│   ├── config/                                     # config Airflow (gitignored, contiene .gitkeep)
│   ├── dags/
│   │   └── etl_ods4.py                             # DAG: inserta /opt/airflow/src en sys.path
│   ├── data/
│   │   ├── raw/
│   │   │   ├── educacionCol.csv                    # dataset primario SNIES (gitignored)
│   │   │   ├── descripcion_dataset_api.txt         # documentación de la API ICETEX
│   │   │   └── educacionCol_descripcion_columnas_dataset.csv
│   │   ├── processed/                              # CSVs limpios exportados (gitignored)
│   │   │   ├── educacionCol_clean.csv
│   │   │   ├── educacionCol_aggregated.csv
│   │   │   └── creditos_icetex_clean.csv
│   │   └── staging/                                # pickles intermedios del DAG (*.pkl, gitignored)
│   ├── logs/                                       # logs Airflow (gitignored)
│   └── plugins/
├── src/                                            # código del pipeline (flat, sin subpaquetes)
│   ├── main.py                                     # entry point: python src/main.py
│   ├── config.py                                   # carga airflow/.env, expone rutas y URLs
│   ├── extract.py                                  # extract_data (CSV) + extract_icetex_api
│   ├── transform.py                                # clean/aggregate primary + icetex
│   ├── integrate.py                                # FULL OUTER JOIN de ambas fuentes
│   ├── load.py                                     # 6 dims + fact con dict-mapping anti-OOM
│   └── validate.py                                 # runner Great Expectations
├── kafka/                                          # componente de streaming (entrega final)
│   ├── docker-compose.kafka.yaml                   # broker Kafka KRaft (sin Zookeeper), puerto 9092
│   ├── producer_metrics.py                         # lee fact table → publica métricas al topic (bucle)
│   └── consumer_metrics.py                         # consume topic → consola + stream_metrics_log
├── sql/
│   ├── init_dw_matriculas_col.sql                  # DDL: 6 dims + fact + stream_metrics_log
│   ├── bi_queries.sql                              # queries analíticas del dashboard
│   ├── workbench_diagram.mwb                       # modelo MySQL Workbench
│   └── workbench_diagram.mwb.bak
├── gx/                                             # Great Expectations (auto-generado, gitignored)
│   ├── great_expectations.yml
│   ├── expectations/
│   ├── checkpoints/
│   └── uncommitted/data_docs/                      # reportes HTML navegables
├── notebooks/
│   └── eda.ipynb                                   # EDA + profiling de ambas fuentes
├── diagrams/
│   ├── architecture_diagramV2.svg                  # arquitectura del pipeline
│   ├── star_schemma_dw_matriculas_colV2.svg/.png   # modelo dimensional
│   ├── classic_star_schemma_dw_matriculas_col.*    # modelo clásico (1ra entrega)
│   ├── dag_disign.png                              # diseño del DAG
│   ├── dashboard_lookerV2.png                      # captura del dashboard
│   └── dashboard_ODS4.png
├── requirements.txt                                # deps del pipeline local
├── ETL_ETLProject_SecondDelivery.pdf               # enunciado de la entrega
├── ETL_ETLProject_FirstDelivery.pdf
├── .gitignore
└── README.md
```

**Convención de rutas en el código:**

- `src/main.py` resuelve `project_root = Path(__file__).resolve().parent.parent`.
- `.env` se carga desde `project_root / "airflow" / ".env"`.
- Datos crudos: `project_root / "airflow" / "data" / "raw"`.
- Datos procesados: `project_root / "airflow" / "data" / "processed"`.
- Scripts SQL: `project_root / "sql"`.
- Dentro de Docker, `airflow/data/` se monta en `/opt/airflow/data/`.

---

## 10. Instrucciones de Ejecución (Local)

### Prerrequisitos

- Python 3.12+
- Docker y Docker Compose (para MySQL)
- El dataset SNIES **no se versiona**: el pipeline lo **descarga automáticamente** desde Google
  Drive (variable `PRIMARY_CSV_GDRIVE_ID` en `airflow/.env`) hacia `airflow/data/raw/educacionCol.csv`
  si no existe. La descarga es idempotente (solo ocurre la primera vez). Requiere conexión a internet.

### Paso 1 — Entorno virtual e instalación de dependencias

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Paso 2 — Variables de entorno

Copia `airflow/.env.example` a `airflow/.env` y edita las credenciales:

```bash
cp airflow/.env.example airflow/.env
```

```env
MYSQL_USER=root
MYSQL_PASSWORD=tu_password
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3307
MYSQL_DW_DB=dw_matriculas_col
MYSQL_ROOT_PASSWORD=tu_password
SOCRATA_APP_TOKEN=          # Opcional; aumenta el rate limit de la API ICETEX
```

> Usa `127.0.0.1` (no `localhost`) si ejecutas el pipeline localmente contra el contenedor Docker.

### Paso 3 — Iniciar el servicio MySQL

```bash
cd airflow
docker compose up -d mysql-dw
```

Espera ~10 segundos a que el contenedor inicialice. El DDL se ejecutará automáticamente como init-script del contenedor Y también de forma lazy al inicio del pipeline (`init_database_if_not_exists`).

### Paso 4 — Ejecutar el pipeline

Desde la raíz del proyecto:

```bash
python3 src/main.py
```

El pipeline:
1. Verifica si la base de datos `dw_matriculas_col` existe; si no, ejecuta el DDL.
2. Extrae el CSV primario y consume la API de ICETEX (con paginación).
3. Limpia, transforma y agrega ambas fuentes.
4. Integra mediante FULL OUTER JOIN.
5. Carga 6 dimensiones y la tabla de hechos en MySQL.

### Salidas esperadas

```
airflow/data/processed/
  ├── educacionCol_clean.csv        # Dataset primario limpio (granularidad fina)
  ├── educacionCol_aggregated.csv   # Dataset primario agregado al grano común
  └── creditos_icetex_clean.csv     # Dataset ICETEX limpio y agregado
```

### Re-ejecución limpia

El pipeline **no es idempotente** sobre datos ya cargados. Para un re-run limpio, elimina la base de datos antes de volver a ejecutar:

```sql
DROP DATABASE dw_matriculas_col;
```

### Verificación rápida

```sql
USE dw_matriculas_col;
SHOW TABLES;
SELECT COUNT(*) FROM fact_educacion_superior;
SELECT SUM(total_matriculados) FROM fact_educacion_superior;
SELECT SUM(nuevos_beneficiarios_credito) FROM fact_educacion_superior;
```

---

## 10. Diseño del DAG de Airflow

El DAG `etl_ods4` (`airflow/dags/etl_ods4.py`) replica el pipeline local con `PythonOperator` por cada función. Las tareas intercambian datos mediante pickles en `/opt/airflow/data/staging/`.

![Diseño del DAG de Airflow](diagrams/dag_disign.png)

### Tareas del DAG (9)

| Task ID | Módulo | Descripción |
|---|---|---|
| `extract_primary` | `extract.py` | Lee CSV SNIES → pickle |
| `extract_icetex` | `extract.py` | Consume API Socrata paginada → pickle |
| `clean_primary` | `transform.py` | Limpieza y homologación SNIES |
| `clean_icetex` | `transform.py` | Limpieza y homologación ICETEX |
| `aggregate_primary` | `transform.py` | Agregación al grano del DW |
| `aggregate_icetex` | `transform.py` | Agregación al grano del DW |
| `integrate` | `integrate.py` | FULL OUTER JOIN de ambas fuentes |
| `validate_gx` | `validate.py` | Suite GX — aborta con `RuntimeError` si falla |
| `load_dw` | `load.py` | Carga dimensiones + fact en MySQL |

### Adaptaciones para Docker

- `config.py` detecta automáticamente el entorno Docker (`/opt/airflow/src` existe) y ajusta rutas.
- `docker-compose.yaml` sobreescribe `MYSQL_HOST=mysql-dw` y `MYSQL_PORT=3306` para la red interna.
- Volúmenes montados: `src/:ro`, `sql/:ro`, `gx/` (lectura/escritura para Data Docs), `data/`.

### Ejecución del DAG

```bash
cd airflow
docker compose up -d
docker compose exec airflow-scheduler airflow dags test etl_ods4 2026-04-17
```

Los logs se encuentran en `airflow/logs/` y los Data Docs de GX en `gx/uncommitted/data_docs/local_site/`.

---

## 12. Componente de Streaming con Apache Kafka

La entrega final añade un componente de streaming que **publica métricas derivadas de la tabla de
hechos** (`fact_educacion_superior`) a un topic de Kafka y las consume en tiempo real. Es el único
componente nuevo respecto a la segunda entrega; el pipeline batch, el modelo dimensional, la
validación y el dashboard se conservan sin cambios.

> **Requisito clave del enunciado:** el producer lee métricas **derivadas del Data Warehouse**, no
> del CSV original. La fuente del stream es siempre la fact table en MySQL.

### 12.1. Arquitectura del streaming

```
fact_educacion_superior (MySQL DW)
        │  (consulta SQL agregada, reutiliza la lógica de sql/bi_queries.sql)
        ▼
kafka/producer_metrics.py  ──►  Topic Kafka 'dw-metrics-stream'  ──►  kafka/consumer_metrics.py
        (bucle cada N s)                                                 │
                                                                         ├─► Monitoreo en consola (tiempo real)
                                                                         └─► Persistencia en stream_metrics_log (MySQL)
```

- **Broker:** un único contenedor Kafka en **modo KRaft (sin Zookeeper)**, definido en
  `kafka/docker-compose.kafka.yaml`, aislado del stack de Airflow. Expone `localhost:9092`.
- **Producer (`kafka/producer_metrics.py`):** consulta la fact table cada `STREAM_DELAY_SECONDS`,
  construye eventos JSON y los publica al topic en bucle (Ctrl+C para detener). Reutiliza
  `MYSQL_URL` y las constantes Kafka de `src/config.py`.
- **Consumer (`kafka/consumer_metrics.py`):** se suscribe al topic, **muestra** cada métrica en
  consola y la **persiste** en la tabla `stream_metrics_log`.

### 12.2. Métricas seleccionadas (≥3, derivadas de la fact table)

Las tres métricas son significativas para el monitoreo del proceso de negocio (equidad y cobertura
del crédito educativo) y reutilizan la lógica analítica de `sql/bi_queries.sql`:

| Métrica (`metric_name`) | Dimensión | Cálculo sobre la fact table | Significado de negocio |
|---|---|---|---|
| `tasa_cobertura_credito` | `departamento` (`dim_ubicacion`) | `SUM(beneficiarios)/NULLIF(SUM(matriculados),0)*100` | Penetración del crédito ICETEX frente a la matrícula; detecta "desiertos de financiación". |
| `beneficiarios_por_estrato` | `estrato` (`dim_estrato`, excluye estrato 0) | `SUM(nuevos_beneficiarios_credito)` | Equidad: a qué estratos llega la financiación. |
| `matriculados_por_sector` + `beneficiarios_por_sector` | `sector_ies` (`dim_sector_ies`, excluye `desconocido`) | `SUM(total_matriculados)` y `SUM(nuevos_beneficiarios_credito)` | Orientación de matrícula y crédito hacia IES oficiales vs privadas. |

### 12.3. Formato del evento (JSON)

Todos los eventos comparten un esquema común para que el consumer los procese de forma uniforme:

```json
{
  "metric_name": "tasa_cobertura_credito",
  "dimension_key": "departamento",
  "dimension_value": "ANTIOQUIA",
  "metric_value": 12.34,
  "event_timestamp": "2026-05-27T15:04:05.123456+00:00"
}
```

### 12.4. Tabla de persistencia `stream_metrics_log`

El consumer persiste cada evento en esta tabla (creada por el DDL,
`sql/init_dw_matriculas_col.sql`, de forma idempotente):

| Columna | Descripción |
|---|---|
| `id` | PK autoincremental |
| `metric_name`, `dimension_key`, `dimension_value`, `metric_value` | Contenido del evento |
| `event_timestamp` | Timestamp asignado por el producer al publicar |
| `kafka_offset`, `kafka_partition` | Trazabilidad del mensaje en Kafka |
| `received_at` | Momento de persistencia en el consumer |

### 12.5. Cómo correr el producer y el consumer

**Prerrequisito:** el Data Warehouse debe estar poblado (haber ejecutado el pipeline batch,
`python3 src/main.py`, o el DAG de Airflow).

```bash
# 1. Levantar el broker Kafka (modo KRaft, contenedor único)
docker compose -f kafka/docker-compose.kafka.yaml up -d

# (opcional) verificar que el broker responde
docker exec etl_kafka kafka-topics --bootstrap-server localhost:9092 --list

# 2. Terminal A — consumer (se queda escuchando y monitoreando)
python3 kafka/consumer_metrics.py

# 3. Terminal B — producer (publica métricas en bucle; Ctrl+C para detener)
python3 kafka/producer_metrics.py
```

Si el DW está en el contenedor Docker, asegúrate de que `airflow/.env` apunte a `MYSQL_HOST=127.0.0.1`
y `MYSQL_PORT=3307`, igual que para el pipeline local.

Para detener Kafka: `docker compose -f kafka/docker-compose.kafka.yaml down` (añade `-v` para borrar
también el volumen del broker).

---

## 13. Monitoreo en Tiempo Real e Interpretación

Con el producer y el consumer en ejecución, el consumer imprime una línea por cada métrica recibida:

```
📥 [offset=0] metric=tasa_cobertura_credito departamento=ANTIOQUIA value=12.34
📥 [offset=1] metric=beneficiarios_por_estrato estrato=Estrato 2 value=18450.0
📥 [offset=2] metric=matriculados_por_sector sector_ies=oficial value=9876543.0
📥 [offset=3] metric=beneficiarios_por_sector sector_ies=oficial value=120345.0
```

En paralelo, cada evento queda persistido en `stream_metrics_log`, lo que permite consultar el
histórico del monitoreo:

```sql
USE dw_matriculas_col;
-- Cuántos eventos se han monitoreado por métrica
SELECT metric_name, COUNT(*) AS eventos
FROM stream_metrics_log
GROUP BY metric_name;

-- Última lectura de cobertura por departamento
SELECT dimension_value AS departamento, metric_value AS cobertura_pct, received_at
FROM stream_metrics_log
WHERE metric_name = 'tasa_cobertura_credito'
ORDER BY received_at DESC, metric_value DESC
LIMIT 10;
```

**Interpretación:** la salida en tiempo real funciona como un panel de monitoreo del indicador de
equidad del sistema. Un operador puede vigilar, ciclo a ciclo, qué departamentos mantienen baja
cobertura de crédito (desiertos de financiación), cómo se distribuyen los beneficiarios por estrato
y si la financiación se concentra en IES oficiales o privadas — las mismas preguntas de negocio que
motivan el proyecto, ahora observables de forma continua.

---

## 14. Business Objectives Achievement

El objetivo de negocio central es **medir la equidad y cobertura de la financiación estatal (ICETEX)
frente a la oferta educativa (SNIES)** para evaluar la efectividad de las políticas de crédito
respecto al ODS 4. Cada KPI, componente del dashboard y métrica de streaming se vincula a una
pregunta analítica y a una decisión que habilita.

| Objetivo de negocio | Pregunta analítica | KPI / Métrica | Evidencia (Dashboard / Streaming) | Decisión que soporta |
|---|---|---|---|---|
| Detectar desiertos de financiación | ¿Dónde hay alta matrícula pero baja cobertura de crédito? | `tasa_cobertura_credito` por depto. | Mapa coroplético (Query 1.1) **+ stream `tasa_cobertura_credito`** | Priorizar regiones para ampliar oferta de crédito ICETEX. |
| Evaluar equidad socioeconómica | ¿La financiación llega equitativamente a todos los estratos? | `beneficiarios_por_estrato` | Barras por estrato (Query 1.2) **+ stream `beneficiarios_por_estrato`** | Ajustar focalización del crédito hacia estratos bajos. |
| Analizar orientación público/privado | ¿El crédito se dirige más a IES oficiales o privadas? | matriculados vs beneficiarios por sector | Tendencia por sector (Query 1.3) **+ stream `*_por_sector`** | Revisar convenios con IES según sector. |
| Caracterizar la oferta educativa | ¿Cómo evoluciona la matrícula por nivel y territorio? | matrícula por nivel / Top 10 deptos. | Queries 2.1, 2.2 (Looker) | Planeación de cobertura educativa. |
| Medir brecha de género | ¿Qué brecha existe por nivel de formación? | `porcentaje_mujeres` por nivel | Query 2.3 (Looker) | Programas de equidad de género. |

**Cómo cada capa soporta el negocio:**
- **Pipeline ETL (Airflow):** automatiza y hace reproducible la integración SNIES + ICETEX.
- **Validación (Great Expectations):** garantiza que las métricas de equidad se calculen sobre datos
  confiables (sin nulos en llaves, rangos válidos, grano único).
- **Modelo dimensional:** el grano común habilita cruzar matrícula y crédito en una sola fact table.
- **Dashboard (Looker Studio):** entrega los KPIs e insights estáticos para decisión estratégica.
- **Streaming (Kafka):** convierte esos mismos indicadores en un monitoreo continuo del proceso.

---

## 15. Supuestos, Limitaciones y Mejoras Futuras

### Supuestos y decisiones técnicas documentadas

- **Kafka en modo KRaft (sin Zookeeper):** aunque en ejercicios previos usamos Zookeeper, aquí se
  despliega Kafka en modo KRaft. Justificación: Zookeeper está **deprecado desde Kafka 3.5 y
  eliminado en Kafka 4.0**; para un broker único de uso local no aporta coordinación útil y solo
  agrega un contenedor extra. El enunciado exige Apache Kafka, no Zookeeper.
- **`docker-compose` separado para Kafka:** se aísla del stack de Airflow para no afectar el pipeline
  batch existente (red y ciclo de vida independientes).
- **Continuidad simulada por re-consulta en bucle:** la fact table es estática; la "publicación
  continua" exigida se emula re-consultando y re-publicando las métricas cada `STREAM_DELAY_SECONDS`.
- **Proxy de departamento (heredado de la 2ª entrega):** `dim_ubicacion` mezcla departamento de
  oferta (SNIES) y de origen (ICETEX); las métricas streameadas heredan este supuesto.
- **Consumer con `group_id` fijo y `auto_offset_reset=earliest`:** al reconectar reproduce el
  histórico del topic, evitando perder mensajes en desarrollo.

### Limitaciones

- El stream refleja un DW estático: no hay novedad real entre ciclos salvo que se recargue el DW.
- Broker de un solo nodo (factor de replicación 1): sin tolerancia a fallos, adecuado solo para
  desarrollo/demostración.
- El grano departamental sacrifica el detalle de IES/programa/municipio en el modelo integrado.

### Mejoras futuras

- Alimentar el stream desde un proceso incremental real (CDC sobre la fact table) en lugar de
  re-consulta periódica.
- Cluster Kafka multi-broker con replicación para alta disponibilidad.
- Conectar `stream_metrics_log` a un panel en vivo (p. ej. Grafana) para visualizar la serie temporal
  del monitoreo.
