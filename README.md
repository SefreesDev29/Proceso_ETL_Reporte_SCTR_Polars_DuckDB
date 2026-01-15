# 🚀 Pipeline ETL Emisión SCTR - Polars con DuckDB

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Polars](https://img.shields.io/badge/Polars-1.36.1-3776AB?style=for-the-badge&logo=polars&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.4.3-3776AB?style=for-the-badge&logo=duckdb&logoColor=white)](https://www.python.org/)
[![License: CC BY-NC 4.0](https://img.shields.io/badge/License-CC%20BY--NC%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc/4.0/)

Este proyecto implementa una arquitectura **ETL (Extract, Transform, Load)** moderna para el procesamiento masivo de archivos Excel de pólizas SCTR. 

---

## 🏗️ Arquitectura del Flujo

* **Origen:** Archivos Excel en Carpetas Locales.
* **Procesamiento:** Python (Polars, FastExcel, DuckDB).
* **Orquestación:** Ejecución interactiva vía CMD.
* **Salida:** Archivo Parquet consolidado para consumo en tablero de Power BI.

---

## 🛠️ Requisitos Previos

* **Sistema Operativo:** Windows 10/11 (Probado), macOS o Linux.
* **Python:** Versión `>=3.11` requerida (Gestionado con `uv`).

---

## 🚀 Instalación y Configuración del Entorno de Desarrollo (Setup)

Sigue estos pasos si estás clonando este repositorio por primera vez.

### 1. Clonar y Preparar Entorno (uv)
1.  Clonar el repositorio

```sh
git clone [https://github.com/SefreesDev29/Proceso_ETL_Reporte_SCTR_Polars_DuckDB.git](https://github.com/SefreesDev29/Proceso_ETL_Reporte_SCTR_Polars_DuckDB.git)
```

2.  Este proyecto utiliza `uv` para la gestión de dependencias. Instala las librerías necesarias:

```sh
# Sincronizar entorno virtual
uv sync

# En caso no tenga instalado uv
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# O instalar desde PyPI
pipx install uv
pip install uv

```