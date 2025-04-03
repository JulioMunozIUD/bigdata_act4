# bigdata_act4

Este proyecto simula un entorno de Big Data en la nube para procesar información de la API de Rick and Morty, siguiendo un flujo de trabajo ETL (Extract, Transform, Load) con tres fases principales:

* Ingesta: Extracción de datos de la API y almacenamiento inicial en SQLite

* Preprocesamiento: Limpieza y normalización de los datos

* Enriquecimiento: Integración con datos adicionales (scripts de episodios)


## Clonar el Repositorio

* https://github.com/JulioMunozIUD/bigdata_act4.git

## Crear y Activar un Entorno Virtual

* python -m venv venv
* source venv/bin/activate  # En Linux/Mac
* venv\Scripts\activate     # En Windows

## Instalar Dependencias

* pip install --upgrade pip
* pip install -e .

## Ejecución del Script de ingesta (ingestion.py)

El script realiza las siguientes acciones:

* Obtención de Datos desde una API.

* Creación de la Base de Datos

* Inserción de Datos en la Base de Datos

* Usando pandas Convierte los datos en un DataFrame para facilitar su manipulación.

* Guarda los datos en un archivo ingestion.xlsx.

* Genera un informe de auditoría en ingestion.txt.

## Ejecución del Script de Limpieza (cleaning.py)

El script realiza las siguientes acciones:

* Carga datos desde ingestion.db.

* Realiza análisis exploratorio.

* Identifica y elimina duplicados.

* Sustituye valores vacíos y nulos.

* Ajusta tipos de datos según un esquema definido.

* Guarda los datos limpios en cleaned_data.xlsx.

* Genera un informe de auditoría en cleaning_report.txt.

## Ejecución del Script de enriquecimiento (enrichement.py)

El script realiza las siguientes acciones:

* Extracción: Se cargan los datos base desde cleaned_data.xlsx.

* Lectura de Datos Adicionales: Se obtiene RickAndMortyScripts.xlsx.

* Enriquecimiento: Se fusionan ambos datasets en la hoja episodes.

* Generación de Evidencia: Se genera enriched_data.xlsx y el reporte de auditoría enriched_report.txt.

* Guarda los datos limpios en cleaned_data.xlsx.

* Genera un informe de auditoría en enriched_report.txt.

## Workflow Automatizado en GitHub Actions

El proceso del  proyecto integrador de Big Data se ejecuta automáticamente en GitHub Actions    cuando hay un push en la rama main. El workflow bigdata.yml sigue los siguientes pasos:

* Clonar el repositorio.

* Configurar Python en la versión 3.9.2.

* Crear y activar un entorno virtual.

* Actualizar pip e instalar las dependencias.

* Ejecutar ingestion.py

* Ejecutar cleaning.py

* Ejecutar enrichement.py

* Generar y almacenar los archivos resultantes.

* Realizar un commit automático con los cambios generados.


## Estructura del proyecto
```
├── setup.py
├── README.md
├── .github
│   └── workflows
│       └── bigdata.yml
├── docs
│   └── arquitectura_modelo.pdf
└── src
    ├── static
    │   └── auditoria
    │       ├── enriched_report.txt
    │       ├── cleaning_report.txt
    │       └── ingestion.txt
    ├── db
    │   ├── ingestion.db
    │   ├── RickAndMortyScripts.xlsx
    │   └── cleaned_data.xlsx
    ├── xlsx
    │   ├── enriched_data.xlsx
    │   ├──cleaned_data.xlsx
    │   └──ingestion.xlsx
    └── enrichment.py
```
  
        