# mcd-big-data
 mcd-big-data

# Análisis de Datos Intradía con PySpark

Este proyecto se centra en el procesamiento y análisis de datos intradía provenientes de operaciones bursátiles utilizando **PySpark**. El objetivo es realizar operaciones como carga de datos desde archivos delimitados por tabulación, agregaciones y cálculos de estadísticas sobre los datos, y filtrar registros con eficiencia a gran escala utilizando RDDs y DataFrames de PySpark.

## Estructura de los datos

Los datos de ejemplo contienen información intradía sobre operaciones bursátiles, con campos como:

- `trade_time`: Tiempo de operación con precisión a nivel de milisegundos.
- `match_number`: Número de coincidencia de la operación.
- `instrument_id`: ID del instrumento en la bolsa.
- `volume`: Cantidad de titulos.
- `price`: Precio.
- `buyer_id`: ID de la casa de bolsa del comprador.
- `seller_id`: ID de la casa de bolsa del vendedor.
- Otros campos relacionados con la operación, como indicadores de subasta, tipo de operación, etc.

### Ejemplo de una fila de datos:

trade_time | match_number | instrument_id | volume | price | buyer_id | seller_id | symbol

2024-07-01 07:30:00.000 | 1 | 5 | 1 | 62.5 | 112 | 112 | WALMEX*


## Funcionalidades

El proyecto incluye las siguientes funcionalidades:

1. **Carga de múltiples archivos CSV**: Los archivos delimitados por tabulaciones (`.txt`) se cargan desde Google Drive o el sistema de archivos local, y se leen utilizando PySpark.
2. **Agrupación por fecha**: Se realiza una agregación de los datos a nivel de fecha (ignorando la hora) para facilitar el análisis diario.
3. **Filtrado eficiente de registros**: Se aplican filtros sobre grandes volúmenes de datos para obtener subconjuntos específicos, como operaciones realizadas por un participante específico.
4. **Cálculo de estadísticas**: Se realizan operaciones como suma de cantidades ejecutadas, ordenadas por fecha.

## Requisitos

Para ejecutar este proyecto, necesitarás:

- **Python 3.8+**
- **PySpark 3.0+**
- **Google Colab** o un entorno local con PySpark instalado
- **Google Drive** (si deseas almacenar o acceder a los archivos CSV desde Drive)

## Instalación

### Opción 1: Ejecutar en Google Colab

1. Abre Google Colab y monta tu Google Drive:

   ```python
   from google.colab import drive
   drive.mount('/content/drive')
