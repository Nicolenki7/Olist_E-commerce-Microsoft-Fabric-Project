# Ruta abierta · qué queda y qué falta

## Estado original (se conserva)

Este proyecto se construyó en **Microsoft Fabric** con Power Query M y Dataflows Gen2.
La evidencia está en el repo: las siete consultas `ETL_*.m`, el dashboard (`.pbix` y `.pdf`),
el informe en Word y el modelo dimensional con geolocalización.

## Por qué no hay un port ejecutable todavía

El dataset de origen es público pero **no está versionado**: vive en
`kaggle.com/datasets/olistbr/brazilian-ecommerce` y requiere cuenta y aceptación de términos.
Son unos 45 MB repartidos en nueve tablas; meterlos al repositorio lo haría pesado y no es lo que se quiere
para un portfolio.

## Camino concreto para reabrirlo

1. **Descargar** el dataset desde Kaggle (o dejar un `download.py` que lo baje con `kaggle.json` del usuario).
2. **Portar las siete consultas** a SQL: cada `ETL_*.m` es una transformación de una tabla y se traduce uno a uno.
   El orden de dependencias importa: primero las dimensiones, después `ETL_FactOrders`.
3. **Construir el esquema estrella** en DuckDB: cinco dimensiones (`DimCustomer`, `DimProduct`, `DimSeller`,
   `DimState`, `DimGeoloc`) y una tabla de hechos.
4. **Traducir el `.pbix`** a vistas SQL con las mismas métricas de ingresos, distribución geográfica
y tiempos de entrega.
5. **Añadir el test de contrato** —separadores, cabeceras, nulos por columna— como en `ETL_Egatur_INE_Esp`.

## Referencia ya funcionando

`Tech_GlobalStore_Retail_Analysis-Fabric` tiene el patrón resuelto en su carpeta `stack-abierto/`:
esquema estrella en DuckDB, vistas de KPIs y controles de calidad que devuelven código de error.

## Nota técnica

GitHub etiquetaba este repositorio como *Wolfram Language* porque los archivos `.m` de Power Query
comparten extensión con Mathematica. El `.gitattributes` de la raíz lo corrige marcándolos como no detectables.
