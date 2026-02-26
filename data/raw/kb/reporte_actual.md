# Reporte de Estado Actual - CALA Analytics 
*(Actualizado: 24 de Febrero de 2026)*

Este documento contiene los resultados del último procesamiento del pipeline de datos.

## Resumen Ejecutivo
* **Atenciones Procesadas con éxito**: 10,000 registros.
* **Registros Descartados (Duplicados)**: 300 registros.
* **Errores de Calidad (JSON Malformado)**: 499 hallazgos críticos (revisar `quality_report.json` para IDs específicos).

## Resultados Financieros
* **Facturación Total de la Carga**: $2,315,973,166.13 COP.
* **Promedio de Facturación por Atención**: $231,597.32 COP.

## Distribución por Canales
* **APP**: 3,406 atenciones.
* **WEB**: 3,352 atenciones.
* **CALL_CENTER**: 3,242 atenciones.

## Nota de Calidad
Todos los documentos de clientes han sido normalizados (campos alfanuméricos corregidos) y las ciudades han sido estandarizadas para asegurar la integridad de los reportes en BigQuery.
