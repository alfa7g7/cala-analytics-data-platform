# Glosario de Negocio y Manual Técnico - CALA Analytics

## Conceptos Técnicos de Salud
* **CUPS (Clasificación Única de Procedimientos en Salud)**: Es el ordenamiento lógico y detallado de los procedimientos y servicios en salud que se realizan en el país. Son códigos numéricos (ej: 4644, 8902) que identifican cada intervención médica.

* **Diagnóstico (DX)**: Código estandarizado (usualmente basado en CIE-10) que identifica la enfermedad o condición del paciente durante la atención. El dato del **diagnóstico** se encuentra dentro del campo `json_detalle`.

* **Canal de Ingreso**: Método por el cual el paciente solicita la atención. Los canales oficiales son: **WEB**, **APP**, y **CALL_CENTER**.

## Tipos de Eventos en la App
* **LOGIN**: Representa una autenticación exitosa del usuario en la plataforma digital. Es el primer paso para realizar cualquier transacción.
* **CLICK**: Una interacción estándar del usuario navegando por los diferentes módulos de la App.
* **ERROR**: Excepción en el frontend o backend que le impidió al usuario completar un flujo o atención.
* **COMPRA**: Transacción digital monetaria confirmada por la pasarela de pagos.

## Indicadores Clave de Desempeño (KPIs)
* **Total Atenciones**: Conteo total de registros únicos procesados tras la deduplicación.

* **Valor Facturado**: Suma total de los costos asociados a las atenciones médicas. Se **calcula** sumando el valor de cada atención procesada.

* **Promedio por Atención**: Se **calcula** dividiendo el **total facturado** entre el número de **atenciones**.

* **Tasa de Errores de Calidad**: Porcentaje de registros con JSON malformado o campos críticos nulos frente al total de la carga. El **reporte de calidad** detalla cuántos **errores hubo** en el proceso.

## Reglas del Pipeline y Limpieza de Datos
* **Limpieza de Documentos**: Se **limpian** los documentos eliminando caracteres no numéricos (ej: '41632483n' pasa a '41632483').

* **Normalización de Ciudades**: Se **limpian** los nombres de las ciudades corrigiendo acentos y encoding (ej: 'MedellÃ­n' pasa a 'Medellín'). Esto resuelve qué **pasa con las ciudades** mal escritas.

* **Manejo de Duplicados**: Si un ID de atención aparece más de una vez, se conserva únicamente el registro con la fecha de atención más reciente. Así se **manejan los duplicados**.

* **Procesamiento de json_detalle**: El **pipeline** parsea este campo para extraer el **médico** y el **diagnóstico**.

## Resultados y Propuesta Técnica
* **Almacenamiento**: Los resultados **donde se guardan** es en archivos de formato Parquet en la carpeta `output/processed`.
* **Propuesta Técnica**: La **propuesta tecnica** utiliza una arquitectura de datos moderna con BigQuery para analítica y Airflow para orquestación.
