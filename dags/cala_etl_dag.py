from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import sys
import os

# Add src to path to import the pipeline
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.pipeline.main import DataPipeline

default_args = {
    'owner': 'cala_analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cala_data_platform_etl',
    default_args=default_args,
    description='End-to-end ETL for CALA Analytics',
    schedule_interval='@daily',
    catchup=False,
    tags=['cala', 'gcp', 'rag'],
) as dag:

    def run_pipeline():
        pipeline = DataPipeline(input_dir='/data/raw', output_dir='/output/processed')
        pipeline.run()

    task_extract_transform = PythonOperator(
        task_id='extract_transform',
        python_callable=run_pipeline,
    )

    # Note: In a real Cloud Composer environment, we would use GCS to BigQuery operators.
    # This is a representation compatible with BigQueryInsertJobOperator.
    task_load_stg = BigQueryInsertJobOperator(
        task_id='load_to_staging',
        configuration={
            "query": {
                "query": "CREATE OR REPLACE TABLE `cala_analytics.stg_atenciones` AS SELECT * FROM EXTERNAL_QUERY(...)", # Simplified
                "useLegacySql": False,
            }
        },
    )

    task_build_kpis = BigQueryInsertJobOperator(
        task_id='build_kpis',
        configuration={
            "query": {
                "query": """
                    MERGE `cala_analytics.kpi_table` T
                    USING (
                        SELECT fecha_proceso, SUM(valor_facturado) as total_facturado
                        FROM `cala_analytics.fct_atenciones`
                        WHERE fecha_proceso = DATE('{{ ds }}')
                        GROUP BY 1
                    ) S
                    ON T.fecha = S.fecha_proceso
                    WHEN MATCHED THEN
                        UPDATE SET total_facturado = S.total_facturado
                    WHEN NOT MATCHED THEN
                        INSERT (fecha, total_facturado)
                        VALUES(S.fecha_proceso, S.total_facturado)
                """,
                "useLegacySql": False,
            }
        },
    )

    task_extract_transform >> task_load_stg >> task_build_kpis
