from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ROOT = "C:\Users\ndaoud\AppData\Local\Programs\Python\Python311\soda_installation"  # Remplacez par le chemin de votre projet

def run_soda_scan(project_root, scan_name, checks_subpath):
    from soda.scan import Scan
    print(f"Running Soda Scan: {scan_name}")
    config_file = f"{project_root}/soda/configuration.yml"
    checks_path = f"{project_root}/soda/checks.yml"
    data_source = "customers_certif"  
    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)
    result = scan.execute()
    print(scan.get_logs_text())
    if result != 0:
        raise ValueError(f'Soda Scan failed: {scan_name}')
    return result

with DAG(
    "btoc_customers_data_quality",
    default_args=default_args,
    description="DAG to perform data quality checks on btoc_customers table",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['soda', 'data quality']
) as dag:
    
    start = DummyOperator(task_id="start")
    
    transform_data = DummyOperator(task_id="transform_data")
    
    ingest_data = DummyOperator(task_id="ingest_data")
    
    generate_report = DummyOperator(task_id="generate_report")

    checks_transform = PythonVirtualenvOperator(
        task_id="checks_transform",
        python_callable=run_soda_scan,
        requirements=["-i https://pypi.cloud.soda.io", "soda-postgres", "soda-scientific"],
        system_site_packages=False,
        op_kwargs={
            "project_root": PROJECT_ROOT,
            "scan_name": "btoc_customers_transform_check",
            "checks_subpath": "transform/transform_checks.yml"
        },
    )

    checks_ingest = PythonVirtualenvOperator(
        task_id="checks_ingest",
        python_callable=run_soda_scan,
        requirements=["-i https://pypi.cloud.soda.io", "soda-postgres", "soda-scientific"],
        system_site_packages=False,
        op_kwargs={
            "project_root": PROJECT_ROOT,
            "scan_name": "btoc_customers_ingest_check",
            "checks_subpath": "ingest/ingest_checks.yml"
        },
    )

    checks_report = PythonVirtualenvOperator(
        task_id="checks_report",
        python_callable=run_soda_scan,
        requirements=["-i https://pypi.cloud.soda.io", "soda-postgres", "soda-scientific"],
        system_site_packages=False,
        op_kwargs={
            "project_root": PROJECT_ROOT,
            "scan_name": "btoc_customers_report_check",
            "checks_subpath": "report/report_checks.yml"
        },
    )

    end = DummyOperator(task_id="end")

    # Définir l'ordre d'exécution des tâches
    start >> transform_data >> checks_transform >> ingest_data >> checks_ingest >> generate_report >> checks_report >> end
