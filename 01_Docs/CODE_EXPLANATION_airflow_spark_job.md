# Airflow DAG Explanation: `airflow_spark_job.py`
This file is the "Project Manager." It does not process data; instead, it orchestrates the infrastructure, schedules the tasks, and sets the execution order.

---

```python
from datetime import datetime, timedelta
from airflow import DAG
```
*   **What it does:** Imports necessary timing functions from Python's standard `datetime` library (used for scheduling) and imports the core `DAG` class from Airflow, which is the foundational object for creating a workflow.

```python
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
```
*   **What it does:** Imports three specific Google Cloud "Operators". Operators are essentially pre-built templates in Airflow that know exactly how to talk to Google Cloud APIs to perform specific actions (create, submit, and delete).

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 13),
}
```
*   **What it does:** Creates a dictionary of baseline settings that will be applied to every task in the DAG.
    *   `depends_on_past: False` means this run doesn't care if yesterday's run failed.
    *   `retries: 1` and `retry_delay: timedelta(minutes=5)` mean if a task fails, Airflow will wait 5 minutes and try exactly one more time before completely failing.
    *   `start_date` tells Airflow the earliest date this pipeline is theoretically allowed to run.

```python
dag = DAG(
    'employee_data_analyis_with_spark',
    default_args=default_args,
    description='A DAG to setup dataproc and run Spark job on that',
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
)
```
*   **What it does:** Instantiates the actual DAG object.
    *   `'employee_data_analyis_with_spark'` is the unique ID that appears in the Airflow Web UI.
    *   `schedule_interval=None` means this pipeline will not run automatically on a timer; it must be triggered manually.
    *   `catchup=False` prevents Airflow from trying to run historical executions from the `start_date` up to today.
    *   `tags=['dev']` adds a searchable label in the UI.

```python
CLUSTER_NAME = 'dataproc-spark-airflow-demo'
PROJECT_ID = 'project-d1694a9a-9dde-4e3c-974'
REGION = 'us-central1'
```
*   **What it does:** Sets global constants for the GCP infrastructure. Keeping these as variables at the top makes the code much easier to update if the project or region changes later.

```python
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1, 
        'machine_type_uri': 'n1-standard-2', 
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.2.26-debian12' 
    }
}
```
*   **What it does:** A dictionary defining the exact hardware footprint of the Dataproc cluster. It dictates that it needs 1 Master Node and 2 Worker Nodes, specifies they should use `n1-standard-2` virtual machines, sets their hard drive sizes to 30GB, and dictates the OS/Hadoop software version (`debian12`).

```python
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)
```
*   **What it does:** Defines the very first Task (node) in the DAG. It takes the `CLUSTER_CONFIG` built above and tells Airflow to instruct GCP to build it.

```python
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://airflow-test-projects-gds-dev/airflow-project-1/spark-job/emp_batch_job.py"
        },
}
```
*   **What it does:** Defines a dictionary that packages up the instructions for the Spark job. It tells GCP which cluster to run on, and exactly where to find the Python script (`emp_batch_job.py`) inside Google Cloud Storage.

```python
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job_on_dataproc',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)
```
*   **What it does:** Defines the second Task. It takes the `PYSPARK_JOB` dictionary and submits the work to the running cluster.

```python
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',
    dag=dag,
)
```
*   **What it does:** Defines the third and final Task, which is to destroy the cluster to save money. 
    *   **Crucial detail:** `trigger_rule='all_done'` overrides Airflow's default behavior. By default, if a previous task fails, the downstream tasks are skipped. Setting this to `all_done` guarantees that Airflow will attempt to delete the cluster *even if the Spark script crashes*, preventing a runaway cloud bill.

```python
create_cluster >> submit_pyspark_job >> delete_cluster
```
*   **What it does:** Uses Python bitshift operators (`>>`) to set the execution lineage (the flowchart arrows). It explicitly tells Airflow: "You must create the cluster first, then submit the job, and only when that finishes, delete the cluster."