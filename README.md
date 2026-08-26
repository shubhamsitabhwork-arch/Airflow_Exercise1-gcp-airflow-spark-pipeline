# GCP Airflow & PySpark ETL Pipeline

This repository contains an automated Data Engineering pipeline orchestrated by Apache Airflow. The pipeline provisions an ephemeral Google Cloud Dataproc cluster, executes a distributed PySpark batch job to process and join CSV files stored in Google Cloud Storage (GCS), and cleanly tears down the cluster upon completion to optimize costs.

## 🏗️ Architecture & Pipeline Flow

The pipeline consists of three main steps managed by Airflow:
1. **Create:** Provision a GCP Dataproc Cluster.
2. **Execute:** Run a PySpark job to read, filter, join, and write data.
3. **Delete:** Tear down the cluster to save costs.

### Process Flowchart
<img width="4980" height="3888" alt="image" src="https://github.com/user-attachments/assets/43eed792-7658-4b5c-9576-d21e5277297c" />


## 📁 Project Structure
airflow_spark_job.py: Airflow DAG definition for cluster management and job submission.

emp_batch_job.py: PySpark script containing the ETL logic.

employee.csv: Source data containing employee details.

department.csv: Source data containing department mapping.

## 🚀 Setup and Deployment
**GCP Requirements:**

A GCP Project with billing enabled.

APIs enabled: Cloud Dataproc API, Cloud Storage API.

A Service Account with Dataproc Worker and Storage Object Admin permissions.

**Deployment Steps:**

Upload employee.csv and department.csv to your GCS bucket: gs://airflow-projetcs-gds-dev/airflow-project-1/data/.

Upload emp_batch_job.py to gs://airflow-projetcs-gds-dev/airflow-project-1/spark-job/.

Place airflow_spark_job.py in your Airflow dags/ folder.

**Execution:**

Navigate to your Airflow UI.

Locate the employee_data_analysis_with_spark DAG.

Trigger the DAG manually.
