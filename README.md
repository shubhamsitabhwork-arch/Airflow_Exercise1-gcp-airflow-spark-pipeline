# GCP Airflow & PySpark ETL Pipeline

This repository contains an automated Data Engineering pipeline orchestrated by Apache Airflow. The pipeline provisions an ephemeral Google Cloud Dataproc cluster, executes a distributed PySpark batch job to process and join CSV files stored in Google Cloud Storage (GCS), and cleanly tears down the cluster upon completion to optimize costs.

## 🏗️ Architecture & Pipeline Flow

The pipeline consists of three main steps managed by Airflow:
1. **Create:** Provision a GCP Dataproc Cluster.
2. **Execute:** Run a PySpark job to read, filter, join, and write data.
3. **Delete:** Tear down the cluster to save costs.

### Process Flowchart
*(If you are viewing this on GitHub, the flowchart below will render automatically. You can also paste this code into Draw.io > Insert > Advanced > Mermaid).*

```mermaid
graph TD
    classDef airflow fill:#f9f6fd,stroke:#b19cd9,stroke-width:2px;
    classDef storage fill:#e8f5e9,stroke:#4caf50,stroke-width:2px;
    classDef spark fill:#fff3e0,stroke:#ff9800,stroke-width:2px;

    subgraph DAG [Airflow Orchestration]
        T1(1. Create Dataproc Cluster):::airflow
        T2(2. Submit PySpark Job):::airflow
        T3(3. Delete Dataproc Cluster):::airflow
    end

    subgraph GCS [Google Cloud Storage]
        F1[(employee.csv)]:::storage
        F2[(department.csv)]:::storage
        F3[(Output Folder)]:::storage
    end

    subgraph Spark [PySpark Execution on Dataproc]
        S1[Read CSVs & Infer Schema]:::spark
        S2[Filter: Salary > 50000]:::spark
        S3[Inner Join on dept_id]:::spark
        S4[Write output back to GCS]:::spark
    end

    T1 -->|On Success| T2
    T2 -->|Trigger: all_done| T3
    T2 -.->|Executes emp_batch_job.py| S1
    F1 -.->|Read| S1
    F2 -.->|Read| S1
    S1 --> S2
    S2 --> S3
    S3 --> S4
    S4 -.->|Write Overwrite| F3


📁 Project Structure
airflow_spark_job.py: Airflow DAG definition for cluster management and job submission.

emp_batch_job.py: PySpark script containing the ETL logic.

employee.csv: Source data containing employee details.

department.csv: Source data containing department mapping.

🚀 Setup and Deployment
GCP Requirements:

A GCP Project with billing enabled.

APIs enabled: Cloud Dataproc API, Cloud Storage API.

A Service Account with Dataproc Worker and Storage Object Admin permissions.

Deployment Steps:

Upload employee.csv and department.csv to your GCS bucket: gs://airflow-projetcs-gds-dev/airflow-project-1/data/.

Upload emp_batch_job.py to gs://airflow-projetcs-gds-dev/airflow-project-1/spark-job/.

Place airflow_spark_job.py in your Airflow dags/ folder.

Execution:

Navigate to your Airflow UI.

Locate the employee_data_analysis_with_spark DAG.

Trigger the DAG manually.