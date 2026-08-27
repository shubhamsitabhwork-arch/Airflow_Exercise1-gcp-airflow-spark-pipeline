# PySpark Script Explanation: `emp_batch_job.py`
This file is the "data worker" of the project. It is written in PySpark and is executed directly on the temporary Dataproc cluster to transform the data.

---

```python
from pyspark.sql import SparkSession
```
*   **What it does:** Imports `SparkSession` from the PySpark SQL library. `SparkSession` is the single entry point required to interact with underlying Spark functionality and create DataFrames.

```python
def process_data():
```
*   **What it does:** Defines a Python function named `process_data` that encapsulates the entire data processing logic. Wrapping code in a function is a Python best practice to keep it organized and reusable.

```python
    spark = SparkSession.builder.appName("GCPDataprocJob").getOrCreate()
```
*   **What it does:** Initializes the Spark application. 
    *   `.builder` constructs the session.
    *   `.appName("GCPDataprocJob")` names your job so you can easily identify it in the Spark UI or logs.
    *   `.getOrCreate()` tells Spark to either fetch an existing session (if one is running) or create a new one to save memory.

```python
    # Define your GCS bucket and paths
    bucket = "airflow-test-projects-gds-dev"
    emp_data_path = f"gs://{bucket}/airflow-project-1/data/employee.csv"
    dept_data_path = f"gs://{bucket}/airflow-project-1/data/department.csv"
    output_path = f"gs://{bucket}/airflow-project-1/output"
```
*   **What it does:** Sets up the string variables for the Google Cloud Storage (GCS) locations. 
    *   It defines the main `bucket` name. 
    *   It uses Python "f-strings" (`f"..."`) to dynamically inject the `bucket` variable into the exact URI paths for the input CSVs and the final output folder. `gs://` tells Spark to look in Google Cloud Storage.

```python
    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)
    department = spark.read.csv(dept_data_path, header=True, inferSchema=True)
```
*   **What it does:** Instructs Spark to read the CSV files from GCS into Spark DataFrames (tables). 
    *   `header=True` tells Spark that the first row contains column names.
    *   `inferSchema=True` tells Spark to automatically figure out the data types (e.g., interpreting numbers as Integers instead of Strings).

```python
    # Filter employee data
    filtered_employee = employee.filter(employee.salary > 50000)
```
*   **What it does:** Creates a new DataFrame called `filtered_employee` by applying a transformation. It scans the `employee` DataFrame and keeps only the rows where the value in the `salary` column is strictly greater than 50,000.

```python
    # Join datasets
    joined_data = filtered_employee.join(department, "dept_id", "inner")
```
*   **What it does:** Merges the `filtered_employee` DataFrame with the `department` DataFrame. 
    *   `"dept_id"` specifies the common column to join on.
    *   `"inner"` specifies an inner join, meaning it will only keep records where a `dept_id` exists in *both* tables.

```python
    # Write output
    joined_data.write.csv(output_path, mode="overwrite", header=True)
```
*   **What it does:** Saves the final `joined_data` DataFrame back to the GCS bucket. 
    *   `mode="overwrite"` ensures that if the job runs multiple times, it will replace the old output folder instead of crashing due to files already existing.
    *   `header=True` writes the column names at the top of the output CSV files.

```python
    spark.stop()
```
*   **What it does:** Gracefully shuts down the Spark application and releases the memory/resources it was using on the cluster.

```python
if __name__ == "__main__":
    process_data()
```
*   **What it does:** This is a standard Python idiom. It checks if this script is being run directly. If true, it triggers the `process_data()` function defined above.