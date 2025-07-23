# ETL Toll Data Pipeline with Apache Airflow

This project demonstrates a complete ETL pipeline for processing toll data using Apache Airflow. It consists of several Bash tasks orchestrated in a DAG that performs extraction, transformation, and consolidation of data from multiple sources.

##  Technologies
- Apache Airflow
- Bash scripting
- CSV, TSV, and fixed-width data handling
- Python

##  Tasks Overview
1. **Unzip Raw Data**  
   Extracts the compressed `.tgz` archive.

2. **Extract CSV Data**  
   Extracts the first 4 fields from a CSV.

3. **Extract TSV Data**  
   Extracts fields 5-7 from a TSV file.

4. **Extract Fixed Width Data**  
   Extracts columns 59-67 from a fixed-width formatted text file.

5. **Consolidate Data**  
   Combines the extracted data into a single CSV file.

6. **Transform Data**  
   Modifies specific columns (e.g. converts fields to uppercase) for final output.

##  File
- `ETL_toll_data.py`: Airflow DAG defining all ETL steps.

##  How to Use
1. Place the DAG file inside your Airflow `dags/` directory.
2. Make sure all paths in the script match your local Airflow setup.
3. Start the Airflow scheduler and webserver.
4. Trigger the DAG `ETL_toll_data` via the Airflow UI.

---

##  Author
Created as part of a learning project on data engineering with Airflow.

