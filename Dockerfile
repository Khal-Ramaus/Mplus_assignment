FROM apache/airflow:3.1.2
USER airflow
RUN pip install pandas numpy openpyxl