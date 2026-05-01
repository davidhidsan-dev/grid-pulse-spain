FROM apache/airflow:2.10.5-python3.11

COPY airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
