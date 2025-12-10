ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.11.0-python3.11
FROM ${AIRFLOW_IMAGE_NAME}

ARG AIRFLOW_USER=airflow
USER ${AIRFLOW_USER}

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
