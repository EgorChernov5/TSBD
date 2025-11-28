FROM apache/airflow:2.11.0

USER root

RUN apt-get update && apt-get install -y libpq-dev gcc python3-dev ffmpeg libsm6 libxext6 fonts-liberation && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN fc-cache -f -v

USER airflow
WORKDIR /app/code

COPY deploy/images/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY src src
COPY nodes nodes

ENV PYTHONPATH="/app/code:${PYTHONPATH}"

COPY --chown=airflow:root deploy/config/airflow.cfg /opt/airflow/config/airflow.cfg
COPY --chown=airflow:root deploy/dags /opt/airflow/dags
