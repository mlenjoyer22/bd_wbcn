ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.4.1-python3.10"
FROM ${AIRFLOW_BASE_IMAGE}

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.8.2 \
    pandas \
    pydantic \
    requests
