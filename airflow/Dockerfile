FROM apache/airflow:latest-python3.12

USER root

RUN mkdir -p /opt/airflow/logs /tmp && \
    chown -R 50000:0 /opt/airflow/logs

COPY requirements.txt /tmp/

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt
