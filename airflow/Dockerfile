FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt update && \
    apt-get -y install libpq-dev gcc && \
    apt-get clean;

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt