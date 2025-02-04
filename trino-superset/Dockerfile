FROM apache/superset:latest

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config

RUN pip install mysqlclient trino[sqlalchemy]

ENV ADMIN_USERNAME=admin
ENV ADMIN_EMAIL=nhanbui15122003@gmail.com
ENV ADMIN_PASSWORD=admin

COPY ./superset-conf/superset-init.sh /superset-init.sh

COPY ./superset-conf/superset_config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset
ENTRYPOINT [ "/superset-init.sh" ]
