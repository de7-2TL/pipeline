FROM apache/airflow:2.9.1-python3.11

COPY requirements.txt .
COPY lib /opt/airflow/lib


RUN pip install --no-cache-dir -r requirements.txt
USER root
RUN chown -R airflow /opt/airflow/lib
USER airflow
RUN sh /opt/airflow/lib/install_lib.sh
