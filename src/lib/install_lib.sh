# for docker image builds that have local lib dependencies, install those
# libs in editable mode inside the running containers
pip install -e /opt/airflow/lib/lib-a
pip install -e /opt/airflow/lib/lib-b