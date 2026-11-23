FROM apache/airflow:3.3.1

# Install Airflow and dbt dependencies
COPY airflow/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements.txt

# Package Airflow DAGs
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/

# Package dbt project, including models and macros
COPY --chown=airflow:root snowflake_dbt_project/ /opt/airflow/dbt/
