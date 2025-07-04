FROM apache/airflow:2.6.0
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy our DAG into Airflow’s dags directory
COPY --chown=airflow:root dags/medallion_pipeline_dag.py /opt/airflow/dags/
# Set Airflow config via ENV (disable examples, point to Postgres)
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
