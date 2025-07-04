from datetime import timedelta
import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default args for Airflow tasks
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Bronze→Silver→Gold data pipeline',
    schedule_interval=None,   # or e.g. '@daily'
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example']
) as dag:

    def load_customers():
        """Bronze: Load customer dimension CSV into Postgres."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_csv('/opt/airflow/data/dim_customers.csv')
        df.to_sql('bronze_customers', engine, if_exists='replace', index=False)

    def load_products():
        """Bronze: Load product dimension CSV into Postgres."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_csv('/opt/airflow/data/dim_products.csv')
        df.to_sql('bronze_products', engine, if_exists='replace', index=False)

    def load_dates():
        """Bronze: Load date dimension CSV into Postgres."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_csv('/opt/airflow/data/dim_date.csv')
        df.to_sql('bronze_dates', engine, if_exists='replace', index=False)

    def load_targets():
        """Bronze: Load targets CSV into Postgres."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_csv('/opt/airflow/data/dim_targets_orders.csv')
        df.to_sql('bronze_targets', engine, if_exists='replace', index=False)

    def load_order_lines():
        """Bronze: Load order lines fact CSV into Postgres."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_csv('/opt/airflow/data/fact_order_lines.csv')
        # Rename columns with spaces for consistency
        df = df.rename(columns={
            'In Full':'In_Full', 'On Time':'On_Time', 'On Time In Full':'On_Time_In_Full'
        })
        df.to_sql('bronze_order_lines', engine, if_exists='replace', index=False)

    def load_order_agg():
        """Bronze: Load orders aggregate fact CSV into Postgres."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_csv('/opt/airflow/data/fact_orders_aggregate.csv')
        # Rename columns to clear names
        df = df.rename(columns={'IF':'In_Full', 'otif':'On_Time_In_Full'})
        df.to_sql('bronze_orders_agg', engine, if_exists='replace', index=False)

    def transform_silver():
        """Silver: Clean and join raw tables into a single silver table."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        # Read raw tables into Pandas
        customers = pd.read_sql('SELECT * FROM bronze_customers', engine)
        products = pd.read_sql('SELECT * FROM bronze_products', engine)
        dates = pd.read_sql('SELECT * FROM bronze_dates', engine)
        targets = pd.read_sql('SELECT * FROM bronze_targets', engine)
        lines = pd.read_sql('SELECT * FROM bronze_order_lines', engine)
        orders = pd.read_sql('SELECT * FROM bronze_orders_agg', engine)

        # Join order lines with order aggregates on order_id (and date/customer)
        # Drop duplicate columns from lines (we trust orders table for metrics)
        lines = lines.drop(columns=['In_Full','On_Time','On_Time_In_Full'])
        merged = pd.merge(lines, orders, how='left',
                          on=['order_id','order_placement_date','customer_id'])

        # Join with dimension tables for enrichment
        merged = pd.merge(merged, customers, on='customer_id', how='left')
        merged = pd.merge(merged, products, on='product_id', how='left')
        merged = pd.merge(merged, targets, on='customer_id', how='left')
        # Join on dates (match order date to date dimension)
        merged = pd.merge(merged, dates, left_on='order_placement_date', right_on='date', how='left')
        merged = merged.drop(columns=['date'])  # drop duplicate join column

        # Convert date strings to proper date type (YYYY-MM-DD)
        merged['order_placement_date'] = pd.to_datetime(merged['order_placement_date'], format='%d-%b-%y')
        merged['agreed_delivery_date']  = pd.to_datetime(merged['agreed_delivery_date'], format='%d-%b-%y')
        merged['actual_delivery_date']  = pd.to_datetime(merged['actual_delivery_date'], format='%d-%b-%y')

        # Write cleaned/joined data to Silver table
        merged.to_sql('silver_orders', engine, if_exists='replace', index=False)

    def create_gold():
        """Gold: Aggregate Silver table into final summary metrics."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = hook.get_sqlalchemy_engine()
        # Read silver table
        silver = pd.read_sql('SELECT * FROM silver_orders', engine)

        # Compute metrics by city and month (mmm_yy column from date dim)
        # First dedupe by order to avoid double-counting multi-line orders
        silver_orders = silver[['order_id','city','mmm_yy','On_Time_y','In_Full_y','On_Time_In_Full_y']].drop_duplicates()
        summary = silver_orders.groupby(['city','mmm_yy']).agg(
            total_orders=('order_id','nunique'),
            on_time_rate=('On_Time_y','mean'),
            in_full_rate=('In_Full_y','mean'),
            otif_rate=('On_Time_In_Full_y','mean')
        ).reset_index()

        # Convert rates to percentages
        summary['on_time_pct']  = summary['on_time_rate'] * 100
        summary['in_full_pct']  = summary['in_full_rate'] * 100
        summary['otif_pct']     = summary['otif_rate'] * 100
        # Select and rename final columns
        gold_df = summary[['city','mmm_yy','total_orders','on_time_pct','in_full_pct','otif_pct']]

        # Write final Gold table back to Postgres (or output CSV)
        gold_df.to_sql('gold_summary', engine, if_exists='replace', index=False)
        gold_df.to_csv('/opt/airflow/data/final_gold_layer.csv', index=False)  # also export CSV

    # Define tasks using PythonOperator
    t1 = PythonOperator(task_id='load_customers', python_callable=load_customers)
    t2 = PythonOperator(task_id='load_products', python_callable=load_products)
    t3 = PythonOperator(task_id='load_dates', python_callable=load_dates)
    t4 = PythonOperator(task_id='load_targets', python_callable=load_targets)
    t5 = PythonOperator(task_id='load_order_lines', python_callable=load_order_lines)
    t6 = PythonOperator(task_id='load_order_agg', python_callable=load_order_agg)
    t7 = PythonOperator(task_id='transform_silver', python_callable=transform_silver)
    t8 = PythonOperator(task_id='create_gold', python_callable=create_gold)

    # Set task dependencies (Bronze → Silver → Gold)
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
