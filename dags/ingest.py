from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import io

# Define default arguments for all tasks in the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

def get_global_csv_files_from_github():
    """Get list of all CSV files from the GitHub directory"""
    api_url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports"
    
    response = requests.get(api_url)
    response.raise_for_status()
    
    files = response.json()
    csv_files = [f for f in files if f['name'].endswith('.csv')]
    
    # Store file info for next task
    return [{'name': f['name'], 'download_url': f['download_url']} for f in csv_files]


def load_global_data_to_postgres(**context):
    """Download CSVs and load into PostgreSQL"""
    ti = context['ti']
    csv_files = ti.xcom_pull(task_ids='get_global_csv_files')
    
    # Get PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    for file_info in csv_files:
        try:
            print(f"Processing {file_info['name']}...")
            
            # Download CSV
            response = requests.get(file_info['download_url'])
            response.raise_for_status()
            
            # Read CSV into pandas
            df = pd.read_csv(io.StringIO(response.text))
            
            # Clean column names (remove spaces, special chars)
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')
            
            # Extract date from filename (e.g., 01-22-2020.csv)
            date_str = file_info['name'].replace('.csv', '')
            
            # Create table name from filename: global_01_22_2020
            table_name = f"global_{date_str.replace('-', '_')}"
            
            # Load to PostgreSQL bronze schema (one table per CSV file)
            df.to_sql(
                table_name,
                engine,
                schema='bronze',
                if_exists='replace',  # Replace if table already exists
                index=False,
                method='multi'
            )
            
            print(f"Loaded {len(df)} rows from {file_info['name']}")
        except Exception as e:
            print(f"Error processing {file_info['name']}: {str(e)}")
            continue
    
    print(f"Successfully loaded {len(csv_files)} files!")


def get_us_csv_files_from_github():
    """Get list of all US CSV files from the GitHub directory"""
    api_url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports_us"
    
    response = requests.get(api_url)
    response.raise_for_status()
    
    files = response.json()
    csv_files = [f for f in files if f['name'].endswith('.csv')]
    
    # Store file info for next task
    return [{'name': f['name'], 'download_url': f['download_url']} for f in csv_files]


def load_us_data_to_postgres(**context):
    """Download US CSVs and load into PostgreSQL"""
    ti = context['ti']
    csv_files = ti.xcom_pull(task_ids='get_us_csv_files')
    
    # Get PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    for file_info in csv_files:
        try:
            print(f"Processing {file_info['name']}...")
            
            # Download CSV
            response = requests.get(file_info['download_url'])
            response.raise_for_status()
            
            # Read CSV into pandas
            df = pd.read_csv(io.StringIO(response.text))
            
            # Clean column names (remove spaces, special chars)
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_')
            
            # Extract date from filename (e.g., 01-22-2020.csv)
            date_str = file_info['name'].replace('.csv', '')
            
            # Create table name from filename: us_01_22_2020
            table_name = f"us_{date_str.replace('-', '_')}"
            
            # Load to PostgreSQL bronze schema (one table per CSV file)
            df.to_sql(
                table_name,
                engine,
                schema='bronze',
                if_exists='replace',  # Replace if table already exists
                index=False,
                method='multi'
            )
            
            print(f"Loaded {len(df)} rows from {file_info['name']}")
        except Exception as e:
            print(f"Error processing {file_info['name']}: {str(e)}")
            continue
    
    print(f"Successfully loaded {len(csv_files)} files!")


with DAG(
      dag_id='ota-ingest',
      dag_display_name='Ingest COVID-19 Data to PostgreSQL',
      description='Ingest COVID-19 CSV data to PostgreSQL',
      tags=['ota', 'ingest'],
      catchup=False,
      default_args=default_args
) as dag:
      
      get_global_csv_files = PythonOperator(
            task_id='get_global_csv_files',
            python_callable=get_global_csv_files_from_github,
      )
      
      load_global_data_to_postgres = PythonOperator(
            task_id='load_global_data_to_postgres',
            python_callable=load_global_data_to_postgres,
      )

      get_us_csv_files = PythonOperator(
            task_id='get_us_csv_files',
            python_callable=get_us_csv_files_from_github,
      )
      
      load_us_data_to_postgres = PythonOperator(
            task_id='load_us_data_to_postgres',
            python_callable=load_us_data_to_postgres,
      )
      
      # Task dependencies - get tasks run in parallel, each followed by their load task
      [get_global_csv_files, get_us_csv_files] >> load_global_data_to_postgres >> load_us_data_to_postgres
