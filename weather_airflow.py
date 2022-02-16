from airflow import DAG
from google.cloud import bigquery
from google.cloud import storage
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
import datetime as dt
import logging

default_args = {
    'owner': 'progmarine-dag',
    'start_date': dt.datetime.today(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}
project_id = 'elite-thunder-340219'
dataset = 'progmarine_'
BUCKET_NAME = 'europe-west1-progmarine-dag-e16533c0-bucket'

def file():
    import requests
    import pandas as pd
    api = 'c0a65097ebe6426d96c204833220302'
    cities = ['Tokyo','Delhi','Shanghai','Sao Paulo','Mexico' ,'Cairo','Mumbai',
              'Beijing','Dhaka','Osaka','New York','Karachi','Buenos Aires',
              'Chongqing','Istanbul','Kolkata','Manila','Lagos','Rio de Janeiro',
              'Tianjin','Kinshasa','Guangzhou','Los Angeles','Moscow','Shenzhen',
              'Lahore','Bangalore','Paris','Bogota','Jakarta','Chennai','Lima',
              'Bangkok','Seoul','Hyderabad','London','Tehran', 'Chicago','Chengdu',
              'Nanjing','Wuhan','Ho Chi Minh City','Luanda','Ahmedabad','Kuala Lumpur',
              'Xian','Hong Kong','Dongguan','Hangzhou','Kiev','Almaty']
    df = pd.DataFrame()
    temp = []
    city = []
    for i in citie
        res = requests.get(f"http://api.weatherapi.com/v1/current.json?key={api}&q={i}").json()['current']['temp_c']
        temp.append(res)
        city.append(i)
    df['City'] = city
    df['Temp'] = temp

    logging.info(df.head(5))
    return df.to_csv('weather_info.csv',index=False)
file_ = 'weather_info.csv'
def load_data():
    client = storage.Client(project=project_id)

    bucket = client.get_bucket(BUCKET_NAME)
    filename = '%s' % (file_)

    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)  
    

    logging.info('Upload complete')

with DAG('test',
         default_args=default_args,
         schedule_interval='*/20 * * * *',
         ) as dag:

    get_data = PythonOperator(
            task_id='get_data',
            python_callable=file,
            dag= dag
            )
    load_data = PythonOperator(
            task_id='load_data',
            python_callable=load_data,
            dag= dag
            )
  
    transfer_data = GoogleCloudStorageToBigQueryOperator(
            task_id = 'transfer_data',
            bucket = BUCKET_NAME,
            source_objects = ['weather_info.csv'],
            destination_project_dataset_table = f'{project_id}:{dataset}.test',
            schema_fields=[{"name": "CITY", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "TEMP", "type": "FLOAT", "mode": "NULLABLE"},
                          ],
            source_format='CSV',
            skip_leading_rows=1,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE', 
            # use this first time uploading to bq
            # write_disposition='WRITE_APPEND', 
            # schema_update_options=['ALLOW_FIELD_RELAXATION', 'ALLOW_FIELD_ADDITION'],
            autodetect=True,
            dag=dag
            )

    create_view = BigQueryCreateEmptyTableOperator(
    task_id="create_view",
    dataset_id=f'{dataset}',
    table_id='test_view',

    view={
        "query": f"SELECT * FROM `{project_id}.{dataset}.test` ORDER BY TEMP DESC LIMIT 10",
        "useLegacySql": False,
    },
)
    get_data >> load_data >> transfer_data >> create_view

