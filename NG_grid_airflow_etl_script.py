# Import ncessary libraries

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time


# Extract

# Fetch data from TCN url... 
# handle retries and backoffs on failure... 
# store fetched DataFrame in XCom for downstream use by the 'clean_data' function.

def fetch_data(ds, **kwargs):
    url = "https://www.niggrid.org/GenerationProfile2"
    session = requests.Session()
    session.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    }
    
    date = datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=1)
    retries = 3
    backoff_factor = 2
    
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            viewstate = soup.find('input', {'id': '__VIEWSTATE'})['value']
            eventvalidation = soup.find('input', {'id': '__EVENTVALIDATION'})['value']
            data = {
                '__VIEWSTATE': viewstate,
                '__EVENTVALIDATION': eventvalidation,
                'ctl00$MainContent$txtReadingDate': date.strftime('%Y/%m/%d'),
                'ctl00$MainContent$btnGetReadings': 'Get Generation'
            }
            response = session.post(url, data=data, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'id': 'MainContent_gvGeneration'})
            all_rows = []
            for row in table.find_all('tr')[1:]:
                columns = row.find_all('td')
                row_data = [col.text.strip() for col in columns]
                all_rows.append(row_data)
            df = pd.DataFrame(all_rows, columns=[th.text for th in table.find('tr').find_all('th')])
            ti = kwargs['ti']
            ti.xcom_push(key='energy_data', value=df.to_json(date_format='iso', orient='split'))
            return df
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(backoff_factor ** attempt)
    return pd.DataFrame()





# Transform

# Retrieve the fetched data from XCom...
# Perform cleaning operations...
# Store the cleaned DataFrame in XCom for use by the 'load_to_s3' function.

def clean_data(ti):
    df_json = ti.xcom_pull(task_ids='fetch_data', key='energy_data')
    if df_json:
        df_fetch = pd.read_json(df_json, orient='split')
        if not df_fetch.empty:
            if '#' in df_fetch.columns:
                df_fetch.drop(['#'], axis=1, inplace=True)
            if df_fetch.at[df_fetch.index[-1], 'Genco'] == '':
                df_fetch.drop(df_fetch.index[-1], inplace=True)
            for i in df_fetch.columns:
                if i != 'Genco':
                    df_fetch[i] = df_fetch[i].str.replace(',', '').astype(float)
            ti.xcom_push(key='df_clean', value=df_fetch.to_json(date_format='iso', orient='split'))
        else:
            print("DataFrame is empty after cleaning.")
    else:
        print("No data received from fetch_data.")





# Load

# Retrieve the cleaned data from XCom...
# Convert it to CSV format...
# Upload the CSV file to an S3 bucket.

def load_to_s3(ti, ds):
    df_json = ti.xcom_pull(task_ids='clean_data', key='df_clean')
    if df_json:
        df_clean = pd.read_json(df_json, orient='split')
        current_date = datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=1)
        s3_path = f"energy_gen_data_{current_date.strftime('%Y_%m_%d')}.csv"
        s3_hook = S3Hook(aws_conn_id='aws_conn')
        s3_hook.load_string(df_clean.to_csv(index=False), s3_path, bucket_name='ng-grid-data-bucket', replace=True)
    else:
        print("No data received from previous task.")




# Instantiating default arguments for Airflow

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 11),
    'email': ['tosin.ademiluabi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Setting up DAG to run at 06:00 UTC everyday

with DAG('daily_energy_data_collection', 
         default_args=default_args, 
         description='A DAG to fetch, clean, and upload energy data daily', 
         schedule_interval='0 6 * * *', 
         catchup=False, 
         max_active_runs=1) as dag:
    
    
    
    extract_data = PythonOperator(
        task_id='fetch_data',
        provide_context=True,
        python_callable=fetch_data
    )
    
    transform_data = PythonOperator(
        task_id='clean_data',
        provide_context=True,
        python_callable=clean_data
    )

    load_to_s3 = PythonOperator(
        task_id='load_to_s3',

        provide_context=True,
        python_callable=load_to_s3
    )



extract_data >> transform_data >> load_to_s3 
