import requests
import pandas as pd
from airflow import DAG
from sqlalchemy.engine import URL
from sqlalchemy import text
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
import datetime
import logging

default_args={
    'owner':'Obarskaya Tatiana',
    'email':'obarskaia.tatiana@gmail.com',
    'start_date': datetime.datetime(2023, 11, 27),
    'email_on_failure': False,
    'email_on_retry': False,
}

DOMEN = 'www.wildberries.ru'

def get_pvz_info(domen):
    url = f'https://{domen}/webapi/spa/modules/pickups'
    headers = {'User-Agent': "Mozilla/5.0", 'content-type': "application/json", 'x-requested-with': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers)
    data = r.json()
    interest_keys = ['id', 'address', 'coordinates', 'fittingRooms', 'workTime', 'isWb',
                    'pickupType', 'dtype', 'isExternalPostamat', 'status', 'deliveryPrice']
    all_pvz_data = []
    for d in data['value']['pickups']:
        pvz = {}
        for key in interest_keys:
            try:
                pvz[key] = d[key]
            except KeyError:
                pvz[key] = None
        all_pvz_data.append(pvz)
    all_pvz_data = pd.DataFrame(all_pvz_data)
    print("[INFO] координаты точек выдачи получены")
    return all_pvz_data

def _pipeline():
    logging.info('Program has started. We will request wb points')
    all_pvz_data = get_pvz_info(domen=DOMEN)
    if all_pvz_data.shape[0] > 0:
        logging.info('We got all wb pvz coordinates')

with DAG(
    dag_id='wb_parse_status',
    default_args=default_args,
    description='Parsing wb pvz statuses and coordinates',
    schedule=None, #"0 0-23/4 * * *",
    catchup=False,
) as dag:

    wb_parsing=PythonOperator(
        task_id = 'pipeline',
        python_callable=_pipeline,
    )
    wb_parsing
