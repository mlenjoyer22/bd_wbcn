from datetime import datetime, timedelta
import time
import telebot
import os
import copy
import numpy as np
import pandas as pd
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from sklearn.neighbors import BallTree
import keplergl
from keplergl import KeplerGl

default_args={
    'owner':'mlenjoyer',
    'email':'mlenjoyer22@gmail.com',
    'start_date': datetime(2023, 11, 27),
    'email_on_failure': False,
    'email_on_retry': False,
}

USER = 'obarskaia.tatiana@gmail.com'
TOKEN = '1ede87981b6644c8322673431c59ccb9'
SLEEP_TIME = 5 # sleep time in between requests, not counting processing time
DISTANCE = 2000 # radius to search for loaded wb point
DOMEN = 'www.wildberries.ru'
BOT_TOKEN = "6520068688:AAFsISoEMAMhZNVnBkBX09YtOfGel1WhgZQ"
BOT_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"


PARAM_DICT={
    'user':USER, # Имя пользователя при регистрации на ads-api.ru
    'token':TOKEN, # Токен API на ads-api.ru
    'category_id':7, # 1-недвижимость, 2-квартиры, 3-комнаты, 4-дома, 5-ЗУ, 6-гаражи, 7-коммерческая. 
    'price1':None, # Цена от
    'price2':450000, # Цена до
    'date1':(datetime.today() - timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S"), # Дата от
    'date2':datetime.today().strftime("%Y-%m-%d %H:%M:%S"), # Дата до
    'city':'Москва', # Город или регион поиска. Можно несколько, пример: "Москва|Тула"
    'nedvigimost_type':'2', # 1-продам, 2-сдам, 3-куплю, 4-сниму. Можно через запятую.
    'source':'1, 4', # 1 - avito.ru, 2 - irr.ru, 3 - realty.yandex.ru, 4 - cian.ru
    'withcoords':1, # Если 1 - только объявления с координатами
}

config = {'version': 'v1',
 'config': {'visState': {'filters': [],
   'layers': [{'id': 'mgi0vy',
     'type': 'point',
     'config': {'dataId': 'Realty from cian',
      'label': 'Realty',
      'color': [130, 154, 227],
      'highlightColor': [252, 242, 26, 255],
      'columns': {'lat': 'lat', 'lng': 'lon', 'altitude': None},
      'isVisible': True,
      'visConfig': {'radius': 33.2,
       'fixedRadius': False,
       'opacity': 0.67,
       'outline': False,
       'thickness': 2,
       'strokeColor': None,
       'colorRange': {'name': 'Uber Viz Diverging 1.5',
        'type': 'diverging',
        'category': 'Uber',
        'colors': ['#00939C',
         '#5DBABF',
         '#BAE1E2',
         '#F8C0AA',
         '#DD7755',
         '#C22E00']},
       'strokeColorRange': {'name': 'Global Warming',
        'type': 'sequential',
        'category': 'Uber',
        'colors': ['#5A1846',
         '#900C3F',
         '#C70039',
         '#E3611C',
         '#F1920E',
         '#FFC300']},
       'radiusRange': [0, 50],
       'filled': True},
      'hidden': False,
      'textLabel': [{'field': None,
        'color': [255, 255, 255],
        'size': 18,
        'offset': [0, 0],
        'anchor': 'start',
        'alignment': 'center'}]},
     'visualChannels': {'colorField': {'name': 'loaded_offices_cnt',
       'type': 'integer'},
      'colorScale': 'quantize',
      'strokeColorField': None,
      'strokeColorScale': 'quantile',
      'sizeField': None,
      'sizeScale': 'linear'}},
    {'id': '5j7h73m',
     'type': 'point',
     'config': {'dataId': 'WB Loaded PVZ',
      'label': 'Wb Loaded',
      'color': [231, 159, 213],
      'highlightColor': [252, 242, 26, 255],
      'columns': {'lat': 'lat', 'lng': 'lon', 'altitude': None},
      'isVisible': True,
      'visConfig': {'radius': 14.6,
       'fixedRadius': False,
       'opacity': 0.8,
       'outline': False,
       'thickness': 2,
       'strokeColor': None,
       'colorRange': {'name': 'ColorBrewer PuRd-3',
        'type': 'sequential',
        'category': 'ColorBrewer',
        'colors': ['#dd1c77', '#c994c7', '#e7e1ef'],
        'reversed': True},
       'strokeColorRange': {'name': 'Global Warming',
        'type': 'sequential',
        'category': 'Uber',
        'colors': ['#5A1846',
         '#900C3F',
         '#C70039',
         '#E3611C',
         '#F1920E',
         '#FFC300']},
       'radiusRange': [0, 50],
       'filled': True},
      'hidden': False,
      'textLabel': [{'field': None,
        'color': [255, 255, 255],
        'size': 18,
        'offset': [0, 0],
        'anchor': 'start',
        'alignment': 'center'}]},
     'visualChannels': {'colorField': {'name': 'deliveryPrice',
       'type': 'integer'},
      'colorScale': 'quantize',
      'strokeColorField': None,
      'strokeColorScale': 'quantile',
      'sizeField': None,
      'sizeScale': 'linear'}},
    {'id': 'y3vlf09',
     'type': 'point',
     'config': {'dataId': 'WB not Loaded PVZ',
      'label': 'Wb Non Loaded',
      'color': [30, 150, 190],
      'highlightColor': [252, 242, 26, 255],
      'columns': {'lat': 'lat', 'lng': 'lon', 'altitude': None},
      'isVisible': False,
      'visConfig': {'radius': 10,
       'fixedRadius': False,
       'opacity': 0.8,
       'outline': False,
       'thickness': 2,
       'strokeColor': None,
       'colorRange': {'name': 'Global Warming',
        'type': 'sequential',
        'category': 'Uber',
        'colors': ['#5A1846',
         '#900C3F',
         '#C70039',
         '#E3611C',
         '#F1920E',
         '#FFC300']},
       'strokeColorRange': {'name': 'Global Warming',
        'type': 'sequential',
        'category': 'Uber',
        'colors': ['#5A1846',
         '#900C3F',
         '#C70039',
         '#E3611C',
         '#F1920E',
         '#FFC300']},
       'radiusRange': [0, 50],
       'filled': True},
      'hidden': False,
      'textLabel': [{'field': None,
        'color': [255, 255, 255],
        'size': 18,
        'offset': [0, 0],
        'anchor': 'start',
        'alignment': 'center'}]},
     'visualChannels': {'colorField': None,
      'colorScale': 'quantile',
      'strokeColorField': None,
      'strokeColorScale': 'quantile',
      'sizeField': None,
      'sizeScale': 'linear'}}],
   'interactionConfig': {'tooltip': {'fieldsToShow': {'Realty from cian': [{'name': 'ref',
        'format': None},
        {'name':'price', 'format': None},
       {'name': 'square', 'format': None},
       {'name': 'loaded_offices_cnt', 'format': None},
       {'name': 'phone', 'format': None},
       {'name': 'km_do_metro', 'format': None}],
      'WB Loaded PVZ': [{'name': 'office_id', 'format': None},
       {'name': 'deliveryPrice', 'format': None}],
      'WB not Loaded PVZ': [{'name': 'office_id', 'format': None},
       {'name': 'deliveryPrice', 'format': None}]},
     'compareMode': False,
     'compareType': 'absolute',
     'enabled': True},
    'brush': {'size': 0.5, 'enabled': False},
    'geocoder': {'enabled': False},
    'coordinate': {'enabled': False}},
   'layerBlending': 'normal',
   'splitMaps': [],
   'animationConfig': {'currentTime': None, 'speed': 1}},
  'mapState': {'bearing': 0,
   'dragRotate': False,
   'latitude': 55.776780524595935,
   'longitude': 37.59319029411319,
   'pitch': 0,
   'zoom': 10.365605466119206,
   'isSplit': True},
  'mapStyle': {'styleType': 'dark',
   'topLayerGroups': {'label': True,
    'road': True,
    'building': True,
    'water': True,
    'land': True},
   'visibleLayerGroups': {'label': True,
    'road': True,
    'border': False,
    'building': True,
    'water': True,
    'land': True,
    '3d building': False},
   'threeDBuildingColor': [9.665468314072013,
    17.18305478057247,
    31.1442867897876],
   'mapStyles': {}}}}

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
    logging.info("[INFO] координаты точек выдачи получены")
    return all_pvz_data

def generate_url(par_dict):
    url = 'https://ads-api.ru/main/api?'
    for key in par_dict.keys():
        if par_dict[key]: # if key in dict is not None
            url += f'&{key}={str(par_dict[key])}' # adding this part of request to URL   
    return url

def parse_realty(): 
    realty_df = pd.DataFrame()
    while True:
        time.sleep(SLEEP_TIME)
        url = generate_url(PARAM_DICT)
        try:
            response = requests.get(url)
            if response.status_code == 200 and 'data' in response.json():
                times = [el['time'] for el in response.json()['data']]
                if len(times) == 50:
                    PARAM_DICT['date2'] = min(times)
                    realty_df = pd.concat([realty_df, pd.DataFrame(response.json()['data'])])
                elif len(times) < 50:
                    realty_df = pd.concat([realty_df, pd.DataFrame(response.json()['data'])])
                    return realty_df
        except Exception as e:
            print('', e, '', sep='\n')
            return realty_df

def process_realty_df(realty_df):
    realty_df['lat'] = realty_df['coords'].apply(lambda x: x['lat'])
    realty_df['lon']  = realty_df['coords'].apply(lambda x: x['lng'])
    realty_df['lat'] = realty_df['lat'].astype('float')
    realty_df['lon'] = realty_df['lon'].astype('float')
    realty_df = realty_df[['url', 'title', 'price', 'phone', 'time', 'city', 'metro', 'address', 'description','id', 'km_do_metro', 'param_4922', 'lat', 'lon']]
    realty_df = realty_df.rename(columns={'param_4922':'square'})
    realty_df = realty_df.reset_index(drop=True)
    return realty_df


def find_loaded_neighbours(wb_pvz_loaded, realty_df):
    left_df = realty_df.copy()
    right_df = wb_pvz_loaded.copy()
    R = 6371008.7714 # in meters
    left_df['lat_rad'] = left_df['lat'].apply(np.deg2rad)
    left_df['lon_rad'] = left_df['lon'].apply(np.deg2rad)
    right_df['lat_rad'] = right_df['lat'].apply(np.deg2rad)
    right_df['lon_rad'] = right_df['lon'].apply(np.deg2rad)

    tree = BallTree(right_df[['lat_rad', 'lon_rad']], metric='haversine', leaf_size=2)

    radius = DISTANCE / R 
    indices, distances = tree.query_radius(left_df[['lat_rad', 'lon_rad']], r=radius, 
                                           return_distance=True, sort_results=True)
    office_ids = [list(right_df.iloc[indices[i]].office_id) for i in range(len(indices))]
    delivery_prices = [list(right_df.iloc[indices[i]].deliveryPrice) for i in range(len(indices))]

    distances = [[round(dist * R) for dist in distances[i]][:] for i in range(len(distances))]
    office_ids = [[id for id, dist in zip(off_ids, dists) if dist > 100] for (off_ids, dists) in zip(office_ids, distances)]
    distances = [[dist for id, dist in zip(off_ids, dists) if dist > 100] for (off_ids, dists) in zip(office_ids, distances)]
    mean_delivery_price = [np.mean(right_df[right_df.office_id.isin(ofcs)].deliveryPrice) for ofcs in office_ids]

    cnt_office_ids = [len(lst) for lst in office_ids]
    left_df['loaded_offices_cnt'] = cnt_office_ids
    left_df['loaded_offices_dists'] = distances
    left_df['loaded_offices_ids'] = office_ids
    left_df['mean_delivery_price'] = mean_delivery_price
    left_df = left_df.drop(['lon_rad', 'lat_rad'], axis=1)
    return left_df

def filter_loaded_advertisement(loaded_cian):
    loaded_cian = loaded_cian.sort_values(by=['loaded_offices_cnt', 'mean_delivery_price'], ascending=[False, False])
    loaded_cian = loaded_cian[loaded_cian['loaded_offices_cnt'] > 0]
    loaded_cian = loaded_cian.reset_index(drop=True)
    return loaded_cian

def define_restricted_realty_load(realty_df, wb_pvz):
    left_df = realty_df.copy()
    right_df = wb_pvz.copy()
    R = 6371008.7714 # in meters
    logging.info(' '.join(right_df.columns))
    left_df['lat_rad'] = left_df['lat'].apply(np.deg2rad)
    left_df['lon_rad'] = left_df['lon'].apply(np.deg2rad)
    right_df['lat_rad'] = right_df['lat'].apply(np.deg2rad)
    right_df['lon_rad'] = right_df['lon'].apply(np.deg2rad)

    tree = BallTree(right_df[['lat_rad', 'lon_rad']], metric='haversine', leaf_size=2)

    radius = DISTANCE / R
    indices, distances = tree.query_radius(left_df[['lat_rad', 'lon_rad']], r=radius, 
                                           return_distance=True, sort_results=True)
    office_ids = [list(right_df.iloc[indices[i]].office_id) for i in range(len(indices))]
    delivery_prices = [list(right_df.iloc[indices[i]].deliveryPrice) for i in range(len(indices))]
    distances = [[round(dist * R) for dist in distances[i]][:] for i in range(len(distances))]

    # проверка, что wb может открываться в этом месте - расстояние до ближайшего пвз > 100 метров
    distance_restriction = [len([dist for dist in dists if dist < 100]) for dists in distances]
    realty_with_restriction = left_df[pd.Series([True if cls==0 else False for cls in distance_restriction])].copy()
    # запрашиваем еще раз соседей для сокращенного датасет после проверки дистанции
    indices, distances = tree.query_radius(realty_with_restriction[['lat_rad', 'lon_rad']], r=radius, 
                                           return_distance=True, sort_results=True)
    distances = [[round(dist * R) for dist in distances[i]][:] for i in range(len(distances))]

    loaded_status_bool = [list(right_df.iloc[ind].deliveryPrice > 0) for ind in indices]
    loaded_offices = [right_df.iloc[ind][right_df.iloc[ind].deliveryPrice > 0].office_id.tolist() for ind in indices]
    loaded_prices = [right_df.iloc[ind][right_df.iloc[ind].deliveryPrice > 0].deliveryPrice.tolist() for ind in indices]
    cnt_loaded = [len(lst) for lst in loaded_offices]
    mean_price = [round(np.mean(lst), 2) if len(lst) > 0 else None for lst in loaded_prices]

    realty_with_restriction['loaded_offices_cnt'] = cnt_loaded
    realty_with_restriction['loaded_office_id'] = loaded_offices
    realty_with_restriction['mean_delivery_price'] = mean_price

    realty_loaded = realty_with_restriction[realty_with_restriction['loaded_offices_cnt'] > 0].copy()
    realty_loaded = realty_loaded.drop(['lon_rad', 'lat_rad'], axis=1)
    realty_loaded = realty_loaded.sort_values(by=['loaded_offices_cnt', 'mean_delivery_price'], ascending=[False, False]).reset_index(drop=True)
    return realty_loaded


def define_working_chat():
    r = requests.get(BOT_URL)
    results = r.json()['result']
    working_chats = set([res['message']['chat']['id'] for res in results])
    return working_chats


def send_map_to_chat(chats, path):
    bot=telebot.TeleBot(BOT_TOKEN)
    for chat in chats:
        f = open(path,"rb")
        bot.send_document(chat, f)


def make_map(realty_loaded, wb_pvz_loaded):
    Map_new = KeplerGl(height=1000, config=config)
    Map_new.add_data(realty_loaded[['ref','price', 'square', 'loaded_offices_cnt', 'phone', 'lat', 'lon']], name='Realty from cian')
    Map_new.add_data(wb_pvz_loaded[['office_id', 'deliveryPrice', 'lat', 'lon']], name='WB Loaded PVZ')
    dt = str(datetime.today().strftime("%Hh_%Y_%m_%d"))
    Map_new.save_to_html(file_name='/opt/hadoop/airflow/dags/mlen/cian_realty_map_' + dt + '.html', config=config)
    working_chats = define_working_chat()
    send_map_to_chat(working_chats, '/opt/hadoop/airflow/dags/mlen/cian_realty_map_' + dt + '.html')


def _parsing_of_wb_points():
    logging.info('Program has started. We will request wb points')
    all_pvz_data = get_pvz_info(domen=DOMEN)
    all_pvz_data = all_pvz_data.rename(columns={'id': 'office_id'})
    all_pvz_data['lat'] = all_pvz_data['coordinates'].apply(lambda x: x[0])
    all_pvz_data['lon'] = all_pvz_data['coordinates'].apply(lambda x: x[1])
    all_pvz_data = all_pvz_data.drop('coordinates', axis=1)
    logging.info('We got all wb pvz coordinates. Starting to write to hdfs')
    spark = SparkSession.builder.master('local').appName('parse_wb').getOrCreate()
    wb_pvz = spark.createDataFrame(all_pvz_data)
    logging.info('Writing wildberries dataframe to HDFS')
    wb_pvz.repartition(1).write.mode('overwrite').parquet('/user/mlen/af/wb/')


def _parsing_of_cian():
    logging.info('We are going to parse cian realty now')
    realty_df = parse_realty()
    logging.info('Parsing of cian realty is done.')
    logging.info('Final realty_df of shape:', realty_df.shape)
    realty_df = process_realty_df(realty_df)
    spark = SparkSession.builder.master('local').appName('parce_cian').getOrCreate()
    cian_df = spark.createDataFrame(realty_df)
    logging.info('Writing cian dataframe to HDFS')
    #cian_df.repartition(1).write.mode('overwrite').parquet('/user/mlen/af/cian/')


def _reading_hdfs_filtring():
    spark = SparkSession.builder.master("local").appName('wb').config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar").getOrCreate()
    logging.info('Reading from HDFS')
    wb_pvz = spark.read.option('header', 'true').parquet('/user/mlen/af/wb/').toPandas()
    realty_df = spark.read.option('header', 'true').parquet('/user/mlen/af/cian_hard/').toPandas()
    logging.info('Realty_DF of shape', realty_df.shape)
    logging.info('WB PVZ of shape', wb_pvz.shape)
    realty_loaded = define_restricted_realty_load(realty_df, wb_pvz)
    logging.info('Everything is done.')
    logging.info('Final DF after filtring:', realty_loaded.shape)
    wb_pvz_loaded = wb_pvz[wb_pvz['deliveryPrice'].notna()]
    make_map(realty_loaded, wb_pvz_loaded)

with DAG(
    dag_id='mlen_wb_cian_map',
    default_args=default_args,
    description='Parsing wb pvz statuses and coordinates',
    schedule=None, #"0 0-23/4 * * *",
    catchup=False,
) as dag:
    wb_parsing=PythonOperator(
        task_id = 'wb_parse',
        python_callable=_parsing_of_wb_points,
    )
    ci_parsing = PythonOperator(
        task_id = 'cian_parse',
        python_callable=_parsing_of_cian,
    )
    result_merge = PythonOperator(
        task_id = 'result',
        python_callable=_reading_hdfs_filtring,
    )
    wb_parsing >> ci_parsing >> result_merge

