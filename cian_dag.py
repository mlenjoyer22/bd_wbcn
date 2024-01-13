import requests
import pandas as pd
from airflow import DAG
from sqlalchemy.engine import URL
from sqlalchemy import text
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
import datetime
import logging
from bs4 import BeautifulSoup
import json
import time
import re
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import math
import pandas as pd
import locale
from tqdm.contrib.concurrent import process_map 
from tqdm.notebook import tqdm as log_progress
from selenium import webdriver
# - это нужно чтобы единообразно обрабатывать даты обновления объявлений, такие как "сегодня" и "вчера"
locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')

### GLOBAL CONST
DEBUG=True
path_to_data = './spaces_data.csv'
# url for free proxies
proxi_base_url = 'https://www.sslproxies.org'
# регулярка которая воспринимает только корректные ip-шники
correct_ip_regex = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
pattern = re.compile(r"https://www\.cian\.([A-Za-z0-9]+(/[A-Za-z0-9]+)+)/\&*", re.IGNORECASE)
# as base i took some filters - выбрал некоторые интересные мне фильтры
start_url = 'https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&minarea=30&offer_type=offices&office_type[0]=2&office_type[1]=3&office_type[2]=5&office_type[3]=11&office_type[4]=12&region=1'
####
AIM_COUNT = 300
####

default_args={
    'owner':'Obarskaya Tatiana',
    'email':'obarskaia.tatiana@gmail.com',
    'start_date': datetime.datetime(2023, 11, 27),
    'email_on_failure': False,
    'email_on_retry': False,
}

class WebDriver:
    def __init__(self):
        chrome_options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome( options=chrome_options)            

    def __enter__(self):
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()


def get_count_spaces(driver):   
    driver.get(start_url)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    count_spaces = int(''.join(re.sub(r'\<[^>]*\>', '', str(soup.find('h5'))).split(' ')[1:-1]))
    return count_spaces

def get_all_links(driver,url, spaces, count, debug=False):
    start_val = len(spaces)
    pattern = re.compile(r"(https://www\.cian\.[A-Za-z0-9]+(/[A-Za-z0-9]+)+)/\?*", re.IGNORECASE)
    driver.get(url)
    elems = driver.find_elements("xpath", "//a[@href]")
    for elem in elems:    
        source = pattern.match(elem.get_attribute("href"))
        if source:
            source = source.group(1)
            if "rent/commercial" in source:            
                spaces.add(source)
    diff = len(spaces) - start_val
    if debug:
        logging.info(f"Got {diff} offer on this page,\nleft {math.ceil((count - len(spaces))/diff)} pages")
    return len(spaces)

def get_all_spaces(driver, count):
    count_spaces = get_count_spaces(driver)
    current_url = start_url
    spaces = set()
    spaces_at_page = get_all_links(driver, current_url, spaces, count, DEBUG)
    i = 2
    while len(spaces) < count and i < math.floor(count_spaces/spaces_at_page):
        try:
            get_all_links(driver, current_url+f'&p={i}', spaces, count, DEBUG)
        except:
            pass        
        i+=1
    if DEBUG:
        logging.info(f"Got {len(spaces)} offers")
    return list(spaces)

def parse_space(driver, ref):
    driver.get(ref)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    try:
        flat = Flat(ref, soup)
        return flat.to_df()
    except Exception as e:        
        logging.info(str(e) + ' ' + ref + '_problem object')
    return None

class Flat():
    ref = None
    price = None
    address = None
    phone = None
    by_owner = None
    podSnos = None
    sq = None
    lat = None
    lng = None

    def __init__(self, ref, soup):
        self.ref = ref
        script_tags = soup.find_all("script")
        for tag in script_tags:
            if tag.text != None:
                if self.price == None and  'dealType' in str(tag.text):
                    js_content = json.loads(str(tag.text).split('"pageviewSite",')[1][:-1])
                    self.price = js_content['products'][0]['price']
                    self.podSnos = js_content['products'][0]['podSnos']
                    self.by_owner = js_content['products'][0]['owner']
                    self.phone = js_content['page']['offerPhone']        
                if self.lat==None and 'coordinates' in str(tag.text):
                    self.lat, self.lng = tag.text.replace('"lng":','').split('"coordinates":{"lat":')[1].replace('}',',').split(',')[:2]                    
        self.address = soup.find("div", {"data-name":"Geo"}).find("span", {"itemprop":"name"})['content']
        self.sq = float(soup.find("h1", {'class':"a10a3f92e9--title--vlZwT"}).get_text().split(' ')[-2:-1][0].replace(',', '.'))
                   
    def to_df(self):
        data = {
            'ref': self.ref,
            'price': self.price,
            'address': self.address,
            'sq': self.sq,
            'phone': self.phone,
            'podSnos':self.podSnos,
            'by_owner':self.by_owner,
            'lat':self.lat,
            'lng':self.lng
        }
        df = pd.DataFrame(data, index=[0])
        return df    


def _pipeline():
    logging.info('Program has started. We will get actual spaces for rent from cian')
    common_df = pd.DataFrame()
    with WebDriver() as driver:
        count_spaces = get_count_spaces(driver)
        if (DEBUG):
            logging.info(f"Got {count_spaces} spaces")
        time.sleep(1)
        list_spaces = get_all_spaces(driver, AIM_COUNT)        
        cur_flat_df = pd.DataFrame()
        for space in list_spaces:            
            time.sleep(1)
            try:
                cur_flat_df = parse_space(driver, space)
            except:
                driver = WebDriver()
            finally:
                if not cur_flat_df.empty:
                    common_df = pd.concat([common_df,cur_flat_df])   
                if DEBUG:
                    logging.info('success concat')                             
        common_df.to_csv(path_to_data, index=False)
    logging.info('We got ')

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
