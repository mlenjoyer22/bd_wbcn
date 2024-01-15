import ssl
from bs4 import BeautifulSoup
import json
import time
import urllib
import re
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import math
import pandas as pd
import requests

DEBUG = True
proxi_base_url = 'https://www.sslproxies.org'
correct_ip_regex = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
start_url = 'https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&minarea=30&offer_type=offices&office_type[0]=2&office_type[1]=3&office_type[2]=5&office_type[3]=11&office_type[4]=12&region=1'

def scrap_proxi():
    response = requests.get(proxi_base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    lines = soup.find_all("tr", {'class':''})
    set_proxies = set()
    for i in lines[1:]:    
        print('iter')
        p = re.sub(r'\<[^>]*\>', '', str(i.td)) 
        c = re.sub(r'\<[^>]*\>', '', str(i.find("td", {'class':'hm'})))
        if len(re.findall(correct_ip_regex, p)) == 1 and c == 'Russian Federation':
            set_proxies.add(p)    
    return set_proxies

def check_req_res(data):
    #print('обычно' not in data)
    return 'обычно' not in data

def get_soup(url):
    soup = ''
    ssl._create_default_https_context = ssl._create_unverified_context
    with open("./headers.txt") as headers:
        while soup == '':
            for header in headers:
                if soup == '':
                    header = headers.readline()
                    user_agent = {'User-agent': str(header)[:-1]}
                    response  = requests.get(url, headers = user_agent, proxies=urllib.request.getproxies())
                    soup = BeautifulSoup(response.content, 'lxml')
                    if check_req_res(soup.text):                    
                        return soup
                    
def get_all_spaces(soup, count_spaces, count):
    spaces = set()
    spaces_at_page = get_all_links(soup, spaces, count, DEBUG)
    spaces.union(spaces_at_page)
    i = 2
    while len(spaces) < count and i < math.floor(count_spaces/len(spaces_at_page)):
        try:
            get_all_links(get_soup(start_url+f'&p={i}'), spaces, count, DEBUG)
        except:
            pass        
        i+=1
    if DEBUG:
        print(f"Спарсили {len(spaces)} предложений")
    return list(spaces)

def get_all_links(soup, spaces, count, debug=False):
    start_val = len(spaces)
    #pattern = re.compile(r"https:\/\/www\.cian\.ru\/rent\/commercial\/[0-9]+\/", re.IGNORECASE)
    pattern = f'https://www.cian.ru/rent/commercial/\d+/'
    #data = get_data(url)
    #sources = soup.find_all('a', {'href': pattern})    
    #sources = pattern.findall(soup.text)
    tag_a = soup.find_all('cianId')
    for i in tag_a:
        refs = i.find_all('cianId')
        for ref in refs:
            if ref !=None:            
                if re.match(pattern, ref):
                    print('Ссылка соответствует шаблону')
                    spaces.add(ref)
    #diff = len(spaces) - start_val
    #if debug:
    #    print(f"На этой странице нашли {diff} предложений,\nзначит нужно будет обойти еще {math.ceil((count - len(spaces))/diff)} страниц")
    return spaces

def parse_count(soup):
    count_spaces = int(re.findall('(\s+([0-9]+\s+)+)', soup.find("h5").text
                )[0][0].replace(' ', ''))
    return count_spaces

if __name__ == '__main__':
    count_spaces = parse_count(get_soup(start_url))
    if (DEBUG):
        print(count_spaces)
    time.sleep(1)
    # по умолчанию парсим по 1000 объектов в сутки
    list_spaces = get_all_spaces(get_soup(start_url),count_spaces, 10)
    print(len(list_spaces), list_spaces[:3])
    # list_spaces = get_all_spaces(count_spaces, 10)
    # progress = 0
    # cur_flat_df = pd.DataFrame()
    # for space in list_spaces:            
    #     time.sleep(1)
    #     try:
    #         cur_flat_df = parse_space(driver, space)
    #     except:
    #         driver = WebDriver()
    #     finally:
    #         if not cur_flat_df.empty:
    #             common_df = pd.concat([common_df,cur_flat_df])   
    #             if DEBUG:
    #                 print('success concat')                             
    # common_df.to_csv(path_to_data, index=False)
