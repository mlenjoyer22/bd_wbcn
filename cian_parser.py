from bs4 import BeautifulSoup
import json
import time
import re
from fake_useragent import UserAgent
import math
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# - это нужно чтобы единообразно обрабатывать даты обновления объявлений, такие как "сегодня" и "вчера"


class WebDriver:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        #chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--enable-javascript")
        self.driver = webdriver.Chrome(options=chrome_options) 
        # chrome_options = webdriver.ChromeOptions()
        # self.driver = webdriver.Chrome( options=chrome_options)            

    def __enter__(self):
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()

### GLOBAL CONST
DEBUG=True
AIM_COUNT = 300
path_to_data = './spaces_data_lg.pqt'
# url for free proxies
proxi_base_url = 'https://www.sslproxies.org'
# регулярка которая воспринимает только корректные ip-шники
correct_ip_regex = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
pattern = re.compile(r"https://www\.cian\.([A-Za-z0-9]+(/[A-Za-z0-9]+)+)/\&*", re.IGNORECASE)
# as base i took some filters - выбрал некоторые интересные мне фильтры
#2 'https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&foot_min=25&metro%5B0%5D=12&metro%5B1%5D=81&metro%5B2%5D=97&metro%5B3%5D=134&metro%5B4%5D=154&metro%5B5%5D=159&minarea=30&offer_type=offices&office_type%5B0%5D=2&office_type%5B1%5D=3&office_type%5B2%5D=5&office_type%5B3%5D=11&office_type%5B4%5D=12&only_foot=2'
# https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&foot_min=25&metro%5B0%5D=50&metro%5B1%5D=61&metro%5B2%5D=78&metro%5B3%5D=103&metro%5B4%5D=519&minarea=30&offer_type=offices&office_type%5B0%5D=2&office_type%5B1%5D=3&office_type%5B2%5D=5&office_type%5B3%5D=11&office_type%5B4%5D=12&only_foot=2
start_url = 'https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&foot_min=25&metro%5B0%5D=32&metro%5B10%5D=339&metro%5B11%5D=361&metro%5B12%5D=362&metro%5B13%5D=363&metro%5B14%5D=364&metro%5B15%5D=447&metro%5B16%5D=449&metro%5B1%5D=40&metro%5B2%5D=48&metro%5B3%5D=55&metro%5B4%5D=65&metro%5B5%5D=92&metro%5B6%5D=156&metro%5B7%5D=281&metro%5B8%5D=284&metro%5B9%5D=285&minarea=30&offer_type=offices&office_type%5B0%5D=2&office_type%5B1%5D=3&office_type%5B2%5D=5&office_type%5B3%5D=11&office_type%5B4%5D=12&only_foot=2'
#.by
####

# OK      
def get_count_spaces(driver):   
    driver.get(start_url)
    soup = BeautifulSoup(driver.page_source, 'html')    
    #count_spaces = int(re.findall('(\s+([0-9]+\s+)+)', soup.find("h5").text
    #            )[0][0].replace(' ', ''))
    count_spaces = int(re.findall('(\s+([0-9]+\s+)+)', soup.find("h5").text)[0][0].replace(' ', ''))
    print(count_spaces)
    #count_spaces = int(''.join(re.sub(r'\<[^>]*\>', '', str(soup.find('h5'))).split(' ')[1:-1]))
    return count_spaces

# собираем все ссылки со страницы с квартирами
def get_all_links(driver,url, spaces, count, debug=False):
    start_val = len(spaces)
    pattern = re.compile(r"(https://www\.cian\.[A-Za-z0-9]+(/[A-Za-z0-9]+)+)/\?*", re.IGNORECASE)
    driver.get(url)
    elems = driver.find_elements("xpath", "//a[@href]")
    for elem in elems:    
        source = pattern.match(elem.get_attribute("href"))
        if source:
            source = source.group(1)
            #print(source)
            if "rent/commercial" in source:            
                spaces.add(source)
    diff = len(spaces) - start_val
    if debug:
        print(f"На этой странице нашли {diff} предложений,\nзначит нужно будет обойти еще {math.ceil((count - len(spaces))/diff)} страниц")
    return len(spaces)

def get_all_spaces(driver, count_spaces, count):
    #count_spaces = get_count_spaces(driver)
    current_url = start_url
    spaces = set() # готовим множетсво под ссылки на квартиры
    spaces_at_page = get_all_links(driver, current_url, spaces, count, DEBUG)
    # можно было бы парсить ссылки на следующую страницу и ходить по ним,
    # но кажется проще итерироваться по номерам страниц добавляя к исходной ссылке 
    # &p={i} - для i-й страницы
    i = 2 # первую страницу уже обработали
    while len(spaces) < count and i < math.floor(count_spaces/spaces_at_page) and i < 30:
        try:
            get_all_links(driver, current_url+f'&p={i}', spaces, count, DEBUG)
        except:
            pass        
        i+=1
    if DEBUG:
        print(f"Спарсили {len(spaces)} предложений")
    return list(spaces)

def parse_space(driver, ref):
    driver.get(ref)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    try:
        flat = Flat(ref, soup)
        return flat.to_df()
    except Exception as e:
        print(str(e))
        print(ref)
    return None

class Flat():
    id = None
    ref = None
    price = None
    address = None
    phone = None
    by_owner = None
    podSnos = None
    square = None
    lat = None
    lng = None

    def __init__(self, ref, soup):
        self.id = ref.split(f'/')[-1]
        self.ref = ref
        script_tags = soup.find_all("script")
        try:
            for tag in script_tags:
                if tag.text != None:
                    if self.price == None and 'dealType' in str(tag.text):
                        #print(str(tag.text).split('"pageviewSite",')[1][:-1])
                        try:
                            js_content = json.loads(str(tag.text).split('"pageview",')[1][:-1])
                            #self.price = js_content['products'][0]['price']
                            self.podSnos = js_content['products'][0]['podSnos']
                            self.by_owner = js_content['products'][0]['owner']
                            self.phone = js_content['page']['offerPhone']
                        except:
                            pass
            # script_tags = soup.find_all('script type="text/javascript"')
            # for tag in script_tags:
            #     if tag.text != None:
                    if self.lat==None and 'coordinates' in str(tag.text):
                        self.lat, self.lng = tag.text.replace('"lng":','').split('"coordinates":{"lat":')[1].replace('}',',').split(',')[:2]
                        #print(self.lat, self.long)                
        except:
            pass
        #self.price = soup.find("span", {"itemprop":"price"}).get_text().strip("₽").replace(" ", '')
        #self.price = soup.find("span", {"class": ".a10a3f92e9--amount--ON6i1 > span:nth-child(1)"})
        if self.price == None:
            try:
                self.price = str(soup.find("div", {"data-testid":"price-amount"}).contents[0].contents[0]).replace('\xa0', '').split('–')[0].replace('₽/мес.','')
            except:
                self.price = 9999999999999
        self.address = soup.find("div", {"data-name":"Geo"}).find("span", {"itemprop":"name"})['content']
        self.square = float(soup.find("h1", {'class':"a10a3f92e9--title--vlZwT"}).get_text().split(' ')[-2:-1][0].replace(',', '.'))
        

    # кастим класс к датафрейму                
    def to_df(self):
        data = {
            'id': self.id,
            'ref': self.ref,
            'price': self.price,
            'address': self.address,
            'square': self.square,
            'phone': self.phone,
            'podSnos':self.podSnos,
            'by_owner':self.by_owner,
            'lat':self.lat,
            'lng':self.lng
        }
        df = pd.DataFrame(data, index=[0])
        return df


if __name__ == '__main__':
    common_df = pd.DataFrame()
    with WebDriver() as driver:
        #list_spaces = ['https://www.cian.ru/rent/commercial/281918954']
        count_spaces = get_count_spaces(driver)
        if (DEBUG):
            print(count_spaces)
        #time.sleep(1)
        # по умолчанию парсим по 1000 объектов в сутки
        list_spaces = get_all_spaces(driver, count_spaces, AIM_COUNT)
        progress = 0
        cur_flat_df = pd.DataFrame()
        for space in list_spaces[:min(AIM_COUNT,len(list_spaces))]:            
        #     #time.sleep(1)
            if len(common_df)%50 == 0: print(f"done {len(common_df)}/{min(AIM_COUNT,len(list_spaces))}")
            try:
                cur_flat_df = parse_space(driver, space)
            except:
                pass
                # passdriver = WebDriver()
            finally:
                if not cur_flat_df.empty:
                    common_df = pd.concat([common_df,cur_flat_df])   
                    if DEBUG:
                        print('success concat')                             
        common_df.to_csv("test.csv", index=False)
        common_df.to_parquet(path_to_data, index=False)
