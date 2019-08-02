import copy
import sys, os

import mongoengine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

sys.path.append(os.path.abspath(os.path.join('..', 'scrapping')))
from scrapping import Scrappa, MercantilScrapper, Link, RawScrapped, MercadoPublicoScrapper, Person
import platform
import threading

print(platform.system())
IP = os.environ.get('IP')
DB = os.environ.get('DB')

if platform.system() == "Darwin":
    connection = mongoengine.connect(DB, alias='default')
else:
    connection = mongoengine.connect(DB, alias='default', host=IP, port=27017)


def flatten(l):
    return [item for sublist in l for item in sublist]


class MyThread(threading.Thread):
    def __init__(self, objs, scrappa, scrapping_function):
        super(MyThread, self).__init__()
        self.objs = objs
        self.scrapping_function = scrapping_function
        self.scrappa = scrappa

    def run(self):
        for obj in self.objs:
            self.scrapping_function(obj, self.scrappa)


def download_directory(timeout=40, directory=None):
    mercantil_directorio_links = directory or []

    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    scrappa = Scrappa(timeout=timeout, browser=webdriver.Chrome, browser_args={'chrome_options': chrome_options})
    repeated, inserted = 0, 0

    for md_link in mercantil_directorio_links:
        scrappa._get(md_link)
        myElem = WebDriverWait(scrappa.driver, timeout).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'empresa1')))
        soup = BeautifulSoup(scrappa.driver.page_source, 'lxml')
        links = MercantilScrapper.list(soup)
        for link in links:
            print(f"{link['host']}{link['link']}")
            try:
                Link.objects.create(
                    link=f"{link['host']}{link['link']}", name=link['name'], host=link['host'],
                    data={'category': link['category'], 'directory': f"{link['host']}{link['directory']}"}
                )
                inserted += 1
            except mongoengine.NotUniqueError as e:
                repeated += 1
    print('repeated', repeated)
    print('inserted', inserted)
    print('total', inserted + repeated)


def download_company_from_mercantil(link, scrappa):
    print(link)
    if connection[DB]['companies'].find({'raws.link': link}).count() > 0:
        return
    scrapped = MercantilScrapper.detail(scrappa.soup(link))
    if scrapped in [None, ''] or scrapped['rut'] in [None, '']:
        print('Scrapping failed')
        return
    raw = {'link': link, 'data': scrapped, 'source': 'mercantil.com'}
    print(scrapped['rut'])
    company_query = connection[DB]['companies'].find({'_id': scrapped['rut']})
    if company_query.count() > 0:
        connection[DB]['companies'].update({'_id': scrapped['rut']}, {'$push': {'raws': raw}})
        print(f'RUT {scrapped["rut"]} already in database: {link}')
        return
        # raise AssertionError(f'RUT {scrapped["rut"]} already in database')
    else:
        treated_scrapped = copy.deepcopy(scrapped)
        treated_scrapped['addresses'] = [treated_scrapped.pop('address')]
        treated_scrapped['raws'] = [raw]
        treated_scrapped['_id'] = treated_scrapped['rut']
        connection[DB]['companies'].insert(treated_scrapped)


def download_companies(timeout=10):
    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    links = [[f"{c['host']}{c['link']}" for c in d['companies']] for d in connection[DB]['directories'].find()]
    links = flatten(links)
    ngroups = 3
    threads = []
    for seed in range(ngroups):
        group = (links[i] for i in range(seed, len(links), ngroups))
        thread = MyThread(
            group,
            Scrappa(timeout=timeout, browser=webdriver.Firefox),
            download_company_from_mercantil
        )
        threads.append(thread)
        thread.start()
    return threads


def fetch_mercadopublico_info(timeout=20):
    non_scrapeds = [str(id) for id in
                    [c['_id'] for c in connection[DB]['companies'].find({'raws.source': {'$ne': 'mercadopublico.cl'}})]]
    ngroups = 3
    threads = []
    for seed in range(ngroups):
        group = [non_scrapeds[i] for i in range(seed, len(non_scrapeds), ngroups)]
        thread = MyThread(group, Scrappa(timeout=timeout, browser=webdriver.Firefox), update_with_mercadopublico)
        threads.append(thread)
        thread.start()
    return threads


def fetch_genealog_info(timeout=20):
    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    scrappa = Scrappa(timeout=timeout, browser=webdriver.Chrome, browser_args={'chrome_options': chrome_options})
    repeated, inserted = 0, 0

    scrappa._get('https://www.genealog.cl/Geneanexus/search')
    search_input = scrappa.driver.find_element_by_name("s")
    search_input.send_keys('76545968-0')
    scrappa.driver.find_element_by_xpath('//*[@id="searchSubmitInitial"]/button').click()
    WebDriverWait(scrappa.driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="results-content"]/tr[3]')))
    soup = BeautifulSoup(scrappa.driver.page_source, 'lxml')
    profile_link = soup.select_one('#results-content > tr.person.empresa > td.tdRut > a').attrs['href']
    scrappa._get(profile_link)
    WebDriverWait(scrappa.driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="OwnEvent_showContact"]/td/button')))
    scrappa.driver.find_element_by_xpath('//*[@id="OwnEvent_showContact"]/td/button').click()
    WebDriverWait(scrappa.driver, timeout).until(
        EC.text_to_be_present_in_element((By.XPATH, '//*[@id="results-content"]/tr[8]/td[2]/a'), '@'))
    soup = BeautifulSoup(scrappa.driver.page_source, 'lxml')
    print(soup.select('#results-content > tr:nth-child(8) > td.parseOnAsk > span'))
    print(soup.select('#results-content > tr:nth-child(8) > td.parseOnAsk'))
    print(soup.select('#results-content > tr:nth-child(8) > td.parseOnAsk > a'))
    WebDriverWait(scrappa.driver, timeout).until(
        EC.text_to_be_present_in_element((By.XPATH, '//*[@id="results-content"]/tr[9]/td[2]/div[1]/a'), '56'))
    print(soup.select('#results-content > tr:nth-child(9) > td.telefonos.parseOnAsk'))


def update_with_mercadopublico(company_id, scrappa):
    company = connection[DB]['companies'].find({'_id': company_id})[0]
    rut = company['rut']
    link = f"http://webportal.mercadopublico.cl/proveedor/{rut}"
    print(rut, link)
    scrapped = MercadoPublicoScrapper.detail(scrappa.soup(link))
    raw = {'link': link, 'data': scrapped, 'source': 'mercadopublico.cl'}
    if scrapped in [None, '']:
        print(f'Scraping returned null')
        connection[DB]['companies'].update({'_id': rut}, {'$addToSet': {'raws': raw}})
        return
    if scrapped['rut'] != rut:
        raise AssertionError(f'rut {scrapped["rut"]} from scraped page and {rut} from database do not match.')
    treated_scrapped = copy.deepcopy(scrapped)
    print(rut, treated_scrapped['rut'])
    connection[DB]['companies'].update(
        {'_id': rut},
        {
            '$addToSet': {
                'emails': treated_scrapped.pop('email'),
                'phones': treated_scrapped.pop('phone'),
                'addresses': treated_scrapped.pop('address'),
                'raws': raw,
            },
            '$set': {
                'person': {'name': treated_scrapped.pop('contact_name')},
                **treated_scrapped
            }
        },
    )


def fetch_areas(scrappa):
    areas = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16, 17, 19, 1265, 1267, 1268, 1269, 1270, 1428]
    url = lambda area: f'https://www.mercantil.com/areas.aspx?area_code={area}'
    for area in areas:
        exists = connection[DB]['areas'].find({'_id': area}).count() > 0
        if not exists:
            soup = scrappa.soup(url(area))
            data = MercantilScrapper.area(soup)
            data['_id'] = area
            connection[DB]['areas'].update({'_id': data['_id']}, data, True)


def fetch_subareas(scrappa, codes=None):
    url = lambda code: f"https://www.mercantil.com/sareas.aspx?lang=esp&area_code={code}"
    if codes is None:
        areas = connection[DB]['areas'].find()
        sareas = [[[sa for sa in g['sub_areas']] for g in area['groups']] for area in areas]
        sareas = flatten(flatten(sareas))
        codes = [sarea['code'] for sarea in sareas]

    def get_sarea(code):
        exists = connection[DB]['sub_areas'].find({'_id': code}).count() > 0
        if not exists:
            soup = scrappa.soup(url(code))
            data = MercantilScrapper.sub_area(soup)
            data['_id'] = code
            connection[DB]['sub_areas'].update({'_id': data['_id']}, data, True)

    for code in codes:
        get_sarea(code)


def fetch_directories(scrappa, subarea_codes=None):
    repeated, inserted, timeout = 0, 0, 30
    if subarea_codes is None:
        sub_areas = connection[DB]['sub_areas'].find()
    else:
        sub_areas = connection[DB]['sub_areas'].find({'_id': {'$in': [str(s) for s in subarea_codes]}})
    directory_urls = [[di['link'] for di in sa['directories']] for sa in sub_areas]
    directory_urls = flatten(directory_urls)

    for link in directory_urls:
        exists = connection[DB]['directories'].find({'_id': link}).count() > 0
        if not exists:
            scrappa._get(link)
            WebDriverWait(scrappa.driver, timeout).until(EC.presence_of_element_located((By.CLASS_NAME, 'empresa1')))
            soup = BeautifulSoup(scrappa.driver.page_source, 'lxml')
            links = MercantilScrapper.list(soup)
            data = {
                'name': soup.select_one('#titsup').text,
                '_id': link,
                'companies': links,
            }
            connection[DB]['directories'].update({'_id': data['_id']}, data, True)


def scrap():
    threads_mercantil = download_companies(30)
    threads_mercadopublico = fetch_mercadopublico_info(30)

    # Wait for all threads to complete
    for t in threads_mercantil:
        t.join()
    for t in threads_mercadopublico:
        t.join()


# fetch_genealog_info()
'''
download_directory(directory=[
    "https://www.mercantil.com/arriendo-de-maquinaria-para-construccion/1960",
    "https://www.mercantil.com/arriendo-de-gruas-de-horquilla/176",
    "https://www.mercantil.com/arriendo-de-camiones-pluma/5808",
    "https://www.mercantil.com/arriendo-de-gruas-pluma/355",
    "https://www.mercantil.com/arriendo-de-maquinaria-para-movimientos-de-tierra/1950",
    "https://www.mercantil.com/arriendo-de-gruas/177",
    "https://www.mercantil.com/arriendo-de-retroexcavadoras/9573",
    "https://www.mercantil.com/arriendo-de-maquinaria-para-la-mineria/3098",
    "https://www.mercantil.com/arriendo-de-camiones-grua/3862",
    "https://www.mercantil.com/arriendo-de-minicargadores/6129",
    "https://www.mercantil.com/arriendo-de-camiones/3573",
])
'''
scrap()
