import copy
import sys, os

import mongoengine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

sys.path.append(os.path.abspath(os.path.join('..', 'scrapping')))
from scrapping import Scrappa, MercantilScrapper, Link, Company, RawScrapped, MercadoPublicoScrapper, Person
import platform
import threading

print(platform.system())
osx_ip = os.environ.get('IP')

if platform.system() == "Darwin":
    mongoengine.connect(os.environ.get('DB'), alias='default')
else:
    mongoengine.connect(os.environ.get('DB'), alias='default', host=osx_ip, port=27017)


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
    print(link.link)
    if Company.objects.filter(raws__link__contains=link.link).count() > 0:
        return
    scrapped = MercantilScrapper.detail(scrappa.soup(link.link))
    if scrapped in [None, ''] or scrapped['rut'] in [None, '']:
        print('Scrapping failed')
        return
    raw = RawScrapped(link=link.link, data=scrapped, source='mercantil.com')
    print(scrapped['rut'])
    link.update(set__data__rut=scrapped['rut'])
    company_query = Company.objects.filter(rut=scrapped['rut'])
    if company_query.count() > 0:
        company_query[0].update(push__raws=raw)
        print(f'RUT {scrapped["rut"]} already in database: {link.link}')
        return
        # raise AssertionError(f'RUT {scrapped["rut"]} already in database')
    else:
        treated_scrapped = copy.deepcopy(scrapped)
        treated_scrapped['addresses'] = [treated_scrapped.pop('address')]
        Company.objects.create(raws=[raw], **treated_scrapped, projects=['cargoo'])


def download_companies(timeout=10):
    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    links = Link.objects.all()
    ngroups = 4
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


def non_scraped_companies():
    while Company.objects.filter(raws__source__ne='mercadopublico.cl').count() > 0:
        company = Company.objects.filter(raws__source__ne='mercadopublico.cl')[0]
        yield company


def fetch_mercadopublico_info(timeout=20):
    non_scrapeds = [str(id) for id in Company.objects.filter(raws__source__ne='mercadopublico.cl').values_list('id')]
    ngroups = 4
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
    company = Company.objects.get(id=company_id)
    rut = company.rut
    link = f"http://webportal.mercadopublico.cl/proveedor/{rut}"
    print(rut, link)
    scrapped = MercadoPublicoScrapper.detail(scrappa.soup(link))
    raw = RawScrapped(link=link, data=scrapped, source='mercadopublico.cl')
    if scrapped in [None, '']:
        print(f'Scraping returned null')
        company.update(add_to_set__raws=raw, add_to_set__projects='cargoo')
        return
    if scrapped['rut'] != rut:
        raise AssertionError(f'rut {scrapped["rut"]} from scraped page and {rut} from database do not match.')
    treated_scrapped = copy.deepcopy(scrapped)
    print(rut, treated_scrapped['rut'])
    company.update(
        add_to_set__emails=treated_scrapped.pop('email'),
        add_to_set__phones=treated_scrapped.pop('phone'),
        add_to_set__addresses=treated_scrapped.pop('address'),
        set__person=Person(name=treated_scrapped.pop('contact_name')),
        add_to_set__raws=raw,
        add_to_set__projects='cargoo',
        **treated_scrapped
    )


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
