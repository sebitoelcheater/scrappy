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
osx_ip = '192.168.74.105'

if platform.system() == "Darwin":
    mongoengine.connect('messero_scrapping', alias='default')
else:
    mongoengine.connect('messero_scrapping', alias='default', host=osx_ip, port=27017)


class MyThread(threading.Thread):
    def __init__(self, objs, scrappa, scrapping_function):
        super(MyThread, self).__init__()
        self.objs = objs
        self.scrapping_function = scrapping_function
        self.scrappa = scrappa

    def run(self):
        for obj in self.objs:
            self.scrapping_function(obj, self.scrappa)


def download_directory(timeout=40):
    mercantil_directorio_links = [
        "https://www.mercantil.com/restaurantes/1502",
        "https://www.mercantil.com/comida-rapida/5246",
        "https://www.mercantil.com/banqueteros/248",
        "https://www.mercantil.com/pastelerias/1316",
        "https://www.mercantil.com/preparacion-de-comida/8182",
        "https://www.mercantil.com/restaurantes-comidas-rapidas/3878",
        "https://www.mercantil.com/restaurantes-comida-chilena/3509",
        "https://www.mercantil.com/restaurantes-comida-japonesa/3515",
        "https://www.mercantil.com/restaurantes-carnes-y-parrilladas/3502",
        "https://www.mercantil.com/pizzas/2753",
        "https://www.mercantil.com/restaurantes-comida-peruana/3517",
        "https://www.mercantil.com/pescados-y-mariscos-congelados/4545",
        "https://www.mercantil.com/restaurantes-comida-china/3510",
        "https://www.mercantil.com/sandwiches/3019",
        "https://www.mercantil.com/restaurantes-comida-internacional/3513",
        "https://www.mercantil.com/fuentes-de-soda/2257",
        "https://www.mercantil.com/restaurantes-pescados-y-mariscos/3518",
        "https://www.mercantil.com/comidas-a-domicilio/3831",
        "https://www.mercantil.com/colaciones/1883",
        "https://www.mercantil.com/bares/833",
        "https://www.mercantil.com/restaurantes-comida-italiana/3514",
        "https://www.mercantil.com/tortas/3264",
        "https://www.mercantil.com/gastronomia/7969",
        "https://www.mercantil.com/comida-china/5251",
        "https://www.mercantil.com/restaurantes-comida-espanola/3511",
        "https://www.mercantil.com/parrilladas/3055",
        "https://www.mercantil.com/restaurantes-peruanos/5269",
        "https://www.mercantil.com/restaurantes-comida-arabe/3505",
        "https://www.mercantil.com/alimentos-preparados/7951",
        "https://www.mercantil.com/restaurantes-comida-americana/3504",
        "https://www.mercantil.com/restaurantes-comida-francesa/3512",
        "https://www.mercantil.com/postres/2795",
        "https://www.mercantil.com/restaurantes-vegetarianos/3451",
        "https://www.mercantil.com/restaurantes-autoservicios/4775",
        "https://www.mercantil.com/alimentos-orientales/4344",
        "https://www.mercantil.com/restaurantes-comida-mexicana/3516",
        "https://www.mercantil.com/carne-envasada/7992",
        "https://www.mercantil.com/restaurantes-comida-tailandesa/4854",
        "https://www.mercantil.com/desayunos-a-domicilio/4583",
        "https://www.mercantil.com/restaurantes-carnes/5255",
        "https://www.mercantil.com/comidas-entrega-a-domicilio/5241",
        "https://www.mercantil.com/boites/956",
        "https://www.mercantil.com/restaurantes-comida-alemana/3503",
        "https://www.mercantil.com/restaurantes-pollos-y-pavos/3840",
        "https://www.mercantil.com/restaurantes-comida-argentina/3507",
        "https://www.mercantil.com/articulos-para-reposteria/4706",
        "https://www.mercantil.com/restaurantes-comida-hindu/4855",
        "https://www.mercantil.com/restaurantes-comida-catalana/3506",
        "https://www.mercantil.com/comida-china-a-domicilio/5250",
        "https://www.mercantil.com/restaurantes-carnes-premium/5256",
        "https://www.mercantil.com/restaurantes-comida-cubana/4947",
        "https://www.mercantil.com/ensaladas-preparadas/5122",
        "https://www.mercantil.com/tanguerias/3223",
        "https://www.mercantil.com/restaurantes-comida-brasilera/3508",
        "https://www.mercantil.com/restaurantes-comida-suiza/3866",
        "https://www.mercantil.com/mazapanes/4287",
        "https://www.mercantil.com/restaurantes-comida-vietnamita/4688",
        "https://www.mercantil.com/restaurantes-comida-ecuatoriana/4755",
    ]

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
    if Company.objects.filter(rut=scrapped['rut']).count() > 0:
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

# download_directory()
scrap()
