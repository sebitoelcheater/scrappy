import copy
import sys, os
from time import sleep
import mongoengine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from libs.scrappa import Scrappa
from libs.scrappers.mercadopublico_scrapper import MercadoPublicoScrapper
from libs.scrappers.mercantil_scrapper import MercantilScrapper
import platform
import threading

sys.path.append(os.path.abspath(os.path.join('..', 'scrapping')))
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
                connection[DB]['links'].create_one({
                    'link': f"{link['host']}{link['link']}", 'name': link['name'], 'host': link['host'],
                    'data': {'category': link['category'], 'directory': f"{link['host']}{link['directory']}"}
                })
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
    links = [[f"{c['host']}{c['link']}" for c in d['companies']] for d in connection[DB]['directories'].find()]
    links = flatten(links)
    ngroups = 4
    threads = []

    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    for seed in range(ngroups):
        scrappa = Scrappa(timeout=timeout, browser=webdriver.Chrome, browser_args={'chrome_options': chrome_options})
        group = (links[i] for i in range(seed, len(links), ngroups))
        thread = MyThread(
            group,
            scrappa,
            download_company_from_mercantil
        )
        threads.append(thread)
        thread.start()
    return threads


def fetch_mercadopublico_info(companies=None, timeout=20):
    if not companies:
        companies = connection[DB]['companies'].find({'raws.source': {'$ne': 'mercadopublico.cl'}})
    ngroups = 8
    threads = []
    ruts = [str(id) for id in [c['_id'] for c in companies]]

    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    for seed in range(ngroups):
        scrappa = Scrappa(timeout=timeout, browser=webdriver.Chrome, browser_args={'chrome_options': chrome_options})
        group = [ruts[i] for i in range(seed, len(ruts), ngroups)]
        thread = MyThread(group, scrappa, update_with_mercadopublico)
        threads.append(thread)
        thread.start()
    return threads


def fetch_genealog_info(companies=None, timeout=20):
    if not companies:
        companies = connection[DB]['companies'].find({'raws.source': {'$ne': 'genealog.cl'}})
    ruts = [str(id) for id in [c['_id'] for c in companies]]
    ngroups = 8
    threads = []

    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    for seed in range(ngroups):
        scrappa = Scrappa(timeout=timeout, browser=webdriver.Chrome, browser_args={'chrome_options': chrome_options})
        group = [ruts[i] for i in range(seed, len(ruts), ngroups)]
        thread = MyThread(group, scrappa, update_with_genealog)
        threads.append(thread)
        thread.start()
    return threads


def update_with_genealog(rut, scrappa, timeout=20):
    repeated, inserted = 0, 0

    if connection[DB]['companies'].find({'_id': rut, 'raws.source': 'genealog.cl'}).count() > 0:
        print(f'{rut}: already scrapped from genealog.cl')
        return
    print(rut)
    scrappa._get('https://www.genealog.cl/Geneanexus/search')
    search_input = scrappa.driver.find_element_by_name("s")
    try:
        search_input.send_keys(rut)
    except TimeoutException as e:
        print(f'{rut}: TimeoutException (write rut in input field)')
        return
    try:
        scrappa.driver.find_element_by_xpath('//*[@id="searchSubmitInitial"]/button').click()
    except TimeoutException as e:
        print(f'{rut}: TimeoutException (click in search button)')
        return
    try:
        WebDriverWait(scrappa.driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#time_elapsed_secs b')))
    except TimeoutException as e:
        print(f'{rut}: No results ("#time_elapsed_secs b" not found)')
        return
    soup = BeautifulSoup(scrappa.driver.page_source, 'lxml')
    if soup.select_one('#results-content .tdRut a') is None:
        print(f'{rut}: No results (no links found)')
        return
    profile_link = soup.select_one('#results-content .tdRut a').attrs['href']
    scrappa._get(profile_link)
    ADDITIONAL_INFO = True
    try:
        # WebDriverWait(scrappa.driver, timeout).until(
        #    EC.presence_of_element_located((By.CSS_SELECTOR, '#OwnEvent_showContact > td > button')))
        button = False
        while (not button) or button.is_displayed():
            button = scrappa.driver.find_element_by_css_selector('#OwnEvent_showContact > td > button')
            button.click()
        sleep(1)
        WebDriverWait(scrappa.driver, timeout).until(
            EC.visibility_of_any_elements_located((By.CSS_SELECTOR, '.parseOnAsk a')))
    except NoSuchElementException as e:
        print(f'{rut}: no button found')
    except TimeoutException as e:
        print(f'{rut}: no additional info')
        ADDITIONAL_INFO = False
    soup = BeautifulSoup(scrappa.driver.page_source, 'lxml')
    table = soup.select_one('#results-content')
    if table is None:
        print(f"{rut}: No content's table")
        return
    rows = table.findAll('tr')

    scrapped = {
        'title': soup.select_one('#results-content > tr.tabla-titulo > td > h2'),
        'description': soup.select_one('.description'),
        'slogan': soup.select_one('.slogan'),
    }
    for key in list(scrapped):
        value = scrapped[key]
        if value is None:
            del scrapped[key]
        else:
            scrapped[key] = value.text
    elements = {}
    for row in rows:
        columns = row.findAll('td')
        if len(columns) == 2:
            scrapped[columns[0].text.replace('\n', '')] = columns[1].prettify()
            elements[columns[0].text.replace('\n', '')] = columns[1]
    raw = {'link': profile_link, 'data': scrapped, 'source': 'genealog.cl'}
    contacts = []
    if 'Contacto' in elements:
        for email_link in elements['Contacto'].select('a') or []:
            previous = [p for p in email_link.previous_elements][1:]
            name = ''
            for previous_element in previous:
                if previous_element.name in ['br', 'td', 'span']:
                    break
                name = str(previous_element) + name
            contacts.append(
                {
                    'name': name,
                    'email': email_link.text,
                }
            )
    phones = [t.text.replace('\n', '').replace(' ', '') for t in
              elements['Teléfonos'].findAll('a')] if 'Teléfonos' in elements else []
    connection[DB]['companies'].update(
        {'_id': rut},
        {'$addToSet': {'raws': raw, 'phones': {'$each': phones}, 'emails': {'$each': contacts}}}
    )
    print(f'{rut}: scrapped from genealog.cl')


def update_with_mercadopublico(company_id, scrappa):
    company = connection[DB]['companies'].find({'_id': company_id})[0]
    rut = company['rut']
    link = f"http://webportal.mercadopublico.cl/proveedor/{rut}"
    if link in [r['link'] for r in company['raws']]:
        print(f"{rut}: already scrapped: {link}")
        return
    scrapped = MercadoPublicoScrapper.detail(scrappa.soup(link))
    raw = {'link': link, 'data': scrapped, 'source': 'mercadopublico.cl'}
    if scrapped in [None, '']:
        print(f'{rut}: Scraping returned null')
        connection[DB]['companies'].update({'_id': rut}, {'$addToSet': {'raws': raw}})
        return
    if scrapped['rut'] != rut:
        raise AssertionError(f'rut {scrapped["rut"]} from scraped page and {rut} from database do not match.')
    treated_scrapped = copy.deepcopy(scrapped)
    print(f"{rut} ({treated_scrapped['rut']}): scrapped from mercadopublico.cl")
    connection[DB]['companies'].update(
        {'_id': rut},
        {
            '$addToSet': {
                'emails': {'email': treated_scrapped.pop('email')},
                'phones': treated_scrapped.pop('phone'),
                'addresses': treated_scrapped.pop('address'),
                'raws': raw,
            },
            '$set': {
                'person': {'name': treated_scrapped.pop('contact_name'), 'email': treated_scrapped.pop('email')},
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


def fetch_directories(subarea_codes=None, scrappa=None):
    repeated, inserted, timeout = 0, 0, 23
    if subarea_codes is None:
        sub_areas = connection[DB]['sub_areas'].find()
    else:
        sub_areas = connection[DB]['sub_areas'].find({'_id': {'$in': [str(s) for s in subarea_codes]}})
    directory_urls = [[di['link'] for di in sa['directories']] for sa in sub_areas]
    directory_urls = flatten(directory_urls)

    for link in directory_urls:
        exists = connection[DB]['directories'].find({'_id': link}).count() > 0
        if not exists:
            print(f"create: {link}")
            scrappa._get(link)
            try:
                WebDriverWait(scrappa.driver, timeout).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'empresa1')))
            except TimeoutException as e:
                continue
            soup = BeautifulSoup(scrappa.driver.page_source, 'lxml')
            links = MercantilScrapper.list(soup)
            data = {
                'name': soup.select_one('#titsup').text,
                '_id': link,
                'companies': links,
            }
            connection[DB]['directories'].update({'_id': data['_id']}, data, True)
        else:
            print(f"exists: {link}")


def scrap():
    categories = [
        "CONTRATISTAS EN CONSTRUCCION",
        "MATERIALES PARA CONSTRUCCION",
        "EQUIPOS PARA CONSTRUCCION",
        "CONSTRUCCION DE EDIFICIOS",
        "ARRIENDO DE MAQUINARIA PARA CONSTRUCCION",
        "CONSTRUCCIONES DEPORTIVAS",
        "MAQUINARIA PARA CONSTRUCCION",
        "CONSTRUCCION DE CAMINOS",
        "ARIDOS PARA CONSTRUCCION",
        "CONSTRUCCION",
        "INGENIERIA Y CONSTRUCCION",
        "ARRIENDO DE GRUAS PARA CONSTRUCCION",
        "CONSTRUCCION DE EQUIPO FERROVIARIO",
        "CONSTRUCCION DE SALAS PARA COMPUTACION",
        "CONSTRUCCION DE VIAS FERREAS",
        "CONSTRUCCION DE JARDINES",
        "CONSTRUCCIONES AGROINDUSTRIALES",
        "CONSTRUCCION DE CANCHAS DE GOLF",
        "CONSTRUCCION DE PLANTAS INDUSTRIALES",
        "CONSTRUCCION DE COLEGIOS",
        "CONSTRUCCION DE CASAS",
        "CONSTRUCCION DE LOCALES COMERCIALES Y OFICINAS",
        "CONSTRUCCION DE MUROS DE CONTENCION",
        "CONSTRUCCION DE ESTACIONAMIENTOS",
        "CONSTRUCCIONES ECOLOGICAS",
        "CONSTRUCCIONES MODULARES",
        "CONSTRUCCION DE TUNELES",
        "CONSTRUCCIONES DE HORMIGON",
        "CONSTRUCCIONES PORTATILES EN MODULO",
        "CONSTRUCCION DE PISCINAS",
        "CONSTRUCCION DE POZOS",
        "CONSTRUCCION DE TRANQUES",
        "CONSTRUCCIONES INDUSTRIALES",
        "CONSTRUCCIONES METALICAS",
        "CONSTRUCCIONES NAVALES",
        "CONSTRUCCIONES PORTUARIAS",
        "CONSTRUCCIONES SANITARIAS",
        "CONSTRUCCIONES SUBMARINAS",
        "MAQUINARIAS Y EQUIPOS PARA LA CONSTRUCCION",
        "ARRIENDO DE MAQUINARIAS Y EQUIPOS PARA LA CONSTRUCCION",
        "OBRAS MENORES EN CONSTRUCCION",
        "MATERIALES DE CONSTRUCCION",
        "GRUAS PARA CONSTRUCCION",
        "REPUESTOS PARA MAQUINARIA PARA CONSTRUCCION",
        "ELEVADORES PARA CONSTRUCCIONES",
        "VENTA DE MAQUINARIA PARA LA CONSTRUCCION",
        "ARTICULOS PARA CONSTRUCCION",
        "CONSTRUCCION OBRAS CIVILES",
        "REPARACION DE MAQUINARIA PARA LA CONSTRUCCION",
        "MATERIAL DE CONSTRUCCION",
        "VENTA DE MATERIALES DE CONSTRUCCION",
        "MAQUINARIAS DE CONSTRUCCION",
        "CONSTRUCCION CIVIL",
        "CONSTRUCCION DE RADIER",
        "MOLDAJES PARA LA CONSTRUCCION",
        "PROYECTOS DE CONSTRUCCION",
        "CONSTRUCCIONES",
        "ARQUITECTURA Y CONSTRUCCION",
        "VENTA DE EQUIPOS PARA LA CONSTRUCCION",
        "INGENIERIA EN CONSTRUCCION",
        "CONSTRUCCION DE GALPONES",
        "DISEÑO Y CONSTRUCCION DE ESTRUCTURAS METALICAS",
        "ASFALTOS PARA CONSTRUCCION",
        "OBRAS MENORES EN CONSTRUCCION Y REMODELACION",
        "CONSTRUCCION DE QUINCHOS",
        "CONSTRUCCION DE SEGUNDOS PISOS",
        "CONSTRUCCION Y FABRICACION DE ESTRUCTURAS METALICAS",
        "CONSTRUCCION DE CASAS DE MADERA",
        "CONSTRUCCION Y MANTENCION AREAS VERDES",
        "ARRIENDO Y VENTA DE MAQUINARIAS PARA LA CONSTRUCCION",
        "BAÑOS CONSTRUCCION Y ACCESORIOS",
        "CONSTRUCCIONES INTEGRALES",
        "MAQUINARIAS",
        "ARRIENDO DE MAQUINARIAS",
        "EQUIPOS Y MAQUINARIA USADA",
        "MAQUINARIA FORESTAL",
        "MAQUINARIA PARA MINERIA",
        "REPARACION DE MAQUINARIA FORESTAL",
        "ARRIENDO DE MAQUINARIA AGRICOLA",
        "ARRIENDO DE MAQUINARIA PARA CONSTRUCCION",
        "EQUIPOS Y PARTES PARA MAQUINARIA ELECTRICA",
        "MAQUINARIA PARA CONSTRUCCION",
        "TRANSPORTE DE MAQUINARIAS",
        "TRANSPORTE DE MAQUINARIA PESADA",
        "TRASLADOS DE MAQUINARIAS",
        "ARRIENDO DE MAQUINARIA PARA MOVIMIENTOS DE TIERRA",
        "MAQUINARIA PARA LA INDUSTRIA ALIMENTARIA",
        "VENTA DE MAQUINARIA",
        "ARRIENDO DE MAQUINARIA PESADA",
        "MAQUINARIA PARA MOVIMIENTOS DE TIERRA",
        "ARRIENDO DE MAQUINARIA PARA LA MINERIA",
        "VENTA DE CAMIONES Y MAQUINARIAS",
        "MAQUINARIA AGRICOLA",
        "REPUESTOS PARA MAQUINARIA AGRICOLA",
        "MAQUINARIA PESADA",
        "TRASLADO DE MAQUINARIAS",
        "MAQUINARIA INDUSTRIAL",
        "IMPORTADORES DE MAQUINARIAS",
        "MAQUINARIAS Y EQUIPOS PARA LA CONSTRUCCION",
        "REPUESTOS PARA MAQUINARIA INDUSTRIAL",
        "REPARACION DE MAQUINARIA AGRICOLA",
        "REPARACION DE MAQUINARIA PESADA",
        "COMPRA Y VENTA DE MAQUINARIA AGRICOLA",
        "ARRIENDO DE MAQUINARIAS Y EQUIPOS PARA LA CONSTRUCCION",
        "MAQUINARIA PARA PAVIMENTACION",
        "MAQUINARIA PARA BARRER NIEVE",
        "MAQUINARIA PARA MADERAS",
        "REPARACION DE MAQUINARIA PARA LA MINERIA",
        "MAQUINARIA METALMECANICA",
        "MAQUINARIA PARA EXCAVACIONES",
        "REPUESTOS PARA MAQUINARIA PARA CONSTRUCCION",
        "REPUESTOS PARA MAQUINARIA PARA MINERIA",
        "VENTA DE MAQUINARIA PARA LA CONSTRUCCION",
        "VENTA DE REPUESTOS Y MAQUINARIA AGRICOLA",
        "SERVICIO TÉCNICO DE MAQUINARIA AGRÍCOLA",
        "HERRAMIENTAS Y MAQUINARIAS",
        "REPARACION DE MAQUINARIA PARA LA CONSTRUCCION",
        "REPUESTOS PARA MAQUINARIA PESADA",
        "MAQUINARIA PARA LA INDUSTRIA VITIVINICOLA",
        "MAQUINARIAS DE CONSTRUCCION",
        "MAQUINARIA PARA LA INDUSTRIA MADERERA",
        "REPUESTOS PARA MAQUINARIA FORESTAL",
        "IMPORTACION DE MAQUINARIAS",
        "VENTA Y ARRIENDO DE MAQUINARIAS",
        "VENTA DE MAQUINARIA PARA LA MINERIA",
        "MAQUINARIA PARA LA AGROINDUSTRIA",
        "INSUMOS PARA MAQUINARIA PESADA",
        "MANTENCION DE MAQUINARIA PESADA",
        "MAQUINARIA PARA FUNDICION",
        "REPUESTOS PARA MAQUINARIA DE SONDAJES",
        "ARRIENDO Y VENTA DE MAQUINARIAS PARA LA CONSTRUCCION",
        "MAQUINARIA HIDRAULICA",
        "GRUAS PARA MAQUINARIAS",
        "ARRIENDO DE BODEGAS",
        "ARRIENDO DE GRUAS DE HORQUILLA",
        "ARRIENDO DE MAQUINARIAS",
        "ARRIENDO DE MAQUINARIA AGRICOLA",
        "ARRIENDO DE MAQUINARIA PARA CONSTRUCCION",
        "ARRIENDO DE GRUAS PLUMA",
        "ARRIENDO DE CAMIONES PLUMA",
        "ARRIENDO DE GRUAS",
        "ARRIENDO DE MAQUINARIA PARA MOVIMIENTOS DE TIERRA",
        "ARRIENDO DE GENERADORES",
        "ARRIENDO DE RETROEXCAVADORAS",
        "ARRIENDO DE CAMIONES GRUA",
        "ARRIENDO DE CAMIONES",
        "ARRIENDO CAMION 3/4",
        "ARRIENDO DE MINICARGADORES",
        "ARRIENDO DE GRUAS PARA CONSTRUCCION",
        "ARRIENDO DE GRUPOS ELECTROGENOS",
        "ARRIENDO DE CAMIONES A PRODUCTORA",
        "ARRIENDO DE CAMIONES ALJIBE",
        "ARRIENDO DE CAMIONES TOLVA",
        "ARRIENDO DE MAQUINARIA PESADA",
        "ARRIENDO DE MAQUINARIA PARA LA MINERIA",
        "ARRIENDO DE CONTENEDORES",
        "ARRIENDO DE CAMIONETAS Y MINIBUSES",
        "ARRIENDO DE BUSES",
        "ARRIENDO DE BAÑOS QUIMICOS",
        "ARRIENDO DE GRUAS PARA VEHICULOS",
        "ARRIENDO GRUAS PLUMAS ARTICULADAS",
        "ARRIENDO DE TRACTORES",
        "VENTA Y ARRIENDO DE CONTENEDORES",
        "ARRIENDO CAMION GRUA PLUMA",
        "ARRIENDO DE GRUAS TELESCOPICAS",
        "ARRIENDO DE ANDAMIOS",
        "ARRIENDO DE MAQUINAS Y EQUIPOS PARA ASEO INDUSTRIAL",
        "ARRIENDO DE MAQUINARIAS Y EQUIPOS PARA LA CONSTRUCCION",
        "ARRIENDO DE GRUAS ALTO TONELAJE",
        "ARRIENDO DE COMPACTADORES DE SUELO",
        "ARRIENDO DE GENERADORES ELECTRICOS",
        "ARRIENDO DE PLATAFORMAS ELEVADORAS",
        "ARRIENDO DE MANIPULADOR TELESCOPICO",
        "ARRIENDO BRAZO ARTICULADO",
        "ARRIENDO DE CANASTILLO",
        "ARRIENDO DE EXCAVADORAS",
        "ARRIENDO DE PLATAFORMAS AEREAS",
        "ARRIENDO DE PLATAFORMAS DE ALTURA",
        "ARRIENDO ALZA HOMBRES",
        "ARRIENDO DE PLATAFORMAS GENIE",
        "ARRIENDO TRANSPALETAS",
        "ARRIENDOS DE GENERADORES ELECTRICOS",
        "ARRIENDO DE COMPRESORES",
        "ARRIENDO PARA FIESTAS Y EVENTOS",
        "ARRIENDO DE EQUIPOS DE ILUMINACION",
        "ARRIENDO DE HERRAMIENTAS PARA TENDIDOS ELECTRICOS",
        "ARRIENDO DE CAMIONETAS MINERAS",
        "ARRIENDO DE BOMBAS",
        "ARRIENDO DE BOMBAS SUMERGIBLES",
        "ARRIENDO DE TORRES DE ILUMINACION",
        "ARRIENDO DE RODILLOS",
        "ARRIENDO DE CAMIONES ALZA HOMBRES",
        "ARRIENDO DE HERRAMIENTAS ELECTRICAS",
        "ARRIENDO CARGADOR FRONTAL",
        "ARRIENDO CAMION PLUMA CAMA",
        "ARRIENDO DE RODILLOS COMPACTADORES",
        "VENTA Y ARRIENDO DE MAQUINARIAS",
        "ARRIENDO DE ESTANQUES",
        "ARRIENDO DE BOMBAS PARA AGOTAMIENTO DE AGUAS",
        "ARRIENDO DE VEHICULOS MINEROS",
        "ARRIENDO DE EQUIPOS DE AIRE ACONDICIONADO",
        "ARRIENDO DE HERRAMIENTAS Y EQUIPOS",
        "ARRIENDO DE SANITARIOS QUIMICOS",
        "ARRIENDO MANIPULADOR TELESCOPIO",
        "ARRIENDO DE INSTRUMENTOS TOPOGRAFICOS",
        "ARRIENDOS Y VENTAS DE BAÑOS QUIMICOS",
        "ARRIENDO DE EQUIPO PARA GRANALLADO",
        "ARRIENDO DE DUCHAS PORTATILES",
        "ARRIENDO Y VENTA DE MAQUINARIAS PARA LA CONSTRUCCION",
        "ARRIENDO DE BAÑOS PORTATILES",
        "ARRIENDO DE ESCALAS TELESCOPICAS"
    ]
    companies = connection[DB]['companies'].find({'categories.name': {'$in': categories}})
    companies = [c for c in companies]
    # companies.reverse()
    # threads_mercantil = download_companies(30)

    # Wait for all threads to complete
    # for t in threads_mercantil:
    #    t.join()
    # Wait for all threads to complete
    # threads_genealog = fetch_genealog_info(companies, 30)
    # for t in threads_genealog:
    #    t.join()

    threads_mercadopublico = fetch_mercadopublico_info(companies, 5)
    for t in threads_mercadopublico:
        t.join()


def get_directories_threads(links, timeout=20):
    ngroups = 4
    threads = []

    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    for seed in range(ngroups):
        scrappa = Scrappa(timeout=timeout, browser=webdriver.Chrome, browser_args={'chrome_options': chrome_options})
        group = [[links[i]] for i in range(seed, len(links), ngroups)]
        thread = MyThread(group, scrappa, fetch_directories)
        threads.append(thread)
        thread.start()
    return threads


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
fetch_subareas(scrappa)
scrap()
#
scrappa = Scrappa(timeout=20, browser=webdriver.Firefox)
fetch_directories(scrappa,
                  [1403, 1133, 1132, 1131, 1076, 1080, 1116, 1117, 1419, 1118, 1121, 1119, 1120, 1421, 1420, 1382, 1422,
                   1123, 1122, 1239, 1237])
scrappa.driver.close()
'''

'''
threads_genealog = fetch_genealog_info(30)

# Wait for all threads to complete
for t in threads_genealog:
    t.join()
'''
scrap()


def directories():
    threads_mercantil_directories = get_directories_threads(
        [1403, 1133, 1132, 1131, 1076, 1080, 1116, 1117, 1419, 1118, 1121, 1119, 1120, 1421, 1420, 1382, 1422,
         1123, 1122, 1239, 1237],
        30)

    # Wait for all threads to complete
    for t in threads_mercantil_directories:
        t.join()
