import copy
import sys, os

import mongoengine
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

path = os.path.abspath(os.path.join('.', 'scrapping'))
print('path')
print(path)
sys.path.append(path)
from scrapping import Scrappa, MercantilScrapper, Link, Company, RawScrapped, MercadoPublicoScrapper, Person
import platform
import threading

print(platform.system())
osx_ip = '192.168.74.105'

if platform.system() == "Darwin":
    print(':D')
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
        "https://www.mercantil.com/medicos-centros-medicos/1882",
        "https://www.mercantil.com/medicos/1205",
        "https://www.mercantil.com/clinicas-veterinarias/1896",
        "https://www.mercantil.com/dentistas-clinicas-dentales/4542",
        "https://www.mercantil.com/psicologos/2588",
        "https://www.mercantil.com/hospitales/995",
        "https://www.mercantil.com/medicos-oftalmologia/4329",
        "https://www.mercantil.com/medicos-ginecologia/4450",
        "https://www.mercantil.com/medicos-psiquiatria/4466",
        "https://www.mercantil.com/terapias-alternativas/4521",
        "https://www.mercantil.com/casas-de-reposo/1825",
        "https://www.mercantil.com/kinesiologos/2416",
        "https://www.mercantil.com/medicos-pediatria/4465",
        "https://www.mercantil.com/medicos-traumatologia-y-ortopedia/4471",
        "https://www.mercantil.com/medicos-otorrinolaringologia/4327",
        "https://www.mercantil.com/medicos-dermatologia-y-venereologia/4441",
        "https://www.mercantil.com/medicos-medicina-general/4454",
        "https://www.mercantil.com/medicos-urologia/4472",
        "https://www.mercantil.com/medicos-cardiologia/4432",
        "https://www.mercantil.com/kinesiologia/3960",
        "https://www.mercantil.com/medicos-neurologia/4458",
        "https://www.mercantil.com/medicos-radiologia/4467",
        "https://www.mercantil.com/medicos-cirugia-plastica/4439",
        "https://www.mercantil.com/psicologos-infanto-juvenil/2814",
        "https://www.mercantil.com/psicopedagogos/2815",
        "https://www.mercantil.com/medicos-endocrinologia/4446",
        "https://www.mercantil.com/clinicas-psiquiatricas/3924",
        "https://www.mercantil.com/medicos-reumatologia/8191",
        "https://www.mercantil.com/medicos-medicina-interna/4456",
        "https://www.mercantil.com/medicos-obesidad/4460",
        "https://www.mercantil.com/centros-de-rehabilitacion/1294",
        "https://www.mercantil.com/medicos-cirugia-general/4549",
        "https://www.mercantil.com/medicos-gastroenterologia/4222",
        "https://www.mercantil.com/medicos-cirugia-abdominal-y-digestiva/4434",
        "https://www.mercantil.com/medicos-medicina-general-atencion-domiciliaria/4455",
        "https://www.mercantil.com/medicos-neurocirugia/4457",
        "https://www.mercantil.com/medicos-neurologia-infantil/4459",
        "https://www.mercantil.com/acupuntura/71",
        "https://www.mercantil.com/medicos-cirugia-vascular/4440",
        "https://www.mercantil.com/medicos-ecografia/4443",
        "https://www.mercantil.com/medicos-obstetricia/4462",
        "https://www.mercantil.com/centros-de-dialisis/1721",
        "https://www.mercantil.com/clinicas-oftalmologicas/3923",
        "https://www.mercantil.com/medicos-geriatria/4449",
        "https://www.mercantil.com/medicos-cirugia-infantil/4437",
        "https://www.mercantil.com/implantes-dentales/5030",
        "https://www.mercantil.com/medicos-diabetes-y-nutricion/4442",
        "https://www.mercantil.com/homeopatia/989",
        "https://www.mercantil.com/medicos-psiquiatria-infantil-y-adolescentes/4741",
        "https://www.mercantil.com/servicios-de-asistencia/1849",
        "https://www.mercantil.com/medicos-cancerologia/4431",
        "https://www.mercantil.com/medicos-endocrinologia-infantil/4447",
        "https://www.mercantil.com/medicos-broncopulmonares-y-tuberculosis/4430",
        "https://www.mercantil.com/medicos-medicina-fisica-y-rehabilitacion/4453",
        "https://www.mercantil.com/medicos-sexologia/4469",
        "https://www.mercantil.com/quiropractica/4734",
        "https://www.mercantil.com/medicos-cirugia-de-la-mama/4436",
        "https://www.mercantil.com/medicina-natural/4426",
        "https://www.mercantil.com/medicos-electroencefalografia/4550",
        "https://www.mercantil.com/medicos-varices-ulceras-varicosas-y-hemorroides/4473",
        "https://www.mercantil.com/medicos-cardiologia-infantil/4433",
        "https://www.mercantil.com/centros-naturistas/1307",
        "https://www.mercantil.com/vacunatorios/4714",
        "https://www.mercantil.com/clinicas-cirugia-plastica/3671",
        "https://www.mercantil.com/medicos-cirugia-ortopedica-y-traumatologia-infantil/4438",
        "https://www.mercantil.com/medicos-endoscopia/4448",
        "https://www.mercantil.com/urgencias-medicas/3299",
        "https://www.mercantil.com/ecotomografias/2161",
        "https://www.mercantil.com/medicos-cirugia-cardiovascular/4435",
        "https://www.mercantil.com/medicos-ginecologia-infantil/4551",
        "https://www.mercantil.com/hospitalizacion-domiciliaria/4415",
        "https://www.mercantil.com/kinesiologia-infantil/4670",
        "https://www.mercantil.com/medicos-medicina-nuclear/4684",
        "https://www.mercantil.com/medicos-inmunologia/4658",
        "https://www.mercantil.com/apiterapia/4637",
        "https://www.mercantil.com/medicos-homeopatia/4721",
        "https://www.mercantil.com/medicos-otoneurologia/4653",
        "https://www.mercantil.com/matronas/2589",
        "https://www.mercantil.com/medicos-toxicologia/4470",
        "https://www.mercantil.com/medicos-electrocardiografia/4444",
        "https://www.mercantil.com/medicos-medicina-del-dolor-y-paliativa/4452",
        "https://www.mercantil.com/medicos-alcoholismo/4666",
        "https://www.mercantil.com/medicos-medicina-del-sueno/4738",
        "https://www.mercantil.com/centros-de-ejercicios-sin-esfuerzo/2410",
        "https://www.mercantil.com/dietistas/1748",
    ]
    chrome_options = webdriver.ChromeOptions()
    prefs = {'profile.managed_default_content_settings.images': 2}
    chrome_options.add_experimental_option("prefs", prefs)
    scrappa = Scrappa(timeout=timeout, browser=webdriver.Chrome, browser_args={'chrome_options': chrome_options})
    repeated, inserted = 0, 0

    for md_link in mercantil_directorio_links:
        print(md_link)
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
