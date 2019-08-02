import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
from mongoengine import *
from selenium.common.exceptions import UnexpectedAlertPresentException, TimeoutException, NoAlertPresentException
from selenium.webdriver.support.wait import WebDriverWait


def get_text(html_element):
    try:
        return html_element.text
    except Exception:
        return None


class Scrapper:
    @classmethod
    def scraps(cls, soups, scrap_fun):
        data = pd.DataFrame()
        for soup in soups:
            dct = scrap_fun(soup)
            if dct is not None:
                data = data.append(dct)
        return data


class MercantilScrapper(Scrapper):
    @classmethod
    def detail(cls, soup):
        details = soup.find('div', {'class': 'primer_detalle'})
        host = 'https://www.mercantil.com'
        if details is None:
            return
        categories = soup.find('div', {'id': 'MecoActivities'}).findAll('a', {'class': 'metas'})
        dct = {
            'legal_name': get_text(details.find('span', {'itemprop': 'name'})),
            'name': get_text(soup.find('a', {'itemprop': 'name'})),
            'rut': get_text(details.find('div', {'id': 'tatyid'}).find('span')),
            'address': ", ".join(s.text for s in details.find('div', {'id': 'address'}).findAll('span')),
            'phones': [a.text for a in details.find('div', {'id': 'phon_phones'}).findAll('a')],
            'categories': [{'link': f"{host}{a.attrs['href']}", 'name': a.text} for a in categories],
            'description': soup.select_one('#n_empresa') and soup.select_one('#n_empresa').text,
            'contacts': [s.text for s in soup.select('.contacto_acordeon > p > strong')],
            'location': {
                'latitude': soup.select_one('[itemprop=latitude]').attrs['content'],
                'longitude': soup.select_one('[itemprop=longitude]').attrs['content']
            },
        }
        try:
            latitude = soup.select_one('[itemprop=latitude]').attrs['content']
            longitude = soup.select_one('[itemprop=longitude]').attrs['content']
            dct['location'] = {
                'latitude': latitude,
                'longitude': longitude,
            }
        except:
            pass
        if details.find('span', {'itemprop': "url"}):
            dct['web'] = details.find('span', {'itemprop': "url"}).find('a').attrs['href']
        if soup.find('div', {'id': "qty_per"}):
            dct['number_of_workers'] = get_text(soup.find('div', {'id': "qty_per"}).find('p'))
        if soup.find('div', {'id': "desc_emp"}):
            dct['size'] = get_text(soup.find('div', {'id': "desc_emp"}).find('p'))
        return dct

    @classmethod
    def area(cls, soup):
        host = 'https://www.mercantil.com'
        groups_soup = soup.select('#Contenido_selsubarea > div')
        groups = []
        for group_soup in groups_soup:
            group = {'name': group_soup.find('p').text, 'sub_areas': []}
            for sarea_soup in group_soup.find_all('a'):
                sarea = {
                    'name': sarea_soup.text,
                    'code': sarea_soup.attrs['href'].split('=')[-1],
                    'link': f"{host}/{sarea_soup.attrs['href']}"
                }
                group['sub_areas'].append(sarea)
            groups.append(group)
        dct = {
            'name': soup.select_one('#Contenido_areaname').text.replace('  ', ''),
            'groups': groups
        }
        return dct

    @classmethod
    def sub_area(cls, soup):
        directory_soups = soup.select('#selativities > div')
        host = 'https://www.mercantil.com'
        directories = []
        for directory_soup in directory_soups:
            a = directory_soup.find('a')
            directories.append({
                'link': f"{host}{a.attrs['href']}",
                'name': a.text
            })
        dct = {
            'name': soup.select_one('#titarea').text.replace('  ', ''),
            'directories': directories
        }
        return dct

    @classmethod
    def list(cls, soup):
        companies = []
        title = soup.find('a', {'id': 'titsup'})
        for company_container in soup.findAll('div', {'class': 'empresa1'}):
            a = company_container.find('a', {'id': 'compLink'})
            try:
                dct = {
                    'link': a.attrs['href'],
                    'host': 'https://www.mercantil.com',
                    'name': a.text,
                }
            except Exception as e:
                continue
            try:
                if title: dct['category'] = title.text
                if title: dct['directory'] = title.attrs['href']
                dct['comuna'] = company_container.find('div', {'class': 'cont_prev'}).find('p').text
            except Exception as e:
                pass
            companies.append(dct)
        return companies


class MercadoPublicoScrapper(Scrapper):
    @classmethod
    def detail(cls, soup):
        dct = {}
        sells = soup.select_one('#ventasHistoricas')
        if sells:
            dct['historical_sales'] = sells.text
        if soup.find('span', {'id': 'lblNombreFantasia'}):
            dct['name'] = get_text(soup.find('span', {'id': 'lblNombreFantasia'}))
        if soup.find('div', {'class': 'company-contact'}) is not None:
            dct['contact_name'] = get_text(soup.find('a', {'id': 'lblPersonaContacto'}))
            dct['phone'] = get_text(soup.find('a', {'id': 'lblTelefonoContacto'}))
            dct['email'] = get_text(soup.find('a', {'id': 'lblMail'}))
            dct['address'] = get_text(soup.find('span', {'id': 'lblDireccion'}))
        if soup.find('div', {'class': 'company-info'}) is not None:
            dct['rut'] = soup.find('span', {'id': 'lblRut'}).text.replace('.', '')
            dct['commercial_business'] = get_text(soup.find('span', {'id': 'lblGiro'}))
            dct['business_name'] = get_text(soup.find('span', {'id': 'lblRazonSocial'}))
        if soup.find('div', {'class': 'company-description'}) is not None:
            dct['description'] = get_text(soup.find('span', {'id': 'lblDescripcionEmpresa'}))
        if len(dct) == 0:
            return
        return dct


class Scrappa:
    def _get(self, link):
        # self._set_driver()
        self.driver.stop_client()
        try:
            self.driver.get('about:blank')
        except UnexpectedAlertPresentException as e:
            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass
        try:
            self.driver.get(link)
            return self.driver.page_source
        except UnexpectedAlertPresentException as e:
            try:
                self.driver.switch_to.alert.accept()
            except NoAlertPresentException:
                pass
            try:
                return self.driver.page_source
            except TimeoutException as e:
                self._set_driver()
                return self._get(link)
        except TimeoutException as e:
            self._set_driver()
            return self._get(link)

    def _set_driver(self):
        if hasattr(self, 'driver') and self.driver is not None:
            self.driver.quit()
        self.driver = self.browser(**self.browser_args)
        self.driver.set_page_load_timeout(self.timeout)
        self.driver.implicitly_wait(self.timeout)
        self.wait = WebDriverWait(self.driver, self.timeout)

    def _reload(self):
        self.driver.stop_client()
        self.driver.start_client()

    def __init__(self, timeout=5, browser=webdriver.Chrome, browser_args=None):
        self.timeout = timeout
        self.browser = browser
        self.browser_args = {} if browser_args is None else browser_args
        self._set_driver()

    def soup(self, link):
        page_source = self._get(link)
        return BeautifulSoup(page_source, 'lxml')

    def soups(self, links):
        for link in links:
            yield self.soup(link)


class Person(EmbeddedDocument):
    name = StringField()
    emails = ListField(StringField())
    phones = ListField(StringField())


class Link(Document):
    host = StringField(required=False)
    name = StringField(required=False)
    link = StringField(primary_key=True)
    data = DictField()


class RawScrapped(EmbeddedDocument):
    link = StringField(unique=True)
    source = StringField()
    data = DynamicField()
    timestamp = DateTimeField(required=False, default=datetime.datetime.utcnow)


class Category(EmbeddedDocument):
    link = StringField()
    name = StringField()


class Company(Document):
    meta = {'ordering': ['created_at']}

    created_at = DateTimeField(required=False)
    rut = StringField(sparse=True, unique=True, required=False)
    phones = ListField(StringField(), required=False)
    emails = ListField(StringField(), required=False)
    addresses = ListField(StringField(), required=False)
    commercial_business = StringField(required=False)
    business_name = StringField(required=False)
    legal_name = StringField(required=False)
    name = StringField(required=False)
    description = StringField(required=False)
    person = EmbeddedDocumentField(Person)
    raws = ListField(EmbeddedDocumentField(RawScrapped), default=[], required=False)
    web = StringField()
    number_of_workers = StringField()
    size = StringField()
    categories = EmbeddedDocumentListField(Category, required=False)
    projects = ListField(StringField(), required=False)

    def save(self, *args, **kwargs):
        if not self.created_at:
            self.created_at = datetime.datetime.utcnow()
        return super(Company, self).save(*args, **kwargs)
