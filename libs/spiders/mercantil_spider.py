from hammock import Hammock
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from libs.scrappers.mercantil_scrapper import MercantilScrapper
from libs.spider import Spider
from pydash import py_ as _


class MercantilSpider(Spider):
    def __init__(self, ruts=None, ids=None, *args, **kwargs):
        super(MercantilSpider, self).__init__(*args, **kwargs)
        self.ruts = ruts
        self.ids = ids

    # deprecated
    def search(self, rut):
        browser = self.browser
        try:
            browser._get(f'https://www.mercantil.com/results.aspx?keywords={rut}', False)
            WebDriverWait(browser.driver, 120).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#errorBusqueda, p a#compLink, .contError'))
            )
        except TimeoutException:
            print('retrying', rut)
            self.browser._reload()
            return self.search(rut)
        a_result_element = self.browser.get_soup().select_one('p a#compLink')
        if a_result_element is None:
            return None
        return f'https://www.mercantil.com{a_result_element.attrs["href"]}'

    def get_company_from_webpage(self, url):
        browser = self.browser
        browser._get(url, False)
        scrapped = MercantilScrapper.detail(browser.get_soup())
        return {'data': scrapped, 'url': url, 'source': 'mercantil'}

    def query_api(self, company_id):
        api = Hammock('https://www.mercantil.com/ficha.aspx')
        actions = [
            'GetPortContacts',
            'GetPhones',
            'GetCompanyInfo',
            'GetPortUrls',
        ]
        obj = {}
        for action in actions:
            response = api(action).POST(json={'meco_code': company_id})
            if response.status_code != 200:
                print('EXCEPTION WITH', company_id)
                return None
            data = response.json()['d']
            if data == []:
                continue
            obj[action] = response.json()['d']
        return None if obj == {} else obj

    def run(self, collection):
        for company_id in self.ids:
            if collection.find_one({'_id': company_id}) is not None:
                continue
            try:
                data = self.query_api(company_id)
                tax_id = _.get(data, 'GetCompanyInfo.0.Comp_Taxid')
                rut = None
                if tax_id is not None:
                    rut = _.get(data, 'GetCompanyInfo.0.Comp_Taxid').replace('-', '')
                yield rut, company_id, data
            except:
                continue

    def old_do(self):
        for rut in self.ruts:
            result = None
            company_link = self.search(rut)
            if company_link is not None:
                result = self.get_company_from_webpage(company_link)
            yield rut, result
