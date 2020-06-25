from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from libs.spider import Spider


class GenealogSpider(Spider):
    def __init__(self, ruts, *args, **kwargs):
        super(GenealogSpider, self).__init__(*args, **kwargs)
        self.ruts = ruts

    def search(self, rut):
        browser = self.browser
        browser._get('https://www.genealog.cl/Geneanexus/search')
        try:
            WebDriverWait(browser.driver, 120).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#search-form input[name=s]'))
            )
            browser.driver.find_element_by_css_selector('#search-form input[name=s]').send_keys(rut + Keys.RETURN)
            WebDriverWait(browser.driver, 120).until(
                EC.invisibility_of_element((By.CSS_SELECTOR, '#progress'))
            )
        except TimeoutException:
            print('retrying', rut)
            self.browser._reload()
            return self.search(rut)
        a_result_element = self.browser.get_soup().select_one('#results-content > tr.person > td.tdRut > a')
        if a_result_element is None:
            return None
        url = a_result_element.attrs['href']
        browser._get(url)
        if self.browser.get_soup().select_one('#OwnEvent_showContact > td > button') is not None:
            browser.driver.find_element_by_css_selector('#OwnEvent_showContact > td > button').click()
        raw = self.extract_information()
        if raw is None:
            return None
        return {'data': raw, 'source': 'genealog', 'url': url}

    def extract_information(self):
        soup = BeautifulSoup(self.browser.driver.page_source, 'lxml')
        table = soup.select_one('#results-content')
        if table is None:
            return None
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
        return scrapped

    def run(self):
        for rut in self.ruts:
            yield rut, self.search(rut)
