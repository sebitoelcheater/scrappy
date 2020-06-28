import json
import re
from bs4 import BeautifulSoup
from hammock import Hammock
from py_mini_racer.py_mini_racer import MiniRacer
from pydash import py_ as _
from libs.spider import Spider
import base64

with open('libs/mercantil.js', encoding='utf8') as file:
    javascript = "".join(file)


class GenealogSpider(Spider):
    def __init__(self, ruts, mutex, *args, **kwargs):
        super(GenealogSpider, self).__init__(*args, **kwargs)
        self.ruts = ruts
        self.mutex = mutex

    def get_company_link(self, rut):
        api = Hammock('https://www.genealog.cl/Geneanexus/search')
        request = api.POST(
            headers={'content-type': 'application/x-www-form-urlencoded'},
            data={"value": rut}
        )
        if request.status_code != 200:
            return
        search = re.search("var result = ({[^;]*})", request.text)
        if search is None:
            return
        data = json.loads(search.group(1))
        if data['content'] == []:
            return
        evaluation = MiniRacer().eval(
            f"{javascript}setRegex({json.dumps(data['regex'])});getRutLink({json.dumps(data['content'])})"
        )
        return evaluation

    def fetch_link(self, url):
        api = Hammock(url)
        response = api.GET()
        return response.text

    def decode_content(self, soup):
        obj = {}
        for item in soup.select('.parseOnAsk'):
            obj["_".join(item.attrs['class'])] = []
            for stripped_string in item.stripped_strings:
                decoded = base64.b64decode(stripped_string).decode('UTF-8')
                obj["_".join(item.attrs['class'])].append(str(BeautifulSoup(decoded)))
        return obj

    def search(self, rut):
        url = self.get_company_link(rut)
        if url is None:
            return None
        content = self.fetch_link(url)
        soup = BeautifulSoup(content)
        raw_decoded = self.decode_content(soup)
        raw = self.extract_information(soup)
        if raw is None:
            return None
        rut_key = _.find(_.keys(raw), lambda k: 'RUT' in k)
        if rut_key is None:
            print(rut, 'NO RUT KEY FOUND')
        if rut not in re.sub(r'\.*-*', '', raw[rut_key]):
            print(rut, 'RUT DOES NOT MATCH')
        return {'data': {**raw, **raw_decoded}, 'source': 'genealog', 'url': url}

    def extract_information(self, soup):
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
