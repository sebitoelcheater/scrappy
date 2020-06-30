from bs4 import BeautifulSoup, NavigableString

from libs.parser import Parser
from libs.refiner import Refiner
from pydash import py_ as _
import re


class GenealogParser(Parser):
    def title(self):
        return

    def business_name(self):
        return

    def rut(self):
        return

    def area(self):
        return

    def sub_area(self):
        return

    def activity(self):
        return

    def region(self):
        return

    def commune(self):
        return

    def city(self):
        return

    def taxpayer_type(self):
        return

    def taxpayer_subtype(self):
        return

    def start_date(self):
        return

    def workers(self):
        return

    def contacts(self):
        return

    def phones(self):
        phones = []
        for phones_html in _.get(self.raw_dict, 'telefonos_parseOnAsk', []):
            soup = BeautifulSoup(phones_html)
            for phone_div in soup.find_all('div', {'class': 'telefono'}):
                [phones.append(s) for s in phone_div.find('a').stripped_strings]
        for index, phone in enumerate(phones):
            _.set(self.final_value, f'contacts[{index}].phones[0]', phone)
        return

    def names(self):
        contacts = []
        for contact_html in _.get(self.raw_dict, 'parseOnAsk', []):
            soup = BeautifulSoup(contact_html)
            contact = None
            for child in soup.find('body').children:
                if child.name == 'span':
                    if contact is not None:
                        contacts.append(contact)
                    contact = {}
                    strings = [s for s in child.stripped_strings]
                    if len(strings) > 0:
                        contact['name'] = [s for s in child.stripped_strings][0]
                elif child.name == 'a':
                    if 'emails' not in contact:
                        contact['emails'] = []
                    [contact['emails'].append(s) for s in child.stripped_strings]
                elif isinstance(child, NavigableString):
                    position_search_result = re.search("\((.*)\)", child.string)
                    if position_search_result:
                        contact['work_position'] = _.title_case(position_search_result.group(1))
            if contact is not None:
                contacts.append(contact)
        self.final_value = _.merge(self.final_value, {'contacts': contacts})
        return

    def urls(self):
        urls = []
        for url_html in self.raw_dict['urls_parseOnAsk']:
            soup = BeautifulSoup(url_html)
            soup.find_all('a')
            for a in soup.find_all('a'):
                if 'href' in a.attrs:
                    urls.append(a.attrs['href'])
        return urls

    def websites(self):
        return

    def description(self):
        return

    def social_networks(self):
        social_networks = []
        [[div.find_all('a') for div in BeautifulSoup(s).find_all('div', {'class': 'social'})] for s in
         self.raw_dict['socials_parseOnAsk']]
        for socia_html in self.raw_dict['socials_parseOnAsk']:
            for social_div in BeautifulSoup(socia_html).find_all('div', {'class': 'social'}):
                for link in social_div.find_all('a'):
                    if 'href' in link.attrs:
                        social_networks.append(link.attrs['href'])
        return social_networks

    def skip(self):
        return


class GenealogRefiner(Refiner):
    def __init__(self, raw):
        super(GenealogRefiner, self).__init__()
        self.raw = raw

    def run(self):
        if self.raw is None:
            return None
        parser = GenealogParser(self.raw)
        switcher = {
            "title": 'title',
            "Razón Social": 'business_name',
            "Razón Social ": 'business_name',
            "Razón Social  ": 'business_name',
            " Razón Social": 'business_name',
            "RUT": 'rut',
            "Rubro": 'area',
            "Subrubro": 'sub_area',
            "Actividades Económicas o Giros": 'activity',
            "Región": 'region',
            "Comuna": 'commune',
            "Ciudad:": 'city',
            "Tipo Contribuyente": 'taxpayer_type',
            "Subtipo Contribuyente": 'taxpayer_subtype',
            "Fecha de Inicio": 'start_date',
            "Cantidad de personas": 'workers',
            'Teléfonos': 'skip',
            'Contacto': 'skip',
            "Sitios web": 'websites',
            "description": 'description',
            "Redes Sociales": 'social_networks',
            "RUT ": 'rut',
            "RUT  ": 'rut',
            " RUT": 'rut',
            "  RUT": 'rut',
            " RUT ": 'rut',
            "slogan": 'rut',
            "parseOnAsk": 'names',
            "telefonos_parseOnAsk": 'phones',
            "urls_parseOnAsk": 'urls',
            "socials_parseOnAsk": 'social_networks',
        }
        for key, value in self.raw.items():
            parser.parse(switcher[key])
        value = parser.get_value()
        if value == {}:
            return None
        return value
