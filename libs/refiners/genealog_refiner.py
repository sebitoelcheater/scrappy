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
        contact_html = _.get(self.raw_dict, 'Contacto', None)
        soup = BeautifulSoup(contact_html)
        contacts = []
        contact = None
        for child in soup.find('td').children:
            if child.name == 'span':
                if contact is not None:
                    contacts.append(contact)
                contact = {}
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
        phones_html = _.get(self.raw_dict, 'Teléfonos')
        if phones_html is not None:
            soup = BeautifulSoup(phones_html)
            phones = []
            for phone_div in soup.find_all('div', {'class': 'telefono'}):
                [phones.append(s) for s in phone_div.find('a').stripped_strings]
            if len(contacts):
                _.set(contacts, '0.phones', phones)
        return _.uniq_by(contacts, 'name')

    def phones(self):
        return

    def websites(self):
        return

    def description(self):
        return

    def social_networks(self):
        return


class GenealogRefiner(Refiner):
    def __init__(self, company):
        super(GenealogRefiner, self).__init__()
        self.company = company

    def run(self):
        raw_genealog = _.get(self.company, 'raws.genealog.raw', None)
        parser = GenealogParser(raw_genealog)
        if raw_genealog is None:
            return None
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
            "Contacto": 'contacts',
            "Teléfonos": 'phones',
            "Sitios web": 'websites',
            "description": 'description',
            "Redes Sociales": 'social_networks',
            "RUT ": 'rut',
            "RUT  ": 'rut',
            " RUT": 'rut',
            "  RUT": 'rut',
            " RUT ": 'rut',
            "slogan": 'rut',
        }
        for key, value in raw_genealog.items():
            parser.parse(switcher[key])
        value = parser.get_value()
        if value == {}:
            return None
        return value
