from libs.parser import Parser
from libs.refiner import Refiner
from pydash import py_ as _


class GenealogParser(Parser):
    def parse_all(self):
        for field in self.parsers():
            self.parse(field)

    def parsers(self):
        return [
            'GetPortContacts',
            'GetPhones',
            'GetCompanyInfo',
            'GetPortUrls',
        ]

    def GetPortContacts(self):
        if 'GetPortContacts' not in self.raw_dict:
            return
        contacts = []
        for raw_contact in _.get(self.raw_dict, 'GetPortContacts', []):
            contact = {}
            email = _.trim(_.get(raw_contact, 'Cont_Email', None))
            if email is not None and email != '':
                _.set(contact, 'emails', [email])
            name = _.trim(_.get(raw_contact, 'Cont_Name', None))
            first_last_name = _.trim(_.get(raw_contact, 'Cont_FirstName', None))
            second_last_name = _.trim(_.get(raw_contact, 'Cont_LastName', None))
            last_name = " ".join([s for s in [first_last_name, second_last_name] if s is not None and s != ''])
            if name is not None and name != '':
                contact['first_name'] = name
            if last_name is not None and last_name != '':
                contact['last_name'] = last_name
            full_name = ' '.join([s for s in [name, first_last_name, second_last_name] if s is not None and s != ''])
            if full_name is not None and last_name != '':
                contact['name'] = full_name
            if len(contact) > 0:
                contacts.append(contact)
            cargo = _.trim(_.get(raw_contact, 'No_Cargo', None))
            depto = _.trim(_.get(raw_contact, 'Posi_DescEsp', None))
            if cargo is not None and cargo != '':
                contact['work_position'] = _.title_case(_.trim(cargo))
            if depto is not None and depto != '':
                contact['department'] = _.title_case(_.trim(depto))
        self.final_value = _.merge(self.final_value, {'contacts': contacts})
        return

    def GetPhones(self):
        if 'GetPhones' not in self.raw_dict:
            return
        for index, raw_phone in enumerate(self.raw_dict['GetPhones']):
            phone = raw_phone['Phon_Phone']
            ddn = raw_phone['Phon_DDN']
            _.set(self.final_value, f'contacts[{index}].phones[0]', f'+56{ddn}{phone}')
        return

    def GetCompanyInfo(self):
        if 'GetCompanyInfo' not in self.raw_dict:
            return
        for index, raw_company_info in enumerate(self.raw_dict['GetCompanyInfo']):
            _.set(self.final_value, 'name', raw_company_info['Comp_TradeName'])
        return

    def GetPortUrls(self):
        urls = []
        if 'GetPortUrls' not in self.raw_dict:
            return
        for raw_url in self.raw_dict['GetPortUrls']:
            url = _.trim(raw_url['Urls_Urls'])
            if url is not None and url != '':
                urls.append(url)
        return urls


class MercantilRefiner(Refiner):
    def __init__(self, raw):
        super(MercantilRefiner, self).__init__()
        self.raw = raw

    def run(self):
        if self.raw is None:
            return None
        parser = GenealogParser(self.raw)
        parser.parse_all()
        value = parser.get_value()
        if value == {}:
            return None
        return value
