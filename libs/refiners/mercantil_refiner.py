from libs.parser import Parser
from libs.refiner import Refiner
from pydash import py_ as _


class GenealogParser(Parser):
    def parse_all(self):
        for field in self.parsers():
            self.parse(field)

    def parsers(self):
        return [
            'contacts'
        ]

    def contacts(self):
        return


class MercantilRefiner(Refiner):
    def __init__(self, company):
        super(MercantilRefiner, self).__init__()
        self.company = company

    def run(self):
        parsed_mercantil = _.get(self.company, 'raws.mercantil.parsed', None)
        parser = GenealogParser(parsed_mercantil)
        if parsed_mercantil is None:
            return None
        parser.parse_all()
        value = parser.get_value()
        if value == {}:
            return None
        return value
