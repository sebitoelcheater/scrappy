from libs.scrapper import Scrapper
from libs.utils import get_text


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
        if soup.select_one('#lblLinkSitioWeb'):
            dct['web'] = soup.select_one('#lblLinkSitioWeb').attrs['href']
        if len(dct) == 0:
            return
        return dct
