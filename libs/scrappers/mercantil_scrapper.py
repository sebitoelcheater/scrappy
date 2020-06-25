from libs.scrapper import Scrapper
from libs.utils import get_text


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
            'phones': [a.text for a in details.find('div', {'id': 'phon_phones'}).findAll('a')],
            'categories': [{'link': f"{host}{a.attrs['href']}", 'name': a.text} for a in categories],
            'description': soup.select_one('#n_empresa') and soup.select_one('#n_empresa').text,
            'contacts': [s.text for s in soup.select('.contacto_acordeon > p > strong')],
        }
        try:
            _.set(dct, 'address.street', soup.select_one('span[streetaddress]').text)
            _.set(dct, 'address.commune', soup.select_one('span[itemprop="addressLocality"]').text)
            _.set(dct, 'address.province', soup.select_one('span[itemprop="addressRegion"]').text)
        except:
            pass
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
