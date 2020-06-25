from libs.scrappa import Scrappa
from libs.threadsafe import threadsafe_generator


def get_text(html_element):
    try:
        return html_element.text
    except Exception:
        return None


@threadsafe_generator
def map_key(items, key, total=0):
    counter = 0
    for item in items:
        counter += 1
        print(f"{counter}/{total}")
        yield item[key]


def create_and_run_mercantil_spider(ids, db):
    from libs.spiders.mercantil_spider import MercantilSpider
    spider = MercantilSpider(ids=ids)
    for rut, company_id, data in spider.run(db['mercantil']):
        if company_id is not None:
            mercantil = {}
            mercantil['_id'] = company_id
            if rut is not None:
                mercantil['rut'] = rut
            if data is not None:
                mercantil['raw'] = data
            db['mercantil'].insert_one(mercantil)
        print('mercantil', company_id, rut, 'OK' if data is not None else 'NOT FOUND')


def create_and_run_genealog_spider(ruts, collection):
    from libs.spiders.genealog_spider import GenealogSpider
    spider = GenealogSpider(ruts, Scrappa(timeout=20, whitelist_domains=['ajax.cloudflare.com', 'www.genealog.cl']))
    for rut, data in spider.run():
        collection.update_one(
            {'rut': rut},
            {'$set': {f'raws.genealog': {'raw': data['data'], 'url': data['url']} if data is not None else None}}
        )
        print('genealog', rut, 'OK' if data is not None else 'NOT FOUND')


def id_generator(ids):
    for n in ids:
        n = f"{n}"
        yield f"30{n.zfill(7)}"