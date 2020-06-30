import platform
from threading import Thread, Lock
from mongoengine import connect
from pydash import py_ as _
from libs.refiners.genealog_refiner import GenealogRefiner
from libs.refiners.mercantil_refiner import MercantilRefiner
from libs.threadsafe import threadsafe_generator
from libs.utils import create_and_run_mercantil_spider, create_and_run_genealog_spider, map_key, id_generator

DB_NAME = 'scrappy'
if platform.system() == "Darwin":
    connection = connect(DB_NAME, alias='default')
else:
    connection = connect(DB_NAME, alias='default', host='', port=27017)


def run_genealog_spiders(use_ruts_file=False):
    company_filter = {
        '$or': [
            {'economic_heading.id': 'F'},
            {'economic_activity.id': {'$in': ['681011', '465300', '465901', '465909']}},
            {'economic_sub_heading.id': {'$in': ['771', '773', '829']}}
        ]
    }
    genealog_col = connection[DB_NAME]['sii_companies'].find({
        **{'raws.genealog': {'$exists': False}},
        **company_filter
    })

    def genealog_collection():
        items = genealog_col.limit(5000)
        while items.count() > 0:
            for item in items:
                yield item
            items = genealog_col.limit(5000)

    ruts_genealog = map_key(genealog_collection(), 'rut', genealog_col.count())
    threads = []

    @threadsafe_generator
    def genealog_ruts():
        with open('fixtures/ruts.txt') as file:
            for line in file:
                rut = line.replace('\n', '')
                if connection[DB_NAME]['genealog'].find_one({'_id': rut}) is None:
                    yield line.replace('\n', '')
    if use_ruts_file:
        ruts_genealog = genealog_ruts()
    mutex = Lock()
    for i in range(1):
        thread = Thread(target=create_and_run_genealog_spider,
                        args=(ruts_genealog, connection[DB_NAME]['genealog'], mutex))
        threads.append(thread)
        thread.start()
    return threads


def run_mercantil_spiders():
    threads = []
    for i in range(0):
        ids = id_generator(range(i * 60000 + 1, (i + 1) * 60000 + 1))
        thread = Thread(target=create_and_run_mercantil_spider,
                        args=(ids, connection[DB_NAME]))
        threads.append(thread)
        thread.start()
    return threads


def run_genealog_refiner():
    for genealog_record in connection[DB_NAME]['genealog'].find({
        'raw': {'$exists': True, '$ne': None},
        'parsed': {'$exists': False}
    }):
        parsed = GenealogRefiner(genealog_record['raw']).run()
        if parsed is not None:
            connection[DB_NAME]['genealog'].update_one(
                {'_id': genealog_record['_id']},
                {'$set': {'parsed': parsed}}
            )


def run_mercantil_refiner():
    for mercantil_record in connection[DB_NAME]['mercantil'].find({
        'raw': {'$exists': True, '$ne': None},
        'parsed': {'$exists': False}
    }):
        parsed = MercantilRefiner(mercantil_record['raw']).run()
        if parsed is not None:
            connection[DB_NAME]['mercantil'].update_one(
                {'_id': mercantil_record['_id']},
                {'$set': {'parsed': parsed}}
            )


def print_contacts():
    for company in connection[DB_NAME]['sii_companies'].find(
            {'contacts.emails': {'$exists': True}, 'contacted': {'$exists': False}}):
        for contact in _.get(company, 'contacts', []):
            if 'emails' in contact:
                print(';'.join([
                    company['rut'],
                    contact['name'],
                    contact['work_position'],
                    company['name'],
                    company['economic_heading']['id'],
                    company['economic_heading']['name'],
                    company['economic_sub_heading']['id'],
                    company['economic_sub_heading']['name'],
                    company['economic_activity']['id'],
                    company['economic_activity']['name'],
                    str(company['tranche_according_to_sales']),
                    ', '.join(_.get(contact, 'emails', [])),
                    ', '.join(_.get(contact, 'phones', [])),
                ]))


def run_spiders():
    threads = []
    threads += run_genealog_spiders(True)
    # threads += run_mercantil_spiders()
    [t.join() for t in threads]


if __name__ == '__main__':
    # run_genealog_refiner()
    run_spiders()
    # print_contacts()
