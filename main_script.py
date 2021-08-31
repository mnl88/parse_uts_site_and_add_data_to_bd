import json
import logging
import os
import re
import time
from datetime import datetime
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, Float
from sqlalchemy import create_engine
# from sqlalchemy.orm import Session, sessionmaker


# If .env not in the current directory
# BASEDIR = os.path.abspath(os.path.dirname(__file__))
# load_dotenv(os.path.join(BASEDIR, '.env1'))


load_dotenv()


logging.basicConfig(
    # filename='parser.log',
    # filemode='a',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

URL = urlparse('https://market.chelpipe.ru/rest/catalog/list/')


def parsing_elements_count() -> int:
    data = {
        'sectionId': 0,
        'showBy': 1,
    }
    with httpx.Client() as client:
        response = client.post(url=URL.geturl(), data=data)

    response_dict = response.json()
    elements_count = response_dict['data']['elements_count']
    return elements_count


def parsing_goods(tubes_count: int, units: str):
    data = {
        'sectionId': 0,
        'showBy': tubes_count,
        'units': units
    }
    with httpx.Client(timeout=100) as client:
        response = client.post(url=URL.geturl(), data=data)
    response.raise_for_status()  # пробросит исключение если статус 4** или 5**
    corrected_text_response = response.text.replace('\"{\'', '{\"').replace('\'', '\"').replace('\\\"', '').replace(
        '}\"}', '}}')
    response_dict = json.loads(corrected_text_response)
    return response_dict


def make_tube_list(parsed_data):
    tube_list = []
    for item in parsed_data['data']["items"]:
        address = str(item['items']['store']['value'])
        tube = {
            'parse_datetime': datetime.now(),
            'avail_on_tracking': 0,
            'avail_free': 0,
            'avail_reserved': 0,
            'tube_id': int(item['id']),
            'url': URL.scheme + "://" + URL.hostname + item['url'],
            'brand': item['analytics']['ecommerce']['click']['products'][0]['brand'],
            'category': item['analytics']['ecommerce']['click']['products'][0]['category'],
            'steel_trademark': item['analytics']['ecommerce']['click']['products'][0]['dimension4'],
            'size_1': float(item['analytics']['ecommerce']['click']['products'][0]['dimension5']),
            'side': float(item['analytics']['ecommerce']['click']['products'][0]['dimension6']),
            'availability': item['analytics']['ecommerce']['click']['products'][0]['dimension11'],
            'length_type': item['analytics']['ecommerce']['click']['products'][0]['dimension15'],
            'ntd': item['items']['gost']['value'],
            'city': address.split('(')[0].strip(),
        }
        str1 = item['items']['avail']['value'].split("</span>")
        str1.pop()
        for str2 in str1:
            str2 = re.sub(r'(\<(/?[^>]+)>)', '', str2)
            if str2.find(' скоро поступит') != -1:
                tube['avail_on_tracking'] = float(str2.replace(' скоро поступит', ''))
            elif str2.find('/') != -1:
                tube['avail_free'] = float(str2.split(' / ')[0])
                tube['avail_reserved'] = float(str2.split(' / ')[1])
            else:
                tube['avail_free'] = float(str2.replace('Под заказ', '0'))
        try:
            tube['price_for_meter'] = float(item['analytics']['ecommerce']['click']['products'][0]['dimension7'])
            tube['price'] = float(item['analytics']['ecommerce']['click']['products'][0]['metric8'])
        except Exception as e:
            pass
        try:
            tube['size_2'] = float(item['analytics']['ecommerce']['click']['products'][0]['dimension16'])
        except Exception as e:
            pass
        try:
            address_clarification = address.split('(')[1].replace(')', '').strip()
            tube.update({'address_clarification': address_clarification})
        except:
            pass
        if tube['city'] != '—':
            tube_list.append(tube)

        tube['avail_at_warehouse'] = tube['avail_free'] + tube['avail_reserved']

    return tube_list


def get_all_goods():
    logging.info(f"Начинаем парсить ТД Уралтрубосталь")
    elements_count = parsing_elements_count()  # получаем общее кол-во позиций труб
    data_in_tons = parsing_goods(units='ton', tubes_count=elements_count)  # получаем данные
    goods = make_tube_list(data_in_tons)  # преобразуем данные список словарей

    # logging.info(data['last_update_datetime'])
    # logging.info(data['parse_datetime'])
    # logging.info(data['goods'][:10])
    return goods


def create_bd():
    url = os.getenv("DB_URL")
    engine = create_engine(
        url=url,
        # encoding='latin1',
        # echo=True,
        echo=False,
    )

    metadata = MetaData()

    uts_tubes = Table('uts_tubes', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('tube_id', Integer),
                      Column('city', String),
                      Column('address_clarification', String),
                      Column('category', String),
                      Column('ntd', String),
                      Column('steel_trademark', String),
                      Column('price', Float),
                      Column('price_for_meter', Float),
                      Column('size_1', Float),
                      Column('size_2', Float),
                      Column('side', Float),
                      Column('avail_free', Float),
                      Column('avail_reserved', Float),
                      Column('avail_on_tracking', Float),
                      Column('length_type', String),
                      Column('brand', String),
                      Column('url', String),
                      Column('parse_datetime', DateTime(timezone=False)),
                      )

    metadata.create_all(engine)
    # metadata.drop_all(engine)

    # Session = sessionmaker(bind=engine)
    # session = Session()

    return engine, uts_tubes


def main():
    # парсим сайт УТС, получаем список словарей
    goods = get_all_goods()

    # Создаём БД
    logger.info('Создаём БД')
    engine, uts_tubes = create_bd()

    goods_amount = len(goods)
    logging.info(f'Кол-во полученых объектов = {goods_amount}')

    # Записываем всё в БД
    logger.info('Записываем всё в БД')
    for good in goods:
        with engine.connect() as conn:
            conn.execute(uts_tubes.insert(), good)


if __name__ == '__main__':
    start_time = time.time()
    main()
    logger.info(f"Execution time: {time.time() - start_time} seconds")
