import json
import requests
import pymysql

from db_connection import connect_to_rds


def lambda_handler(event, context):
    conn, cursor = connect_to_rds()

    cursor.execute('SELECT * FROM coryndb.coins')
    db = cursor.fetchall()

    market_list = []
    for row in db:
        market_list.append(row[0])

    data = requests.get('https://api.upbit.com/v1/ticker?markets='+','.join(market_list))
    text = data.text
    prices = json.loads(text)

    for price in prices:
        market = price['market']
        opening_price = price['opening_price']
        high_price = price['high_price']
        low_price = price['low_price']
        trade_price = price['trade_price']
        closed_price = price['prev_closing_price']
        change_rate = price['signed_change_rate']
        change_price = price['signed_change_price']
        trade_volume = price['acc_trade_volume_24h']
        timestamp = price['trade_timestamp']
        source = 'upbit'
        unit = 'day'
        currency = market.split('-')[0]

        query = "INSERT INTO coryndb.prices(market, opening_price, high_price, low_price, trade_price, closed_price, change_rate, change_price, trade_volume, timestamp, source, unit, currency) VALUES('{0}',{1},{2},{3},{4},{5},{6},{7},{8},{9},'{10}','{11}','{12}')".format(market, opening_price, high_price, low_price, trade_price, closed_price, change_rate, change_price, trade_volume, timestamp, source, unit, currency)
        cursor.execute(query)
        
    conn.commit()
    conn.close()