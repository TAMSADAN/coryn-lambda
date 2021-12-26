import json
import pymysql
import requests

from db_connection import connect_to_rds


def lambda_handler(event, context):
    data = requests.get('https://api.upbit.com/v1/market/all')
    text = data.text
        
    coins = json.loads(text)
        
    conn, cursor = connect_to_rds()

    cursor.execute('SELECT * FROM coryndb.coins')
    db = cursor.fetchall()
    
    # Fetch all market on DB
    db_market_list = []
    for row in db:
        db_market_list.append(row[0])
    
    # Update new coins
    new_market_list = []
    for coin in coins:
        market = coin['market']
        korean_name = coin['korean_name']
        english_name = coin['english_name']
        logo_uri = 'https://static.upbit.com/logos/' + market.split('-')[-1] + '.png'
        query = "INSERT IGNORE INTO coryndb.coins(market, korean_name, english_name, logo_uri) VALUES('{0}', '{1}', '{2}', '{3}')".format(market, korean_name, english_name, logo_uri)
        cursor.execute(query)

        new_market_list.append(market)
    
    # Remove unsupported coins on upbit
    for market in db_market_list:
        if market not in new_market_list:
            query = "DELETE FROM coryndb.coins WHERE market='{0}'".format(market)
            cursor.execute(query)
        
    conn.commit()
    conn.close()
        
    
