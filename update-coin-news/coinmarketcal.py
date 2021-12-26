import re
import json
import requests
import pymysql
from bs4 import BeautifulSoup

def crawl_coinmarketcal(conn, cursor, coins):
    base_url = 'https://coinmarketcal.com'

    for i in range(1,10):
        page = requests.get(base_url + '/ko/?page=' + str(i), headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(page.content, 'html.parser')

        news_cards = soup.select('.col-xl-3.col-lg-4.col-md-6.py-3')
        
        is_finished = False

        for card in news_cards:
            coin = card.select_one('.card__coins')
            if '그리고' in coin.get_text().split():
                coin = coin.select_one('a')['data-content']
            else:
                coin = coin.get_text()
            targeting_market = re.findall(r'\(([^)]+)', coin)

            targeting_date = '-'.join(reversed(card.select_one('.card__date.mt-0').get_text().replace('월','').split(' ')[:3]))
            title = card.select_one('.card__title.mb-0.ellipsis').get_text().replace("'",'').replace('"','')
            posted_date = '-'.join(reversed(card.select_one('.added-date').get_text().replace('추가됨  ','').replace('월','').split(' ')))
            news_type = 'good'
            source = 'coinmarketcal'
            url = base_url + card.select_one('div.card__body>a')['href']

            # Check if it's already crawled one
            query = "SELECT * from coryndb.news WHERE url='{}'".format(url)
            cursor.execute(query)
            if cursor.fetchone() != None:
                is_finished = True
                break
            
            # Insert to news table
            query = "INSERT INTO coryndb.news(title, posted_date, targeting_date, news_type, source, url) VALUES('{}', '{}', '{}', '{}', '{}', '{}')".format(title, posted_date, targeting_date, news_type, source, url)
            cursor.execute(query)
            new_news_id = cursor.lastrowid

            # If it's news targeting market existing on DB, then insert to coin-news matring table
            for coin in coins:
                if coin[0].split('-')[-1] in targeting_market: # coin[0] = market
                    query = "INSERT INTO coryndb.coins_news(market, news_id) VALUES('{}', {})".format(coin[0], new_news_id)
                    cursor.execute(query)
                
        if is_finished == True: break
    
    conn.commit()
    