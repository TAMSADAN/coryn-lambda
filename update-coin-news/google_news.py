import json
import requests
import pymysql
from bs4 import BeautifulSoup

def crawl_google_news(conn, cursor, coins):
    base_url = 'https://news.google.com'
    prefix_url = '/search?q='
    postfix_url = '&hl=ko&gl=KR&ceid=KR%3Ako'

    for coin in coins:
        
        market = coin[0]
        ticker = market.split('-')[-1]
        korean_name = coin[1]
        url = base_url + prefix_url + korean_name + '%20' + '"' + ticker + '"' + '%20when%3A1d'+ postfix_url

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        
        news_cards = soup.select('div.xrnccd')
        for card in news_cards:
            title = card.select_one('h3.ipQwMb.ekueJc.RD0gLb > a').get_text().replace("'",'"')
            url = base_url + card.select_one('h3.ipQwMb.ekueJc.RD0gLb > a')['href'][1:]
            source = card.select_one('div.SVJrMe > a.wEwyrc.AVN2gc.uQIVzc.Sksgp').get_text()
            posted_date = card.select_one('time.WW6dff.uQIVzc.Sksgp')
            if posted_date != None: posted_date = posted_date['datetime'][:10]
            # targeting_date = null
            news_type = 'normal'

            query = "INSERT IGNORE INTO coin_news(title, posted_date, targeting_date, ticker, type, source, url) VALUES('{}', '{}', null, '{}', '{}', '{}', '{}')".format(title, posted_date if posted_date else 'null', ticker, news_type, source, url)
            cursor.execute(query)

    conn.commit()