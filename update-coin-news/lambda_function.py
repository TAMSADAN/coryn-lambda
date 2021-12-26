import pymysql

from coinmarketcal import crawl_coinmarketcal
from google_news import crawl_google_news
from db_connection import connect_to_rds


def lambda_handler(event, context):
    conn, cursor = connect_to_rds()

    # Fetch all coins
    cursor.execute("SELECT * from coryndb.coins")
    coins = cursor.fetchall()
    
    # Crawling process
    crawl_coinmarketcal(conn, cursor, coins)
    crawl_google_news(conn, cursor, coins)
    
    conn.close()