import pymysql

from db_connection import connect_to_rds

def lambda_handler(event, context):
    ticker = event['ticker']
    type = event['type']
    limit = event['limit']
    
    conn, cursor = connect_to_rds()
    
    query = "SELECT * FROM coin_news"
    
    if ticker != "" or type != "":
        query += " WHERE"
    
    if ticker != "":
        query += " ticker ='{}'".format(ticker)
    
    if ticker != "" and type != "":
        query += " AND"
        
    if type != "":
        query += " type = '{}'".format(type)
        
    query += " ORDER BY id DESC"
    
    if limit != "":
        query += " LIMIT " + limit
    
    cursor.execute(query)
    result = cursor.fetchall()
    
    conn.close()
    
    news = []
    
    for row in result:
        news.append(
            {
                'id' : row[0],
                'title' : row[1],
                'posted_date' : str(row[2]),
                'targeting_date' : str(row[3]) if row[3] is not None else None,
                'ticker' : row[4],
                'type' : row[5],
                'source' : row[6],
                'url' : row[7]
            }
        )
    
    return {
        'news' : news
    }