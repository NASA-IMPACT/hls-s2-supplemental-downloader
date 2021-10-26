import argparse
import boto3
import psycopg2
import json
import os
import sys

def query_rds_for_undownloaded_urls():

    """
    Returns a list of undownloaded URLs and saves them to a file
    """
    argParser = argparse.ArgumentParser()
    
    argParser.add_argument("-d", "--days", default=5, type=int, help="number of days in the past from today")
    
    args = argParser.parse_args()
    
    num_days = args.days
    
    client = boto3.client('secretsmanager')
    
    response = client.get_secret_value(
            SecretId=os.environ("SecretID")
    )
    
    secretDict = json.loads(response['SecretString'])
    
    query = f"""
    select *
    from granule
    where ((ingestiondate < NOW() - INTERVAL \'{num_days} days\') and (downloaded is false) 
    	   and ingestiondate >= date_trunc(\'month\', CURRENT_DATE))
    """
    
    try:
        conn = psycopg2.connect(host=secretDict['host'], port=secretDict['port'], database=secretDict['dbname'], user=secretDict['username'], password=secretDict['password'])
        cur = conn.cursor()
        cur.execute(query)
        query_results = [r[7] for r in cur.fetchall()] # r[7] for URL column else cur.fetchall() for all cols
        # print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e))
    
    
    with open('query_rds_results.txt', 'w') as f:
        print(*query_results, sep="\n", file=f)
    
    conn.close()


if __name__ == '__main__':
   query_rds_for_undownloaded_urls()
