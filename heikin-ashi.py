import requests
import os
from dotenv import load_dotenv
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from influxdb_client.client.write_api import SYNCHRONOUS
    
load_dotenv()

API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
base_currency = 'USD'
quote_currency = 'CHF'

def fetch_forex_data(api_key):
    url = 'https://www.alphavantage.co/query?function=FX_INTRADAY&from_symbol='+base_currency+'&to_symbol='+quote_currency+'&interval=1min&apikey=' + api_key
    response = requests.get(url)
    data = response.json()
    return data


def calculate_heikin_ashi(data):
    try:
        df = pd.DataFrame(data['Time Series FX (1min)']).T
    except Exception as e:
        logging.log('Forex Dataframe error', e) 
        return
    df.columns = ['open', 'high', 'low', 'close']
    df = df.astype(float)
    
    # Calculate Heikin-Ashi
    ha_df = pd.DataFrame(index=df.index)
    ha_df['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_df['open'] = (df['open'].shift() + df['close'].shift()) / 2
    ha_df['high'] = df[['high', 'open', 'close']].max(axis=1)
    ha_df['low'] = df[['low', 'open', 'close']].min(axis=1)
    
    return ha_df



def store_to_influxdb(data, bucket, org, token, url):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    for timestamp, row in data.iterrows():
        point = Point("forex").tag("symbol", base_currency+"/"+quote_currency) \
              .field("open", row['open']) \
              .field("high", row['high']) \
              .field("low", row['low']) \
              .field("close", row['close']) \
              .time(timestamp, WritePrecision.S)
        write_api.write(bucket=bucket, record=point)


def job():
    try:
        data = fetch_forex_data(api_key=API_KEY)
        #print(data)
    except Exception as e:
        logging.log('Failed fetching data',e)

    try:
        ha_data = calculate_heikin_ashi(data)
        #print(ha_data)
    except Exception as e:
        logging.log('Failed calculating Heikin ashi',e)

    try:
        store_to_influxdb(ha_data, bucket='forex', org="my-org", token=os.environ.get("INFLUXDB_TOKEN"), url="http://localhost:8086")
    except Exception as e:
        logging.log('Failed to store values in influxDB',e)


scheduler = BlockingScheduler()
scheduler.add_job(job, 'interval', minutes=1)
scheduler.start()
