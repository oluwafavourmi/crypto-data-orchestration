from prefect import flow, task
from datetime import timedelta, datetime
import requests
import bs4
import pandas as pd
import json
import time
import psycopg
import sqlalchemy

#  link = requests.get("https://coincodex.com/crypto/bitcoin/historical-data/")
link = "https://coincodex.com/crypto/bitcoin/historical-data/"

@task
def load_site():
    time.sleep(2)
    website = requests.get(link)
    return website 

@task
def scrape_contents():
    time.sleep(3)
    soup = bs4.BeautifulSoup(load_site().content, 'html.parser')
    table = soup.find('table')
    table_rows = soup.find_all('tr')
    rows = table.find_all('tr')

    table_data = []
    for row in table_rows:
        cells = row.find_all("td")  # Or "th" for header cells
        row_data = [cell.text.strip() for cell in cells]
        table_data.append(row_data)
    return table_data

@task
def create_dataframe():
    time.sleep(5)
    col = ['Date Start', 'Date End', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap']
    df = pd.DataFrame(scrape_contents(), columns=col)

    df = df.iloc[1:]
    df2 = df.drop('Date End', axis='columns')
    df3 = df2.reindex(index=df2.index[::-1])
    df3['Date'] = pd.to_datetime(df['Date Start'], format='%b %d, %Y')
    df4 = df3.drop(['Date Start'], axis='columns')
    df5 = df4.reset_index()
    df6 = df5.drop('index', axis='columns')

    open = pd.DataFrame(df6['Open'].str.replace('$', ''))
    high = pd.DataFrame(df6['High'].str.replace('$', ''))
    low = pd.DataFrame(df6['Low'].str.replace('$', ''))
    close = pd.DataFrame(df6['Close'].str.replace('$', ''))
    volume = pd.DataFrame(df6['Volume'].str.replace('$', ''))
    market = pd.DataFrame(df6['Market Cap'].str.replace('$', ''))
    date = df6['Date']


   # open = pd.DataFrame(df6['Open'])
   # high = pd.DataFrame(df6['High'])
   # low = pd.DataFrame(df6['Low'])
   #close = pd.DataFrame(df6['Close'])
    #volume = pd.DataFrame(df6['Volume'])
    #market = pd.DataFrame(df6['Market Cap'])
    #date = df6['Date']



    columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Market Cap', 'Date']
    df7 = pd.concat([date, open, high, low, close, volume, market ], axis='columns')

    return df7

@task
def download_data():
    time.sleep(7)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'BTCUSDPrices' + dt_string
    create_dataframe().to_csv(f"{dt_string}.csv", index = False)

@flow
def pipeline_flow():
    loaded_site = load_site()
    scraped = scrape_contents()
    dataframe = create_dataframe()
    download = download_data()

    return download

if __name__ == "__main__":
    pipeline_flow()