import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer, Float, String
import re

engine = create_engine("postgresql+psycopg2://postgres:iamkennyakomolafe@localhost:5432/BTCUSD_db")

df = pd.read_csv("BTCUSD.csv")

df['Date'] = pd.to_datetime(df['Date'])

df['Open'] = df['Open'].astype(str).apply(lambda x:re.sub(r'[^\d.-]', '', x))
df['Open'] = pd.to_numeric(df['Open'], errors='coerce').astype('Int64')

df['High'] = df['High'].astype(str).apply(lambda x:re.sub(r'[^\d.-]', '', x))
df['High'] = pd.to_numeric(df['High'], errors='coerce').astype('Int64')

df['Low']  =  df['Low'].astype(str).apply(lambda x:re.sub(r'[^\d.-]', '', x))
df['Low'] = pd.to_numeric(df['Low'], errors='coerce').astype('Int64')

df['Close'] = df['Close'].astype(str).apply(lambda x:re.sub(r'[^\d.-]', '', x))
df['Close'] = pd.to_numeric(df['Close'], errors='coerce').astype('Int64')

df['Volume'] = df['Volume']
df['Market Cap'] = df['Market Cap']

#print(df.dtypes)

df.to_sql("btcdata", 
          engine, 
          if_exists="append", 
          index=False,
          dtype={
              "Date":Date,
              "Open":Integer,
              "High":Integer,
              "Low":Integer,
              "Close":Integer,
              "Volume":String,
              "Market Cap":String
          }
          
)

print("Loaded Successfully")