#!/usr/bin/env python
# coding: utf-8

# The idea here would be to gather the stock data from NSE using NSEpy, and
# store in a database, preferebly a SQL DB, for further usage.
# 
# Apart from the regular OHLC data, we need to obtain prices like
# * crude price
# * forex rate (US vs INR)
# * VIX rates

# import libraries
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from nsepy import get_rbi_ref_history
from datetime import date
from nsepy import get_history

# create a sqlite connection
engine = create_engine('sqlite://', echo=False)

# collect relevant data
# date format is YYYY,MM,DD
sbin = get_history(symbol='SBIN', start=date(2018,11,1),end=date(2018,11,13))
vix = get_history(symbol='INDIAVIX', start=date(2018,11,1),end=date(2018,11,13),index=True)
rbi_ref = get_rbi_ref_history(date(2018,11,1),end=date(2018,11,13))
rbi_ref = rbi_ref['1 USD']

url = "https://www.quandl.com/api/v3/datasets/CHRIS/CME_CL1.csv"
crude_price = pd.read_csv(url, index_col=1, parse_dates=True)

#Get Crude Prices
url = "https://www.quandl.com/api/v3/datasets/CHRIS/CME_CL1.csv"
crude_price = pd.read_csv(url, index_col=1, parse_dates=True)
crude_price = crude_price.drop(['High','Low','Change','Settle','Volume','Previous Day Open Interest'], axis = 1)

# drop unnecessary columns from the SBIN
sbin = sbin.drop(['Symbol', 'Series', 'Prev Close','Last', 'VWAP', 'Volume', 'Turnover', 'Trades', 'Deliverable Volume', '%Deliverble'], axis = 1)
# drop unnecssary columns from vix
vix = vix.drop(['Close'],axis = 1)

# push all the data to SQLlite
sbin.to_sql('sbin',con=engine)
vix.to_sql('vix',con=engine)
rbi_ref.to_sql('rbi_ref',con=engine)
crude_price.to_sql('crude_price',con=engine)

# retrieve data from SQLlite
sbin = engine.execute('SELECT * from sbin').fetchall()
vix = engine.execute('SELECT * from vix').fetchall()
rbi_ref = engine.execute('SELECT * from rbi_ref').fetchall()
crude_price = engine.execute('SELECT * from crude_price').fetchall()

# Split list to dataframe
sbin = pd.DataFrame(sbin,columns=['Date','Open','High','Low','Close'])
vix = pd.DataFrame(vix,columns=['Date','Open','High','Low','Previous','Change','%Change'])
rbi_ref = pd.DataFrame(rbi_ref,columns=['Date','USD'])
crude_price = pd.DataFrame(crude_price,columns=['Open','Date','Close'])
crude_price = crude_price.drop(['Open'],axis=1)
