#!/usr/bin/env python
# coding: utf-8

# The idea here would be to gather the stock data from NSE using NSEpy, and
# store in a database, preferebly a SQL DB, for further usage.
# 

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

#Get Crude Prices from quandl.com
url = "https://www.quandl.com/api/v3/datasets/CHRIS/CME_CL1.csv"
crude_price = pd.read_csv(url, index_col=1, parse_dates=True)
crude_price = crude_price.drop(['High','Low','Change','Settle','Volume','Previous Day Open Interest'], axis = 1)

# drop unnecessary columns from the SBIN
sbin = sbin.drop(['Symbol', 'Series', 'Prev Close','Last', 'VWAP', 'Volume', 'Turnover', 'Trades', 'Deliverable Volume', '%Deliverble'], axis = 1)
# drop unnecssary columns from vix
vix = vix.drop(['Close'],axis = 1)

# push all the data to SQLlite
# this is needed to
# 1. add the Date column to the results
# 2. execute SQL queries obtaining data
# 3. get the list of the common dates across the data collected.
#    the crude prices collected have dates for about 30 days in reverse order and so we need
#    to collect the common dates across data collected.
sbin.to_sql('sbin',con=engine)
vix.to_sql('vix',con=engine)
rbi_ref.to_sql('rbi_ref',con=engine)
crude_price.to_sql('crude_price',con=engine)

# retrieve dates from SQLlite
sbin_date = engine.execute('SELECT Date from sbin').fetchall()
vix_date = engine.execute('SELECT Date from vix').fetchall()
rbi_ref_date = engine.execute('SELECT Date from rbi_ref').fetchall()
crude_price_date = engine.execute('SELECT Date from crude_price').fetchall()

sbin_date = pd.DataFrame(sbin_date,columns=['Date'])
vix_date = pd.DataFrame(vix_date,columns=['Date'])
rbi_ref_date = pd.DataFrame(rbi_ref_date,columns=['Date'])
crude_price_date = pd.DataFrame(crude_price_date,columns=['Date'])

# obtain the common dates
common_dates = np.intersect1d(sbin_date['Date'], np.intersect1d(vix_date['Date'],np.intersect1d(rbi_ref_date['Date'],crude_price_date['Date'])))

# creating a SQL comman using OR. We need this format to save execution cycles
# while generating data for different dates
sql_dates =''
for dates in common_dates:
    #query_stock = "SELECT * from sbin WHERE Date ='" + dates + "'"
    if (dates == common_dates[-1]):
        sql_dates = sql_dates + "Date='" + dates + "'"
    else:
        sql_dates = sql_dates + "Date='" + dates + "' OR "

# collect the data from different tables

# columns from sbin [Open High Low Close]
sbin_sql_command = 'SELECT Open,High,Low,Close FROM sbin WHERE ' + sql_dates + ' '
sbin_data = engine.execute(sbin_sql_command).fetchall()

# columns from vix [Open High Low Previous Change]
vix_sql_command  = 'SELECT Open,High,Low,Previous,Change FROM vix WHERE ' + sql_dates + ' '
vix_data  = engine.execute(vix_sql_command).fetchall()

# columns from rbi_ref [*]
rbi_ref_sql_command = 'SELECT * FROM rbi_ref WHERE ' + sql_dates + ' '
rbi_ref_data = engine.execute(rbi_ref_sql_command).fetchall()

# columns from crude_price [Last]
crude_price_sql_command = 'SELECT Last FROM crude_price WHERE ' + sql_dates + ' '
crude_price_data = engine.execute(crude_price_sql_command).fetchall()

# splitting the obtained list into dataframe with user defined column names
sbin = pd.DataFrame(sbin_data, columns=['Stock_Open','Stock_High','Stock_Low','Stock_Close'])
vix = pd.DataFrame(vix_data,columns=['Vix_Open','Vix_High','Vix_Low','Vix_Previous','Vix_Change'])
rbi_ref = pd.DataFrame(rbi_ref_data,columns=['Date','USD'])
crude_price = pd.DataFrame(crude_price_data,columns=['Crude_Close'])

# join all the DataFrames
# base_dataframe is the collection of all the data required for the analysis

base_dataframe = rbi_ref.join(sbin)
base_dataframe = base_dataframe.join(vix)
base_dataframe = base_dataframe.join(crude_price)
base_dataframe

''' SAMPLE OUTPUT
Date	USD	Stock_Open	Stock_High	Stock_Low	Stock_Close	Vix_Open	Vix_High	Vix_Low	Vix_Previous	Vix_Change	Crude_Close
0	2018-11-01	73.8295	283.00	289.0	278.85	285.90	19.7975	20.1300	18.8975	19.7975	-0.61	55.28
1	2018-11-02	72.8798	287.90	292.8	283.20	285.35	19.1925	19.1925	17.4850	19.1925	-0.96	58.86
2	2018-11-05	73.0740	286.45	300.0	283.50	294.95	18.2300	19.8225	17.7350	18.2300	1.25	59.87
3	2018-11-06	73.0097	296.10	298.3	283.90	286.45	19.4825	19.4825	16.1800	19.4825	-0.98	61.77
4	2018-11-09	72.7347	287.00	287.5	282.45	283.25	17.8775	18.7550	16.7550	17.8775	-0.11	62.69
5	2018-11-12	72.9078	283.70	283.7	276.85	277.95	17.7650	19.4625	16.4750	17.7650	1.60	62.86
6	2018-11-13	72.5853	276.30	279.7	273.35	278.05	19.3675	19.6850	17.1225	19.3675	-0.67	63.54
'''
