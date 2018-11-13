#!/usr/bin/env python
# coding: utf-8

# The idea here would be to gather the stock data from NSE using NSEpy, and
# store in a database, preferebly a SQL DB, for further usage.
# 
# Apart from the regular OHLC data, we need to obtain prices like
# * crude price
# * forex rate (US vs INR)
# * VIX rates

START_DATE = "2018,1,1"
END_DATE = "2018,9,1"


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)

from datetime import date
from nsepy import get_history


pnb = get_history(symbol='PNB', start=date(2018,1,1),end=date(2018,9,1))
sbin = get_history(symbol='SBIN', start=date(2018,1,1),end=date(2018,9,1))

sbin.head()

sbin.to_sql('sbin',con=engine)
pnb.to_sql('pnb',con=engine)

'''
engine.execute('SELECT * from sbin').fetchall()

[('2018-01-01', 'PNB', 'EQ', 171.4, 172.95, 173.4, 168.9, 169.7, 169.75, 171.31, 7869149, 134803219700000.0, 35893, 1423821, 0.1809),
 ('2018-01-02', 'PNB', 'EQ', 169.75, 170.5, 170.9, 165.4, 166.15, 166.4, 167.26, 13729132, 229638327805000.03, 54266, 3567217, 0.25980000000000003),

'''
vix = get_history(symbol='INDIAVIX', start=date(2018,1,1),end=date(2018,1,1),index=True)
vix.to_sql('vix',con=engine)

'''
vix.head()

            Open	High	Low	Close	Previous	Change	%Change
Date	     						
            NaN 	NaN 	NaN 	NaN 	NaN 	NaN 	NaN
2018-01-01	12.67	13.6125	12.545	13.3525	12.67	0.68	0.0539
'''
from nsepy import get_rbi_ref_history
rbi_ref = get_rbi_ref_history(date(2015,1,1), date(2015,1,10))
rbi_ref['1 USD'].to_sql('curr_rates', con = engine)
engine.execute('SELECT * from curr_rates').fetchall()

engine.table_names()
