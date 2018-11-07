#!/usr/bin/env python
# coding: utf-8

# The idea here would be to gather the stock data from NSE using NSEpy, and
# store in a database, preferebly a SQL DB, for further usage.
# 
# Apart from the regular OHLC data, we need to obtain prices like
# * crude price
# * forex rate (US vs INR)
# * VIX rates

# In[14]:


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)

from datetime import date
from nsepy import get_history


# read a configuration file that would have the list of the stocks to get data for
# this configuration file would also have the fields START_DATE and END_DATE marking
# the date range for which the data has to be collected

# In[15]:


sbin = get_history(symbol='PNB', start=date(2018,1,1),end=date(2018,9,1))


# In[16]:


sbin = get_history(symbol='SBIN', start=date(2018,1,1),end=date(2018,9,1))


# In[17]:


sbin


# In[11]:


sbin.to_sql('sbin',con=engine)


# In[12]:


engine.execute('SELECT * from sbin').fetchall()


# In[ ]:





# In[ ]:




