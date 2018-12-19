# ###############################################################################
# Script to download the bhavcopy for any specific days
# The script does the following:
# a. Gets the user input for the number of days it has to fetch records for
# b. If the user input is not provided, it defaults to 1day i.e. the current date
# c. The script goes ahead and downloads the bhavcopies for all the dates
# d. collates all the bhavcopies in one place (DB)
#
# Assumptions:
# The script collects data from the NSE website
#
# written and maintained by Subeer Kumar
################################################################################

# import the modules needed for the script
import pandas as pd
import os
import zipfile
import requests
import zipfile
import io
import sys
from urllib.request import urlretrieve
from pathlib import Path
from datetime import datetime, timedelta

# see how many days, the records are needed for
if (int(sys.argv[1]) < 1):
   print ("The script can not be invoked for less than 1 day")
else:
   # the script is invoked for a valid number of days
   print ("Starting the script now")

ts = datetime.now()
ts = ts.strftime("%d/%m/%Y")
for_date_parsed = datetime.strptime(ts, "%d/%m/%Y")
cwd = os.getcwd()
month = for_date_parsed.strftime("%b").upper()
year = for_date_parsed.year
day = "%02d" % for_date_parsed.day
url = f"https://www.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{day}{month}{year}bhav.csv.zip"
file_path = os.path.join(f"cm{day}{month}{year}bhav.csv.zip")

request_response = requests.get(url)
downloaded_zipfile = zipfile.ZipFile(io.BytesIO(request_response.content))
downloaded_zipfile.extractall()
print (file_path)

file_name = os.path.join(f"cm{day}{month}{year}bhav.csv")
dataframe = pd.read_csv(file_name)
