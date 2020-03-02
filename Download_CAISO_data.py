# -*- coding: utf-8 -*-
"""
Created on Wed Jul 25 17:16:52 2018

@author: pqwa001
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import calendar
import sys

matplotlib.rcParams['figure.figsize'] = (15, 10)

from datetime import datetime
from datetime import timedelta
import calendar
import urllib.request
from zipfile import ZipFile
from io import StringIO
import time

# Get 15-minute-market prices between start-date and end-date.

'''
adapted from here: https://github.com/cwBerkeley/code/blob/master/CAISO-oasisAPI-operations.R 
but specifically for 15-min market prices.

Here we only get the price data from one node, since marginal cost of energy is the same throughout the whole network. 
Differences in locational marignal prices are based on things like congestion, line losses, GHG, etc
RTPD = real-time pre-dispatch market, a.k.a. fifteen-minute market
'''
#date format: - 20150101   20180101
#pass this argument if you want a spcific date - , start_date = '20200217', end_date = '20200218'
def get_lmp_prices_RT(node, market_run_id='RTM'):
    base_url = "http://oasis.caiso.com/oasisapi/SingleZip?"
    start_date = datetime.today()
    end_date = datetime.today() + timedelta(days=1)
    start_date = start_date.strftime('%Y%m%d')
    end_date = end_date.strftime('%Y%m%d')
    # Add timedelta of 8 hrs to account for timezone (times for data requests are in GMT)
    start_dt = datetime.strptime(start_date, '%Y%m%d') + timedelta(hours=8)
    start = start_dt
        
    end_dt = datetime.strptime(end_date, '%Y%m%d') + timedelta(hours=8)
    
    # Download 30 days of data at a time. The max # of days per request is 31.
    end = min([end_dt, start_dt + timedelta(days=30)])
    path ='C:\\Users\\psth002\Box\\CAISOdata\\EveryDayDATA\\RT\\'
    csv_file =path+ "RT_" +market_run_id + "_" + start_date + "_" + end_date + "_" + node + '.csv'
    
    #print(csv_file)
    
    while (start_dt < end_dt):
        # get url query for data in this timeframe; read it to file; extract contents from file; rewrite contents into a joint file
        url = base_url + 'resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime=' + start_dt.strftime('%Y%m%dT%H:%M-0000') + '&enddatetime=' + end.strftime('%Y%m%dT%H:%M-0000') + '&market_run_id=' + market_run_id + '&node=' + node
        #print(url)
        urllib.request.urlretrieve(url, 'temp.zip')
        tempzip = ZipFile('temp.zip')
        filename = tempzip.namelist()[0]
        data_string = tempzip.read(filename).decode("utf-8")
        fmm_data = pd.read_csv(StringIO(data_string))
        #put heaters into the file only if it's the first line of the file, so we don't have extraneous random headers
        fmm_data.to_csv(csv_file, mode='a', index=False, header=(start == start_dt))
        
        start_dt = end
        end = min([end_dt, start_dt + timedelta(days=30)])
        
        print("Got data up to " + start_dt.strftime('%Y-%m-%d'))

def get_lmp_prices_DA(node, market_run_id='DAM'):
    base_url = "http://oasis.caiso.com/oasisapi/SingleZip?"
    start_date = datetime.today().strftime('%Y%m%d')
    end_date = datetime.today() + timedelta(days=1)
    end_date = end_date.strftime('%Y%m%d')
    
    # Add timedelta of 8 hrs to account for timezone (times for data requests are in GMT)
    start_dt = datetime.strptime(start_date, '%Y%m%d') + timedelta(hours=8)
    start = start_dt
        
    end_dt = datetime.strptime(end_date, '%Y%m%d') + timedelta(hours=8)
    
    # Download 30 days of data at a time. The max # of days per request is 31.
    end = min([end_dt, start_dt + timedelta(days=30)])
    path ='C:\\Users\\psth002\Box\\CAISOdata\\EveryDayDATA\\DA\\'
    csv_file = path+"DA_" + market_run_id + "_" + start_date + "_" + end_date + "_" + node + '.csv'
       
    #print(csv_file)
    
    while (start_dt < end_dt):
        # get url query for data in this timeframe; read it to file; extract contents from file; rewrite contents into a joint file
        url = base_url + 'resultformat=6&queryname=PRC_LMP&version=1&startdatetime=' + start_dt.strftime('%Y%m%dT%H:%M-0000') + '&enddatetime=' + end.strftime('%Y%m%dT%H:%M-0000') + '&market_run_id=' + market_run_id + '&node=' + node
        #print(url)
        urllib.request.urlretrieve(url, 'temp.zip')
        tempzip = ZipFile('temp.zip')
        filename = tempzip.namelist()[0]
        data_string = tempzip.read(filename).decode("utf-8")
        fmm_data = pd.read_csv(StringIO(data_string))
        #put heaters into the file only if it's the first line of the file, so we don't have extraneous random headers
        fmm_data.to_csv(csv_file, mode='a', index=False, header=(start == start_dt))
        
        start_dt = end
        end = min([end_dt, start_dt + timedelta(days=30)])
        
        print("Got data up to " + start_dt.strftime('%Y-%m-%d'))
    


def get_lmp_prices_FMM(node, market_run_id='RTPD'):
    base_url = "http://oasis.caiso.com/oasisapi/SingleZip?"
    start_date = datetime.today().strftime('%Y%m%d')
    end_date = datetime.today() + timedelta(days=1)
    end_date = end_date.strftime('%Y%m%d')
    # Add timedelta of 8 hrs to account for timezone (times for data requests are in GMT)
    start_dt = datetime.strptime(start_date, '%Y%m%d') + timedelta(hours=8)
    start = start_dt
        
    end_dt = datetime.strptime(end_date, '%Y%m%d') + timedelta(hours=8)
    
    # Download 30 days of data at a time. The max # of days per request is 31.
    end = min([end_dt, start_dt + timedelta(days=30)])
    path ='C:\\Users\\psth002\Box\\CAISOdata\\EveryDayDATA\\FMM\\'
    csv_file = path +"FMM_" + market_run_id + "_" + start_date + "_" + end_date + "_" + node + '.csv'
       
    #print(csv_file)
    
    while (start_dt < end_dt):
        # get url query for data in this timeframe; read it to file; extract contents from file; rewrite contents into a joint file
        url = base_url + 'resultformat=6&queryname=PRC_RTPD_LMP&version=3&startdatetime=' + start_dt.strftime('%Y%m%dT%H:%M-0000') + '&enddatetime=' + end.strftime('%Y%m%dT%H:%M-0000') + '&market_run_id=' + market_run_id + '&node=' + node
        #print(url)
        urllib.request.urlretrieve(url, 'temp.zip')
        tempzip = ZipFile('temp.zip')
        filename = tempzip.namelist()[0]
        data_string = tempzip.read(filename).decode("utf-8")
        fmm_data = pd.read_csv(StringIO(data_string))
        #put heaters into the file only if it's the first line of the file, so we don't have extraneous random headers
        fmm_data.to_csv(csv_file, mode='a', index=False, header=(start == start_dt))
        
        start_dt = end
        end = min([end_dt, start_dt + timedelta(days=30)])
        
        print("Got data up to " + start_dt.strftime('%Y-%m-%d'))
        time.sleep(3)
        
#get_lmp_prices_RT(node='TH_ZP26_GEN-APND')
get_lmp_prices_RT(node='TH_SP15_GEN-APND')
#get_lmp_prices_RT(node='TH_NP15_GEN-APND')
 
#get_lmp_prices_DA(node='TH_ZP26_GEN-APND')
get_lmp_prices_DA(node='TH_SP15_GEN-APND')
#get_lmp_prices_DA(node='TH_NP15_GEN-APND')

#get_lmp_prices_FMM(node='TH_ZP26_GEN-APND')
get_lmp_prices_FMM(node='TH_SP15_GEN-APND')
#get_lmp_prices_FMM(node='TH_NP15_GEN-APND')


