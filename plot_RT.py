

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from datetime import datetime
from datetime import timedelta
import calendar

from io import StringIO
import time

start_date = datetime.today()
end_date = datetime.today() + timedelta(days=1)
start_date = start_date.strftime('%Y%m%d')
end_date = end_date.strftime('%Y%m%d')
genpath = 'C:\\Users\\psth002\Box\\CAISOdata\\EveryDayDATA\\RT\\'
csvfile =genpath+ 'RT_RTM_'+start_date+'_'+end_date+'_TH_SP15_GEN-APND.csv'
path = 'C:\\Users\\psth002\Box\\CAISOdata\\RTData\\out_RT.csv'
#'RT_RTM_20200217_20200218_TH_SP15_GEN-APND.csv'
data1 =  pd.read_csv(csvfile, thousands=',')

rt_d1 = data1[data1['LMP_TYPE'] == 'LMP']

rt_data1 = rt_d1.drop_duplicates(subset=['INTERVALSTARTTIME_GMT'], keep=False) # drop duplicate records
rt_data1 = rt_data1.sort_values(['INTERVALENDTIME_GMT'])
rt_data1 = rt_data1.reset_index(drop=True)
rt_price1 = rt_data1[['INTERVALENDTIME_GMT', 'OPR_DT', 'OPR_HR', 'OPR_INTERVAL', 'VALUE']]


rt_data1.fillna(0,inplace=True)
df = pd.concat([rt_price1], ignore_index=True)
df['VALUE'] = df['VALUE'].astype(float)
print(type(df.iloc[1]['VALUE']))
ar =[]
for i in range(len(df)): 
  if(df.iloc[i]['VALUE'] <0 and df.iloc[i+1]['VALUE'] <0 and df.iloc[i+2]['VALUE'] <0):
     ar.append(df.iloc[i+2])

cs = pd.DataFrame(ar, columns = ['INTERVALENDTIME_GMT', 'OPR_DT', 'OPR_HR', 'OPR_INTERVAL', 'VALUE'])
#cs.to_csv(path, index = False, index_label=False)
df_new = pd.read_csv(path)

big = pd.concat([df_new, cs], ignore_index=True)
big.to_csv(path,  index=False, index_label=False)
