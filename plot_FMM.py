

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from datetime import datetime
from datetime import timedelta
import calendar

from io import StringIO
import time

start_date = datetime.today().strftime('%Y%m%d')
end_date = datetime.today() + timedelta(days=1)
end_date = end_date.strftime('%Y%m%d')
genpath = 'C:\\Users\\psth002\Box\\CAISOdata\\EveryDayDATA\\FMM\\'

csv_file = genpath+'FMM_RTPD_'+start_date+'_'+end_date+'_TH_SP15_GEN-APND.csv'
path = 'C:\\Users\\psth002\Box\\CAISOdata\\FMMData\\out_FMM.csv'
data1 =  pd.read_csv(csv_file, thousands=',')

rt_d1 = data1[data1['LMP_TYPE'] == 'LMP']

rt_data1 = rt_d1.drop_duplicates(subset=['INTERVALSTARTTIME_GMT'], keep=False) # drop duplicate records
rt_data1 = rt_data1.sort_values(['INTERVALENDTIME_GMT'])
rt_data1 = rt_data1.reset_index(drop=True)
rt_price1 = rt_data1[['INTERVALENDTIME_GMT', 'OPR_DT', 'OPR_HR', 'OPR_INTERVAL', 'PRC']]

rt_data1.fillna(0,inplace=True)
df = pd.concat([rt_price1], ignore_index=True)
df['PRC'] = df['PRC'].astype(float)
ar =[]
for i in range(len(df)): 
  if(df.iloc[i]['PRC'] <0 and df.iloc[i+1]['PRC'] <0 and df.iloc[i+2]['PRC'] <0):
     ar.append(df.iloc[i+2])

cs = pd.DataFrame(ar, columns = ['INTERVALENDTIME_GMT', 'OPR_DT', 'OPR_HR', 'OPR_INTERVAL', 'PRC'])
#cs.to_csv(path, index = False, index_label=False)
df_new = pd.read_csv(path)

big = pd.concat([df_new, cs], ignore_index=True)
big.to_csv(path,  index=False, index_label=False)
