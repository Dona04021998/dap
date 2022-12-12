# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 16:21:54 2022

@author: ADMIN
"""

import plotly.express as px
import pymongo
import pandas as pd
import json
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = 'browser'

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["AirlineData"]

def date_conc(r):
    if(len(str(r['MONTH']))==1):
        dt='2020'+'-0'+str(r['MONTH'])
    if(len(str(r['MONTH']))==2):
        dt='2020'+'-'+str(r['MONTH'])
    if(len(str(r['DAY_OF_MONTH']))==1):
        dt=dt+'-0'+str(r['DAY_OF_MONTH'])
    if(len(str(r['DAY_OF_MONTH']))==2):
        dt=dt+'-'+str(r['DAY_OF_MONTH'])
    return dt


def dt_format(r):
    if '/' in r:
        r1=r.split('/')
        if len(r1[0]) ==1:
            r1d='0'+r1[0]
        if len(r1[0]) ==2:
            r1d=r1[0]
        if len(r1[1])==1:
            r1m='0'+r1[1]
        if len(r1[1])==2:
            r1m=r1[1]
        dt=str(r1[2])+'-'+str(r1m)+'-'+str(r1d)
    else:
        dt=r
    return dt
# ONTIME_REPORTING_01
col = db["ONTIME_REPORTING_01"]

df=pd.DataFrame(list(col.find()))



df['dt']=df.apply( lambda x: date_conc(x),axis=1)


# ONTIME_REPORTING_02
col2 = db["ONTIME_REPORTING_02"]

df2=pd.DataFrame(list(col2.find()))
df2['dt']=df2.apply( lambda x: date_conc(x),axis=1)

# ONTIME_REPORTING_03
col2_rep = db["ONTIME_REPORTING_03"]

df2_rep=pd.DataFrame(list(col2_rep.find()))
df2_rep['dt']=df2_rep.apply( lambda x: date_conc(x),axis=1)

# ONTIME_REPORTING_04
col4_rep = db["ONTIME_REPORTING_04"]

df4_rep=pd.DataFrame(list(col4_rep.find()))
df4_rep['dt']=df4_rep.apply( lambda x: date_conc(x),axis=1)

# ONTIME_REPORTING_05
col5_rep = db["ONTIME_REPORTING_05"]

df5_rep=pd.DataFrame(list(col5_rep.find()))
df5_rep['dt']=df5_rep.apply( lambda x: date_conc(x),axis=1)

# ONTIME_REPORTING_06
col6_rep = db["ONTIME_REPORTING_06"]

df6_rep=pd.DataFrame(list(col6_rep.find()))
df6_rep['dt']=df6_rep.apply( lambda x: date_conc(x),axis=1)

# ONTIME_REPORTING_07
col7_rep = db["ONTIME_REPORTING_07"]

df7_rep=pd.DataFrame(list(col7_rep.find()))
df7_rep['dt']=df7_rep.apply( lambda x: date_conc(x),axis=1)


# ONTIME_REPORTING_08
col8_rep = db["ONTIME_REPORTING_08"]

df8_rep=pd.DataFrame(list(col8_rep.find()))
df8_rep['dt']=df8_rep.apply( lambda x: date_conc(x),axis=1)

# ONTIME_REPORTING_09
col9_rep = db["ONTIME_REPORTING_09"]

df9_rep=pd.DataFrame(list(col9_rep.find()))
df9_rep['dt']=df9_rep.apply( lambda x: date_conc(x),axis=1)

# ONTIME_REPORTING_10
col10_rep = db["ONTIME_REPORTING_10"]

df10_rep=pd.DataFrame(list(col10_rep.find()))
df10_rep['dt']=df10_rep.apply( lambda x: date_conc(x),axis=1)


# ONTIME_REPORTING_11
col11_rep = db["ONTIME_REPORTING_11"]

df11_rep=pd.DataFrame(list(col11_rep.find()))
df11_rep['dt']=df11_rep.apply( lambda x: date_conc(x),axis=1)


# ONTIME_REPORTING_12
col12_rep = db["ONTIME_REPORTING_12"]

df12_rep=pd.DataFrame(list(col12_rep.find()))
df12_rep['dt']=df12_rep.apply( lambda x: date_conc(x),axis=1)


# airportlist
airport_list = db["airports_list"]
air_list=pd.DataFrame(list(airport_list.find()))
air_df=air_list.drop_duplicates(subset='DISPLAY_AIRPORT_NAME', keep="first")

# weather
airport_weather = db["airport_weather_2019"]
airport_weather_df=pd.DataFrame(list(airport_weather.find()))
airport_weath=airport_weather_df.drop_duplicates(subset=['NAME','DATE'], keep="first")

air_weath_list_df=pd.merge(airport_weath,air_df,on='NAME',how='inner')

air_weath_list_df['dt']=air_weath_list_df['DATE'].apply(lambda x: dt_format(x))

       

comb=[df,df2,df2_rep,df4_rep,df5_rep,df6_rep,df7_rep,df8_rep,df9_rep,df10_rep,df11_rep,df12_rep]
df3=pd.concat(comb)
df_excl=df3[~df3['OP_UNIQUE_CARRIER'].isin(['OO','UA','YX','MQ','B6','OH'])]

col_p10 = db.P10_EMPLOYEES
p10_employees_df = pd.DataFrame(list(col_p10.find()))



col_carr = db.CARRIER_DECODE
df_carr = pd.DataFrame(list(col_carr.find()))
carrier_decode_df=df_carr.drop_duplicates(subset='OP_UNIQUE_CARRIER', keep="first")

p10_employees_df_1=p10_employees_df.drop_duplicates(subset='OP_UNIQUE_CARRIER', keep="first")

df_carrier1 = pd.merge(carrier_decode_df, p10_employees_df_1, on='OP_UNIQUE_CARRIER')

f1_1=pd.merge(df_carrier1,df3,on='OP_UNIQUE_CARRIER')

f1=pd.merge(f1_1,air_weath_list_df,on=['ORIGIN_AIRPORT_ID','dt'],how='inner')

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:root@localhost:5432/AirlineData')


f1.to_sql('airport_table',con=engine,if_exists='replace')        


