# -*- coding: utf-8 -*-
"""
Created on Mon Dec 09 10:14:04 2022

@author: Dona
"""

import pymongo
import pandas as pd
import json
import numpy as np
import xml.etree.ElementTree as ETree

xmldata="D:\\NCI Modules\\Sem 1\\DAP\\airports_list.xml"

client = pymongo.MongoClient("mongodb://localhost:27017")

prstree = ETree.parse(xmldata)
root = prstree.getroot()

airportlist = []

all_data1 = []

for x in root.iter('row'):
    originid = x.find('ORIGIN_AIRPORT_ID').text
    airportname = x.find('DISPLAY_AIRPORT_NAME').text
    cityname = x.find('ORIGIN_CITY_NAME').text
    name = x.find('NAME').text

    airportlist = [ originid, airportname, cityname, name]
    all_data1.append(airportlist)

df23 = pd.DataFrame(all_data1, columns =['ORIGIN_AIRPORT_ID', 'DISPLAY_AIRPORT_NAME','ORIGIN_CITY_NAME','NAME'])
data23 = df23.to_dict(orient="records")


#Tr
db = client["AirlineData"]
db.airports_list.insert_many(data23)


