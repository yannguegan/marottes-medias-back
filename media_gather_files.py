# coding: utf-8
# !/usr/bin/python3.6
# This script opens all the daily JSON files with entities detected
# and gather data in a big file '3months.json" for each media. 
# This new file is used to create the line chart data.

import html
import chardet
import requests
import pandas as pd
import json
import time
import datetime
import re
import os
import unicodedata
from bs4 import BeautifulSoup
from pprint import pprint
from dateutil.parser import parse

print('')
print('/// Librairies correctly imported')

# Global variables we will use
context = 'dev'
print('Current environment:',context)

path_to_export = '[LOCAL-PATH]'
if context == 'prod':
    path_to_export = '[SERVER-PATH]'

media_list_URL = '[GOOGLE-SHEET-CSV-URL]'
this_week = datetime.datetime.now().isocalendar()[1]
this_week = str(datetime.datetime.now().year) + '.' + str(this_week)
this_month = str(datetime.datetime.now().year) + '.' + str(datetime.datetime.now().month)
this_day = datetime.datetime.now()

print('This week is:', this_week)
print('This month is:', this_month)
print('This day is:', this_day)

# Load media list
try:
    media_list = pd.read_csv(media_list_URL, index_col='code')
    if media_list['media'][1] == 'NaN':
        print('Error when reading media list ! Using previously saved list')
        media_list = pd.read_csv(path_to_export + 'csv/media_list.csv', index_col=0)
    else:
        media_list.to_csv(path_to_export + 'csv/media_list.csv', index_label="code")
except:
    print('Error when loading media list ! Using previously saved list')
    media_list = pd.read_csv(path_to_export + 'csv/media_list.csv', index_col=0)

media_list['code'] = media_list.index.values
media_list = media_list.reindex(media_list.index.rename('id'))
media_list = media_list[media_list['scrap'] == 'oui']

if context == 'dev':
    media_list = media_list[:1000]
media = media_list.to_dict(orient='records')
nb_media = len(media)
print('')
print('/// Media list ready with', nb_media, 'media')

for i,medium in enumerate(media):
    medium['name'] = medium['media']
    print('Getting analyses for medium:', medium['name'])
    for el in ['analysesByDay']:
        this_path = path_to_export + medium['code'] + '/' + el
        file_list = os.listdir(this_path)
        three_months = []
        
        # Open each file and add it to main array
        for file in file_list:
            if file != '3month.json':
                file_date = file.replace('.json', '')
                file_true_date = parse(file_date)
                age = (this_day - file_true_date).days
                if age < 91:
                    content_file = open(this_path + '/' + file).read()
                    content = json.loads(content_file)
                    content['day'] = file_date
                    three_months.append(content)
        with open(this_path + '/3month.json', 'w+') as fp:
            json.dump(three_months, fp)        
            
print('')
print('/// Main file saved for all media') 