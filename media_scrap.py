# coding: utf-8
# !/usr/bin/python3.6
# This script scraps stories available in the RSS feed 
# of each medium, gets the title, the intro and the publication 
# date, cleans and stores the data in separate JSON files.

# Libraries we need
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
this_day = str(datetime.datetime.now().year) + '-' + str(datetime.datetime.now().month) + '-' + str(datetime.datetime.now().day)

print('This week is:', this_week)
print('This month is:', this_month)
print('This day is:', this_day)
print('')
print('/// Global variables defined')

# Functions declarations
def get_XML_content(url):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    soup = ''
    valid_feed = True
    feed_element = 'undefined'
    feed_element_desc = 'undefined'
    feed_element_date = 'undefined'
    try:
        page = requests.get(url,headers=hdr)
        # page = urlopen(req)
        soup = BeautifulSoup(page.text, 'xml')
        # print(soup)
    except requests.exceptions.RequestException as e:
        valid_feed = False
        
    # Look for the name of the item elements in feed
    if valid_feed == True:
        els = ['entry', 'item']
        for el in els:
            items = soup.findAll(el)
            if len(items) > 0:
                feed_element = el
        if feed_element == 'undefined':
            valid_feed = False
            
    # Look for the name of the description elements in feed
    if valid_feed == True:
        desc_els = ['description', 'summary', 'content']
        for el in desc_els:
            items = soup.findAll(feed_element)
            desc = items[0].find(el)
            if desc is not None:
                feed_element_desc = el
                break
        if feed_element_desc == 'undefined':
            valid_feed = False

    # Look for the name of the date elements in feed
    if valid_feed == True:
        date_els = ['pubDate', 'updated','date']
        for el in date_els:
            # print('date el:',el)
            # print(items[0])
            items = soup.findAll(feed_element)
            date = items[0].find(el)
            
            if date is not None:
                feed_element_date  = el
        if feed_element_date == 'undefined':
            valid_feed = False

    # Check if elements we need are found in RSS feed
    if valid_feed == True:
        items = soup.findAll(feed_element)
        title = items[0].find('title')
        url = items[0].find('link')     
        if title is None or url is None:
            valid_feed = False
        
    # Save info in feed dict
    content = {
        'soup': soup,
        'validFeed': valid_feed,
        'feedElement': feed_element,
        'feedDescElement': feed_element_desc,
        'feedDateElement': feed_element_date
    }
    
    return content

def clear_all_outputs():
    os.system('clear')
    # clear_output(wait=True)

def save_medium(medium):
    with open(path_to_export + medium['code'] + '/stories/all.json', 'w+') as fp:
        json.dump(medium['stories'], fp)
    
print('')
print('/// Functions correctly declared')

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

# Truncate table if 'dev'
if context == 'dev':
    media_list = media_list[0:1000]
    
print('')
print('/// Media list loaded')

# Prepare dictionary with media info
media = []
for row in media_list.iterrows():
    medium = {
        'code': row[1]['code'],
        'name': row[1]['media'],
        'rss': row[1]['rss'],
        'domain': row[1]['domaine'],
        'stories': [],
        'validFeed': True
    }
    media.append(medium)
nb_media=len(media)

print('')
print('/// Media dictionary ready,', len(media), 'medias found')

# Create directories
for medium in media:
    if os.path.isdir(path_to_export + medium['code']) == False:
        os.makedirs(path_to_export + medium['code'])
        print('Directory:', medium['code'], 'created')
    for directory in ['stories', 'analysesBy7days', 'analysesByMonth', 'analysesByWeek', 'analysesByDay']:
        if os.path.isdir(path_to_export + medium['code'] + '/' + directory) == False:
            os.makedirs(path_to_export + medium['code']  + '/' + directory)
            print('Directory:', directory, 'created')

# Load existing stories and analysis for each media
for i,medium in enumerate(media):
    clear_all_outputs()
    print('Loading existing stories for medium',(i+1),'/',nb_media,":",medium['name'])
    try:
        medium_content_file = open(path_to_export + medium['code'] + '/stories/all.json').read()
        medium_content = json.loads(medium_content_file)
        medium['stories'] = medium_content
        print(len(medium['stories']), 'stories found')
    except:
        print('No file found or file error for medium',(i+1),'/',nb_media,":",medium['name'])
        medium['stories'] = []

print('')
print('/// Stories loaded for all media')

# Scrap RSS data for each media
for i,medium in enumerate(media):
    clear_all_outputs()
    medium['validFeed'] = True
    print('Scraping stories published by medium',(i+1),'/',nb_media,":",medium['name'])
    existing_url = []
    for story in medium['stories']:
        existing_url.append(story['link'])
    content = get_XML_content(medium['rss'])
    if content['validFeed'] == True:
        storiesRSS = content['soup'].findAll(content['feedElement'])
        for storyRSS in storiesRSS:
            try:
                url = storyRSS.find('link').get_text()
                if url == '' or url is None:
                    try:
                        url = storyRSS.find('link').get('href')
                    except:
                        medium['validFeed'] = False
                try:
                    date_string = storyRSS.find(content['feedDateElement']).get_text()
                    date = parse(date_string, ignoretz= True)
                    week = str(date.isocalendar()[0]) + '.' + str(date.isocalendar()[1])
                    month = str(date.isocalendar()[0]) + '.'+ str(date.month)
                except:
                    print('Could not find a valid date in RSS feed for story:', url)
                    medium['validFeed'] = False
                if medium['validFeed'] == True:
                    if url not in existing_url:
                        print('')
                        print('New story found:')
                        print(url)
                        try:
                            description = storyRSS.find(content['feedDescElement']).get_text()
                        except:
                            description = '' 
                        story = {
                            'title': storyRSS.find('title').get_text(),
                            'description': description,
                            'link': url,
                            'date': str(date),
                            'week': week,
                            'month': month
                        }
                        medium['stories'].append(story)
            except:
                print('Hmmm, problem found with this story:')
                print(storyRSS)
    else:
        medium['validFeed'] = False
    
print('')
print('/// RSS scraped for all media')

convert_table = [
    {
        'in':'Ã§',
        'out': 'ç'
    },
    {
        'in':'à¯',
        'out': 'ï'
    },
    {
        'in': 'Ã»',
        'out': 'û'
    },
    {
        'in':'Ã¨',
        'out': 'è'
    },{
        'in': 'Ã©',
        'out': 'é', 
    },{
        'in': 'â',
        'out': '\''
    },{
        'in': 'Ãª',
        'out': 'ê'
    },{
        'in': 'à¢',
        'out': 'â'
    },{
        'in':'Å',
        'out':'œ'
    },{
        'in':'Ã´',
        'out': 'ô'
    },{
        'in':'Ã',
        'out': 'à'
    },{
        'in': 'Â',
        'out': ''
    },{
        'in': 'àa',
        'out': 'Ça'
    }
]
convert_table_bis  = [
    {
        'in':'',
        'out':''
    }
]

# Clean and shorten description for each story
for medium in media:
    for story in medium['stories']:
        clean_description = story['description']
        if medium['code'] != 'CA':
            clean_description = re.sub('<[^<]+?>', '', clean_description)
        clean_description = clean_description.replace('<br />','').replace('<p>','').replace('</p>','')
        clean_description = " ".join(clean_description.split())
        clean_description = html.unescape(clean_description)
        if medium['code'] == 'LM' or medium['code'] == 'VI':
            for convert in convert_table:
                story['title'] = story['title'].replace(convert['in'],convert['out'])
                clean_description = clean_description.replace(convert['in'],convert['out'])
        if medium['code'] == 'MH':
            clean_description_end = re.search('.*\- [0-9][0-9]:[0-9][0-9](.*)', clean_description)
            if clean_description_end is not None:
                clean_description = clean_description_end.group(1)
        story['description'] = clean_description[:500]

print('')
print('/// Stories description prepared')

# Remove oldest stories to save disk space
print('Cleaning stories database…')
now = datetime.datetime.now()
for medium in media:
    print('')
    print(medium['name'])
    stories_to_keep = []
    max_age = 90
    if len(medium['stories']) > 10000:
        max_age = 45
    for story in medium['stories']:
        date = parse(story['date'], ignoretz=True)
        story_age = (now - date).days
        if story_age < max_age:
            stories_to_keep.append(story)
    medium['stories'] = stories_to_keep
    print(len(medium['stories']), 'stories kept')

print('')
print('/// Stories database is clean')

# Save results and status
for m,medium in enumerate(media):
    
    # Load current status
    try:
        medium_status_file = open(path_to_export + medium['code'] + '/status_scrap.json').read()
        medium_status = json.loads(medium_status_file)
    except:
        print('No status file found for medium',m,'/',nb_media,':', medium['name'])
        medium_status = {
            'validFeed': True,
            'lastScrap': 'nc',
            'nbArticles': 0
        }
        
    # Save stories    
    if medium['validFeed'] == True:
        save_medium(medium)
        medium_status['validFeed'] = True
        medium_status['lastScrap'] = this_day
        medium_status['nbArticles'] = len(medium['stories'])
    else:
        medium_status['validFeed'] = False
    
    # Update status
    with open(path_to_export + medium['code'] + '/status_scrap.json', 'w+') as fp:
        json.dump(medium_status, fp)
    clear_all_outputs()
    print('Data files for medium',m,'/',nb_media,':', medium['name'], 'saved')
    
print('')
print('/// Results saved for all media')