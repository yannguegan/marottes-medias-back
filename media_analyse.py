# coding: utf-8
# !/usr/bin/python3.6
# This script prepares a corpus of scraped stories (headline and intro) 
# and send it to TextRazor to perform named entities recognition

# Libraries we need
import requests
import pandas as pd
import json
import time
import datetime
import re
import os
import textrazor
import unicodedata
from bs4 import BeautifulSoup
from pprint import pprint
from dateutil.parser import parse

print('')
print('/// Librairies correctly imported')

# Global variables we will use
context = 'dev'
print('Current environment:',context)
textrazor_api_key = '[TEXT-RAZOR-API-KEY]'

path_to_export = '[LOCAL-PATH]'
if context == 'prod':
    path_to_export = '[SERVER-PATH]'

relevance_threshold = 0.3
confidence_threshold = 6

media_list_URL = '[GOOGLE-SHEET-CSV-URL]'
this_week = datetime.datetime.now().isocalendar()[1]
this_week = str(datetime.datetime.now().year) + '.' + str(this_week)
this_month = str(datetime.datetime.now().year) + '.' + str(datetime.datetime.now().month)

now = datetime.datetime.now()
this_day = now.date()

timespans = ['weekly', 'monthly', '7daysCurrent', '7daysPrevious']

print('This week is:', this_week)
print('This month is:', this_month)
print('This day is:', this_day)

print('')
print('/// Global variables defined')

# Clear all outputs
def clear_all_outputs():
    os.system('clear')
    clear_output(wait=True)

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

# Truncate list when in dev
if context == 'dev':
    media_list = media_list[-1:]
    
# Prepare dictionary with media info
media = []
for row in media_list.iterrows():
    medium = {
        'code': row[1]['code'],
        'name': row[1]['media'],
        'domain': row[1]['domaine']
    }
    media.append(medium)
nb_media=len(media)

print('')
print('/// Media dictionary ready,', len(media), 'medias found')

# Load analysis status for each media
for i,medium in enumerate(media):
    if context == 'prod':
        clear_all_outputs()
    print('Loading analysis status for medium',(i+1),'/',nb_media,":",medium['name'])
    try:
        medium_status_file = open(path_to_export + medium['code'] + '/status_analyse.json').read()
        medium_status = json.loads(medium_status_file)
        medium['status'] = medium_status
        for key in ['nbStories7daysAnalysis', 'latestStoryDate']:
            if key not in medium['status']:
                medium['status'][key] = 'nc' 
    except:
        print('No file found or file error for medium',(i+1),'/',nb_media,":",medium['name'])
        medium['status'] = {
            'nbStories7daysAnalysis': 'nc',
            'latestStoryDate': 'nc'
        }
    print('')

print('')
print('/// Analysis status ready for all media')

# Load existing stories for each media
for i,medium in enumerate(media):
    if context == 'prod':
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

print('')
print('/// Stories loaded for all media')

# Look for average description length
description_length = 0
for medium in media:
    medium_description_length = 0
    for story in medium['stories']:
        medium_description_length += len(story['description'])
    try:
        medium_description_length = round(medium_description_length/len(medium['stories']))
        # print(medium['name'],':',medium_description_length)
        description_length += medium_description_length
    except:
        print('! No story found for:', medium['name'])
        
description_average = round(description_length / len(media))

print('')
print('// Average description length for all media :', description_average)

def prepareCorpus(medium, ts):
    medium['analysis'][ts]['corpus'] = ''
    medium['analysis'][ts]['nbStories'] = 0
    for story in medium['stories']:
        keep_story = False
        
        # 7days analysis corpus
        if ts == '7daysCurrent':
            if story['age'] < 7:
                keep_story = True
        if ts == '7daysPrevious':
            if story['age'] >= 7 and story['age'] < 13:
                keep_story = True
                
        # Weekly and monthly corpus
        if ts == 'weekly':
            if story['week'] == this_week:
                keep_story = True  
        if ts == 'monthly':
            if story['month'] == this_month:
                keep_story = True
            
        # Add story to related corpus    
        if keep_story == True:
            medium['analysis'][ts]['corpus'] += story['title'] + ' ' + story['description'][:description_average]
            medium['analysis'][ts]['nbStories'] += 1
            
    # Truncate corpus
    medium['analysis'][ts]['corpus'] = medium['analysis'][ts]['corpus'].replace('\n',' ')
    corpus_length = len(medium['analysis'][ts]['corpus'])
    medium['analysis'][ts]['corpusLength'] = corpus_length
    medium['analysis'][ts]['corpusTooLong'] = False
    if corpus_length > 180000:
        medium['analysis'][ts]['corpusTooLong'] = True
    medium['analysis'][ts]['corpus'] = medium['analysis'][ts]['corpus'][:180000]
    return medium    

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    
    return next_month - datetime.timedelta(days=next_month.day)

last_month_day = last_day_of_month(now).date()

# Prepare analysis
print('Last month day:', last_month_day)
print('Today:', this_day)
print('Weekday:', datetime.datetime.today().weekday())
print('')
for m, medium in enumerate(media):
    if context == 'prod':
        clear_all_outputs()
    print('Prepare analysis for medium #', m+1,'/',nb_media,':', medium['name'])
    
    # Prepare dict
    medium['analysis'] = {}
    for ts in timespans:
        medium['analysis'][ts] = {}
        
    # Sort stories     
    medium['stories'].sort(key=lambda x: x['date'], reverse=True)
    
    # Prepare true date and age
    stories_with_age = []
    for story in medium['stories']:
        try:
            story['trueDate'] = parse(story['date'], ignoretz=True)
            story['age'] = (now - story['trueDate']).days
            stories_with_age.append(story)
        except:
            pass
    medium['stories'] = stories_with_age
    latest_story_date = medium['stories'][0]['date']
    print('Latest story date:', latest_story_date)
    
    # Prepare corpus for 7days current analysis (has to be done before anything else
    # to get nb of stories in corpus and decide to run analysis or not)
    medium = prepareCorpus(medium, '7daysCurrent')
    
    # Check if analysis has to be made
    for ts in timespans:
        medium['analysis'][ts]['run'] = False
    
    # Perform 7 days analysis if new stories found
    if  str(latest_story_date) != medium['status']['latestStoryDate'] or context == 'dev':
        medium['analysis']['7daysCurrent']['run'] = True
        medium['analysis']['7daysPrevious']['run'] = True
        medium = prepareCorpus(medium, '7daysPrevious')
        print('7days analysis will be performed (current and previous)')
        medium['status']['latestStoryDate'] = latest_story_date
        
    # Perform monthly analysis if today is last day of the month
    if this_day == last_month_day or context == 'dev':
        medium['analysis']['monthly']['run'] = True
        medium = prepareCorpus(medium, 'monthly')
        print('Monthly analysis will be performed')
    
    # Perform monthly analysis if today is saturday
    if datetime.datetime.today().weekday() == 5 or context == 'dev':
        medium['analysis']['weekly']['run'] = True
        medium = prepareCorpus(medium, 'weekly')
        print('Weekly analysis will be performed')
    print('')

print('')
print('/// All analysis have been prepared')

# Perform analysis
textrazor.api_key = textrazor_api_key
client = textrazor.TextRazor(extractors=["entities"])

def do_TextRazor_Analysis(corpus):
    print('')
    print('Corpus start with:')
    print(corpus[0:500])
    response = {}
    if context == 'prod' or context == 'dev':
        try:
            response = client.analyze(corpus)
        except:
            print('!!! TextRazor analysis has failed')
    #if context == 'dev':
    #    print('I am doing the analyse ! Just kidding…')
    
    return response

def normalize(term):
    normalized = ''.join((c for c in unicodedata.normalize('NFD', term) if unicodedata.category(c) != 'Mn'))
    normalized = re.sub(r'( |\(|\)|\'|"|\.|;|\:|\?|!)', '', normalized).lower()
    
    return normalized

def get_entities_list(response):
    entities_list = []
    entities = list(response.entities())
    entities.sort(key=lambda x: x.relevance_score, reverse=True)
    seen = []
    for y,entity in enumerate(entities):
        if entity.id not in seen:
            seen.append(entity.id)
            medium_entity = {
                'code': normalize(entity.id),
                'name': entity.id,
                'wikipedia': entity.wikipedia_link,
                'relevance': entity.relevance_score,
                'confidence': entity.confidence_score,
                'terms': []
            }
            medium_entity['terms'].append(entity.matched_text)
            if medium_entity['relevance'] > 0.2:
                if len(entities_list) <= 20:
                    print(y,'/',medium_entity['name'],'(',medium_entity['relevance'],'/ 1)')
                entities_list.append(medium_entity)
        else:
            term = entity.matched_text
            for current_entity in entities_list:
                if current_entity['name'] == entity.id:
                    if term not in current_entity['terms']:
                        current_entity['terms'].append(term)
                        
    return(entities_list)

def save_analyse(analysis, code, ts):
    del analysis['response']
    del analysis['run']
    del analysis['corpus']
    # pprint(analysis, depth=2)
    export_paths = []
    if ts == 'monthly':
        export_paths.append({
            'directory': 'analysesByMonth',
            'filename' : this_month + '.json'
        })
    if ts == 'weekly':
        export_paths.append({
            'directory': 'analysesByWeek',
            'filename' : this_week + '.json'
        })
    if ts == '7daysCurrent':
        export_paths.append({
            'directory': 'analysesBy7days',
            'filename' : 'current.json'
        })     
        export_paths.append({
            'directory': 'analysesByDay',
            'filename' : str(this_day) + '.json'
        })
    if ts == '7daysPrevious':
        export_paths.append({
            'directory': 'analysesBy7days',
            'filename' : 'previous.json'
        })  
    for export_path in export_paths:
        with open(path_to_export + code + '/' + export_path['directory'] + '/' + export_path['filename'], 'w+') as fp:
            json.dump(analysis, fp)
    print('Analyse files for media', code, 'saved')

def save_status(medium):
    medium['status']['nbStories7daysAnalysis'] = medium['analysis']['7daysCurrent']['nbStories']
    for el in ['lastAnalyse', 'nbEntities' , 'corpusLength' , 'corpusTooLong' , 'nbStories' , 'entities']:
        try:
            medium['status'][el] = medium['analysis']['7daysCurrent'][el]
        except:
            medium['status'][el] = 'nc'
    with open(path_to_export + medium['code'] + '/status_analyse.json', 'w+') as fp:
        json.dump(medium['status'], fp)
    print('Status saved for medium:', medium['name'])

for medium in media:
    if context == 'prod':
        clear_all_outputs()
    for ts in timespans:
        if medium['analysis'][ts]['run'] == True:
            medium['analysis'][ts]['response'] = {}
            print(medium['name'],': analysing',ts,'corpus')
            if medium['analysis'][ts]['nbStories'] > 0:
                medium['analysis'][ts]['response'] = do_TextRazor_Analysis(medium['analysis'][ts]['corpus'])
            else:
                print('! No stories in corpus')
            if medium['analysis'][ts]['response']:
                print('')
                print('Entities:')
                medium['analysis'][ts]['entities'] = get_entities_list(medium['analysis'][ts]['response'])
                medium['analysis'][ts]['lastAnalyse'] = str(this_day)
                medium['analysis'][ts]['nbEntities'] = len(medium['analysis'][ts]['entities'])
                print('')
                save_analyse(medium['analysis'][ts], medium['code'], ts)
                if ts == '7daysCurrent':
                    save_status(medium)
            print('')
            
print('')
print('/// All analysis have been performed')          

# Write CSV file with status info for 7days analysis
index = media_list.index
columns = ['name','lastAnalyse', 'nbEntities' , 'corpusLength' , 'corpusTooLong' , 'nbStories' , 'entities']
table = pd.DataFrame(index=index, columns=columns)
for medium in media:
    analysis = medium['status']
    for col in ['name']:
        table.loc[medium['code'], col] = medium[col]
    for col in ['lastAnalyse', 'corpusLength' , 'corpusTooLong' , 'nbStories', 'nbEntities']:
        try:
            table.loc[medium['code'], col] = analysis[col]
        except:
            table.loc[medium['code'], col] = 'nc'
    for col in ['entities']:
        entities_string = ''
        entities_count = 0
        try:
            for entity in analysis['entities']:
                if entity['confidence'] > confidence_threshold and entity['relevance'] > relevance_threshold:
                    if entities_count > 0:
                        entities_string += ' | '
                    entities_count += 1
                    entities_string += entity['name'] + ' (' + str(entity['relevance']) + ', ' + str(entity['confidence']) + ')'
            table.loc[medium['code'], col] = entities_string
        except:
            table.loc[medium['code'], col] = 'nc'
table.to_csv(path_to_export + 'csv/status.csv' ,index_label='code')
      
print('')
print('/// CSV status file saved') 
