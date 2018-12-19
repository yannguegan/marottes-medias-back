# coding: utf-8
# !/usr/bin/python3.6
# This script gathers entities and media data to create the
# JSON file loaded front-end to render the main dashboard

# Libraries we need
import requests
import pandas as pd
import json
import re
import os
import time
import datetime
import scipy.stats as ss
from bs4 import BeautifulSoup
from pprint import pprint
from dateutil.parser import parse
import urllib.parse
from collections import OrderedDict
import math

print('')
print('/// Librairies correctly imported')

# Global variables we will use
context = 'dev'
print('Current environment:',context)

relevance_threshold = 0.35
confidence_threshold = 5
count_media_min = 0

media_list_URL = '[GOOGLE-SHEET-CSV-URL]'
entities_to_ignore_URL = '[GOOGLE-SHEET-CSV-URL]'

search_url = {
    'path': 'https://www.google.fr/search?q=site:',
    'timeParam':'&tbs=qdr:w'
} 

this_week = datetime.datetime.now().isocalendar()[1]
this_week = str(datetime.datetime.now().year) + '.' + str(this_week)
this_month = str(datetime.datetime.now().year) + '.' + str(datetime.datetime.now().month)
data_path = '[LOCAL-PATH}'
if context == 'prod':
     data_path = '[SERVER-PATH]'
analyses_timespans = ['current', 'previous']

print('')
print('/// Global variables ready')

# Functions we need
def get_csv_content(url, sep, file, index):
    try:
        table = pd.read_csv(url, delimiter=sep, index_col=index)
        print('URL successfuly loaded:',url)
        if table.iloc[0,0] == 'NaN':
            print('Error in table found at URL:',url)
            print('Using latest saved version of:', file + '.csv')
            table = pd.read_csv(data_path + 'csv/' + file + '.csv')
        else: 
            print('Writing CSV file for backup:', file + '.csv')
            table.to_csv(data_path + 'csv/' + file + '.csv', index_label=index)
    except:
        print('Error loading URL:',url)
        print('Using latest saved version of:', file + '.csv')
        table = pd.read_csv(data_path + 'csv/' + file + '.csv', index_col=index)
        
    return table

print('')
print('/// Functions correcty defined')

media_list = get_csv_content(media_list_URL, ',' , 'media_list', 'code')
media_list['code'] = media_list.index.values
media_list = media_list.reindex(media_list.index.rename('id'))
media_list = media_list[media_list['scrap'] == 'oui']
media_list = media_list[media_list['show'] == 'oui']
if context == 'dev':
    media_list = media_list[:500]
    
# Prepare dictionary with media info
media = []
for row in media_list.iterrows():
    medium = {
        'code': row[0],
        'name': row[1]['media'],
        'rss': row[1]['rss'],
        'domain': row[1]['domaine'],
    }
    media.append(medium)
nb_media=len(media)
print('Media dictionary ready,', len(media), 'medias found')
 
print('')
print('/// Media list loaded and media object ready')

# Load existing analyses for each media
for i,medium in enumerate(media):
    # clear_all_outputs()
    print('Loading existing content for media',(i+1),'/',nb_media,":",medium['name'])
    medium['analysesBy7days'] = {}
    for ts in analyses_timespans:
        try:
            file = open(data_path + '/' + medium['code'] + '/analysesBy7days/' + ts + '.json').read()
            content = json.loads(file)
            medium['analysesBy7days'][ts] = content
        except:
            print('No file',ts + '.json', 'found or file error for media',(i+1),'/',nb_media,":",medium['name'])
            medium['analysesBy7days'][ts] = {
                'entities': []
            }
        
print('')        
print('/// Existing analyses loaded for all media')

# Load list of entities to ignore
entities_to_ignore = get_csv_content(entities_to_ignore_URL, ',', 'ignore','id')
ignore_list = []
for row in entities_to_ignore.iterrows():
    ignore_list.append(row[1]['nom'])
    
print('')
print('/// Entities to ignore list loaded')

# Get entities ranks for current/previous analyse
all_entities = {
    'current': {},
    'previous': {}
}
ranked_entities = {
    'current': [],
    'previous': []
}
for timespan in analyses_timespans:
    for medium in media:
        media_entities = medium['analysesBy7days'][timespan]['entities']
        for entity in media_entities:
            if entity['confidence'] >= confidence_threshold and entity['relevance'] >= relevance_threshold and entity['name'] not in ignore_list:
                if entity['code'] not in all_entities[timespan]:
                    all_entities[timespan][entity['code']] = {
                        'code': entity['code'],
                        'media': [],
                        'mediaCount': 0,
                        'totalScoreRank': 0,
                        'totalScore': 0
                    }
                all_entities[timespan][entity['code']]['media'].append(medium['code'])
                all_entities[timespan][entity['code']]['totalScore'] += entity['relevance']

    for entity_code, data in all_entities[timespan].items():
        media_count = len(all_entities[timespan][entity_code]['media'])
        all_entities[timespan][entity_code]['mediaCount'] = media_count
        all_entities[timespan][entity_code]['totalScore'] = round(all_entities[timespan][entity_code]['totalScore'],3)

    for entity_code, data in all_entities[timespan].items():
        ranked_entities[timespan].append(data)

    ranked_entities[timespan].sort(key=lambda x: x['totalScore'], reverse=True)
    rank = 1
    equally = 0
    previous_entity_score = ranked_entities[timespan][0]['totalScore']
    for entity in ranked_entities[timespan]:
        current_entity_score = entity['totalScore']
        if current_entity_score < previous_entity_score:
            rank += equally
            equally = 1
        if current_entity_score == previous_entity_score: 
            equally += 1
        entity['totalScoreRank'] = rank
        previous_entity_score = current_entity_score

print('')
print('/// All entities listed and ranked')

# Prepare global data file
data = {
    'entities': {},
    'media': {},
    'analyse': {}
}
data['analyse']['countMediaMin'] = count_media_min
data['analyse']['relevanceThreshold'] = relevance_threshold

# Load entities and medis in global data
for i,medium in enumerate(media):
    # Add media to list
    data['media'][medium['code']] = {
        'code': medium['code'],
        'name': medium['name'],
        'domain': medium['domain']
    }
    data['media'][medium['code']]['analyse'] = {}
    
    # Add info about last analysis for each medium
    try:
        data['media'][medium['code']]['analyse']['nbStories'] = medium['analysesBy7days']['current']['nbStories']
        data['media'][medium['code']]['analyse']['lastAnalyse'] = medium['analysesBy7days']['current']['lastAnalyse']
    except:
        data['media'][medium['code']]['analyse']['nbStories'] = 'nc'
        data['media'][medium['code']]['analyse']['lastAnalyse'] = 'nc'
                
    # Add entities
    try:
        all_entities = medium['analysesBy7days']['current']['entities']
        print('7days analysis for',medium['name'],':',len(all_entities),'entities')
        for entity in all_entities:
            if entity['confidence'] >= confidence_threshold and entity['relevance'] >= relevance_threshold  and entity['name'] not in ignore_list:

                # Add entity to list
                # Create entity medium
                entity_medium = {
                    'code': medium['code'],
                    'name': medium['name'],
                    'relevance': entity['relevance'],
                    'confidence': entity['confidence'],
                    }
                
                # Try to find previous relevance score
                previous_entities = medium['analysesBy7days']['previous']['entities']
                entity_medium['previousRelevance'] = '-'
                entity_medium['previousRelevanceEvol'] = 'none'
                for previous_entity in previous_entities:
                    if previous_entity['code'] == entity['code']:
                        entity_medium['previousRelevance'] = previous_entity['relevance']
                        if entity_medium['previousRelevance'] <= entity_medium['relevance']:
                            entity_medium['previousRelevanceEvol'] = 'more'
                        else:
                            entity_medium['previousRelevanceEvol'] = 'less'
                
                # Add entity to dict if not existing
                if entity['code'] not in data['entities']:
                    data['entities'][entity['code']] = {
                        'code': entity['code'],
                        'name': entity['name'],
                        'media': []
                    }

                    # Get search terms for this entity
                    try:
                        data['entities'][entity['code']]['terms'] = entity['terms']
                    except:
                        data['entities'][entity['code']]['terms'] = []
                else:
                    
                    # Add search terms to entity data if not already there
                    try:
                        terms = entity['terms']
                    except:
                        terms = []
                    current_terms = data['entities'][entity['code']]['terms']
                    for term in terms:
                        if term not in current_terms:
                            data['entities'][entity['code']]['terms'].append(term) 
                            
                # Add entity medium to entity data
                data['entities'][entity['code']]['media'].append(entity_medium)
    except:
        print('Could not find entities for 7days analysis of', medium['name'])
    
print('') 
print('/// Media and entities selection completed')

# Build search query for each entity
for name, info in data['entities'].items():
    terms = data['entities'][name]['terms']
    search = ''
    for i,term in enumerate(terms):
        if i == 0:
            search += ' '
        if i > 0:
            search += ' OR '
        search += '"' + term + '"'
    data['entities'][name]['search'] = search
            
# Remove terms array to save file space
for name, info in data['entities'].items():
    del data['entities'][name]['terms']

print('')
print('/// Search queries OK')

# Add search URL for each media in each entity
for name, info in data['entities'].items():
    entity_media = data['entities'][name]['media']
    for entity_medium in entity_media:
        domain = data['media'][entity_medium['code']]['domain']
        medium_search_url = search_url['path'] + domain + urllib.parse.quote_plus(data['entities'][name]['search']) + search_url['timeParam']
        entity_medium['searchURL'] = medium_search_url
        
# Remove search query to save file space
for name, info in data['entities'].items():
    del data['entities'][name]['search']
            
print('')
print('/// Search URLs ready')

# Add average and other calculation for each media in each entity
for name, info in data['entities'].items():
    entity_media = data['entities'][name]['media']
    data['entities'][name]['mediaCount'] = len(entity_media)
    total_score = 0
    for entity_medium in entity_media:
        total_score += entity_medium['relevance']
    data['entities'][name]['averageRelevance'] = round(total_score/len(entity_media),4)
    entity_medium['spreadAverageType'] = 'none'
    for entity_medium in entity_media:
        entity_medium['spreadAverage'] = round((entity_medium['relevance'] - data['entities'][name]['averageRelevance']) / data['entities'][name]['averageRelevance'] * 100,1)
        # print(entity_medium['spreadAverage'])
        if entity_medium['spreadAverage'] > 0:
            entity_medium['spreadAverageType'] = 'more'
        else:
            entity_medium['spreadAverageType'] = 'less'
        entity_medium['spreadAverage'] = math.fabs(entity_medium['spreadAverage'])
        entity_medium['spreadRelevance'] = '-'
        if entity_medium['previousRelevance'] != '-':
            entity_medium['spreadRelevance'] = round((entity_medium['relevance'] - entity_medium['previousRelevance']) / entity_medium['previousRelevance'] * 100,1)
        
print('')
print('/// Calculation done')

# Remove data to save space
for name, info in data['entities'].items():
    entity_media = data['entities'][name]['media']
    for entity_medium in entity_media:
        del entity_medium['confidence']

for name, info in data['media'].items():
    del data['media'][name]['domain']
    
print('')
print('/// Data removed for smaller file')

# Sort medias for each entity by average spread
for name, info in data['entities'].items():
    entity_media = data['entities'][name]['media']
    entity_media.sort(key=lambda x: x['relevance'], reverse=True)

# Make array with entities 
entities_sort = []
for name, info in data['entities'].items():
    entities_sort.append(info)

# Keep entities only media count is above threshold
entities_filtered = []
for entity in entities_sort:
    if entity['mediaCount'] >= count_media_min:
        entities_filtered.append(entity)

data['entities'] = entities_filtered

print('')
print('/// Entities filtered and sorted')

# Add entity rank
for entity in data['entities']:
    entity_code = entity['code']
    for timespan in analyses_timespans:
        for ranked_entity in ranked_entities[timespan]:
            if ranked_entity['code'] == entity_code:
                entity[timespan + 'Rank'] = ranked_entity['totalScoreRank']
                
for entity in data['entities']:
    try:
        diff_rank = entity['currentRank'] - entity['previousRank']
        if diff_rank == 0:
            entity['rankDiff'] = '='
        if diff_rank > 0:
            entity['rankDiff'] = '&#9660; ' + str(diff_rank)
        if diff_rank < 0:
            entity['rankDiff'] = '&#9650; ' + str(abs(diff_rank))
    except:
        entity['rankDiff'] = 'nouveau'
    try:
        del entity['previousRank']
    except:
        pass
data['entities'].sort(key=lambda x: x['currentRank'], reverse=False)

print('')
print('/// Rank added for all entities')

# Write file with all results
data_to_file = {}
data_to_file['7days'] = data

with open(data_path + 'data.json','w+') as fp:
    json.dump(data_to_file, fp)
    
print('')
print('/// JSON global file ready')