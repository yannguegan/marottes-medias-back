# coding: utf-8
# !/usr/bin/python3.6
# When requested, this script gathers and send the data available
# for a given entity. It is used front-end to draw the line chart 
# in the dashboard when the user clicks on an entity.

import datetime
import json
import math
import numpy as np
import os
import pandas as pd
import pydash
from dateutil.parser import parse
from pprint import pprint

print('')
print('/// Librairies correctly imported')

# Set environment
context = 'dev'

# Define path
path = '[LOCAL-PATH}'
if context == 'prod':
    path = '[SERVER-PATH]'
    
# Entities threshold
thr_relevance = 0.3
thr_confidence = 6

print('')
print('/// Current environment is ' + context + ', path is:', path)

# To clear all outputs
def clear_all_outputs():
    os.system('clear')
    # clear_output(wait=True)

media_list_URL = '[GOOGLE-SHEET-CSV-URL]'

# To load media list
def load_media_list():
    try:
        media_list = pd.read_csv(media_list_URL, index_col='code')
        if media_list['media'][1] == 'NaN':
            print('Error when reading media list ! Using previously saved list')
            media_list = pd.read_csv(path + 'csv/media_list.csv', index_col=0)
        else:
            media_list.to_csv(path + 'csv/media_list.csv', index_label="code")
    except:
        print('Error when loading media list ! Using previously saved list')
        media_list = pd.read_csv(path + 'csv/media_list.csv', index_col=0)
    media_list['code'] = media_list.index.values
    media_list = media_list.reindex(media_list.index.rename('id'))
    media_list = media_list[media_list['show'] == 'oui']
    media_list = media_list.rename(columns={'media': 'name'})

    # Truncate list when in dev
    if context == 'dev':
        media_list = media_list[:10]
        
    # Prepare dict
    media = media_list.to_dict(orient='records')
    print('Media dictionary ready:', len(media), 'medias found')

    return media

# To get all media with requested entity
def get_media(request, media):
    request['data'] = []
    today = datetime.datetime.now()
    for m,medium in enumerate(media):
        clear_all_outputs()
        print('')
        print('Looking for entity', request['entity'],'media #',m+1,'/',len(media),':', medium['name'])
        folder_path = path + medium['code'] + '/' + request['folder'] + '/'
        if os.path.isdir(folder_path):
            
            # Load 3month data file
            analysis_file = open(folder_path + '3month.json').read()
            analysis_list = json.loads(analysis_file)   
                
            # Look for requested entity data
            for analysis in analysis_list:
                age = (today - parse(analysis['day'])).days
                if 'entities' in analysis and age < 31:
                    this_entity = pydash.collections.find(analysis['entities'], { 'code': request['entity']}  )
                    if this_entity is not None:
                        if 'entityName' not in request:
                            request['entityName'] = this_entity['name']
                        if this_entity['relevance'] > thr_relevance and this_entity['confidence'] > thr_confidence:

                            # Store entity
                            medium_info = {
                                'name': medium['name'],
                                'code': medium['code'],
                                'relevance': this_entity['relevance'],
                                'confidence': this_entity['confidence']
                            }

                            time = analysis['day']
                            # print(time)
                            # pprint(medium_info)
                            if request['frequency'] != 'daily':
                                
                                # Add missing zero
                                if len(time) == 6:
                                    time_nb = time[5:6]
                                    time_year = time[0:4]
                                    time = time_year + '.' + '0' + time_nb
                            this_time = pydash.collections.find(request['data'], {'time': time})
                            if this_time is None:
                                request['data'].append({
                                        'time': time,
                                        'media': [medium_info]
                                    })
                            else: 
                                this_time['media'].append(medium_info)
    print(len(request['data']), 'timespans found for entity')
    
    return request  

def create_graph(request): 
    today = datetime.datetime.now().date()
    media = load_media_list()
    if request['frequency'] == 'weekly':
        request['folder'] = 'analysesByWeek'
    if request['frequency'] == 'monthly':
        request['folder'] = 'analysesByMonth'
    if request['frequency'] == 'daily':
        request['folder'] = 'analysesByDay'
     
    # Get all media with requested entities
    request = get_media(request, media)
        
    # Create dataframe
    index = []
    columns = []
    for ts in request['data']:
        index.append(ts['time'])
    index.sort(reverse=False)
    for medium in media:
        columns.append(medium['name'])

    request['table'] = pd.DataFrame(index=index, columns=columns)
    for ts in request['data']:
        for medium in ts['media']:
            request['table'].loc[ts['time'],medium['name']] = float(medium['relevance']) 
            
    # Remove media if no days recorded for entity
    for medium in media:
        nb_nan = request['table'][medium['name']].isnull().sum()
        total = len(index)
        # print(medium['name'],nb_nan,total)
        if nb_nan >= total-1:
            del request['table'][medium['name']]
        
    # Get average for each medium
    for medium in request['table'].columns:
        medium_average = request['table'][medium].fillna(thr_relevance).mean(skipna=False)
        request['table'].loc['mediumAverage', medium] = medium_average

    if context == 'dev':
        display(request['table']) 
     
    # Transpose table: media in lines, time in columns
    request['table'] = request['table'].transpose()
    
    # Sort table
    request['table'] = request['table'].sort_values('mediumAverage',ascending=False)
    request['table'] = request['table'].drop('mediumAverage',axis=1)
    
    # Get averages by time
    for time in request['table'].columns:
        request['table'].loc['dayAverage', time] = request['table'][time].fillna(thr_relevance).mean(skipna=False)
                    
    # Replace missing values by empty string
    request['table'] = request['table'].fillna('')
    
    if context == 'dev':
        display(request['table']) 
    
    # Table ready, prepare response dict
    request['response'] = {
        'labels' : [],
        'datasets' : [],
        'info': {}
    }
    request['response']['info']['entity'] = {
        'code': request['entity'],
        'name': request['entityName'],
    }
    request['response']['info']['all'] = []
    for col in request['table'].columns:
        request['response']['labels'].append(col)  
    for idx in request['table'].index: 
        if idx not in ['dayAverage', 'dayAverageMin', 'dayAverageMax']:
            medium_code = pydash.collections.find(media, { 'name': idx})['code']
            request['response']['info']['all'].append({
                'name': idx,
                'code': medium_code
            })
        dataset = {
            'label': idx,
            'data': []
        }
        for col in request['table'].columns: 
            dataset['data'].append({
                'x': col,
                'y': request['table'].loc[idx, col]
            })
        request['response']['datasets'].append(dataset)
    request['response']['info']['updatedAt'] = str(today)
    with open(path + 'cache/' + request['entity'] + '.json', 'w+') as fp:
        json.dump(request['response'], fp)
        print('File', request['entity'] + '.json', 'saved')
        
    return request                              

def get_graph(request):
    
    # Get current date
    today = datetime.datetime.now().date()
    try:
        file = open(path + 'cache/' + request['entity'] + '.json').read()
        data = json.loads(file)
        if data['info']['updatedAt'] != str(today) or context == 'dev':
            print('File was found for this request, but was not created today…')
            request = create_graph(request)
        else:
            print('File found for this request')
            request['response'] = data
    except:
        print('No file found for this entity, creating it…')
        request = create_graph(request)
        
    return request

test_request = {
    'entity': 'didierdeschamps',
    'frequency': 'daily'
}

if context == 'dev':
    response = get_graph(test_request)['response']
    pprint(response)