# coding: utf-8
# !/usr/bin/python3.6
# This script calls media_graph.py for each of the first n entities displayed 
# in the dashboard. When they are clicked on by the user, the line chart loads faster. 
# Yet this cache is not prepared for all entities to limit server costs.

# Libraries we need
import json
import os
import media_graph
import pandas as pd
import pydash
from dateutil.parser import parse
from pprint import pprint

print('')
print('/// Librairies correctly imported')

# Set environment
context = 'dev'

# Define path
path = '[LOCAL-PATH]'
if context == 'prod':
    path = '[SERVER-PATH]'

print('')
print('/// Current environment is ' + context + ', path is:', path)

# To clear all outputs
def clear_all_outputs():
    os.system('clear')
    # clear_output(wait=True)

Load front-end data
data = {}
try:
    data_file = open(path + 'data.json').read()
    data = json.loads(data_file)
except:
    print('!!! Unable to load data.json')

if data:
    entities = data['7days']['entities']
    for entity in entities[:50]:
        clear_all_outputs()
        print('Create cache for entity:', entity['name'])
        request = {
            'entity': entity['code'],
            'frequency': 'daily'
        }
        media_graph.get_graph(request)
    print('')
    print('// Cache ready !')