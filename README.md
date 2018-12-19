# marottes-medias-back

## media_scrap.py

This script scraps stories available in the RSS feed of each medium, gets the title, the intro and the publication date, cleans and stores the data in separate JSON files.

## media_analyse.py

This script prepares a corpus of scraped stories (headline and intro) and send it to TextRazor to perform named entities recognition

## media_prepare_data.py

This script gathers entities and media data to create the JSON file loaded front-end to render the main dashboard

## media_gather_files

This script opens all the daily JSON files with entities detected and gather data in a big file '3months.json" for each media. This new file is used by media_graph.py to create the line chart data.

## media_graph.py

When requested, this script gathers and send the data available for a given entity. It is used front-end to draw the line chart in the dashboard when the user clicks on an entity.

## media_cache.py

This script calls media_graph.py for each of the first n entities displayed in the dashboard. When they are clicked on by the user, the line chart loads faster. Yet this cache is not prepared for all entities to limit server costs.