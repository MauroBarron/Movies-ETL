#!/usr/bin/env python
# coding: utf-8

# In[ ]:


##
## ETL Challenge - Create a function that extracts Wikipedia data, Kaggle metadata and MovieLens rating data
##   Performs necessary data transformations
##   Merges data 
##   Load data to PostgreSQL 
##   Run function correctly against first two data sources

## Functions Sections:
##   Dependencies         
##   Clean Movie function - dependent function for ETL function
##   Parse Dollars function - dependent function for ETL function
##   ETL function - Main Function: loads data and parses data and includes smaller functions.
##   Execute ETL function


def etl_challenge(wiki,kaggle,ratings):   #declare ETL function with three data parameters

    ### Dependencies
    import json  # for handling the JSON files
    import pandas as pd  # Pandas. Always.
    import numpy as np # Our pal NumPy
    import re  # for using regular expressions
    from sqlalchemy import create_engine # needed for loading to SQL
    import time # used for ratings load progress report
    import psycopg2 # for working with PostgreSQL

## Clean Movie function

    def clean_movie(movie):

        ## initial definition of the three data source arguments.
        movie = dict(movie) #create a non-destructive copy

        # combine alternate titles into one list
        alt_titles = {}
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune-Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # function to change column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        # Change column names
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')


        return movie
    
#### Parse Dollars Function
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
    
    

### Initial variables - could be global variables
    # set the local Resources folder as my file directory
    file_dir = "C:/Users/Marishka/Documents/DABootCamp/W9-M8-ETL/Movies-ETL/Resources/"
            
### load  the three data sources
    # Wikipedia movie data.  json file
    try:
        with open (f'{file_dir}/{wiki}', mode='r') as file:
            wiki_movies_raw = json.load(file)
    except FileNotFoundError: 
        print (f"Ooops. Sorry but the {wiki} file does not seem to exist")
    
    # Kaggle movies metadata
    try:
        kaggle_metadata = pd.read_csv(f'{file_dir}/{kaggle}', low_memory=False)
    except FileNotFoundError:
        print (f"Ooops. Sorry but the {kaggle} file does not seem to exist")
        
    # Kaggle ratings data.
    try:
       ratings = pd.read_csv(f'{file_dir}/{ratings}', low_memory=False)       
    except FileNotFoundError:
       print (f"Ooops. Sorry but the {ratings} file does not seem to exist")
    
    
### Wikipedia wikipedia.movies.json Cleaning and Parsing
    ## Wiki Movies preclean filter
    # Filter wiki_movies_raw to those conditions shown here
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
        
    ## Run clean movies function for Wiki data. Returns wiki_movies_df
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    ## Transformation and further cleanup of Wiki Movies Dataframe
    # wiki_movies_df: Extract imdb_id from imdb_link variable using a string expression
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    
    # wiki_movies_df: Remove duplicate imdb_id_links
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    # wiki_movies_df: drop columns where 90% of data is NULL
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    ## wiki_movies_df: Parse Box Office Data
    # drop nulls
    box_office = wiki_movies_df['Box office'].dropna()
    # parse values that are stored as range
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    # Create string pattern forms for parsing
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'  
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    # Parse the box office data using two forms of dollar values
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # drop the original Box Office column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    ## wiki_movies_df: Parse Budget Data
    # create a budget list - dropping the nulls
    budget = wiki_movies_df['Budget'].dropna()
    # convert lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x) 
    ### Remove any values between a dollar sign and a hyphen (for budgets given in ranges):
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    ### Remove citation references.
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    # Create string pattern forms for parsing
    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    # Parse the budget data using two forms of dollar values
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # Finally drop the original Budget column
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    ##Parse the Date field
    # convert list to series.
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Form 1: Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    # Form 2: Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    # Form 3: Full month name, four-digit year (i.e., January 2000)
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    # Form 3:#Four-digit year (i.e. 1984)
    date_form_four = r'\d{4}'
    # Parse the release data column using the four date forms
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    # Drop the original Release data column
    wiki_movies_df.drop('Release date', axis=1, inplace=True)
    
    ## Parse Running Time
    # convert lists to strings
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Define the extract string
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    # Parse using the extract string
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
### Kaggle movies_metadata Cleaning and Parsing
    # Keep only the rows where 'adult' is 'False' and drop the adult column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    ## Now clean Video field. This sets the remaining True falues to False. Don't really get why???
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    # Convert the three numeric fields.
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)  ## why is this method different than the following two
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    ## convert release date to datetime
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
        
### Merge the frames.  the suffixes parameters adds the source DF name to the dupliate/redundant fields
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
### Clean and parse of penultimate movies_df dataframe
    # movies_df - Release date: drop weird 2006 outlier
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    # movies_df - Language Convert lists into Tuples. Some data language points stored as list.
    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
    # movies_df - drop unwanted columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    ## function to fill in missing data for column pair and drops redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
    # Run the fill_missing_data function against varous columns     
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    
    ## movies_df - some column clean up
    # Reorder columns
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor',
                        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    # Rename columns for friendliness
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    
### Ratings Cleaning and Parsing
    # convert timestamp field to datetime
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    # Get som Rating data.  1 Count ratings by movie. 2. renames field to count. 3.pivot on movieid as x var   
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1)                  .pivot(index='movieId',columns='rating', values='count')
    # rename for readibility
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    
### Merge rating rating counts with movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    # handle the nulls in the ratings count - set to zero
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

### Export dataframes to SQL
    # get PostgreSQL password
    from config import db_password
    # Connection String
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    # Create the engine using SQLAlchemy
    engine = create_engine(db_string)
    # Import the Movie Data
    movies_with_ratings_df.to_sql(name='movies', con=engine,  if_exists='replace')

#  Ratings - raw data from CSV to SQL
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        print(f'importing rating rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='replace')
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')

### Run the function
etl_challenge("wikipedia.movies.json", "movies_metadata.csv", "ratings.csv")

