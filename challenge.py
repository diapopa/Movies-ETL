# Import dependencies
import json
import pandas as pd
import numpy as np
import re
import time
from sqlalchemy import create_engine
from config import db_password

# Create a file path of the directory to reference later
file_dir = 'C:/Users/Dianysus/Desktop/UofT/8-ETL'

# Open the wiki_movies file 
with open(f'{file_dir}/Movies-ETL/wikipedia.movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)

# Read the kaggle csv files
kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory=False)
ratings = pd.read_csv(f'{file_dir}/ratings.csv')

# Create a function to clean the move file
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    # Make an empty dict to hold all the alternative titles
    alt_titles = {}
    # Loop through a list of all alternative title keys
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        # Check if the current key exists in the movie object
        if key in movie:
            # If so, remove the key-value pair and add to the alternative titles dictionary
            alt_titles[key] = movie[key]
            movie.pop(key)
    # After looping through every key, add the alternative titles dict to the movie object
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles
    
     # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
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

# Function that fills in missing data for a column pair and drops the redundant column
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)

# ETL function
def ETL(wiki_movies_raw, kaggle_metadata, ratings):
    
    # Create dataframes from the kaggle lists
    kaggle_metadata_df = pd.DataFrame(kaggle_metadata) 
    ratings_df = pd.DataFrame(ratings)
    
    # Save the filtered data to a list
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
    
    # Create a dataframe
    wiki_movies_df = pd.DataFrame(wiki_movies)
    
    # Make a list of cleaned movies with list comprehension
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

    # Set dataframe from clean_movies and print out a list of the columns
    wiki_movies_df = pd.DataFrame(clean_movies)
    sorted(wiki_movies_df.columns.tolist())

    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    box_office = wiki_movies_df['Box office'].dropna()
    
    box_office[box_office.map(lambda x: type(x) != str)]
    
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    form_one = r'\$\d+\.?\d*\s*[mb]illion'
    box_office.str.contains(form_one, flags=re.IGNORECASE).sum()
    
    form_two = r'\$\d{1,3}(?:,\d{3})+'
    box_office.str.contains(form_two, flags=re.IGNORECASE).sum()
    
    matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)
    
    box_office[~matches_form_one & ~matches_form_two]
    
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    box_office.str.extract(f'({form_one}|{form_two})')
    
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    budget = wiki_movies_df['Budget'].dropna()
    
    # Convert any lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Remove any values between a dollar sign and a hyphen
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    budget[~matches_form_one & ~matches_form_two]
    
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    budget[~matches_form_one & ~matches_form_two]
    
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
    
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE).sum()
    
    running_time[running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE) != True]
    
    running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE).sum()
    
    running_time[running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE) != True]
    
    running_time[running_time.str.contains(r'\d*\s*m', flags=re.IGNORECASE) != True] 
    
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
    
    # Keep rows where the adult column is False and drop the adult column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    
    # Convert data types
    try:
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    except:
        pass
    
    try:
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    except:
        pass
    
    try:
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    except:
        pass
    
    try:
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    except:
        pass
    
    try:
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    except:
        pass
    
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle             Drop Wikipedia.
    # running_time             runtime                  Keep Kaggle; fill in zeros with Wikipedia data.
    # budget_wiki              budget_kaggle            Keep Kaggle; fill in zeros with Wikipedia data.
    # box_office               revenue                  Keep Kaggle; fill in zeros with Wikipedia data.
    # release_date_wiki        release_date_kaggle      Drop Wikipedia.
    # Language                 original_language        Drop Wikipedia.
    # Production company(s)    production_companies     Drop Wikipedia.
    
    # Check for missing titles in the kaggle data and fill them in with 0
    movies_df[(movies_df['title_kaggle'] == '') | (movies_df['title_kaggle'].isnull())]
    movies_df.fillna(0)

    # Look for any movie with a release date after 1996 according to Wiki and before 1965 according to Kaggle
    movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')]

    # Get the index of that row
    movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index

    # Drop the row
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    # Drop the title_wiki, release_date_wiki, language, and Prof=duction company(s) columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    # Run the function for the three column pairs
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # Check for any columns with only one value
    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
        if num_values == 1:
            print(f"Only one value in column: {col}")
            
    # Reorder columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    
    # Rename columns
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
    
    # Use groupby on movieId and rating columns and take the count for each group.
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()
    
    # Rename the userId column to count
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) 
    
    # Make movieId the index, the rating values the columns, and the counts for each rating the rows.
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
    
    # Rename the columns
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    # Left merge to join the ratings counts onto movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    # Fill in missing values with zero
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    try:
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
    except e:
        raise f"Connection string failure - Error: {e}"
    
    try:
        movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    except e:
        raise f"Writing to databse failed - Error: {e}"
    # create a variable for the number of rows imported
    rows_imported = 0

    # get the start_time from time.time()
    start_time = time.time()
    
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        try:
            data.to_sql(name='ratings', con=engine, if_exists='replace')
        except e:
            raise f"Writing to databse failed - Error: {e}"
        # increment the number of rows imported by the size of 'data'
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed') 
        
    return

ETL(wiki_movies_raw, kaggle_metadata, ratings)