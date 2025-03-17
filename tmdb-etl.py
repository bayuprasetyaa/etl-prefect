import os
import datetime
from dotenv import load_dotenv

from prefect import flow, task
from prefect_github import GitHubRepository
from prefect_gcp import GcpCredentials

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
import requests

github_block = GitHubRepository.load("github-storage")

# load_dotenv()
# Load API key
API_KEY = os.getenv("TMDB_API_KEYS")
# Get project ID
PROJECT_ID = os.getenv("PROJECT_ID")
# GET Dataset ID
DATASET_ID = os.getenv("DATASET_ID")

# Define the BASE URL
BASE_URL = "https://api.themoviedb.org/3"
# Load BigQuery Credentials
gcp_credentials = GcpCredentials.load("bigquery-credentials")
credentials = gcp_credentials.get_credentials_from_service_account()

# The result
tables = dict()


# Predefined function
def get_data(base_url:str ,endpoint:str, api_key:str) -> dict:
    """Make request from endpoint URL

    Args:
        base_url (str): Base URL.
        endpoint (str): Endpoint URL.
        api_key (str): API KEY.

    Returns:
        dict: Response.
    """
    header = {
        'accept': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    url = base_url + endpoint
    return requests.get(url, headers=header)


def generate_run_name():
    return f"run-TMDBmov-{datetime.datetime.now()}"

# Extraction: Get Trending movies
@task(name="Get Trending Movies", tags=['extract'], log_prints=True)
def get_trending():
    endpoint = '/trending/movie/day'
    response_tm = get_data(BASE_URL,endpoint,API_KEY).json()['results']
    return pd.DataFrame(response_tm)

# Extraction: Get Detail movies
@task(name="Get Movie Details", tags=['extract'], log_prints=True)
def get_details(trending_movie_id: list):
    response_result = []
    for movie in trending_movie_id:
        endpoint = "/movie/" + str(movie)
        response_result.append(get_data(BASE_URL, endpoint, API_KEY).json())
    return pd.DataFrame(response_result)

# Extraction (flow): Get Trend -> Get details
flow(name="Data Extraction",log_prints=True)
def extract():
    tables['trend'] = get_trending()
    tables['details'] = get_details(tables['trend']['id'].tolist())

# Transform: Create movie genre
@task(name="Create movie_genre", tags=['transform'], log_prints=True)
def create_movie_genre():
    tables['movie_genre'] = tables['trend'][['id', 'genre_ids']].explode('genre_ids')

# Transfom: Transform trending movies
@task(name="Transform Trending", tags=['transformation'], log_prints=True)
def transform_trend(df:pd.DataFrame):
    df = df.drop(columns=['original_title', 'media_type', 'genre_ids'])
    df['release_date'] = pd.to_datetime(df['release_date'])
    df['rank'] = df['popularity'].rank(ascending=False)
    df['rank_date'] = datetime.datetime.now()

# Transfom: Transform detail movies
@task(name="Transform details", tags=['transformation'], log_prints=True)
def transform_detail(df:pd.DataFrame):
    df = df.drop(columns='genres')
    df['release_date'] = pd.to_datetime(df['release_date'])

# Transformation (flow)
@flow(name="Transformation", flow_run_name=generate_run_name, log_prints=True)
def transform():
    create_movie_genre()
    transform_trend(tables['trend'])
    transform_detail(tables['details'])

# Load: Update Trending
@task(name="Update trending movies", tags=['load'], log_prints=True)
def update_trending(tables:dict):
    pandas_gbq.to_gbq(
        dataframe=tables,
        destination_table=f'{DATASET_ID}.trend',
        project_id=PROJECT_ID,
        if_exists='replace',
        credentials=credentials)

# Load: Update Details
@task(name="Update movies details", tags=['load'], log_prints=True)
def update_details(tables:dict):
    pandas_gbq.to_gbq(
        dataframe=tables,
        destination_table=f'{DATASET_ID}.details',
        project_id=PROJECT_ID,
        if_exists='replace',
        credentials=credentials)

# Load: Update Movie Genre
@task(name="Update movies movie genre", tags=['load'], log_prints=True)
def update_movie_genre(tables:dict):
    pandas_gbq.to_gbq(
        dataframe=tables,
        destination_table=f'{DATASET_ID}.movie_genre',
        project_id=PROJECT_ID,
        if_exists='replace',
        credentials=credentials)

@flow(name="Load to BigQuery", log_prints=True)
def load_to_gbq():
    update_trending(tables['trend'])
    update_details(tables['details'])
    update_movie_genre(tables['movie_genre'])

# ETL Process
@flow(name="etl-process", log_prints=True)
def etl_process():
    extract()
    transform()
    load_to_gbq()

if __name__=="__main__":
    etl_process()