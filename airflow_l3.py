#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[ ]:


default_args = {
    'owner': 'a-ryzhakina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 4)
}

    
@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False)
def a_ryzhakina_airflow_l3():
    
    @task(retries=2)
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        year = 1994 + hash('a-ryzhakina') % 23
        value = df['Year'] == year
        df = df[value]
        return df
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=2)
    def game_of_the_year(df):
        top_game = df.groupby('Name')['Global_Sales'].sum().idxmax()
        return top_game
    
    # Игры какого жанра были самыми продаваемыми в Европе?
    @task(retries=2)
    def best_genre_in_EU(df):
        top_genre = df.groupby('Genre')['EU_Sales'].sum().idxmax()
        return top_genre
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task(retries=2)
    def best_platform_in_NA(df):
        top_platform = df.query("NA_Sales>1").groupby('Platform',as_index=False).agg({'Name':'count'})        .sort_values('Name',ascending=False).query("Name==Name.max()").Platform.to_list()
        return top_platform
    
    # У какого издателя самые высокие средние продажи в Японии?
    @task(retries=2)
    def best_publisher_in_JP(df):
        top_publisher = df.groupby('Publisher')['JP_Sales'].mean().idxmax()
        return top_publisher
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=2)
    def games_more_in_EU(df):
        games_EU_vs_JP = df.query("EU_Sales>JP_Sales").Name.nunique()
        return games_EU_vs_JP
    
    @task()
    def print_data(top_game, top_genre, top_platform, top_publisher, games_EU_vs_JP):
        year = 1994 + hash('a-ryzhakina') % 23
        
        print(f'The best selling game in {year} in the world:' top_game')
        print(f'The best selling genre in {year} in Europe:' top_genre, sep='\n')
        print(f'The platform with more than a million copies of games sold in NA in {year}:' top_platform, sep='\n')
        print(f'The publisher with the highest average sales in Japan in {year}:' top_publisher, sep='\n')
        print(f'The number of better-sold games in Europe than in Japan in {year}:' games_EU_vs_JP, sep='\n')

    df = get_data()
    top_game = game_of_the_year(df)
    top_genre= best_genre_in_EU(df)
    top_platform = best_platform_in_NA(df)
    top_publisher = best_publisher_in_JP(df)
    games_EU_vs_JP = games_more_in_EU(df)
    
    final_task = print_data(top_game, top_genre, top_platform, top_publisher, games_EU_vs_JP)

    
dag_a_ryzhakina_airflow_l3() =  a_ryzhakina_airflow_l3()

