###################
# import packages #
###################
#database connection
import sqlalchemy
from sqlalchemy import Table,Column,String,Integer,DECIMAL,DateTime,table, column, select
#vpn switcher
from nordvpn_switcher import initialize_VPN,rotate_VPN,terminate_VPN
#data handling and sleep
import pandas as pd
import numpy as np
import random
import re
import time
import tqdm
import hashlib
import gzip
import urllib.request
from datetime import datetime as dt
import os
#user agents for scraping purposes
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
#scraping
import requests
from bs4 import BeautifulSoup as bs
#retrieve useragents#
software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)

#start VPN rotation#
#initialize_VPN(save=1,area_input=['complete rotation'])

###########################
######write functions######
###########################

#database connection and setup#
def set_connection(conn,tables):
    print('\n~~~~~~~~~~~~~~~~~~~~\nConnecting to database...')
    try:
        conn_info = open(conn).read().splitlines()
    except:
        raise Exception('Please provide a file containing:\nusername\npassword\nhost'
                        '\ndatabase')
    else:
        try:
            engine = sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(
                conn_info[0],conn_info[1],conn_info[2],conn_info[3]))
        except:
            raise Exception('Something went wrong while connecting to the database...')
        else:
            print('Connection OK!\n~~~~~~~~~~~~~~~~~~~~\n')
    print('Checking tables\n----------------------------')
    tables_df = pd.read_csv(tables, sep=';', quotechar='"')
    metadata = sqlalchemy.MetaData(engine)
    for tl in list(tables_df.table):
        if sqlalchemy.inspect(engine).dialect.has_table(engine.connect(),tl) is False:
            print('Table "{}" not in DB'.format(tl))
            try:
                exec(tables_df[tables_df.table == tl]['query'].values[0])
                metadata.create_all()
            except:
                raise Exception('Something went wrong while creating the table "{}"'.format(tl))
            else:
                print('Table "{}" created: \33[92m\N{check mark}\33[0m\n'.format(tl))
        else:
            print('\nTable "{}" already in DB: \33[92m\N{check mark}\33[0m\n'.format(tl))
    print('----------------------------\n')
    return engine,metadata
#scraping#
def set_headers(user_agent_rotator):
    useragent_pick = user_agent_rotator.get_random_user_agent()
    headers = {'User-Agent': useragent_pick,
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'none',
               'Accept-Language': 'en-US,en;q=0.8',
               'Connection': 'keep-alive'}
    return headers
def fetch_profiles(gateway,engine,metadata):
    page_view = 1
    movie_src = []
    print('Scraping gateways')
    def generator():
        while True:
            yield
    for _ in tqdm.tqdm(generator()):
        filmlist = requests.get('{}/detail/page/{}/'.format(gateway,str(page_view)),
                           headers=set_headers(user_agent_rotator))
        filmlist_load = bs(filmlist.text, 'html.parser')
        movies = [x['href'].split('/film/')[1][:-1] for x in filmlist_load.find_all(href=True)
                  if str(x['href']).startswith('/film/')]
        if len(movies) == 0:
            print('Done!')
            break
        else:
            movie_src.append(movies)
            time.sleep(random.uniform(2,4))
            page_view += 1
    movie_src = list(set(sum(movie_src,[])))
    print('Scraped {} gateways\n------------------------\n'.format(str(len(movie_src))))

    # retrieve profiles already in DB#
    connection = engine.connect()
    query_prfl = sqlalchemy.select([sqlalchemy.Table('users', metadata, autoload=True,
                                                     autoload_with=engine)])
    profiles_db = set([x['user_id'] for x in connection.execute(query_prfl).fetchall()])

    print('Scraping profile urls')
    i = 0
    for movie in tqdm.tqdm(movie_src):
        time.sleep(random.uniform(3,7))
        profiles_movie = dict({'user_id': [], 'profile_url': []})
        #scrape profiles from movie page for max. 5 minutes
        scraped_pg = []
        added_total = 0
        restrict = 250
        elapsed_time_min = 0
        start_time = time.time()
        while len(scraped_pg) < 30 and elapsed_time_min < 5:
            random_pg = random.sample(set(list(range(restrict))) - set(scraped_pg), 1)[0]
            time.sleep(random.uniform(1, 2))
            rating_page = requests.get('https://letterboxd.com/film/{}/members/page/{}/'.format(
                movie,str(random_pg)))
            if rating_page.status_code == 200:
                scraped_pg.append(random_pg)
                rating_page_html = bs(rating_page.text, "html.parser")
                profiles = [x.find('a').get('href') for x in
                            rating_page_html.find_all(class_="table-person")]
                hashes_prf = []
                for profile in profiles:
                    profiles_movie['profile_url'].append(profile)
                    m = hashlib.md5()
                    m.update(profile.encode('utf-8'))
                    profiles_movie['user_id'].append(m.hexdigest())
                    hashes_prf.append(m.hexdigest())
            else:
                print('Setting new restriction: {}'.format(str(random_pg)))
                restrict = random_pg
                pass
            elapsed_time_min = (time.time() - start_time) / 60
            if elapsed_time_min > 5:
                print("\nWasn't able to scrape 30 pages in 5 minutes, scraping next movie...")
                break
        to_add = list(set(list(profiles_movie['user_id'])) - profiles_db)
        added_total += len(to_add)
        profiles_db.update(set(list(profiles_movie['user_id'])))
        movie_prf_df = pd.DataFrame(profiles_movie)
        movie_prf_df[['dead','selected']] = 0
        movie_prf_df['updated'] = pd.to_datetime('1970-01-01')
        try:
            movie_prf_df[movie_prf_df.user_id.isin(to_add)].drop_duplicates().to_sql("users", engine,index=False,
                                                                   if_exists='append')
        except:
            print('ERROR')
            return movie_prf_df

        i += 1
        if i % 25 == 0:
            rotate_VPN()
    print('Done!')
    print('Scraped {} profiles\n------------------------\n'.format(str(added_total)))
    return movie_prf_df
def scrape_profiles(engine,update=0):
    if update == 1:
        add_query = 'AND selected = 1'
    elif update == 0:
        add_query = "AND updated < '{}'".format(dt.now().date())
    else:
        raise Exception('Only 0 or 1 are allowed for the update argument')
    users = pd.read_sql("SELECT user_id,profile_url FROM users "
                        "WHERE dead = 0 {}".format(add_query),
                        engine).sample(frac=1).reset_index(drop=True)
    i = 0
    for index,row in tqdm.tqdm(users.iterrows()):
        time.sleep(random.uniform(2,6))
        ratings_db = set(pd.read_sql("SELECT film_id FROM ratings "
                                     "WHERE user_id = '{}'".format(row['user_id']),engine).iloc[:,0])
        user_watched = {key: [] for key in ['film_id','rating','love']}
        page,filter_df = 0,0
        while True:
            try:
                page += 1
                openpage = requests.get('https://letterboxd.com{}films/page/'
                                        '{}/'.format(row['profile_url'],str(page)),
                                        headers=set_headers(user_agent_rotator))
            except:
                if page == 1:
                    with engine.begin() as conn:
                        conn.execute("UPDATE users SET dead = 1 WHERE "
                                     "user_id = '{}';".format(row['user_id']))
                else:
                    break
            else:
                loadpage = bs(openpage.text, "html.parser")
                films_page = loadpage.find_all('li', {"class":"poster-container"})
                if len(films_page) > 0:
                    for film in films_page:
                            div_obj = film.find('div')
                            user_watched['film_id'].append(div_obj.get("data-target-link").split('film/')[1][:-1])
                            rating = film.select('span[class*="rating"]')
                            if len(rating) == 1:
                                stars = [x.count("\u2605") for x in rating[0].get_text()]
                                half_stars = [0.5 if rating[0].get_text().count("\u00bd") == 1
                                              else 0]
                            user_watched['rating'].append([np.nan if len(rating) == 0
                                                           else round(sum(stars+half_stars),1)][0])
                            love = film.select('span[class*="icon-liked"]')
                            user_watched['love'].append([0 if len(love) == 0 else 1][0])
                    time.sleep(random.uniform(3,5))
                    if len(ratings_db.intersection(set(user_watched['film_id']))) > 0:
                        filter_df = 1
                        break
                else:
                    break
        if len(user_watched['film_id']) > 0:
            rating_user = pd.DataFrame(user_watched)
            rating_user['user_id'] = row['user_id']
            if filter_df == 1:
                rating_user = rating_user[~(rating_user.film_id.isin(ratings_db))]
            with engine.begin() as conn:
                conn.execute("UPDATE users SET selected = 1,updated = '{}' WHERE "
                             "user_id = '{}';".format(dt.now().date(),row['user_id']))
            try:
                rating_user = rating_user.sort_values(['film_id','rating'],ascending=False).groupby('film_id').head(1)
                rating_user.to_sql("ratings", engine, index=False,if_exists='append')
            except:
                return row['user id']
        i += 1
        if i % 25 == 0:
            rotate_VPN()
def imdb_db_setup(engine):
    files = ['title.basics','title.crew','name.basics']
    for title_df in files:
        url = 'https://datasets.imdbws.com/{}.tsv.gz'.format(title_df)
        out_file = "{}.tsv".format(title_df)
        db_table = "imdb_{}".format(re.sub('\.', '_', title_df))
        id_column = ['nconst' if title_df == 'name.basics' else 'tconst'][0]
        #Check records already available
        if sqlalchemy.inspect(engine).dialect.has_table(engine.connect(), db_table) is True and title_df != 'title.crew':
            print('Checking existing ids in database for {}...'.format(title_df))
            ids_indb = [x[0] for x in engine.connect().execute(
                select(table(db_table,column(id_column)))).fetchall()]
            print('Found {} records'.format(str(len(ids_indb))))
        #Download archive
        try:
            print('Downloading {}'.format(title_df))
            #Read the file inside the .gz archive located at url
            with urllib.request.urlopen(url) as response:
                with gzip.GzipFile(fileobj=response) as uncompressed:
                    file_content = uncompressed.read()
            #Write to file in binary mode 'wb'
            with open(out_file, 'wb') as f:
                f.write(file_content)
            file = pd.read_csv(out_file, sep='\t',na_values='\\N', iterator=True,
                               chunksize=100000, low_memory=False)
            uploaded_size = 0
            print('Uploading...')
            for chunk in file:
                chunk = chunk[~(chunk[id_column].isin(ids_indb))]
                uploaded_size += len(chunk)
                if len(chunk) > 0:
                    if title_df == 'title.basics':
                        chunk.to_sql(db_table, engine,
                                     index=False, if_exists='append',
                                     dtype={"runtimeMinutes": String(9999)})
                    else:
                        chunk.to_sql(db_table, engine,
                                     index=False, if_exists='append')
            print('Uploaded {} new records'.format(uploaded_size))
            print('Done!\n----------')
            del file
            os.remove(out_file)
            if title_df == 'title.crew':
                del ids_indb
        except Exception as e:
            message = e
            print(e)
            break
    message = 'IMDB tables up to date'
    return message
def imdb_link(engine):
    links_null = pd.read_sql("SELECT DISTINCT(ra.film_id)"
                             " FROM letterboxd_algo.ratings ra"
                             " LEFT JOIN letterboxd_algo.letterboxd_imdb li"
                             " ON li.film_id = ra.film_id"
                             " WHERE li.film_id IS NULL;",engine)
    i= 0
    for link in tqdm.tqdm(links_null['film_id']):
        time.sleep(random.uniform(2,6))
        imdb_link = None
        dead = 0
        try:
            loadpage = bs(requests.get('https://letterboxd.com/film/{}/'.format(link),
                                       headers=set_headers(user_agent_rotator)).text, 'html.parser')
        except:
            dead = 1
        else:
            try:
                imdb_link = [re.search('title/(.*?)/',x['href']).group(1) for x
                             in loadpage.find_all(href=True) if 'imdb.com/title/' in x['href']][0]
            except:
                pass
        with engine.begin() as conn:
            conn.execute("INSERT INTO letterboxd_imdb(film_id,tconst,dead,updated)"
                         " VALUES ('{}','{}','{}','{}');".format(link,imdb_link,dead,dt.now().strftime('%Y-%m-%d %H:%M:%S')))
        i += 1
        if i % 100 == 0:
            rotate_VPN()

####################
####collect data####
####################

engine,metadata = set_connection('settings/connection_db.txt','settings/queries_db.csv')
#update_imdb_db = imdb_db_setup(engine)
#profiles = fetch_profiles('https://letterboxd.com/prof_ratigan/list/top-1000-films-of-all-time/',
#               engine,metadata)
#scrape_profiles(engine,update=1)
imdb_link(engine)