# standard library imports
import datetime as dt
import os
import time

# third-party imports
import pandas as pd
import requests
import pyarrow.parquet as pq

import json
import math

"""
Main functions to use:
    1. steamspy_download_all_page(page_start=0,page_end=61):
            for downloading from steamspy api from the all category
    2. process_batches(app_list, download_folder, file_name, batchsize = 100, pause = 1, batch=1):
            for downloading from steam api
"""

def get_steamspy_request(url,download_folder,parameters=None,page=None):
    """Return json-formatted response of a get request using optional parameters.

    Parameters
    ----------
    url : string
    parameters : {'parameter': 'value'}
        parameters to pass as part of get request
    
    Returns
    -------
    json_data
        json-formatted response (dict-like)
        
    Accepted Requests:
        - appdetails
        - genre
        - tag
        - top100in2weeks
        - top100forever
        - top100owned
        - all
        
    reference: https://steamspy.com/api.php

    23/01/18: No Changes

    """

    try:
        response = requests.get(url=url,params=parameters)
    except SSLError as s:
        print(f'SSL Error:{s}')
        
        for i in range(5,0,-1):
            print(f'\rWaiting... {i}')
            time.sleep(1)
        print('\rRetrying.' + ' '*10)
        
        # recursively try again
        return get_requests(url,parameters)
    
    if response:
        
        #FOR PARAMETRIZATION
        #get the date of when data was extracted
        # date_name = dt.datetime.today().strftime("%Y_%m_%d")
        
        #converted to pandas dataframe, inorder to be saved to parquet
        data_pandas = pd.DataFrame.from_dict(response.json(), orient='index')
        
        #extracted the 'key' from the parameters input
        value1 = next(iter(parameters))
    
        #error handling with column 'score_rank', forced change datatype to str
        data_pandas['score_rank']=data_pandas['score_rank'].astype(str)
        
        #FOR PARAMETRIZATION
        #create the folder for the specified date
        # directory = f'data/SteamSpy/{date_name}'
        os.makedirs(download_folder, exist_ok=True)
        
        #FOR PARAMETRIZATION
        #saved to parquet
        data_pandas.to_parquet(f"{download_folder}/steamspy_{parameters[value1]}_page-{page}.parquet",index=False)
        
        return response.json()
        
    else:
        return None


def steamspy_download_all_page(download_folder,page_start=0,page_end=61):
    """
    
    download multiple pages from the all category of steamspy
    making use of the get_steamspy_request function above

    page_start: starting page for downloading steamspy api (default = 0)
    page_end: last page for downloading steamspi api (default = 61)

    22/12/26: Created to download data from steamspy api
    23/1/04: Completed but still on test - will update this comment if changes will happen
    23/01/18: Added default values to page_start = 0 and page_end = 61

    """
    
    start_time = time.time()

    #steam spy url
    url = "https://steamspy.com/api.php"
    
    for page in range(page_start,page_end+1):
        
        params = {'request':'all','page':page}
        data = get_steamspy_request(url,params,page,download_folder)
        
        if data is None:
            print(f'page {page} is None')
    
    end_time = time.time()
    
    total_time = end_time - start_time
    print(f"elapsed time: {total_time}")


def get_steam_request(appid,name):
    """
    Unique parser to handle data from Steam Store API.
    
    Returns : json formatted data (dict-like)
    
    reference: https://wiki.teamfortress.com/wiki/User:RJackson/StorefrontAPI#appdetails

    23/1/10: error(2): getting nonetype due to reaching API request rate limit
                temp soln: error handling with 'try,except' to wait for a few seconds to retry the code
            will do: process request in batches as shown in steam-data-collection reference
                note: 200 requests per 5 minutes
                    -DONE
    23/01/18: No change
    23/1/24: encountered error (3): (adjusted: get_steam_request)
            JSONDecodeError: Expecting value: line 1 column 1 (char 0)
            walang laman ang json link
            fixed: 23/1/24

    """
    url = "http://store.steampowered.com/api/appdetails/"
    parameters = {'appids':appid}
    
    #retrieve json array from steam api using
    response = requests.get(url=url,params=parameters)
    
    #error(2) handling  
    #error(3) handling (23/01/24)
    try:
        #convert to json format
        json_data = response.json()
        
        json_app_data = json_data[str(appid)]
        
        if json_app_data['success']:
            data = json_app_data['data']
        else:
            data={'name':name,'steam_appid':appid}
    
    #error(2) handling (23/01/10)
    except TypeError as t:
        print(f'TypeError: {t}, exceeded rate limit. Waiting 10 seconds')
        
        time.sleep(10)
        
        # recursively try again
        return get_steam_request(appid,name)
    
    #error(3) handling (23/01/24)
    except ValueError as V:  
        
        print(f'Error {V}: returning name:{name} & steam_appid:{appid}')
        data={'name':name,'steam_appid':appid}

    return data 


def get_steamapi_data(app_list,pause):
    """
    compiles the json data gathered from steamAPI

    Returns : compiled json formatted data (dict-like)

    makes use of function: get_steam_request
    
    23/1/18: No Change

    """
    
    app_data=[]
    
    for index, row in app_list.iterrows():
        
        appid = row['appid']
        name = row['name']

        data = get_steam_request(appid,name)
        
        app_data.append(data)
        
        time.sleep(pause)

    return app_data

def download_steamapi_data(app_data,download_folder,file_name):
    """
    downloads the files created by get_steamapi_data

    23/1/04: Created for downloading data from the steamapi
    error(1): some object type columns cant be converted to parquet. can not mix list and non-list, non-null values
        temp soln: converted all types to str instead of object
    23/1/18: No Change
    """
    
    #convert to pandas dataframe
    steam_api = pd.DataFrame.from_dict(app_data,orient='columns')
    
    steam_api=steam_api.astype(str) #used for handling error(1)
        
    #save to parquet
    steam_api.to_parquet(f"{download_folder}/{file_name}.parquet",index=False) 


def process_batches(app_list, directory, file_name, batchsize = 100, pause = 1, batch_num=1):
    """
    Process app data in batches, writing directly to file

    app_list: app list cotaining appid and name
    directory: location where to store data
    file_name: file name

    Keyword argumenrs:

    batchsize: number of apps to write in each batch (default 100)
    pause: time to wait after each api request (defualt 1)
    batch: batch number to start

    updates:
     23/1/19: changed folder system (adjusted: process_batches)
                instead of new folder everyday, it would be new folder every monday of the week
     23/02/10: changed folder system (adjusted: process_batches)
                instead of new folder every week, it would be new folder every month

    """

    batch_done = pd.DataFrame({'appid':[],'name':[]})
    
    app_list_pd = pd.read_parquet(app_list)
    
    app_list_pyarrow = pq.ParquetFile(app_list)
    total_batches = math.ceil(len(app_list_pyarrow)/batchsize) 
    for i,batch in enumerate(app_list_pyarrow.iter_batches(batch_size = batchsize)):
        
        print(f'Downloading batch {batch_num}, {batchsize} apps')
        
        batch_df = batch.to_pandas() 
        
        os.makedirs(directory, exist_ok=True)
        
        file_batch_name = f'{file_name}_batch-{batch_num}'

        try:     
            app_data = get_steamapi_data(batch_df,pause)
            download_steamapi_data(app_data,directory,file_batch_name)
            batch_done = pd.concat([batch_done,batch_df],ignore_index=True)

            #Log for last batch. Will skip downloading if program is rerun on the same week
            if i==total_batches - 1:
                print(f"Processed last batch: batch {batch_num}")
                remaining_apps(batch_done,batch_num,app_list_pd,directory)


        except KeyboardInterrupt:
            print(f'Interrupted by user on {file_batch_name}')
            print(f'Downloading dataframe with the unfinished apps, use this as the new app_list')        

            remaining_apps(batch_done,batch_num,app_list_pd,directory)

            break

        except Exception as e:
            print(f'{file_batch_name} encountered error: {e}') 
            print(f'Downloading dataframe with the unfinished apps, use this as the new app_list')

            remaining_apps(batch_done,batch_num,app_list_pd,directory)

            raise Exception(e)


            
        batch_num = batch_num + 1


def remaining_apps(batch_done,batch_num,app_list_pd,directory):
    """
    saves the remaining app_list into a new parquet file 

    batch_done: apps that are done downloading, to be dropped from the complete list
    batch_num: batch were the downloaded started, will continue downloading from this batch
    app_list_pd: complete list of apps
    directory: folder location 
    
    """
    drop_rows = app_list_pd[app_list_pd['appid'].isin(batch_done['appid'])].index
    remaining_apps = app_list_pd.drop(drop_rows)
    
    count_remaining_apps = remaining_apps['appid'].count()

    os.makedirs(f'{directory}/remaining_apps', exist_ok=True)
    app_list_folder = f'{directory}/remaining_apps'
    app_list_filename = f'remaining_apps({count_remaining_apps}).parquet'
    
    remaining_apps.to_parquet(f'{app_list_folder}/{app_list_filename}')
    
    prepare_index(app_list_folder,'index.txt',app_list_filename,batch_num)
    
    print(f'saved at: {directory}/remaining_apps/remaining_apps({count_remaining_apps}).parquet')


def prepare_index(index_folder,index_filename,app_list,batch):
    """
    this code writes a new line consisting of the current date and time, the 
    app list path, and a batch number to a file at the rel_path location. If
    the file does not exist, it creates a new file and writes the new line. If 
    the file exists, it appends the new line to the start of the file.

    index_folder: location of index file
    index_filename: name of index file
    app_list: the updated app_list where the download will continue from
    batch: start downloading from this batch
    """

    rel_path = os.path.join(index_folder,index_filename)
    app_list_path = os.path.join(index_folder,app_list)
    date_name = dt.datetime.today().strftime("%y-%m-%d-%X")

    try:
        with open(rel_path, 'r') as f:
            content = f.readlines()
    
        content = [f'{date_name},{app_list_path},{batch}\n'] + content
    
    except FileNotFoundError:
        content =  [f'{date_name},{app_list_path},{batch}\n']
    
    with open(rel_path, 'w') as f:
        f.writelines(content)
        
        
def read_index(index_path, index_filename):
    """
    Extracts the latest batch, and updated app_list location to where the data download will continue

    index_path: path where the index file is location
    index_filename: name of file
    """

    rel_path = os.path.join(index_path, index_filename)
    
    with open(rel_path, 'r') as f:
        contents = f.readline().strip()
        date,folder,batch= contents.split(',')
        batch = int(batch)
        
    return folder,batch


def steamapi_start(folder = "data/steamapi", file_name = 'steamapi'):
    """
    Function that starts the download of SteamAPI data. It can start a new batch of download or start 
    from a batch where the download stopped.
    """

    try:
        app_list,batch_num=read_index(f'{folder}/remaining_apps','index.txt')
        print('Continuing from previous download')

        process_batches(app_list,folder,file_name,batch_num=batch_num)
        
    except FileNotFoundError as E:
        print('Start download')

        os.makedirs(folder, exist_ok=True)
        app_list = f'{folder}/../../../app_list.parquet'
        process_batches(app_list,folder,file_name)

