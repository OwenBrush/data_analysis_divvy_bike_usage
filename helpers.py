
from xmlrpc.client import DateTime
from zipfile import ZipFile
import os
import requests
from io import BytesIO
import time
from IPython.display import clear_output
import glob
import pandas as pd
from datetime import datetime
from meteostat import Point, Daily

START_DATE = datetime(2020, 6, 1)
END_DATE = datetime(2022, 5, 31)
CHICAGO = Point(41.881832, -87.62317, 182)
BIKE_SAVE = 'data/bike/'
COVID_SAVE = 'data/covid/covid_data.csv'
WEATHER_SAVE = 'data/weather/weather_data.csv'
SERVER = 'https://divvy-tripdata.s3.amazonaws.com/'
FILE_NAME = '-divvy-tripdata'
DELAY = 5

def download_bike_data(start_date:DateTime=START_DATE, end_date:DateTime=END_DATE) -> None:
    """
    Checks save directory for apropriate csv files and if they are not present, downloads them.
    """ 
    def _downloand_and_extract(file, delay=DELAY) -> None:
        try:
            response = requests.get(SERVER+file+'.zip', stream=True)
            zip = ZipFile(BytesIO(response.content))
            zip.extractall(path=BIKE_SAVE)
        except requests.exceptions.RequestException as e: 
            raise e
        time.sleep(delay)

    def _download_all() -> list:
        failed_attempts = []
        dates = pd.date_range(start_date, end_date, freq='1M') 
        for i, date in enumerate(dates):
            iter_date = str(date.year)+str(date.month).zfill(2)
            file = iter_date+FILE_NAME
            if not os.path.exists(BIKE_SAVE+file+'.csv'):
                print(f'{i+1}/{len(dates)} : Downloading {file+".csv"}')
                try:
                    _downloand_and_extract(file)
                except:
                    failed_attempts.append(file)
                    print(f'Failed to download {SERVER+file+".zip"}')
                    time.sleep(DELAY)
                clear_output(wait=True)

        return failed_attempts

    def _retry_failed_downloads(files:list, retries:int=3) -> list:
        for file in files.copy():
            for i in range (0, retries):
                if not os.path.exists(BIKE_SAVE+file+'.csv'):
                    try:
                        print(f'Re-attemping to download {SERVER+file+".zip"}, attempt #{i+1}')
                        _downloand_and_extract(file, delay=DELAY*2)
                        files.remove(file)
                    except:
                        pass
        return files
            
    failed_attempts = _download_all()
    failed_attempts = _retry_failed_downloads(failed_attempts)

    clear_output(wait=True)
    if len(failed_attempts) == 0:
        print('All divvy bike files downloaded successfully')
    else:
        print(f'Unable to download: {failed_attempts}') 
        
def download_covid_data():
    try:
        data = pd.read_csv('https://data.cityofchicago.org/api/views/naz8-j4nc/rows.csv?accessType=DOWNLOAD')
        data.to_csv(COVID_SAVE)
        print('Covid data downloaded succesfully')
    except requests.exceptions.RequestException as e:
        print('Unable to download covid data') 
        raise e

def download_weather_data(start:DateTime=START_DATE, end:DateTime=END_DATE, location:Point=CHICAGO):
    try:
        data = Daily(location, start, end)
        data = data.fetch()
        data.to_csv(WEATHER_SAVE)
        print('Weather data downloaded succesfully')
    except:
        print('Unable to download weather data')

def assemble_bike_data(save_dir:str=BIKE_SAVE) -> pd.DataFrame:
    """
    Returns DataFrame containing all csv files in given directory"""
    files = glob.glob(save_dir + '/*.csv')
    files.sort()
    return pd.concat(map(pd.read_csv, files), ignore_index=True)



            


