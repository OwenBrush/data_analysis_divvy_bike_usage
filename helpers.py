SAVE_DIR = 'data/'
SERVER = 'https://divvy-tripdata.s3.amazonaws.com/'
FILE_NAME = '-divvy-tripdata'

from zipfile import ZipFile
import os
import requests
from io import BytesIO
import time


def download_data(save_dir:str=SAVE_DIR, years:list=[2020,2021,2022], start_month:int=6, end_month:int=5, retries = 3) -> None:
    """
    Checks save directory for apropriate csv files and if they are not present, downloads them.
    """
    
    def downloand_and_extract(file, delay=3):
        print(f'Downloading {file+".csv"}')
        response = requests.get(SERVER+file+'.zip', stream=True)
        if response:
            zip = ZipFile(BytesIO(response.content))
            zip.extractall(path=SAVE_DIR)
        else:
            print(f'Did not find"{file+".zip"}" at {SERVER}')
        time.sleep(delay)

    month = start_month
    failed_attemps = []



    for y in years:

        if y == years[-1]:
            year_end = end_month
        else:
            year_end = 12
            
        while month < year_end+1:
            file = str(y)+str(month).zfill(2)+FILE_NAME
            if not os.path.exists(SAVE_DIR+file+'.csv'):
                try:
                    downloand_and_extract(file)
                except:
                    print(f'Failed to download {SERVER+file+".zip"}')
                    failed_attemps.append(file)
            month += 1
        month = 1

    for file in failed_attemps:
        for i in range(0,retries):
            if not os.path.exists(SAVE_DIR+file+'.csv'):
                try:
                    print(f'Re-attemping to download {SERVER+file+".zip"}')
                    downloand_and_extract(file, delay=10)
                except:
                    failed_attemps.append(file)
                if i == retries-1:
                    print(f'DID NOT SUCCEED IN DOWNLOADING {SERVER+file+".zip"}')

            



