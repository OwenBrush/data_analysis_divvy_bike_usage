SAVE_DIR = 'data/'
SERVER = 'https://divvy-tripdata.s3.amazonaws.com/'
FILE_NAME = '-divvy-tripdata'
DELAY = 5

from zipfile import ZipFile
import os
import requests
from io import BytesIO
import time
from IPython.display import clear_output

from urllib3 import Retry


def download_data(save_dir:str=SAVE_DIR, start_date:int=202006, n_months=24, retries = 3) -> None:
    """
    Checks save directory for apropriate csv files and if they are not present, downloads them.
    """ 
    def _downloand_and_extract(file, delay=DELAY):
        try:
            response = requests.get(SERVER+file+'.zip', stream=True)
            zip = ZipFile(BytesIO(response.content))
            zip.extractall(path=SAVE_DIR)
        except requests.exceptions.RequestException as e: 
            raise e
        time.sleep(delay)

    def _download_all(date) -> list:
        failed_attempts = []
        for i in range(0,n_months):
            month = (int(str(date)[-2:]) +i) 
            year = int(str(date)[:4]) + int((month-1) / 12)
            iter_date = str(year) + str((month-1) % 12 + 1).zfill(2)
            file = str(iter_date)+FILE_NAME
            if not os.path.exists(SAVE_DIR+file+'.csv'):
                print(f'{i+1}/{n_months} : Downloading {file+".csv"}')
                try:
                    _downloand_and_extract(file)
                except:
                    failed_attempts.append(file)
                    print(f'Failed to download {SERVER+file+".zip"}')
                    time.sleep(DELAY)
                clear_output(wait=True)

        return failed_attempts

    def _retry_failed_downloads(files:list) -> list:
        for file in files.copy():
            for i in range (0, retries):
                if not os.path.exists(SAVE_DIR+file+'.csv'):
                    try:
                        print(f'Re-attemping to download {SERVER+file+".zip"}, attempt #{i+1}')
                        _downloand_and_extract(file, delay=DELAY*2)
                        files.remove(file)
                    except:
                        pass
        return files
            
    failed_attempts = _download_all(start_date)
    failed_attempts = _retry_failed_downloads(failed_attempts)

    clear_output(wait=True)
    if len(failed_attempts) == 0:
        print('All files downloaded successfully')
    else:
        print(f'Unable to download: {failed_attempts}') 




            



