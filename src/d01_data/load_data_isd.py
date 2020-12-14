import datetime
import warnings
from pathlib import Path
import urllib.request
import pandas as pd
import os


warnings.filterwarnings('ignore')


class GetIsdData:
    """
    Contain functions to gather data from ISD and organise the files in directories
    """

    def __init__(self, station_icao):
        self.station_icao = station_icao  # 'SBGR'
        self.end_year = datetime.datetime.today().year - 1
        self.start_year = datetime.datetime.today().year - 11

    def download_isd_data(self):
        """
        Creates the link to download ISD files as well as the directories to put the files
        """
        isd_station = pd.read_csv(
            f'data/00_external/isd_all_stations.csv', index_col=False)
        station_isd = isd_station[isd_station['ICAO']
                                  == self.station_icao]['CODE'].values[0]
        print(f'Downloading {self.station_icao} data')
        for year in range(self.start_year, self.end_year, 1):
            url = f'https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{station_isd}.csv'
            Path(
                f'data/01_raw/isd/{self.station_icao}').mkdir(parents=True, exist_ok=True)
            filename = f'data/01_raw/isd/{self.station_icao}/{year}.csv'
            # Only download if file does not exist
            if not os.path.exists(filename):
                try:
                    urllib.request.urlretrieve(url, filename)
                except urllib.error.HTTPError as exception:
                    print(f'Unfortunately there is no {year} data available'
                          f' for {self.station_icao}: Error {exception.code}')
                    continue
        print('Download complete')
