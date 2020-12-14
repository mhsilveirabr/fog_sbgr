import urllib.request
import os
import re
from pathlib import Path
import warnings
import datetime
import zipfile
import shutil

warnings.filterwarnings('ignore')


class GetInmetData:
    """
    Contain functions to gather data from INMET and organise the files in directories
    """

    def __init__(self, station):
        self.station = station  # 'SAO PAULO - MIRANTE'
        self.end_year = datetime.datetime.today().year - 1
        self.start_year = datetime.datetime.today().year - 11

    def download_inmet_data(self):
        """
        Creates the link to download ISD files as well as the directories to put the files
        """
        print(f'Downloading {self.station} data')
        for year in range(self.start_year, self.end_year, 1):
            url = f'https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip'
            Path(
                f'data/01_raw/inmet/{self.station}/zip/').mkdir(parents=True, exist_ok=True)
            zip_filename = f'data/01_raw/inmet/{self.station}/zip/{year}.zip'
            # Only download if file does not exist
            if not os.path.exists(zip_filename):
                try:
                    urllib.request.urlretrieve(url, zip_filename)
                except urllib.error.HTTPError as exception:
                    print(f'Unfortunately there is no {year} data available'
                          f' for {self.station}: Error {exception.code}')
                    continue
            Path(
                f'data/01_raw/inmet/{self.station}/csv/').mkdir(parents=True, exist_ok=True)
            csv_filepath = f'data/01_raw/inmet/{self.station}/csv/'
            # # Only unzip if file does not exist
            # if not os.path.exists(_filename):
            with zipfile.ZipFile(zip_filename, 'r') as zip:
                files = zip.namelist()
                regex = re.compile(f'^(?=.*({self.station}))(?!.*box).*$')
                for file in files:
                    if regex.match(file):
                        # Extract a single file from zip
                        # zip.extract(file, csv_filepath)
                        # open the entry so we can copy it
                        member = zip.open(file)

                        with open(os.path.join(csv_filepath, os.path.basename(file)), 'wb') as outfile:
                            # copy it directly to the output directory,
                            # without creating the intermediate directory
                            shutil.copyfileobj(member, outfile)

        print('Download complete')
