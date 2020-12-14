import warnings
import pandas as pd
import os
import sys
src_dir = os.path.join(os.getcwd(), '..', 'src')
sys.path.append(src_dir)

from d01_data.load_data_inmet import GetInmetData

warnings.filterwarnings('ignore')


class DataINMET:
    """
    Contains functions to load the raw downloaded files and extract information from them
    """

    def __init__(self, station):
        self.station = station

    def create(self):
        GetInmetData(self.station).download_inmet_data()
        data = self.process_files()
        return data

    def process_files(self):
        """
        Takes all the raw downloaded files and unites them into one file
        """
        csv_path = f'data/01_raw/inmet/{self.station}/csv'
        csv_list = os.listdir(path=csv_path)

        grouped = []
        for file in sorted(csv_list):
            # DATE column is used as index
            inmet_data = pd.read_csv(f'{csv_path}/{file}',
                                     sep=';',
                                     skiprows=8,
                                     encoding='latin_1')
            inmet_data = inmet_data.drop(inmet_data.columns[-1], axis=1)
            inmet_data = inmet_data[['DATA (YYYY-MM-DD)', 'HORA (UTC)',
                                     'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)', 'RADIACAO GLOBAL (KJ/m²)']]
            inmet_data.columns = ['DATE', 'time',
                                  'precipitation', 'radiation']
            inmet_data['DATE'] = inmet_data['DATE'].astype(
                str) + ' ' + inmet_data['time'].apply(lambda x: str(x).zfill(4))
            inmet_data.index = pd.to_datetime(inmet_data['DATE'])
            inmet_data = inmet_data[['precipitation', 'radiation']]
            grouped.append(inmet_data)
        # Stores all data data into a dataframe
        data = pd.concat(grouped, sort=False)
        data = data.apply(lambda x: x.str.replace(
            ',', '.').replace('-9999', 0))
        data.to_csv(
            f'data/02_intermediate/{self.station}_inmet_data.csv', index=False)
        print('Done!')
        return data
