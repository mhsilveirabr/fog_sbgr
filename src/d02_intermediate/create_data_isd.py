import warnings
import numpy as np
import re
import pandas as pd
import os
import sys
src_dir = os.path.join(os.getcwd(), '..', 'src')
sys.path.append(src_dir)
from d00_utils.calculations import calculate_rh
from d01_data.load_data_isd import GetIsdData


warnings.filterwarnings('ignore')


class DataISD:
    """
    Contains functions to load the raw downloaded files and extract information from them
    """

    def __init__(self, station_icao):
        self.station_icao = station_icao

    def create(self):
        GetIsdData(self.station_icao).download_isd_data()
        data = self.unify_files()
        print('Extracting data')
        data = self.extract_data(data)
        codes = pd.read_csv('data/00_external/wx_codes.csv',
                            sep=';', index_col=False, dtype={'Code': np.int32})
        codes_dict = codes['Phenomenon'].to_dict()
        data['phenomenon'] = data['phenomenon'].fillna(
            0).astype(int).replace(codes_dict)
        data.to_csv(f'data/02_intermediate/{self.station_icao}_isd_data.csv')
        print('Done!')
        return data

    def unify_files(self):
        """
        Takes all the raw downloaded files and unites them into one file
        """
        csv_list = os.listdir(path=f'data/01_raw/isd/{self.station_icao}')
        grouped = []
        for file in sorted(csv_list):
            # DATE column is used as index
            try:
                df = pd.read_csv(f'data/01_raw/isd/{self.station_icao}/{file}',
                                 index_col='DATE',
                                 error_bad_lines=False,
                                 engine="python")
            except:
                f'{file} data for {self.station_icao} could not be processed.'
                continue
            grouped.append(df)
        # Stores all data data into a dataframe
        data = pd.concat(grouped, sort=False)
        return data

    def get_variable(self, data, column, column_list):
        """
        This function access the raw 'df' (pandas dataframe),
        uses a list with the 'column' name (str) to explore the variable column,
        receives a 'column_list' of the columns names and
        returns a dataframe with the extracted data for the variable indicated.
        """
        variable = pd.DataFrame(data[column], columns=[column])
        variable = variable[column].str.split(',', expand=True)
        variable.columns = column_list
        return variable

    def extract_data(self, data):
        """
        Contains instructions based on ISD documentation to extract data into a usable format
        License: https://www.ncdc.noaa.gov/isd/data-access
        Documentation: https://www.ncei.noaa.gov/data/global-hourly/doc/isd-format-document.pdf
        """
        # Selecting ONLY METAR observations to avoid redundancies
        data = data[data['REPORT_TYPE'].isin(['FM-15', 'FM-16', 'SY-MT'])]

        # Extracting wind data from WND column
        wind_cols = ['direction', 'quality',
                     'type_code', 'speed', 'speed_quality']
        wind = self.get_variable(data, 'WND', wind_cols)

        # Extracting visibility data from VIS column
        visibility_cols = ['visibility', 'quality',
                           'variability', 'quality_variability']
        visibility = self.get_variable(data, 'VIS', visibility_cols)

        # Extracting first group of sigwx data from MW1 column
        phenomenon_cols = ['phenomenon', 'quality']
        phenomenon1 = self.get_variable(data, 'MW1', phenomenon_cols)

        # Extracting sky cover from GA1 columns
        sky_cover_cols = ['coverage', 'quality', 'base_height', 'base_height_quality', 'cloud_type',
                          'cloud_type_quality']
        sky_cover1 = self.get_variable(data, 'GA1', sky_cover_cols)

        # Extracting Ceiling data from CIG column
        ceiling_cols = ['ceiling', 'quality', 'determination_code', 'cavok']
        ceiling = self.get_variable(data, 'CIG', ceiling_cols)

        # Extracting air temperature data from TMP column
        temperature_cols = ['temperature', 'quality']
        temperature = self.get_variable(data, 'TMP', temperature_cols)

        # Extracting dew point temperature data from DEW column
        dew_cols = ['dew', 'quality']
        dew = self.get_variable(data, 'DEW', dew_cols)

        # Concatenating all data into a base df containing the meteorological variables
        base_data = pd.concat([wind[['direction', 'speed']].fillna(99999),
                               visibility[['visibility']].fillna(99999),
                               phenomenon1[['phenomenon']].fillna(99999),
                               sky_cover1[['coverage']].fillna(99999),
                               ceiling[['ceiling']].fillna(99999),
                               temperature[['temperature']].fillna(99999),
                               dew[['dew']].fillna(99999)],
                              axis=1)

        # Note that there were no information on sea level pressure, which will be extracted from REM column
        # Iterating over the METAR messages to extract the slp values
        metar = data['REM'].to_list()
        pressure = []
        for code in metar:
            # the values are put into the list pressure, which will be appended to the final data frame
            try:
                slp = str(re.findall(r"Q\d.+", code))
                pressure.append(slp[3:7])
            except TypeError:
                pressure.append(np.nan)

        # Sea Level Pressure
        # As we extracted the slp values from METAR message using regex,
        # some typos corrupted the data extracted. Let's just ignore them...
        dirty = [str(i) for i in pressure]

        clean = []
        for value in dirty:
            if not value.isdigit():
                clean.append(np.nan)
            else:
                clean.append(value)

        clean = [float(x) for x in clean]
        base_data['slp'] = clean

        base_data.columns = ['direction', 'speed', 'visibility',
                             'phenomenon', 'coverage',
                             'ceiling', 'temperature', 'dew', 'slp']

        # Some corrections in the data...
        # Wind
        # According with the manual, wind direction as 999 can be missing or variable wind.
        # It can be calm too, as seen by the data (comparing them to METAR)...
        # When the wind is calm, let's set them to 0
        base_data['direction'] = base_data['direction'].astype(int)
        base_data['speed'] = base_data['speed'].astype(int)
        base_data['direction'].loc[(base_data['direction'] == 999) & (
            base_data['speed'] == 0)] = 0
        base_data['speed'].loc[(base_data['direction'] == 999) & (
            base_data['speed'] == 0)] = 0

        # When the wind is variable, let's set only the direction to 0
        base_data['direction'].loc[(base_data['direction'] > 360) & (
            base_data['speed'] != 0)] = 0

        # According to the manual, speed_rate seen as 9999 means it is missing.
        # Or it is just a typo at the METAR. Let's just delete them...
        base_data['speed'].loc[base_data['speed'] == 9999] = 0

        # Visibility
        # The manual says visibility values of 999999 means they are missing.
        # If CAVOK is Y, it means the visibility is greater than 10000 meters...
        # Also, values of visibility above 10,000m must not be considered as restrictive to the operations,
        # thus, let's just set them as unlimited...
        # Ignoring non significant visibility values (above 9000)
        base_data['visibility'] = base_data['visibility'].astype(int)
        base_data['visibility'].loc[base_data['visibility'] >= 10000] = 10000

        # Ceiling
        # According to the manual, ceiling regarded as 99999 means it's missing (from the METAR)
        # and 22000 means unlimited...
        base_data['ceiling'] = base_data['ceiling'].astype(int)
        base_data['ceiling'].loc[base_data['ceiling'] >= 1600] = 1600

        # Coverage
        base_data['coverage'] = base_data['coverage'].astype(int).fillna(0)
        base_data['coverage'].replace(to_replace=99999, value=0, inplace=True)

        # Temperature
        # Temperature and dew are scaled by 10, let's downscale them...
        base_data['temperature'] = base_data['temperature'].astype(int) / 10
        base_data['dew'] = base_data['dew'].astype(int) / 10

        # The manual says temperature/dew values above 9999 means they are missing...
        base_data['temperature'] = base_data['temperature'].astype(int)
        base_data['temperature'].loc[base_data['temperature'] > 99] = np.nan
        base_data['dew'] = base_data['dew'].astype(int)
        base_data['dew'].loc[base_data['dew'] > 99] = np.nan

        # Also, values of pressure greater than 1060 and lesser than 900 are absurd.
        # They are probably typos as well so let's get rid of them...
        base_data['slp'][(base_data['slp'] > 1060) |
                         (base_data['slp'] < 900)] = np.nan

        # Correcting data for standard units
        # Wind direction is in degrees, which is fine...
        # Wind Speed is in meters per second and scaled by 10, let's downscale them and convert to knots...
        base_data['speed'] = base_data['speed'].astype(int) * 0.194384
        base_data['speed'] = base_data['speed'].astype(int)

        # Ceiling is in meters, let's set them to feet
        base_data['ceiling'] = np.around(base_data['ceiling'] * 3.28084)

        # Visibility is in meters, which is fine...

        # Pressure is in Hectopascal, which is fine...

        # Create a column for relative humidity using a previously defined function
        base_data['rh'] = np.around(calculate_rh(
            base_data['temperature'], base_data['dew']))

        base_data.replace(to_replace=99999, value=np.nan, inplace=True)

        return base_data
