import os
import sys
src_dir = os.path.join(os.getcwd(), '..', 'src')
sys.path.append(src_dir)
from d03_processing.create_master_table import Features
from d02_intermediate.create_data_inmet import DataINMET
from d02_intermediate.create_data_isd import DataISD


data_isd = DataISD('SBGR').create()
data_inmet = DataINMET('SAO PAULO - MIRANTE').create()

features = Features(data_isd, data_inmet).create()
print(features)
