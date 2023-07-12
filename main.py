# Description: Main file to create the data warehouse and execute the ETL process.

import os
import pandas as pd
from Components import dwh
from Components import etl

# Define paths
DATA_PATH = os.path.join(os.path.dirname(__file__), 'Input_Data')
DB_PATH = os.path.join(os.path.dirname(__file__), 'DB')

# Variables
db_name = 'R5_Test_DWH.db'

# Execution
if __name__ == '__main__':
    # Read the data from the csv file
    df_R5_inputDS = pd.read_csv(os.path.join(DATA_PATH, 'R5_Test_Dataset_Input.csv'), sep=',',date_format='%d/%m/%Y')
    # Create the data warehouse
    dwh.create_dwh(db_name,DB_PATH)
    # Insert the data into the data warehouse
    etl.insert_data(df_R5_inputDS,db_name, DB_PATH)