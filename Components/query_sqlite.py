import pandas as pd
import sqlite3 

# Create fuction to insert the db name and its path and read the data from each table of the data warehouse
def show_data(table,db_path):
    try:
        # Read the data from the data warehouse
        df = pd.read_sql_query("""SELECT * FROM {}""".format(table), sqlite3.connect(db_path))
    except Exception as e:
        print('Error',e)
    return df

        
