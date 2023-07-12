import pandas as pd
import sqlite3 
import os
import traceback
import datetime as dt

# Fuction to insert data into the data warehouse
def insert_data(df_file,db_name,db_path):
    try:
        if os.path.exists(os.path.join(db_path, db_name)):
            # Check if the tables of the data warehouse are not empty
            conn = sqlite3.connect(os.path.join(db_path, db_name))
            cursor = conn.cursor()
            if pd.read_sql("""SELECT * FROM Fct_Ventas""", conn).empty:
                # Convert negative values to positive of num_documento_cliente
                df_file['num_documento_cliente'] =  df_file['num_documento_cliente'].apply(lambda x: abs(x))
                
                # Dim_Cliente
                df_dim_cliente= df_file[['num_documento_cliente']].drop_duplicates()
                df_dim_cliente['tipo_documento_cliente'] = df_dim_cliente['num_documento_cliente'].apply(lambda x: df_file[df_file['num_documento_cliente']==x]['tipo_documento_cliente'].iloc[0])
                df_dim_cliente = df_dim_cliente.sort_values(by=['num_documento_cliente'])   
                for index, row in df_dim_cliente.iterrows():
                    cursor.execute("""INSERT INTO Dim_Cliente (num_documento_cliente,tipo_documento_cliente) VALUES (?,?)""",(row['num_documento_cliente'],row['tipo_documento_cliente']))
                    
                # Dim_Tienda 
                df_dim_tienda = df_file[['codigo_tienda']].drop_duplicates()
                df_dim_tienda['tipo_tienda'] = df_dim_tienda['codigo_tienda'].apply(lambda x: df_file[df_file['codigo_tienda']==x]['tipo_tienda'].iloc[0])
                df_dim_tienda['latitud_tienda'] = df_dim_tienda['codigo_tienda'].apply(lambda x: df_file[df_file['codigo_tienda']==x]['latitud_tienda'].iloc[0])
                df_dim_tienda['longitud_tienda'] = df_dim_tienda['codigo_tienda'].apply(lambda x: df_file[df_file['codigo_tienda']==x]['longitud_tienda'].iloc[0])
                df_dim_tienda = df_dim_tienda.sort_values(by=['codigo_tienda'])
                for index, row in df_dim_tienda.iterrows():
                    cursor.execute("""INSERT INTO Dim_Tienda (codigo_tienda,tipo_tienda,latitud_tienda,longitud_tienda) VALUES (?,?,?,?)""",(row['codigo_tienda'],row['tipo_tienda'],row['latitud_tienda'],row['longitud_tienda']))
            
                # Dim_Fecha
                df_dim_fecha = df_file[['fecha_compra']].drop_duplicates()
                df_dim_fecha['year'] = df_dim_fecha['fecha_compra'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').year)
                df_dim_fecha['month'] = df_dim_fecha['fecha_compra'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').month)
                df_dim_fecha['day'] = df_dim_fecha['fecha_compra'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').day)
                df_dim_fecha['hour'] = df_dim_fecha['fecha_compra'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').hour)
                df_dim_fecha = df_dim_fecha.sort_values(by=['fecha_compra'])
                df_dim_fecha = df_dim_fecha.drop_duplicates()
                for index, row in df_dim_fecha.iterrows():
                    cursor.execute("""INSERT INTO Dim_Fecha (fecha_compra,year,month,day,hour) VALUES (?,?,?,?,?)""",(row['fecha_compra'],row['year'],row['month'],row['day'],row['hour']))
                    
                # Dim_Barrio
                df_dim_barrio = df_file[['id_barrio']].drop_duplicates()
                df_dim_barrio['nombre_barrio'] = df_dim_barrio['id_barrio'].apply(lambda x: df_file[df_file['id_barrio']==x]['nombre_barrio'].iloc[0])
                df_dim_barrio = df_dim_barrio.sort_values(by=['id_barrio'])
                for index, row in df_dim_barrio.iterrows():
                    cursor.execute("""INSERT INTO Dim_Barrio (id_barrio,nombre_barrio) VALUES (?,?)""",(row['id_barrio'],row['nombre_barrio']))
                    
                # Fct_Ventas
                df_file = df_file.sort_values(by=['fecha_compra'])
                for index, row in df_file.iterrows():
                    cursor.execute("""INSERT INTO Fct_Ventas (id_venta,num_documento_cliente,codigo_tienda,id_barrio,fecha_compra,total_compra) VALUES (?,?,?,?,?,?)""",(index+1,row['num_documento_cliente'],row['codigo_tienda'],row['id_barrio'],row['fecha_compra'],row['total_compra']))
                    
                # Close connection
                conn.commit()
                conn.close()
    except Exception as e:
        print('Error: ',e)
        print(traceback.format_exc())
    return None