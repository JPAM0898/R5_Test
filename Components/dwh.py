import pandas as pd
import sqlite3
import os
import traceback

# Fuction to create the Data Warehouse and save it in the DB folder
def create_dwh(db_name,db_path):
    try:
        # Create connection to sqlite3 
        conn = sqlite3.connect(os.path.join(db_path,db_name)) 
        cursor = conn.cursor()
        # Creation of tables
        cursor.execute("""CREATE TABLE IF NOT EXISTS Dim_Cliente(
            num_documento_cliente INTEGER PRIMARY KEY,
            tipo_documento_cliente INTEGER)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Dim_Tienda(
            codigo_tienda INTEGER PRIMARY KEY,
            tipo_tienda TEXT,
            latitud_tienda REAL,
            longitud_tienda REAL)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Dim_Fecha(
            fecha_compra TEXT PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour TEXT)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Dim_Barrio(
            id_barrio INTEGER PRIMARY KEY,
            nombre_barrio TEXT)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Fct_Ventas(
            id_venta INTEGER PRIMARY KEY,
            num_documento_cliente INTEGER,
            codigo_tienda INTEGER,
            id_barrio INTEGER,
            fecha_compra TEXT,
            total_compra REAL,
            FOREIGN KEY (num_documento_cliente) REFERENCES Dim_Cliente(num_documento_cliente),
            FOREIGN KEY (codigo_tienda) REFERENCES Dim_Tienda(codigo_tienda),
            FOREIGN KEY (id_barrio) REFERENCES Dim_Barrio(id_barrio),
            FOREIGN KEY (fecha_compra) REFERENCES Dim_Fecha(fecha_compra))""")
        # Close connection
        conn.commit()
        conn.close()
    except Exception as e:
        print('Error: ',e )
        print(traceback.format_exc())
    return None

