import pandas as pd
import sqlite3 
import os
import traceback

def answer_questions(db_name,db_path):
    try:
        print('Answering the questions...')
        print('------------------------ Question 1 -------------------------------')
        print('Approach 1: Using SQL')
        # First: ¿Cuáles son las tiendas con compras de al menos 100 clientes diferentes?
        conn = sqlite3.connect(os.path.join(db_path, db_name))
        query_q1_w1 = """SELECT DISTINCT codigo_tienda, COUNT(DISTINCT num_documento_cliente) AS num_clientes
        FROM Fct_Ventas
        GROUP BY codigo_tienda
        HAVING num_clientes >= 100
        ORDER BY num_clientes DESC"""
        df_q1_w1 = pd.read_sql_query(query_q1_w1, conn)
        print(df_q1_w1)
        print('Approach 2: Using SQL + Pandas')
        query_q1_w2 = """SELECT num_documento_cliente, codigo_tienda FROM Fct_Ventas"""
        df_q1_w2 = pd.read_sql_query(query_q1_w2, conn)
        df_q1_w2 = df_q1_w2.groupby('codigo_tienda').agg({'num_documento_cliente': 'nunique'}).reset_index()
        df_q1_w2.rename(columns={'num_documento_cliente': 'num_clientes'}, inplace=True)
        df_q1_w2 = df_q1_w2[df_q1_w2['num_clientes'] >= 100].sort_values(by=['num_clientes'], ascending=False).reset_index(drop=True)
        print(df_q1_w2)
        print('------------------------ Question 2 -------------------------------')
        # Second: ¿Cuáles son los 5 barrios donde la mayor cantidad de clientes únicos realizan compras en tiendas tipo 'Tienda Regional'?
        print('Approach 1: Using SQL')
        query_q2_w1 = """SELECT DISTINCT id_barrio, COUNT(DISTINCT num_documento_cliente) AS num_clientes
        FROM Fct_Ventas
        LEFT JOIN Dim_Tienda AS t
        ON Fct_Ventas.codigo_tienda = t.codigo_tienda
        WHERE t.tipo_tienda = 'Tienda Regional'
        GROUP BY id_barrio
        ORDER BY num_clientes DESC
        LIMIT 5"""
        df_q2_w1 = pd.read_sql_query(query_q2_w1, conn)
        print(df_q2_w1)
        print('Approach 2: Using SQL + Pandas')
        query_q2_w2 = """SELECT num_documento_cliente, id_barrio FROM Fct_Ventas
        LEFT JOIN Dim_Tienda AS t
        ON Fct_Ventas.codigo_tienda = t.codigo_tienda
        WHERE t.tipo_tienda = 'Tienda Regional'"""
        df_q2_w2 = pd.read_sql_query(query_q2_w2, conn)
        df_q2_w2 = df_q2_w2.groupby('id_barrio').agg({'num_documento_cliente': 'nunique'}).reset_index()
        df_q2_w2.rename(columns={'num_documento_cliente': 'num_clientes'}, inplace=True)
        df_q2_w2 = df_q2_w2.sort_values(by=['num_clientes'], ascending=False).reset_index(drop=True).head(5)
        print(df_q2_w2)
        conn.close()
    except Exception as e:
        print('Error: {}'.format(e))
        print(traceback.format_exc())
    return df_q1_w1, df_q2_w1
