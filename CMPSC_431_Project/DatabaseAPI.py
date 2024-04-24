import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError
import pandas as pd

def create_database(dbname,user,password,host='127.0.0.1',port = '5432'):
    try :
        conn = psycopg2.connect(
            dbname = dbname,
            user = user,
            password = password,
            host = host,
            port = port
        )

        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute(f"CREATE DATABASE {dbname}")
        
        cur.close()
        conn.close()
    
    except OperationalError as e:
        print(f"Error: {e}")


def pandas_to_postgresql_type(pandas_type):
    type_mapping = {
        'int64': 'INTEGER',
        'int32': 'INTEGER',
        'float64': 'REAL',
        'float32': 'REAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta64[ns]': 'INTERVAL',
        'category': 'TEXT', 
        'object': 'TEXT'  
    }
    return type_mapping.get(str(pandas_type), 'TEXT')  # Default to TEXT if not found

def create_table(conn, table_name, dataframe, primary_key):
    cur = conn.cursor()
    
    
    columns = []
    for col in dataframe.columns:
        data_type = pandas_to_postgresql_type(dataframe[col].dtype)
        columns.append(f"{col.replace(' ', '_')} {data_type}")
    
    
    if primary_key and primary_key not in dataframe.columns:
        columns.append(f"{primary_key} SERIAL PRIMARY KEY")
    
    
    elif primary_key:
        primary_key_type = pandas_to_postgresql_type(dataframe[primary_key].dtype)
        columns.append(f"{primary_key} {primary_key_type} PRIMARY KEY")
    
    
    create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name), sql.SQL(', ').join(map(sql.SQL, columns))
    )
    
    create_table_query += sql.SQL(";")

    
    cur.execute(create_table_query)
    conn.commit()
    for row in dataframe.itertuples(index=False):
        placeholders = ', '.join(['%s' for _ in row])
        insert_query = sql.SQL("INSERT INTO {} VALUES ({})").format(
            sql.Identifier(table_name), sql.SQL(placeholders)
        )
        cur.execute(insert_query, row)
        conn.commit()
    conn.close()


def connect_db(dbname,user,password,host='127.0.0.1',port = '5432'):
    conn = psycopg2.connect(
        dbname = dbname,
        user = user,
        password = password,
        host = host,
        port = port 
    )
    return conn

def disconnect_db(conn):
    conn.close()


    
