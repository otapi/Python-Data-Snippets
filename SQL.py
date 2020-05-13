import pandas as pd
import pyodbc
import logging

# Code snippets to ETL data with pandas and SQL

DBserver = ""
DBdatabase = ""
DBtable = ""

# Connect
logging.info(f"Connect to SQL Server 2017: {DBserver}, database: {DBtable}")
sql_conn = pyodbc.connect(driver='{SQL Server}', server=DBserver, database=DBdatabase,               
            trusted_connection='yes')

# Load
logging.info(f"Load the table: {DBtable}")
query = f""" SELECT *
            FROM [{DBTable}]"""
df = pd.read_sql(query, sql_conn)

# ETL - Run an SQL command
logging.info(f"Delete All records {DBtable}")
cursor = sql_conn.cursor()
cursor.execute(f"""DELETE FROM [{DBtable}]""")
sql_conn.commit()
cursor.close()

# Export from pandas dataframe into SQL
dfcount = len(df.index)
logging.info(f"Insert {dfcount} records into DB")
cursor = sql_conn.cursor()
for index,row in df.iterrows():
    progress(index, dfcount)
    cursor.execute(f"""INSERT INTO [{DBtable}](
            [Field1],
            [Field2]
        ) values (?,?)""", 
            row['Field1'],
            row['Field2']
        )
sql_conn.commit()
cursor.close()

# Closing connection
sql_conn.close()
logging.info("Completed")
