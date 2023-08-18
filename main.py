# Import required libraries
import pandas as pd
import pyodbc

# Database configuration
driver = 'Microsoft Access Driver (*.mdb, *.accdb)'
server = ''
database = ''
username = ''
password = ''

# Define connection strings for Pyodbc
conn_str2 = (
    f"Driver={{{driver}}};"
    f"DBQ={server}/{database};"
    f"UID={username};"
    f"PWD={password};"
)

# Get data from SSMS using Pyodbc
combined_query = """
    -- Write your SQL query here
"""

connection = pyodbc.connect(conn_str2)
cursor = connection.cursor()
try:
    cursor.execute(combined_query)
    data_to_insert = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
except Exception as e:
    print("Hata:", e)
finally:
    connection.close()

# Establish a connection using Pyodbc
with pyodbc.connect(conn_str2) as connection:
    schema_name = 'your_schema_name'
    table_name = 'your_table_name'
    
    # Join the columns into a comma-separated string
    columns = ', '.join(data_to_insert.columns)
    
    # Create placeholders for the values in the query
    placeholders = ', '.join(['?' for _ in data_to_insert.columns])
    
    # Construct the INSERT query
    query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
    
    # Create a cursor to execute SQL commands
    cursor = connection.cursor()
    
    # Begin a transaction
    connection.autocommit = False
    
    try:
        # Iterate through the rows of the DataFrame and execute the query for each row
        for row in data_to_insert.itertuples(index=False):
            # Create a tuple of values for the query, converting None values
            values = tuple(None if pd.isna(value) else value for value in row)
            cursor.execute(query, values)
        
        # Commit the transaction
        connection.commit()
    
    except Exception as e:
        # Rollback the transaction in case of an error
        connection.rollback()
        print("Error:", e)
    
    finally:
        # Restore autocommit and close the cursor
        connection.autocommit = True
        cursor.close()

