# snowflake_utils.py

from datetime import datetime
import pandas as pd
import snowflake.connector

def map_dtype(dtype):
    """Map pandas dtypes to Snowflake data types."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'VARCHAR'

def infer_column_definitions(df):
    """Infer column definitions from DataFrame."""
    columns = []
    for column in df.columns:
        dtype = map_dtype(df[column].dtype)
        columns.append(f"{column} {dtype}")
    
    # Add AUTOINCREMENT and TIMESTAMP columns as needed
    columns[0] += " AUTOINCREMENT"  # Assuming the first column is the ID column
    columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    return columns

def create_snowflake_table(connection_params, columns):
    """Create a table in Snowflake using the given column definitions."""
    conn = snowflake.connector.connect(**connection_params)
    cur = conn.cursor()
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {connection_params['schema']}.Countries_by_GDP (
        {','.join(columns)}
    );
    """
    
    try:
        cur.execute(create_table_query)
        print("Table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cur.close()
        conn.close()

def insert_data_to_snowflake(connection_params, df):
    """Insert DataFrame data into Snowflake table."""
    conn = snowflake.connector.connect(**connection_params)
    cur = conn.cursor()
    
    # Insert data row by row
    insert_query = f"""
    INSERT INTO {connection_params['schema']}.Countries_by_GDP ({', '.join(df.columns)})
    VALUES ({', '.join(['%s' for _ in df.columns])});
    """
    
    try:
        # Convert DataFrame to list of tuples
        data = [tuple(x) for x in df.to_numpy()]
        
        cur.executemany(insert_query, data)
        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cur.close()
        conn.close()
