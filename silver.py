import sqlite3
import pandas as pd
stage = "silver"
def silver_filesystem(db_path, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create a table if it doesn't exist
    with open(f'sqls/create/{stage}_{table_name}.sql', 'r') as f:
        create_table = f.read()
    cursor.execute(create_table)
    
    with open(f'sqls/transform/{stage}_{table_name}.sql', 'r') as f:
        transform_table = f.read()
    # Insert data into the original_data table

    cursor.executemany('''
        INSERT INTO original_data 
        ("Invoice ID", "Branch", "City", "Customer type", "Gender", "Product line", 
         "Unit price", "Quantity", "Tax 5%", "Total", "Date", "Time", "Payment", 
         "cogs", "gross margin percentage", "gross income", "Rating") 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    

    return True