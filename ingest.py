import sqlite3
import pandas as pd
def ingest_filesystem(db_path, data_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create a table for original data if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS original_data (
            "Invoice ID" TEXT,
            "Branch" TEXT,
            "City" TEXT,
            "Customer type" TEXT,
            "Gender" TEXT,
            "Product line" TEXT,
            "Unit price" TEXT,
            "Quantity" TEXT,
            "Tax 5%" TEXT,
            "Total" TEXT,
            "Date" TEXT,
            "Time" TEXT,
            "Payment" TEXT,
            "cogs" TEXT,
            "gross margin percentage" TEXT,
            "gross income" TEXT,
            "Rating" TEXT
        )
    ''')

    # Insert data into the original_data table
    df = pd.read_csv(data_path)
    data = df.values.tolist()

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