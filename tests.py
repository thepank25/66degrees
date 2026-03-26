import sqlite3

def test_upload(db_path, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create the original_data table if it doesn't exist
    
    data =cursor.execute(f"SELECT count(*) FROM {table_name}")
    print(data.fetchone()[0])     
    conn.commit()
    conn.close()
    return 
test_upload("data.db", "original_data")