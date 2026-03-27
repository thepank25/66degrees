import logging
import pandas as pd 
import sqlite3
import os
import json
import kagglehub
logger = logging.getLogger(__name__)

dataset = "lovishbansal123/sales-of-a-supermarket"
path = "./data/raw/sales_of_a_supermarket"
file_name = "supermarket_sales.csv"

# Not used in this notebook, but can be used to load credentials from a JSON file if needed
def Secure_authentication_function(json_path):
    with open(json_path) as f:
        credentials = json.load(f)
    return credentials


def get_data(dataset, output_dir):
    # Download latest version
    path = kagglehub.dataset_download(dataset, output_dir=output_dir)
    print("Path to dataset files:", path)
    
    return os.path.join(output_dir, file_name)
def load_data_from_csv(file_path, table_name):
    data = pd.read_csv(file_path)
    load_count = data.to_sql(table_name, con=sqlite3.connect("./data.db"), if_exists='replace', index=False) 
    return load_count

def transform_data(db_path, table_name, stage):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create table
    with open(f'sqls/create/{stage}_{table_name}.sql', 'r') as f:
        create_table = f.read()        
    cursor.execute(create_table)
    conn.commit()
    logger.info(f"Created {stage} table {table_name}")
    
    with open(f'sqls/transform/{stage}_{table_name}.sql', 'r') as f:
        transform_table = f.read()
    transformed_data = cursor.execute(transform_table)
    conn.commit()
    conn.close()
    return transformed_data

raw_data_path = get_data(dataset, path)
raw_data = load_data_from_csv(raw_data_path, "raw_data")

# Silver layer
silver_layer = ['supermarket']
outputs = {}
for table in silver_layer:
    outputs[table] = transform_data(db_path="./data.db", table_name=table, stage='silver')

# Gold layer
gold_layer = ['fact_sales', 'dim_branch']
for table in gold_layer:
    transform_data(db_path="./data.db", table_name=table, stage='gold')


# Reporting
reporting_layer = ["sales_report"]
for table in reporting_layer:
    transform_data(db_path="./data.db", table_name=table, stage='reporting')

