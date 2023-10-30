# import library
import os
import pathlib
import time
import threading

# using sqlite for example with db
import sqlite3

# using local path right now, will change it depends on dedicated/existing repo
sql_path = '/Users/andrearubenphilo/99group/sql/tmp'
source_sql = pathlib.Path(sql_path)
dir_sql_list = os.listdir(source_sql)

sql_files = []

for items in dir_sql_list:
    sql_files.append(items)


def datawarehouse_process():
    # set the desired sequence
    desired_sequence = ["agents.sql", "enquiries_per_day.sql", "listing_photos_quality.sql", "page_clicks_per_day.sql", "listing_performances.sql", "listings_features.sql"]
    try:
        source_db = sqlite3.connect('source.db')
        source_cursor = source_db.cursor()

        datawarehouse_db = sqlite3.connect('datawarehouse.db')
        datawarehouse_cursor = datawarehouse_db.cursor()

        # loop the process based on desired_sequece
        for item in desired_sequence:
            if item in sql_files:
                table_name = (f'tmp.{item}').replace('.sql','')
                print(f"running sql query: {item}")
                # with open(sql_file, 'r') as sql_file_content:
                #     # run the query file for data result
                #     sql_query = sql_file_content.read()
                #     source_cursor.execute(sql_query)
                #     results = source_cursor.fetchall()
                #     # Create and Load data into the data lake
                #     datawarehouse_cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} AS {sql_query}')
                #     datawarehouse_db.commit()
                time.sleep(2)
                print(f"data ingested to :{table_name}")
        result = print('datawarehouse process done')
    except Exception as e:
        result = print(str(e))
    return result



dm_sql_path = '/Users/andrearubenphilo/99group/sql/finals'
source_dm_sql = pathlib.Path(dm_sql_path)
dir_dm_sql_list = os.listdir(source_dm_sql)

dm_sql_files = []

for items in dir_dm_sql_list:
    dm_sql_files.append(items)


def datamart_process():
        try:
            datawarehouse_db = sqlite3.connect('datawarehouse.db')
            datawarehouse_cursor = datawarehouse_db.cursor()

            datamart_db = sqlite3.connect('datamart.db')
            datamart_cursor = datamart_db.cursor()

            # loop the process based on desired_sequece
            for index in range(0,len(dm_sql_files)):
                table = dm_sql_files[index]
                table_name = (f'finals.{table}').replace('.sql','')
                print(f"processing data from source table: {table_name}")
                    # with open(sql_file, 'r') as sql_file_content:
                    #     # run the query file for data result
                    #     sql_query = sql_file_content.read()
                    #     datawarehouse_cursor.execute(sql_query)
                    #     results = datawarehouse_cursor.fetchall()
                    #     # Create and Load data into the data lake
                    #     datamart_cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} AS {sql_query}')
                    #     datamart_db.commit()
                time.sleep(2)
                print(f"data ingested to :{table_name}")
            result = print('datamart process done')
        except Exception as e:
            result = print(str(e))
        return result


def main():
    
    datawarehouse_process()
    datamart_process()

if __name__ == "__main__":
    main()



