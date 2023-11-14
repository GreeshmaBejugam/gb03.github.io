# Code for ETL operations on Country-GDP data

# Importing the required libraries
import csv
import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import sqlite3
from datetime import datetime

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = '/home/project/exchange_rate.csv'
output_path = './Largest_banks_data.csv'
def extract(url, table_attribs):
    
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Name": col[0].contents[0],
                         "MC_USD_Billion": col[1].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):

    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    

    def csv_to_dict(csv_path):
        df = {}
        with open(csv_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                # Assuming the first column contains keys and the second column contains values
                key = row[0]
                value = row[1]
                df[key] = value
        return df

    # Example usage
    csv_path = '/home/project/exchange_rate.csv'
    data_dict = csv_to_dict(csv_path)

    print(data_dict)

    df['MC_GBP_Billion'] = [np.round(x * float(data_dict['GBP'],2)) for x in df['MC_USD_Billion']]
    # df['MC_EUR_Billion'] = [np.round(x * int(data_dict['EUR'],2)) for x in df['MC_USD_Billion']]
    # df['MC_INR_Billion'] = [np.round(x * int(data_dict['INR'],2)) for x in df['MC_USD_Billion']]
    # return df


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df,csv_path)

log_progress('Data transformation complete. Initiating loading process')

# load_to_csv(df, csv_path)

# log_progress('Data saved to CSV file')

# sql_connection = sqlite3.connect('Banks.db')

# log_progress('SQL Connection initiated.')

# # load_to_db(df, sql_connection, table_name)

# # log_progress('Data loaded to Database as table. Executing queries')

# # query_statement = f"SELECT * from {table_name} "
# # run_query(query_statement, sql_connection)


# # log_progress('Process Complete.')

# sql_connection.close()

# log_progress('Server Connection closed')

# url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
# table_attribs = ["Name", "MC_USD_Billion"]
# db_name = 'Banks.db'
# table_name = 'Largest_banks'
# csv_path = './Largest_banks_data.csv'
# def extract(url, table_attribs):
#     # url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
#     # table_attribs = ["Name", "MC_USD_Billion"]
#     # db_name = 'Banks.db'
#     # table_name = 'Largest_banks'
#     # csv_path = './Largest_banks_data.csv'
#     page = requests.get(url).text
#     data = BeautifulSoup(page,'html.parser')
#     df = pd.DataFrame(columns=table_attribs)
#     tables = data.find_all('tbody')
#     rows = tables[0].find_all('tr')
#     for row in rows:
#         col = row.find_all('td')
#         if len(col)!=0:
#             data_dict = {"Name": col[0].a.contents[0],
#                          "MC_USD_Billion": col[1].contents[0]}
#             df1 = pd.DataFrame(data_dict, index=[0])
#             df = pd.concat([df,df1], ignore_index=True)


            
                
#     ''' This function aims to extract the required
#     information from the website and save it to a data frame. The
#     function returns the data frame for further processing. '''

    #  return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
