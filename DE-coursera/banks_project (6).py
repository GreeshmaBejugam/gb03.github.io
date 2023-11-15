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
        if row.find('td') is not None:
            col = row.find_all('td')
            bankname = col[1].find_all('a')[1]['title']
            marketcap = col[2].contents[0][:-1]
            data_dict = {"Name":bankname,
                          "MC_USD_Billion":float(marketcap)}
            # print(data_dict)
            df1 = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    # print(df)
    return df
    

def transform(df, csv_path):

    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    # print(exchange_rate)

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x *exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x *exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    print(df)
    return df
    



    

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


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

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

# sql_connection = sqlite3.connect('Banks.db')

# log_progress('SQL Connection initiated.')

# # load_to_db(df, sql_connection, table_name)

# # log_progress('Data loaded to Database as table. Executing queries')

# # query_statement = f"SELECT * from {table_name} "
# # run_query(query_statement, sql_connection)


# # log_progress('Process Complete.')

# sql_connection.close()

# log_progress('Server Connection closed')



def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
