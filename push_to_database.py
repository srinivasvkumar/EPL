from scrape import *
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv('.env')

functions = [league_table, top_scorers, detail_top, player_table,
             all_time_table, all_time_winner_club, top_scorers_seasons, goals_per_season]

print(os.getcwd())  # Check current working directory
print(os.path.exists('.env'))  # Confirm the file is found
print(os.getenv('CONN_STRING'))


conn_string = os.getenv('CONN_STRING')

if not conn_string:
    raise ValueError("CONN_STRING not found in environment variables")


db = create_engine(conn_string)
conn = db.connect()

for fun in functions:
    function_name = fun.__name__
    result_df = fun()  # Call the function to get the DataFrame
    result_df.to_sql(function_name, con=conn, if_exists='replace', index=False)
    print(f'Pushed data for {function_name}')
    query = 'SELECT * FROM league_table;'
    df = pd.read_sql(query, db)

# Show the results
    print(df)
# Close the database connection
    query = 'SELECT NOW() AS current_datetime, CURRENT_DATE AS current_date;'
    df = pd.read_sql(query, db)

# Print the results
    print(df)

conn.close()
