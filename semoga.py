import pyodbc
import os
import joblib
import numpy as np
from datetime import datetime
from prettytable import PrettyTable

class CustomPreprocessor:
    def transform(self, X):
        return X

def connect_sql_server():
    try:
        SERVER = 'DESKTOP-53R30V6\\MSSQLSERVER01'
        DATABASE = 'Tweet_Command'
        USERNAME = 'sa'
        PASSWORD = '1234'
        
        connectionString = (
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={SERVER};'
            f'DATABASE={DATABASE};'
            f'UID={USERNAME};'
            f'PWD={PASSWORD};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=yes'
        )
        
        sql_connection = pyodbc.connect(connectionString)
        print("Connected to MS SQL Server")
        return sql_connection
    except Exception as e:
        print(f"Error connecting to MS SQL Server: {e}")
        return None

def read_table_tweet_data(sql_connection):
    try:
        cursor = sql_connection.cursor()

        start_hour = 14
        end_hour = 16

        query = """
        SELECT 
            tweet_id, tweet, tweet_date
        FROM 
            Tweet_Detail
        WHERE 
            DATEPART(hour, tweet_date) >= ?
            AND DATEPART(hour, tweet_date) < ?
        """

        cursor.execute(query, (start_hour, end_hour))
        tweet_data = cursor.fetchall()

        if not tweet_data:
            print("No data found for the specified time range.")
        else:
            print(f"Data found: {len(tweet_data)} rows")

        table = PrettyTable()
        table.field_names = ["Tweet ID", "Tweet", "Tweet Date"]

        for row in tweet_data:
            table.add_row(row)

        print("Tweet Data:")
        print(table)

        return tweet_data
    except Exception as e:
        print(f"Error reading tweet data: {e}")
        return []
    finally:
        cursor.close()

def predict_sentiment(texts, model_path='ML/sentiment_analysis_pipeline_twitter_LN.pkl'):
    try:
        model_path = os.path.abspath(model_path)
        print(f"Loading model from: {model_path}")
        
        if not os.path.isfile(model_path):
            print(f"Model file not found: {model_path}")
            return []

        loaded_pipeline = joblib.load(model_path)
        
        valid_texts = [text for text in texts if text is not None and text.strip() != '']
        if not valid_texts:
            print("No valid texts to predict sentiment.")
            return []

        predictions = loaded_pipeline.predict(valid_texts)
        
        print("Predictions:", predictions)
        
        return predictions
    except Exception as e:
        print(f"Error predicting sentiment: {e}")
        return []

def update_sentiment_in_db(sql_connection, tweet_id, sentiment):
    try:
        cursor = sql_connection.cursor()
        
        # Convert sentiment to int if it's a numpy.int64
        if isinstance(sentiment, np.int64):
            sentiment = int(sentiment)

        query = """
        UPDATE Tweet_Detail
        SET sentiment = ?
        WHERE tweet_id = ?
        """

        cursor.execute(query, (sentiment, tweet_id))
        sql_connection.commit()

        print(f"Tweet ID: {tweet_id} updated with sentiment: {sentiment}")
    except Exception as e:
        print(f"Error updating sentiment: {e}")

def main():
    print("Current working directory:", os.getcwd())
    print("Files in 'ML' directory:", os.listdir('ML'))
    
    sql_connection = connect_sql_server()

    if sql_connection:
        tweet_data = read_table_tweet_data(sql_connection)

        if tweet_data:
            tweet_texts = [row[1] for row in tweet_data]
            tweet_ids = [row[0] for row in tweet_data]
            print(f"Number of tweets: {len(tweet_texts)}")
            for i, tweet in enumerate(tweet_texts):
                print(f"Tweet {i}: {tweet}")

            predictions = predict_sentiment(tweet_texts)

            print(f"Number of predictions: {len(predictions)}")

            if len(predictions) != len(tweet_data):
                print("Mismatch between number of predictions and tweet data.")
                return

            for tweet_id, sentiment in zip(tweet_ids, predictions):
                if sentiment is not None:
                    update_sentiment_in_db(sql_connection, tweet_id, sentiment)
                else:
                    print(f"Received NULL sentiment for Tweet ID: {tweet_id}")

if __name__ == "__main__":
    main()
