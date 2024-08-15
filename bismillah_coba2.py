import pyodbc
import os
import joblib
import numpy as np
from datetime import datetime, timedelta
from prettytable import PrettyTable
import time

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

def read_table_tweet_data(sql_connection, start_time, end_time):
    try:
        cursor = sql_connection.cursor()

        query = """
        SELECT 
            tweet_id, tweet, tweet_date
        FROM 
            Tweet_Detail
        WHERE 
            tweet_date >= ? AND tweet_date < ?
        """

        cursor.execute(query, (start_time, end_time))
        tweet_data = cursor.fetchall()

        if not tweet_data:
            print(f"No data found between {start_time} and {end_time}.")
        else:
            print(f"Data found between {start_time} and {end_time}: {len(tweet_data)} rows")

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
        
        if isinstance(sentiment, np.int64):
            sentiment = int(sentiment)

        query = """
        UPDATE Tweet_Detail
        SET sentiment = ?
        WHERE tweet_id = ?
        """
        
        print(f"Executing query: {query} with params: {sentiment}, {tweet_id}")
        cursor.execute(query, (sentiment, tweet_id))
        sql_connection.commit()  # Ensure commit is called here

        print(f"Tweet ID: {tweet_id} updated with sentiment: {sentiment}")
    except Exception as e:
        print(f"Error updating sentiment: {e}")
    finally:
        cursor.close()

def update_auto_reply_in_db(sql_connection, tweet_id, sentiment, user_id):
    try:
        cursor = sql_connection.cursor()
        
        if sentiment == 0:  # Positive sentiment
            reply = "Terima kasih atas feedback positif Anda! Kami senang Anda puas dengan layanan kami."
        elif sentiment == 1:  # Negative sentiment
            reply = "Kami mohon maaf atas ketidaknyamanan yang Anda alami. Tim kami akan segera menghubungi Anda untuk penyelesaian masalah."

        last_reply_time = datetime.now()

        print(f"Preparing to update Tweet ID: {tweet_id} with reply: {reply}")

        query = """
        UPDATE Tweet_Detail
        SET auto_reply = ?, last_reply_time = ?
        WHERE tweet_id = ?
        """

        cursor.execute(query, (reply, last_reply_time, tweet_id))
        sql_connection.commit()  # Ensure commit is called here

        print(f"Tweet ID: {tweet_id} updated with auto-reply: {reply}")
    except Exception as e:
        print(f"Error updating auto-reply: {e}")
    finally:
        cursor.close()

def main():
    print("Current working directory:", os.getcwd())
    print("Files in 'ML' directory:", os.listdir('ML'))
    
    sql_connection = connect_sql_server()

    if sql_connection:
        while True:
            now = datetime.now()
            start_time = now.replace(minute=0, second=0, microsecond=0)  # Start of the current hour
            end_time = start_time + timedelta(hours=1)  # End of the current hour

            tweet_data = read_table_tweet_data(sql_connection, start_time, end_time)

            if tweet_data:
                tweet_texts = [row[1] for row in tweet_data]
                tweet_ids = [row[0] for row in tweet_data]
                user_ids = [row[2] for row in tweet_data]  # Adjust index as per your table schema
                last_reply_times = [row[3] for row in tweet_data]  # Adjust index as per your table schema

                print(f"Number of tweets for hour {start_time.strftime('%H:%M')}: {len(tweet_texts)}")

                for i, tweet in enumerate(tweet_texts):
                    print(f"Tweet {i}: {tweet}")

                predictions = predict_sentiment(tweet_texts)

                print(f"Number of predictions: {len(predictions)}")

                if len(predictions) != len(tweet_data):
                    print("Mismatch between number of predictions and tweet data.")
                    continue

                for tweet_id, sentiment, user_id, last_reply_time in zip(tweet_ids, predictions, user_ids, last_reply_times):
                    if sentiment is not None:
                        # Check if 24 hours have passed since the last reply
                        if last_reply_time is None:
                            print(f"User ID: {user_id} has no previous replies.")
                            update_sentiment_in_db(sql_connection, tweet_id, sentiment)
                            update_auto_reply_in_db(sql_connection, tweet_id, sentiment, user_id)
                        else:
                            time_since_last_reply = datetime.now() - last_reply_time
                            if time_since_last_reply.total_seconds() >= 86400:
                                print(f"24 hours have passed since last reply to User ID: {user_id}.")
                                update_sentiment_in_db(sql_connection, tweet_id, sentiment)
                                update_auto_reply_in_db(sql_connection, tweet_id, sentiment, user_id)
                            else:
                                print(f"Skipping Tweet ID: {tweet_id} - User ID: {user_id} was replied to less than 24 hours ago.")
                    else:
                        print(f"Received NULL sentiment for Tweet ID: {tweet_id}")

            # Wait for the next hour
            next_hour = (start_time + timedelta(hours=1)).timestamp() - datetime.now().timestamp()
            if next_hour > 0:
                print(f"Sleeping for {next_hour} seconds...")
                time.sleep(next_hour)

if __name__ == "__main__":
    main()
