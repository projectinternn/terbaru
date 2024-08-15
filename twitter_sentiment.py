import joblib 
import psycopg2
import pandas as pd
from utils import CustomPreprocessor

##dbconnection
conn = psycopg2.connect(
    dbname="Db_smm",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)



# Load the saved pipeline model
loaded_pipeline = joblib.load('sentiment_analysis_pipeline_twitter_LN.pkl')

# Example of predicting with the loaded pipeline
custom_text = [
    "firstmedia Internetnya sangat lambat",
    "saya merasa puas dengan pelayanan firstmedia",
    "Jaringan ga bagus",
    "Sangat puas dengan layanan firstmedia",
    "firstmedia jaringan nya bagus",
    "firstmedia jaringan nya sangat jelek"
]
predictions = loaded_pipeline.predict(custom_text)

# Print predictions
for text, prediction in zip(custom_text, predictions):
    print(f"Text: {text}")
    print(f"Predicted Label: {prediction}")
    print()