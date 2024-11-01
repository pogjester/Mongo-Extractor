import pymongo
import os
import csv
from dotenv import load_dotenv
import pandas as pd

# Load environment variables (ensure you have your MongoDB URI in .env)
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# Connect to MongoDB
client = pymongo.MongoClient(MONGODB_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Extract the last 5,000 documents (assuming the documents have a timestamp or an '_id' to sort by)
data = list(collection.find().sort('timestamp', pymongo.DESCENDING).limit(5000))

# Reverse the data to maintain chronological order
data.reverse()

# Convert data to a DataFrame
df = pd.DataFrame(data)

df.drop(columns=['_id'], inplace=True)
# Export data to Parquet format
output_file = "output.parquet"
df.to_parquet(output_file, engine='pyarrow', index=False)

print(f"Parquet file saved as {output_file}")

# Close the MongoDB connection
client.close()