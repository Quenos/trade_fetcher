import os
from configparser import ConfigParser
from pymongo import MongoClient
from tastytrade.instruments import Future
from session import ApplicationSession

def get_mongo_client():
    config = ConfigParser()
    config.read('config.ini')

    user = config['MONGODB']['User']
    password = config['MONGODB']['Password']
    uri = config['MONGODB']['URI']

    # Connect to MongoDB
    client = MongoClient(f'mongodb://{user}:{password}@{uri}')
    return client

def update_missing_future_expirations():
    client = get_mongo_client()
    db = client['tastytrade']
    symbol_map_collection = db['symbol_map']

    # Find all documents with underlying_symbol starting with '/' and no underlying_expiration
    query = {
        "underlying_symbol": {"$regex": "^/"},
        "underlying_expiration": {"$exists": False}
    }

    # Create a session for tastytrade API
    session = ApplicationSession().session

    # Process documents in batches
    batch_size = 1000
    processed = 0

    while True:
        # Fetch a batch of documents
        cursor = symbol_map_collection.find(query).limit(batch_size)
        batch = list(cursor)

        if not batch:
            break  # No more documents to process

        for document in batch:
            underlying_symbol = document['underlying_symbol']
            modified_symbol = underlying_symbol[:-7] + underlying_symbol[-6]
            try:
                future = Future.get_future(session, modified_symbol)
                expires_at = future.expires_at

                # Update this specific document
                result = symbol_map_collection.update_one(
                    {"_id": document['_id']},
                    {"$set": {"underlying_expiration": expires_at}}
                )

                if result.modified_count > 0:
                    print(f"Updated document for {underlying_symbol}")
                else:
                    print(f"No update needed for {underlying_symbol}")

            except Exception as e:
                print(f"Error processing {underlying_symbol}: {str(e)}")

        processed += len(batch)
        print(f"Processed {processed} documents")

    client.close()

if __name__ == "__main__":
    update_missing_future_expirations()