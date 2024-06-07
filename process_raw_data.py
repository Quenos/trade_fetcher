import time
from configparser import ConfigParser
from decimal import Decimal

import pymongo

from tastytrade.dxfeed.greeks import Greeks
from tastytrade.dxfeed.trade import Trade


def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal(item) for item in obj]
    else:
        return obj


def process_document(document):
    # Check the type field and call the appropriate method
    if document.get('type') == 'Trade':
        trades = Trade.from_stream(document['content'])
        return [convert_decimal(trade.__dict__) for trade in trades if trade.time != 0]
    elif document.get('type') == 'Greeks':
        greeks = Greeks.from_stream(document['content'])
        return [convert_decimal(greek.__dict__) for greek in greeks if greek.time != 0]
    else:
        raise ValueError(f"Unknown document type: {document.get('type')}")


def handle_document(document, trade_data, greeks_data, collection_source):
    document_id = document['_id']  # Save _id before processing
    document.pop('_id', None)  # Remove _id field from the document
    try:
        result = process_document(document)
        if result:  # Check if result is not empty
            if document.get('type') == 'Trade':
                trade_data.insert_many(result, ordered=False)
            elif document.get('type') == 'Greeks':
                greeks_data.insert_many(result, ordered=False)
    except pymongo.errors.BulkWriteError as e:
        # Check for duplicate key error
        if any(error['code'] == 11000 for error in e.details['writeErrors']):
            print("Duplicate key error, deleting source document")
        else:
            print(f"error: {e}")
    finally:
        # Delete the document from the source collection
        collection_source.delete_one({'_id': document_id})


def main():
    # Read MongoDB configuration from config.ini
    config = ConfigParser()
    config.read('config.ini')

    user = config['MONGODB']['User']
    password = config['MONGODB']['Password']
    uri = config['MONGODB']['URI']

    # Connect to MongoDB
    client = pymongo.MongoClient(
        f'mongodb://{user}:{password}@{uri}')
    db = client['tastytrade']
    collection_source = db['market_data']
    trade_data = db['trade_data']
    greeks_data = db['greeks_data']

    while True:
        # Process existing documents
        for document in collection_source.find():
            handle_document(document, trade_data, greeks_data, collection_source)

        # Sleep for a while before checking for new documents again
        time.sleep(10)


if __name__ == "__main__":
    main()
