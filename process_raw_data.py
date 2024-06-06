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
    # Drop the _id field
    document.pop('_id', None)

    # Check the type field and call the appropriate method
    if document.get('type') == 'Trade':
        trades = Trade.from_stream(document['content'])
        return [convert_decimal(trade.__dict__) for trade in trades if
                '.' in trade.eventSymbol and trade.time != 0]
    elif document.get('type') == 'Greeks':
        greeks = Greeks.from_stream(document['content'])
        return [convert_decimal(greek.__dict__) for greek in greeks if
                'SPX' in greek.eventSymbol and greek.time != 0]
    else:
        raise ValueError(f"Unknown document type: {document.get('type')}")


def main():
    def handle_document(document, trade_data, greeks_data):
        try:
            result = process_document(document)
            if result:  # Check if result is not empty
                if document.get('type') == 'Trade':
                    trade_data.insert_many(result)
                elif document.get('type') == 'Greeks':
                    greeks_data.insert_many(result)
        except Exception as e:
            print(f"error: {e}")

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

    # Process existing documents
    for document in collection_source.find():
        handle_document(document, trade_data, greeks_data)

    # Create a change stream to listen for new documents
    with collection_source.watch() as stream:
        for change in stream:
            if change['operationType'] == 'insert':
                new_document = change['fullDocument']
                handle_document(new_document, trade_data, greeks_data)


if __name__ == "__main__":
    main()
