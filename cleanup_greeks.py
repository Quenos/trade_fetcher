from configparser import ConfigParser
import pymongo


def main():
    # Read MongoDB configuration from config.ini
    config = ConfigParser()
    config.read('config.ini')

    user = config['MONGODB']['User']
    password = config['MONGODB']['Password']
    uri = config['MONGODB']['URI']

    # Connect to MongoDB
    client = pymongo.MongoClient(f'mongodb://{user}:{password}@{uri}')
    db = client['tastytrade']
    greeks_data = db['greeks_data']

    # Get a list of all unique eventSymbols
    event_symbols = greeks_data.distinct('eventSymbol')

    for event_symbol in event_symbols:
        # Find all documents for this eventSymbol sorted by time
        documents = list(
            greeks_data.find({'eventSymbol': event_symbol}).sort('time',
                                                                 pymongo.ASCENDING))

        last_valid_time = None
        ids_to_delete = []

        for doc in documents:
            current_time = doc['time']

            if last_valid_time is None or (
                    current_time - last_valid_time) >= 600000:
                # Update last valid time if this document is kept
                last_valid_time = current_time
            else:
                # Mark this document for deletion
                ids_to_delete.append(doc['_id'])

        if ids_to_delete:
            greeks_data.delete_many({'_id': {'$in': ids_to_delete}})
            print(
                f"Deleted {len(ids_to_delete)} documents for eventSymbol {event_symbol}")

    print("Cleanup complete.")


if __name__ == "__main__":
    main()
