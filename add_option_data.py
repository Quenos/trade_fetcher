from datetime import datetime, date, time
from configparser import ConfigParser
from typing import Any

import pymongo

from session import ApplicationSession
from tastytrade.instruments import (NestedFutureOptionChain,
                                    OptionType, get_option_chain)


def get_mongo_client():
    config = ConfigParser()
    config.read('config.ini')

    user = config['MONGODB']['User']
    password = config['MONGODB']['Password']
    uri = config['MONGODB']['URI']

    # Connect to MongoDB
    client = pymongo.MongoClient(f'mongodb://{user}:{password}@{uri}')
    return client


def process_symbol_map(session: Any, client: pymongo.MongoClient):
    # Connect to the database
    db = client.tastytrade
    # Connect to the collection
    symbol_map_collection = db.symbol_map

    # Iterate through all documents in the collection
    query = {'$or': [{'base_symbol': {'$exists': False}}, {'base_symbol': ''}]}

    for document in symbol_map_collection.find(query):
        streamer_symbol = document.get("streamer_symbol")
        underlying_symbol = document.get("underlying_symbol")
        if underlying_symbol.startswith('/'):
            option_chain = sorted(NestedFutureOptionChain
                                  .get_chain(session, underlying_symbol[:3])
                                  .option_chains[0].expirations,
                                  key=lambda x: x.expiration_date)
            found = False
            expiration_date = None
            option_type = None
            strike_price = None
            base_symbol = underlying_symbol[:3]
            for option in option_chain:
                for strike_price in option.strikes:
                    if strike_price.put_streamer_symbol == streamer_symbol:
                        expiration_date = option.expiration_date
                        option_type = 'PUT'
                        strike_price = strike_price.strike_price
                        found = True
                        break
                    elif strike_price.call_streamer_symbol == streamer_symbol:
                        expiration_date = option.expiration_date
                        option_type = 'CALL'
                        strike_price = float(strike_price.strike_price)
                        found = True
                        break
                    else:
                        pass
                if found:
                    break
            if found:
                expiration_date = datetime.combine(expiration_date,
                                                   time(0,0))

                # Update the document in the collection
                symbol_map_collection.update_one(
                    {'_id': document['_id']},
                    {'$set': {
                        'base_symbol': base_symbol,
                        'expiration_date': expiration_date,
                        'option_type': option_type,
                        'strike_price': float(strike_price)
                    }}
                )
        else:
            option_chain = get_option_chain(session, underlying_symbol)
            found = False
            expiration_date = None
            option_type = None
            strike_price = None
            base_symbol = underlying_symbol
            for options in option_chain.values():
                for option in options:
                    if option.streamer_symbol == streamer_symbol:
                        expiration_date = option.expiration_date
                        option_type = 'CALL' if option.option_type == OptionType.CALL \
                            else 'PUT'
                        strike_price = float(option.strike_price)
                        found = True
                        break
            if found:
                expiration_date = datetime.combine(expiration_date,
                                                   time(0,0))

                # Update the document in the collection
                symbol_map_collection.update_one(
                    {'_id': document['_id']},
                    {'$set': {
                        'base_symbol': base_symbol,
                        'expiration_date': expiration_date,
                        'option_type': option_type,
                        'strike_price': strike_price
                    }}
                )

            print(option_chain)


if __name__ == '__main__':
    session = ApplicationSession().session
    client = get_mongo_client()
    process_symbol_map(session, client)
