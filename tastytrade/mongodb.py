from typing import Any
from configparser import ConfigParser

import sys

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError

from tastytrade.dxfeed import (Candle, Event, EventType, Greeks, Profile,
                               Quote, Summary, TheoPrice, TimeAndSale, Trade,
                               Underlying)


class MongoDB:
    def __init__(self, db_name: str, collection_name: str):
        config: ConfigParser = ConfigParser()
        config.read('config.ini')

        user: str = config['MONGODB']['User']
        password: str = config['MONGODB']['Password']
        uri: str = config['MONGODB']['URI']

        self.client = AsyncIOMotorClient(f'mongodb+srv://{user}:{password}@{uri}')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.buffer = []
        self.buffer_size = 0
        self.max_buffer_size = 10 * 1024  # 10KB

    async def insert(self, data: Any):
        document = {
            'type': data[0],
            'content': data[1]
        }
        document_size = sys.getsizeof(document)

        # Add document to buffer
        self.buffer.append(document)
        self.buffer_size += document_size

        # Check if buffer size exceeds the maximum buffer size
        if self.buffer_size >= self.max_buffer_size:
            await self.flush_buffer()

    async def flush_buffer(self):
        if self.buffer:
            try:
                # Perform bulk write operation with plain dictionaries
                await self.collection.insert_many(self.buffer)
                # print(f"Flushed {len(self.buffer)} documents to the
                # database.")
            except BulkWriteError as bwe:
                print(f"Error during bulk write: {bwe.details}")
            finally:
                # Clear the buffer
                self.buffer = []
                self.buffer_size = 0

    async def find(self):
        document = await self.collection.find_one()
        if document:
            obj = Quote.from_stream(document['content'])
            print(obj)
