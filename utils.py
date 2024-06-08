from datetime import datetime

import pytz


def log_startup(client, script_name) -> None:
    db = client['tastytrade']
    log_collection = db['log']
    log_entry = {
        'script_name': script_name,
        'start_time': datetime.now(pytz.utc)
    }
    log_collection.insert_one(log_entry)