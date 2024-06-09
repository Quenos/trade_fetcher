from datetime import datetime

import pytz


def log(client, script_name, comment='') -> None:
    db = client['tastytrade']
    log_collection = db['log']
    log_entry = {
        'script_name': script_name,
        'start_time': datetime.now(pytz.utc),
        'comment': comment
    }
    log_collection.insert_one(log_entry)