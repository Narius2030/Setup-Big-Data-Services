import sys
sys.path.append("./kafka")

import os
import csv
import json
from pathlib import Path
from dotenv import load_dotenv
from KafkaComponent import Cons
env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

#set the env variable to an IP if not localhost
KAFKA_ADDRESS=os.getenv('KAFKA_ADDRESS')

def write_json_logs(message):
    ## TODO: write data into json
    row = []
    message = message.value().decode('utf-8')
    message = json.loads(message)
    for _, data in message.items():
        try:
            row.append(data)
        except Exception as exc:
            raise Exception(str(exc))
    
    print(row)
    with open("./kafka/test-connection/data/rides_df.csv", "a", encoding="utf-8", newline='\n') as file:
        writer = csv.writer(file)
        writer.writerow(row)
    

if __name__ == '__main__':
    cons = Cons(KAFKA_ADDRESS, 'yellow_taxi_ride.json', 'taxi-rides', write_json_logs)
    cons.run()