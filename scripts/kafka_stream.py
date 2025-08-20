from kafka import KafkaProducer
import logging
import json
import requests
import time
from datetime import datetime, timedelta


def get_data():
    url = 'https://randomuser.me/api/?nat=us,dk,fr,gb&results=10'
    try:
        response = requests.get(url)
        data = response.json()
        data = data['results']
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def format_data(response):
    formatted_data = []
    for item in response:
        try:
            formatted_data.append({
                "first_name": item['name']['first'],
                "last_name": item['name']['last'],
                "gender": item['gender'],
                "street": f"{item['location']['street']['number']} {item['location']['street']['name']}",
                "city": item['location']['city'],
                "state": item['location']['state'],
                "country": item['location']['country'],
                "postcode": item['location']['postcode'],
                "latitude": item['location']['coordinates']['latitude'],
                "longitude": item['location']['coordinates']['longitude'],
                "email": item['email'],
                "uuid": item['login']['uuid'],
                "username": item['login']['username'],
                "password": item['login']['password'],
                "birthdate": item['dob']['date'],
                "age": item['dob']['age'],
                "registered_date": item['registered']['date'],
                "phone": item['phone'],
                "cell": item['cell'],
                "picture": item['picture']['large'],
                "nationality": item['nat']
            })
        except (KeyError, TypeError) as e:
            print(f"Record skipped due to missing data: {e}")
            continue
    return formatted_data

 
def stream_data():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        max_block_ms=30000 
    )
    current_time = time.time()
    while time.time() - current_time < 60:
        data = get_data()
        if data:
            formatted_data = format_data(data)
            try:
                for record in formatted_data:
                    producer.send('user_records', value=record, key=record['uuid'])
            except Exception as e:
                logging.error(f"Error sending data to Kafka: {e}")
                continue