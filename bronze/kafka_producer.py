import json
import random
import time

from kafka import KafkaProducer
import os

def generate_message_variant1():
    id = "sensor-" + str(random.randint(1, 10)).zfill(3)

    message = {
        "id": id,
    }
    return message

def generate_message_variant2():
    sensor = "sensor-" + str(random.randint(1, 10)).zfill(3)

    message = {
        "sensor": sensor,
    }
    return message

def generate_message_variant3():
    deviceId = "sensor-" + str(random.randint(1, 10)).zfill(3)

    message = {
        "deviceId": deviceId,
    }
    return message


if __name__ == '__main__':
    time.sleep(10)
    flights_topic = os.environ.get('flights_topic', 'bronze-flights')
    check_in_topic = os.environ.get('check_in_topic', 'bronze-check-in')
    flight_orders_topic = os.environ.get('flight_orders_topic', 'bronze-flight-orders')
    israel_bank_topic = os.environ.get('israel_bank_topic', 'bronze-israel-bank')
    payme_topic = os.environ.get('payme_topic', 'bronze-payme')

    bootstrap_servers = os.environ.get('BOOTSTRAP_SERVERS', '10.40.0.12:9092')
    # bootstrap_servers = os.environ.get('BOOTSTRAP_SERVERS', 'https://10.40.0.12:9092')
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    print("Starting  producer...")
    while True:
        message1 = generate_message_variant1()
        producer.send(flights_topic, value=message1)
        print(f"Sent to {flights_topic}:", message1)
        time.sleep(1)  # send a message every 3 seconds

        message2 = generate_message_variant2()
        producer.send(check_in_topic, value=message2)
        print(f"Sent to {check_in_topic}:", message2)
        time.sleep(1)  # send a message every 3 seconds

        message3 = generate_message_variant3()
        producer.send(flight_orders_topic, value=message3)
        print(f"Sent to {flight_orders_topic}:", message3)
        time.sleep(1)  # send a message every 3 seconds

        message4 = generate_message_variant3()
        producer.send(israel_bank_topic, value=message4)
        print(f"Sent to {israel_bank_topic}:", message4)
        time.sleep(1)  # send a message every 3 seconds

        message5 = generate_message_variant3()
        producer.send(payme_topic, value=message5)
        print(f"Sent to {payme_topic}:", message5)
        time.sleep(1)  # send a message every 3 seconds
