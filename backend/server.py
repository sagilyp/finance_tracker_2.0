import pika
import json
import database
import multiprocessing
from decimal import Decimal
from datetime import datetime


# Настройки
RABBITMQ_HOST = 'rabbitmq'
NUM_WORKERS = 4  # Можешь увеличить до 8+ если CPU позволяет

REGISTER_QUEUE = 'register_queue'
LOGIN_QUEUE = 'login_queue'
TRANSACTION_QUEUE = 'transaction_queue'
TRANSACTION_GET_QUEUE = 'transaction_get_queue'
DELETE_TRANSACTION_QUEUE = 'delete_transaction_queue'
DELETE_USER_QUEUE = 'delete_user_queue'


def custom_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def respond(ch, properties, response):
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        body=json.dumps(response, default=custom_serializer),
        properties=pika.BasicProperties(correlation_id=properties.correlation_id)
    )


# --- Callback-функции ---
def register_callback(ch, method, properties, body):
    message = json.loads(body)
    username = message['username']
    password = message['password']
    response = {}
    user_id = None
    for i in range(5):
        try:
            new_user_id = database.create_user(username, password)
            if user_id is None:
                user_id = new_user_id
            print(f"[{i+1}/5] User {username} registered with ID {new_user_id}")
        except Exception as e:
            print(f"[{i+1}/5] Duplicate or error registering {username}: {e}")
    response = {'status': 'success', 'user_id': user_id} if user_id else {
        'status': 'failure', 'error': 'All 5 attempts failed'}
    respond(ch, properties, response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def delete_user_callback(ch, method, properties, body):
    message = json.loads(body)
    user_id = message['user_id']
    try:
        database.delete_user(user_id)
        response = {'status': 'success'}
        print(f"User {user_id} deleted.")
    except Exception as e:
        response = {'status': 'failure', 'error': str(e)}
        print(f"Failed to delete user {user_id}: {e}")
    respond(ch, properties, response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def login_callback(ch, method, properties, body):
    message = json.loads(body)
    username = message['username']
    password = message['password']
    user_id = database.authenticate_user(username, password)
    response = {'status': 'success', 'user_id': user_id} if user_id else {
        'status': 'failure', 'user_id': None}
    print(f"User {username} login status: {response['status']}")
    respond(ch, properties, response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def transaction_callback(ch, method, properties, body):
    message = json.loads(body)
    try:
        transaction_id = database.add_transaction(
            message['user_id'],
            message['type'],
            message['category'],
            message['amount']
        )
        response = {'status': 'success', 'transaction_id': transaction_id}
        print(f"Transaction {transaction_id} added.")
    except Exception as e:
        response = {'status': 'failure', 'error': str(e)}
        print(f"Transaction failed: {e}")
    respond(ch, properties, response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def transaction_get_callback(ch, method, properties, body):
    message = json.loads(body)
    try:
        transactions = database.get_transactions(
            message['user_id'], message['type'])
        response = {'status': 'success', 'transactions': transactions}
        print(f"Fetched transactions for user {message['user_id']}")
    except Exception as e:
        response = {'status': 'failure', 'error': str(e)}
        print(f"Failed to fetch transactions: {e}")
    respond(ch, properties, response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def transaction_delete_callback(ch, method, properties, body):
    message = json.loads(body)
    try:
        database.delete_transaction(message['transaction_id'])
        response = {'status': 'success'}
        print(f"Transaction {message['transaction_id']} deleted.")
    except Exception as e:
        response = {'status': 'failure', 'error': str(e)}
        print(f"Failed to delete transaction: {e}")
    respond(ch, properties, response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# --- Один воркер ---
def start_worker():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=REGISTER_QUEUE, durable=True)
    channel.queue_declare(queue=LOGIN_QUEUE, durable=True)
    channel.queue_declare(queue=TRANSACTION_QUEUE, durable=True)
    channel.queue_declare(queue=TRANSACTION_GET_QUEUE, durable=True)
    channel.queue_declare(queue=DELETE_TRANSACTION_QUEUE, durable=True)
    channel.queue_declare(queue=DELETE_USER_QUEUE, durable=True)

    # Рекомендуется явно указать prefetch_count > 1
    channel.basic_qos(prefetch_count=10)

    channel.basic_consume(queue=REGISTER_QUEUE, on_message_callback=register_callback)
    channel.basic_consume(queue=LOGIN_QUEUE, on_message_callback=login_callback)
    channel.basic_consume(queue=TRANSACTION_QUEUE, on_message_callback=transaction_callback)
    channel.basic_consume(queue=TRANSACTION_GET_QUEUE, on_message_callback=transaction_get_callback)
    channel.basic_consume(queue=DELETE_TRANSACTION_QUEUE, on_message_callback=transaction_delete_callback)
    channel.basic_consume(queue=DELETE_USER_QUEUE, on_message_callback=delete_user_callback)

    print(f"[PID {multiprocessing.current_process().pid}] Worker started.")
    channel.start_consuming()


# --- Запуск нескольких воркеров ---
if __name__ == "__main__":
    processes = []
    for _ in range(NUM_WORKERS):
        p = multiprocessing.Process(target=start_worker)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
