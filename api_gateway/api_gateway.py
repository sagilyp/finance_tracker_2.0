import pika
import json
import uuid
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from prometheus_client import Counter, Histogram, start_http_server, generate_latest

# Метрики Prometheus
REQUESTS = Counter('http_requests_total', 'Total HTTP Requests', ['method', 'endpoint', 'http_status'])
LATENCY  = Histogram('http_request_latency_seconds', 'HTTP Request Latency', ['method', 'endpoint'])

# Костыль для демонстрации алерта по времени ответа
ENABLE_LATENCY_HACK = False

RABBITMQ_HOST           = 'rabbitmq'
REGISTER_QUEUE          = 'register_queue'
LOGIN_QUEUE             = 'login_queue'
TRANSACTION_QUEUE       = 'transaction_queue'
TRANSACTION_GET_QUEUE   = 'transaction_get_queue'
DELETE_TRANSACTION_QUEUE= 'delete_transaction_queue'
DELETE_USER_QUEUE       = 'delete_user_queue'

# Thread–local хранение RPC‑клиента
_thread_local = threading.local()

def get_rpc_client():
    """Возвращает поток‑локальный RpcClient, создавая его при необходимости."""
    client = getattr(_thread_local, 'rpc_client', None)
    if client is None or client.channel.is_closed:
        client = RpcClient()
        _thread_local.rpc_client = client
    return client

class RpcClient:
    def __init__(self):
        self.connection = None
        self.channel    = None
        self.callback_queue = None
        self.connect()

    def connect(self):
        """Устанавливает соединение и канал, с retry при ошибках."""
        while True:
            try:
                params = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=120,
                    blocked_connection_timeout=600
                )
                self.connection = pika.BlockingConnection(params)
                self.channel    = self.connection.channel()
                result = self.channel.queue_declare(queue='', exclusive=True)
                self.callback_queue = result.method.queue
                self.channel.basic_consume(
                    queue=self.callback_queue,
                    on_message_callback=self.on_response,
                    auto_ack=True
                )
                print("RabbitMQ connected.")
                break
            except pika.exceptions.AMQPConnectionError:
                print("RabbitMQ connect failed, retrying in 5s…")
                time.sleep(5)

    def on_response(self, ch, method, properties, body):
        """Пришёл ответ — сохраняем в self.response."""
        if properties.correlation_id == self.correlation_id:
            self.response = json.loads(body)

    def call(self, queue_name, message):
        """Посылаем RPC‑запрос и ждём ответа."""
        self.response       = None
        self.correlation_id = str(uuid.uuid4())

        if self.channel is None or self.channel.is_closed:
            self.connect()

        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    reply_to      = self.callback_queue,
                    correlation_id= self.correlation_id,
                    delivery_mode = 2
                )
            )
        except (pika.exceptions.ChannelWrongStateError, pika.exceptions.AMQPConnectionError):
            # при проблеме с каналом — реконнект и повторная публикация
            self.connect()
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    reply_to      = self.callback_queue,
                    correlation_id= self.correlation_id,
                    delivery_mode = 2
                )
            )

        # ждём, пока on_response установит self.response
        while self.response is None:
            try:
                self.connection.process_data_events()
            except pika.exceptions.AMQPConnectionError:
                print("RabbitMQ lost, reconnecting…")
                self.connect()

        return self.response

class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        start = time.time()
        endpoint = self.path

        length = int(self.headers.get('Content-Length', 0))
        raw    = self.rfile.read(length) if length else b''
        message= json.loads(raw) if raw else {}

        client = get_rpc_client()
        status = 500
        resp   = None

        if endpoint == '/api/register':
            resp   = client.call(REGISTER_QUEUE, message)
            status = 200 if resp.get('status') == 'success' else 401

        elif endpoint == '/api/login':
            if ENABLE_LATENCY_HACK:
                time.sleep(0.6)
            resp   = client.call(LOGIN_QUEUE, message)
            status = 200 if resp.get('status') == 'success' else 401

        elif endpoint == '/api/transaction':
            resp   = client.call(TRANSACTION_QUEUE, message)
            status = 200

        else:
            status = 404

        self.send_response(status)
        self.end_headers()
        if resp:
            try:
                self.wfile.write(json.dumps(resp).encode())
            except BrokenPipeError:
                pass

        LATENCY.labels('POST', endpoint).observe(time.time() - start)
        REQUESTS.labels('POST', endpoint, status).inc()

    def do_GET(self):
        start    = time.time()
        endpoint = self.path.split('?')[0]

        if endpoint == '/metrics':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(generate_latest())
            return

        if self.path.startswith('/api/transactions'):
            params = dict(p.split('=') for p in self.path.split('?',1)[1].split('&'))
            client = get_rpc_client()
            resp   = client.call(TRANSACTION_GET_QUEUE, params)
            status = 200
        else:
            resp   = None
            status = 404

        self.send_response(status)
        self.end_headers()
        if resp:
            try:
                self.wfile.write(json.dumps(resp).encode())
            except BrokenPipeError:
                pass

        LATENCY.labels('GET', endpoint).observe(time.time() - start)
        REQUESTS.labels('GET', endpoint, status).inc()

    def do_DELETE(self):
        client = get_rpc_client()

        if self.path.startswith('/api/user/'):
            user_id = self.path.rsplit('/',1)[-1]
            resp    = client.call(DELETE_USER_QUEUE, {'user_id': user_id})
            status  = 200 if resp.get('status') == 'success' else 400

        elif self.path.startswith('/api/transaction/'):
            tx_id   = self.path.rsplit('/',1)[-1]
            resp    = client.call(DELETE_TRANSACTION_QUEUE, {'transaction_id': tx_id})
            status  = 200 if resp.get('status') == 'success' else 400

        else:
            resp   = None
            status = 404

        self.send_response(status)
        self.end_headers()
        if resp:
            try:
                self.wfile.write(json.dumps(resp).encode())
            except BrokenPipeError:
                pass

def run(server_class=ThreadingHTTPServer, handler_class=RequestHandler, port=8000):
    start_http_server(8001)
    print("Prometheus on :8001, API Gateway on :8000")
    server = server_class(('', port), handler_class)
    server.serve_forever()

if __name__ == '__main__':
    run()
