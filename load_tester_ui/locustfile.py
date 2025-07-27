import random
import json
import uuid
from locust import HttpUser, task, between

with open("/app/users.json") as f:
    USERS_DATA = json.load(f)

class WebsiteUser(HttpUser):
    wait_time = between(4, 6)
    user_creds = {}
    user_id = None

    def on_start(self):
        """
        Вызывается при старте каждого виртуального пользователя.
        Регистрирует нового, уникального пользователя для этого теста.
        """
        base_user = random.choice(USERS_DATA)
        
        unique_username = f"{base_user['username']}_{uuid.uuid4()}"
        password = base_user['password']
        
        self.user_creds = {
            "username": unique_username,
            "password": password
        }
        
        with self.client.post(
            "/api/register",
            json=self.user_creds,
            name="/api/register", 
            catch_response=True
        ) as response:
            if response.status_code == 401:
                response.success()
            elif response.status_code != 200:
                response.failure(f"Registration failed for user {self.user_creds['username']} with status {response.status_code}")

    @task(10)
    def add_transaction(self):
        if not hasattr(self, 'user_id') or not self.user_id:
            return  # Не делаем запросы, пока не залогинены

        transaction_data = {
            "user_id": self.user_id,
            "type": random.choice(["income", "expense"]),
            "category": random.choice(["food", "transport", "salary", "gifts"]),
            "amount": random.randint(100, 5000)
        }

        with self.client.post(
            "/api/transaction",
            json=transaction_data,
            name="/api/transaction",
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure(f"Transaction creation failed with status {response.status_code}")
            else:
                response.success()

    @task(1)
    def login(self):
        """
        Основная задача: отправка запроса на логин с учетными данными,
        которые были успешно созданы на этапе on_start.
        """
        with self.client.post(
            "/api/login",
            json=self.user_creds,
            name="/api/login",
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure(f"Login failed with status {response.status_code}")
                self.user_id = None
            else:
                data = response.json()
                self.user_id = data.get('user_id')
                response.success()