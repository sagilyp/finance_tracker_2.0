FROM python:3.9-slim

WORKDIR /app

# Устанавливаем Locust
RUN pip install locust

# Копируем сценарий теста
COPY locustfile.py .

# Запускаем Locust. Веб-интерфейс будет на порту 8089
# Указываем хост nginx, чтобы не вводить его в UI каждый раз
CMD ["locust", "-f", "locustfile.py", "--host", "http://nginx:80"]