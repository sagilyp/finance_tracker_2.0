# grafana-custom/Dockerfile
FROM grafana/grafana:9.5.3

USER root

# Копируем ваши provisioning-файлы внутрь образа
COPY monitoring/grafana/provisioning/ /etc/grafana/provisioning/

# Устанавливаем права на чтение/запись всем нужным пользователям
RUN chown -R grafana:root /etc/grafana/provisioning \
    && chmod -R 755 /etc/grafana/provisioning

USER grafana
