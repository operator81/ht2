# Базовый образ Airflow
FROM apache/airflow:2.7.2

# Установка дополнительных библиотек
USER airflow
RUN pip install --no-cache-dir tqdm  

# Копирование данных и файлов в контейнер
USER root
COPY ./data /opt/airflow/data/
COPY ./scripts/transform_script.py /opt/airflow/dags/
COPY ./dag.py /opt/airflow/dags/

# Настройка прав доступа
RUN chown -R airflow: /opt/airflow/data /opt/airflow/dags

# Возвращение к пользователю airflow
USER airflow
