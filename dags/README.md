git clone https://github.com/apache/airflow _(клонирование репозитория airflow)_

# docker
при разработке и отладке использовался docker. В слуучае исрользования linux можно инициализировать airflow напрямую
Если запускать на windows автоматически будет установленно wsl 

# Перед запуском 
Програама автоматически не создает базу в Postgress. Не обходим вручную создать базу **log_parser**, таблицы создадутся автоматически после запуска.

# команды для docker
cd .\airflow\
docker-compose up airflow-init
docker-compose up -d _(необязательно)_

запуск происходит на localhost8080
пользователь airflow
пароль airflow