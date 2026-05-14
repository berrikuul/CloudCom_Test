# Airflow S3 ETL Pipeline

Проект демонстрирует организацию ETL-процессов с использованием Apache Airflow, PostgreSQL и S3-совместимого объектного хранилища.

Инфраструктура разворачивается в Docker и включает автоматизированные пайплайны для обработки и выгрузки данных.

## Архитектура

Проект включает:

- Apache Airflow
- PostgreSQL
- MinIO (S3-compatible storage)
- Docker Compose

## Реализованный функционал

### ETL Pipeline

Первый DAG выполняет:

- ежедневное копирование данных из таблицы `t1` в `t2`
- предварительную очистку целевой таблицы
- загрузку данных в рамках одной транзакции
- фиксацию времени создания и загрузки данных

### Export Pipeline

Второй DAG выполняет:

- проверку наличия новых данных за текущий интервал
- пропуск выполнения при отсутствии данных (`skipped`)
- формирование дневных партиций
- выгрузку данных в JSON
- загрузку файлов в S3-хранилище

## Структура данных

### Таблица `t1`

Содержит:

- `id BIGSERIAL PRIMARY KEY`
- данные датасета
- `created TIMESTAMP DEFAULT NOW()`

### Таблица `t2`

Содержит:

- `id BIGINT`
- данные датасета
- `created TIMESTAMP`
- `load_time TIMESTAMP DEFAULT NOW()`

## Dataset

В проекте используется датасет:

- Russian Cyrillic Names and Sex Dataset

Для демонстрации загружаются первые 100 записей.

## Используемые технологии

- Python
- Apache Airflow
- PostgreSQL
- MinIO
- Docker
- SQLAlchemy / psycopg2

## Запуск проекта

### Клонирование репозитория

```bash
git clone https://github.com/berrikuul/airflow-s3-etl.git
