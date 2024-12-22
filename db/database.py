import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
from config_data.config import Config, load_config

# Загружаем конфиг в переменную config
config: Config = load_config()

db_config = {
    'dbname': config.db_connect.database,  # Database name
    'user': config.db_connect.user,  # Database user
    'password': config.db_connect.password,  # Database password
    'host': config.db_connect.host,  # Database host
    'port': config.db_connect.port  # Database port
}

@contextmanager
def get_connection():
    """Контекстный менеджер для получения и освобождения соединения с БД."""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        yield conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
    finally:
        if conn is not None:
            conn.close()


def execute_query(query, params=None):
    """Функция для выполнения SQL-запросов."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()


def execute_many_query(query, params=None):
    """Функция для выполнения SQL-запросов."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, params)
            conn.commit()