import psycopg2
from psycopg2 import sql
import pandas as pd
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


def execute_query(query, params=None) -> None:
    """Функция для выполнения SQL-запросов."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()


def get_query_data(query, params=None) -> pd.DataFrame:
    """Функция для получения данных SQL-запросов."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)

            df = pd.DataFrame()
            try:
                df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])
            except Exception as e:
                print(f'No data to return. Error text: {e}')

            conn.commit()
            return df


def execute_many_query(query, params=None) -> None:
    """Функция для выполнения множественного SQL-запроса."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, params)
            conn.commit()