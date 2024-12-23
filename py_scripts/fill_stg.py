from db.table_names import (stg_cards, stg_accounts, stg_clients,
                            stg_blacklist, stg_transactions, stg_terminals)
from db.database import execute_query, execute_many_query, get_query_data
from psycopg2 import sql


# Создаем список с именами stage таблиц
def clean():
    table_names = [value for key, value in globals().items() if ('stg' in key) and ('del' not in key)]
    for table in table_names:
        truncate_query = f"truncate table {table}"
        execute_query(truncate_query)


# Заполняем актуальными данными стейджинговые таблицы из бд
def fill_staging_from_frame(df, table_name):
    if table_name.startswith('blacklist'):
        recipient_table = stg_blacklist
        columns = "entry_dt, passport_num, update_dt"
        values = "VALUES (%s, %s, %s)"

    elif table_name.startswith('terminals'):
        recipient_table = stg_terminals
        columns = "terminal_id, terminal_type, terminal_city, terminal_address, update_dt"
        values = "VALUES (%s, %s, %s, %s, %s)"
    else:
        recipient_table = stg_transactions
        columns = "trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal"
        values = "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    # Insert each row
    query = f"""
            INSERT INTO {recipient_table} ({columns}) {values}
        """
    execute_many_query(query, df.values.tolist())


def fill_data(table_name: str, staging_table: str, del_table: str, columns: list) -> None:
    # Захват данных из источника (измененных с момента последней загрузки) в стейджинг
    query = f"""
    SELECT {', '.join(columns)}, coalesce(update_dt, create_dt) AS update_dt
    FROM info.{table_name}
    WHERE coalesce(update_dt, create_dt) > (
        SELECT max_update_dt 
        FROM public.aadv_meta_dwh 
        WHERE schema_name='public' 
        AND table_name='aadv_dwh_dim_{table_name}_hist'
    )
    """
    data_df = get_query_data(query)

    # Захват в стейджинг ключей из источника полным срезом для вычисления удалений
    del_query = f"SELECT {columns[0]} FROM info.{table_name}"
    del_df = get_query_data(del_query)

    # insert queries
    insert_query = f"""
    INSERT INTO public.{staging_table}({', '.join(columns + ['update_dt'])}) 
    VALUES({', '.join(['%s'] * (len(columns) + 1))})
    """

    del_query = f"""
    INSERT INTO public.{del_table}({columns[0]}) 
    VALUES(%s)
    """

    execute_many_query(insert_query, data_df.values.tolist())
    execute_many_query(del_query, del_df.values.tolist())


def fill_accounts() -> None:
    fill_data('accounts', 'aadv_stg_accounts', 'aadv_stg_accounts_del', ['account', 'valid_to', 'client'])


def fill_cards() -> None:
    fill_data('cards', 'aadv_stg_cards', 'aadv_stg_cards_del', ['card_num', 'account'])


def fill_clients() -> None:
    fill_data('clients', 'aadv_stg_clients', 'aadv_stg_clients_del',
              ['client_id', 'last_name', 'first_name', 'patronymic',
               'date_of_birth', 'passport_num', 'passport_valid_to', 'phone'])


def update_tables(df_terminals, df_blacklist, df_transactions):
    clean()
    fill_staging_from_frame(df_transactions, 'transactions')
    fill_staging_from_frame(df_blacklist, 'blacklist')
    fill_staging_from_frame(df_terminals, 'terminals')

    fill_cards()
    fill_accounts()
    fill_clients()
