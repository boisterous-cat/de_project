from db.table_names import (stg_cards, stg_accounts, stg_clients,
                            stg_blacklist, stg_transactions, stg_terminals)
from db.database import execute_query, execute_many_query
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
        columns = "entry_dt, passport_num"
        values = "VALUES (%s, %s)"

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


def fill_staging_from_base(table_name):
    source_table = "info." + table_name
    if table_name.startswith('accounts'):
        recipient_table = stg_accounts
        columns = "account, valid_to, client, create_dt, update_dt"
    elif table_name.startswith('cards'):
        recipient_table = stg_cards
        columns = "card_num, account, create_dt, update_dt"
    else:
        recipient_table = stg_clients
        columns = ("client_id, last_name, first_name, patronymic, "
                   "date_of_birth, passport_num, passport_valid_to, phone,"
                   "create_dt,update_dt")

    query = f"""
        INSERT INTO {recipient_table} ({columns})
        SELECT {columns}
        FROM {source_table}
    """

    execute_query(query)


def update_tables(df_terminals, df_blacklist, df_transactions):
    clean()
    fill_staging_from_frame(df_transactions, 'transactions')
    fill_staging_from_frame(df_blacklist, 'blacklist')
    fill_staging_from_frame(df_terminals, 'terminals')

    fill_staging_from_base('cards')
    fill_staging_from_base('accounts')
    fill_staging_from_base('clients')

# Добавление нового пользователя
# insert_user = f"INSERT INTO {USERS_TABLE} (name) VALUES (%s);"
# execute_query(insert_user, ('John Doe',))
