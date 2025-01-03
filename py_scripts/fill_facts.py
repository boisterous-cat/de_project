from db.table_names import (stg_cards, stg_accounts, stg_clients,
                            stg_blacklist, stg_transactions, stg_terminals)
from db.database import execute_query, execute_many_query, get_query_data
import os

current_directory = os.path.dirname(__file__)


def fill_transactions() -> None:
    # Construct the path to the SQL script
    sql_script_path = os.path.join(current_directory, '..', 'sql_scripts', 'fact_transactions.sql')

    try:
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()
        execute_query(sql_script)
    except Exception as e:
        print(f"An error occurred: {e}")


def fill_blacklist() -> None:
    # Construct the path to the SQL script
    sql_script_path = os.path.join(current_directory, '..', 'sql_scripts', 'fact_passport_blacklist.sql')

    try:
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()
        execute_query(sql_script)
    except Exception as e:
        print(f"An error occurred: {e}")


def update_tables():
    fill_transactions()
    fill_blacklist()
