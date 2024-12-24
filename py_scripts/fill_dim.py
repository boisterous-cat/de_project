from db.table_names import (stg_cards, stg_accounts, stg_clients,
                            stg_blacklist, stg_transactions, stg_terminals)
from db.database import execute_query, execute_many_query, get_query_data
import os

current_directory = os.path.dirname(__file__)


def fill_accounts() -> None:
    # Construct the path to the SQL script
    sql_script_path = os.path.join(current_directory, '..', 'sql_scripts', 'dim_accounts_hist.sql')

    try:
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()
        execute_query(sql_script)
    except Exception as e:
        print(f"An error occurred: {e}")


def fill_clients() -> None:
    # Construct the path to the SQL script
    sql_script_path = os.path.join(current_directory, '..', 'sql_scripts', 'dim_clients_hist.sql')

    try:
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()
        execute_query(sql_script)
    except Exception as e:
        print(f"An error occurred: {e}")


def fill_cards() -> None:
    # Construct the path to the SQL script
    sql_script_path = os.path.join(current_directory, '..', 'sql_scripts', 'dim_cards_hist.sql')

    try:
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()
        execute_query(sql_script)
    except Exception as e:
        print(f"An error occurred: {e}")


def fill_terminals() -> None:
    # Construct the path to the SQL script
    sql_script_path = os.path.join(current_directory, '..', 'sql_scripts', 'dim_terminals_hist.sql')

    try:
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()
        execute_query(sql_script)
    except Exception as e:
        print(f"An error occurred: {e}")


def update_tables():
    fill_accounts()
    fill_clients()
    fill_cards()
    fill_terminals()
