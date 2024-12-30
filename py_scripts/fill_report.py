from db.database import execute_query
import os

current_directory = os.path.dirname(__file__)


def fill_report() -> None:
    # Construct the path to the SQL script
    sql_script_path = os.path.join(current_directory, '..', 'sql_scripts', 'rep_fraud.sql')

    try:
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()
        execute_query(sql_script)
    except Exception as e:
        print(f"An error occurred: {e}")

