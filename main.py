# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from py_scripts import read_data
import psycopg2
from py_scripts import fill_stg, fill_dim, fill_facts, fill_report


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # read data from files
    df_terminals = read_data.read_xlsx('terminals')
    df_blacklist = read_data.read_xlsx('passport_blacklist')
    df_transaction = read_data.read_csv('transaction')

    # update stage tables
    fill_stg.update_tables(df_terminals, df_blacklist, df_transaction)

    # fill dim tables
    fill_dim.update_tables()

    # fill fact tables
    fill_facts.update_tables()
    fill_report.fill_report()
    print("Everything done")
