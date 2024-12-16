# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from py_scripts import read_data

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    df_terminals = read_data.read_xlsx('terminals')
    df_blacklist = read_data.read_xlsx('passport_blacklist')
    df_transaction = read_data.read_csv('transaction')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
